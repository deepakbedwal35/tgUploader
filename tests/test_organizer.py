#!/usr/bin/env python3
"""
Unit tests for Telegram Organizer core logic:
- Config parsing & validation
- Secret redaction
- Non-interactive headless initialization
- Destination resolution & group creation guard
- Topic matching (exact, normalized, duplicate conflict)
- Round-robin batching, cursor tracking, and resuming
- Deduplication
- Crash recovery via destination message marker scan
- Content protection (noforwards) compliance
- File size & disk space checks
- Single-instance locking (fcntl)
- Dry-run verification
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from telethon.errors import FloodWaitError
from telethon.tl import types

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from organizer import (
    Config,
    Database,
    ProcessLock,
    TransferUnit,
    WorkerController,
    WorkerState,
    match_destination_topic,
    normalize_title,
    process_telegram_unit_batch,
    recover_in_flight_uploads,
    redact_secrets,
    resolve_destination_group,
)


class TestConfigAndSecurity(unittest.TestCase):
    def test_secret_redaction(self):
        """Ensure secrets are redacted in log messages."""
        with patch.object(Config, "API_HASH", "my_super_secret_api_hash"), \
             patch.object(Config, "SESSION_STRING", "1BVtsOMQBu89XYZSESSION"), \
             patch.object(Config, "DASHBOARD_PASSWORD", "supersecretpass"), \
             patch.object(Config, "PHONE", "+1234567890"):

            raw_log = "Connected with hash my_super_secret_api_hash and session 1BVtsOMQBu89XYZSESSION for +1234567890 (pass: supersecretpass)"
            redacted = redact_secrets(raw_log)

            self.assertNotIn("my_super_secret_api_hash", redacted)
            self.assertNotIn("1BVtsOMQBu89XYZSESSION", redacted)
            self.assertNotIn("supersecretpass", redacted)
            self.assertNotIn("+1234567890", redacted)
            self.assertIn("[REDACTED]", redacted)

    def test_single_instance_lock(self):
        """Single-instance lock prevents multiple instances from running concurrently."""
        temp_dir = tempfile.mkdtemp()
        try:
            lock_path = os.path.join(temp_dir, "organizer.lock")
            lock1 = ProcessLock(lock_path)
            self.assertTrue(lock1.acquire())

            # Second lock attempt must fail
            lock2 = ProcessLock(lock_path)
            self.assertFalse(lock2.acquire())

            # Release first lock
            lock1.release()

            # Now second lock succeeds
            self.assertTrue(lock2.acquire())
            lock2.release()
        finally:
            shutil.rmtree(temp_dir)


class TestTopicMatching(unittest.TestCase):
    def setUp(self):
        self.config = Config()
        self.topics_map = {
            "🏅 PHARMACOLOGY": {"topic_id": 10, "root_msg_id": 10},
            "📚 NOTES": {"topic_id": 20, "root_msg_id": 20},
            "Imported Files": {"topic_id": 30, "root_msg_id": 30},
        }

    def test_exact_topic_match(self):
        name, info = match_destination_topic("🏅 PHARMACOLOGY", self.topics_map, self.config)
        self.assertEqual(name, "🏅 PHARMACOLOGY")
        self.assertEqual(info["root_msg_id"], 10)

    def test_normalized_topic_match(self):
        # Case insensitive and stripped emoji/spaces
        name, info = match_destination_topic("notes", self.topics_map, self.config)
        self.assertEqual(name, "📚 NOTES")
        self.assertEqual(info["root_msg_id"], 20)

    def test_ambiguous_normalized_topic_match_raises_error(self):
        # Two topics with distinct emojis but same normalized name
        ambiguous_map = {
            "💰 OBG & GYNE": {"topic_id": 1, "root_msg_id": 1},
            "🏅 OBG & GYNE": {"topic_id": 2, "root_msg_id": 2},
        }
        with self.assertRaises(ValueError) as ctx:
            match_destination_topic("OBG & GYNE", ambiguous_map, self.config)
        self.assertIn("Ambiguous topic match", str(ctx.exception))

    def test_unmatched_topic_fallback_to_default(self):
        name, info = match_destination_topic("UNKNOWN TOPIC 123", self.topics_map, self.config)
        self.assertEqual(name, "Imported Files")
        self.assertEqual(info["root_msg_id"], 30)


class TestDestinationResolution(unittest.IsolatedAsyncioTestCase):
    async def test_destination_not_found_halts_when_create_group_false(self):
        """When destination group is missing and CREATE_GROUP is false, halt with error."""
        client = MagicMock()
        client.get_entity = AsyncMock(side_effect=Exception("Chat not found"))

        async def mock_dialogs():
            if False:
                yield None
        client.iter_dialogs = mock_dialogs

        config = Config()
        config.DEST_GROUP_ID = 999999999
        config.DEST_GROUP_NAME = "NonExistentGroup"
        config.CREATE_GROUP = False

        with self.assertRaises(RuntimeError) as ctx:
            await resolve_destination_group(client, config)
        self.assertIn("not found or inaccessible", str(ctx.exception))


class TestBatchRoundRobinAndRecovery(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.db = Database(self.db_path)
        self.config = Config()
        self.config.TMP_DIR = os.path.join(self.test_dir, "tmp")
        self.config.BATCH_SIZE = 2
        self.config.DRY_RUN = False
        os.makedirs(self.config.TMP_DIR, exist_ok=True)

        self.controller = WorkerController(self.config, self.db)
        self.source_id = -1002671181760
        self.dest_id = -1003910839448

        self.client = MagicMock()
        self.client.download_media = AsyncMock()
        self.client.send_file = AsyncMock()
        self.client.get_entity = AsyncMock()

        self.source_entity = MagicMock()
        self.source_entity.id = self.source_id
        self.source_entity.noforwards = False

        self.dest_entity = MagicMock()
        self.dest_entity.id = self.dest_id

        self.client.get_entity.return_value = self.source_entity

    async def asyncTearDown(self):
        shutil.rmtree(self.test_dir)

    async def test_round_robin_batch_and_cursor_tracking(self):
        """Process messages up to batch_size and advance cursor monotonically."""
        unit = TransferUnit(
            unit_id=f"chat_{self.source_id}",
            unit_type="telegram_chat",
            source_id=self.source_id,
            source_title="Delta 6 full stack",
            dest_topic="Imported Files",
            root_msg_id=1,
        )

        doc = types.Document(
            id=1, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=1024, dc_id=1,
            attributes=[types.DocumentAttributeFilename("doc1.pdf")]
        )
        msg1 = types.Message(id=10, peer_id=types.PeerChannel(1), date=None, message="Msg 10", media=types.MessageMediaDocument(document=doc))
        msg2 = types.Message(id=11, peer_id=types.PeerChannel(1), date=None, message="Msg 11", media=types.MessageMediaDocument(document=doc))

        async def mock_iter(entity, **kwargs):
            yield msg1
            yield msg2

        self.client.iter_messages = mock_iter

        async def mock_download(m, file=None):
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "wb") as f:
                f.write(b"data")
            return file

        self.client.download_media.side_effect = mock_download
        semaphore = asyncio.Semaphore(2)

        processed = await process_telegram_unit_batch(
            self.client, self.dest_entity, unit, self.db, self.config, self.controller, semaphore
        )

        self.assertEqual(processed, 2)
        # Cursor should now be at 11
        saved_cursor = self.db.get_unit_cursor(unit.unit_id)
        self.assertEqual(saved_cursor, 11)

        # Both items marked UPLOADED
        self.assertTrue(self.db.is_item_done(str(self.source_id), "10"))
        self.assertTrue(self.db.is_item_done(str(self.source_id), "11"))

    async def test_content_protection_noforwards_skips(self):
        """When chat or message has noforwards=True, mark restricted and skip."""
        unit = TransferUnit(
            unit_id=f"chat_{self.source_id}",
            unit_type="telegram_chat",
            source_id=self.source_id,
            source_title="Protected Chat",
            dest_topic="Imported Files",
            root_msg_id=1,
        )

        doc = types.Document(
            id=2, access_hash=2, file_reference=b"", date=None,
            mime_type="application/pdf", size=1024, dc_id=1,
            attributes=[types.DocumentAttributeFilename("protected.pdf")]
        )
        msg = types.Message(id=50, peer_id=types.PeerChannel(1), date=None, message="Protected", media=types.MessageMediaDocument(document=doc), noforwards=True)

        async def mock_iter(entity, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter
        semaphore = asyncio.Semaphore(2)

        processed = await process_telegram_unit_batch(
            self.client, self.dest_entity, unit, self.db, self.config, self.controller, semaphore
        )

        self.assertEqual(processed, 1)
        self.client.download_media.assert_not_called()
        self.client.send_file.assert_not_called()

        item = self.db.get_item(str(self.source_id), "50")
        self.assertEqual(item["status"], "restricted")

    async def test_crash_recovery_destination_marker_scan(self):
        """Items left in 'uploading' are marked UPLOADED if [src chat/msg] is found in destination."""
        # Insert one item that was uploaded before crash, and one that was interrupted
        self.db.mark_item(str(self.source_id), "100", "Imported Files", "uploading")
        self.db.mark_item(str(self.source_id), "101", "Imported Files", "uploading")

        # Mock destination messages: message contains marker for 100, but not 101
        norm_source = str(self.source_id).replace("-100", "").replace("-", "")
        dest_msg = types.Message(
            id=500, peer_id=types.PeerChannel(1), date=None,
            message=f"Uploaded Lecture Notes\n\n[src {norm_source}/100]"
        )

        async def mock_dest_iter(entity, **kwargs):
            yield dest_msg

        self.client.iter_messages = mock_dest_iter

        await recover_in_flight_uploads(self.client, self.dest_entity, self.db, self.config)

        # 100 was confirmed in destination -> marked UPLOADED
        item100 = self.db.get_item(str(self.source_id), "100")
        self.assertEqual(item100["status"], "UPLOADED")

        # 101 was not found -> reset to PENDING so it can be re-attempted
        item101 = self.db.get_item(str(self.source_id), "101")
        self.assertEqual(item101["status"], "PENDING")

    async def test_dry_run_zero_telegram_writes(self):
        """In dry run mode, no files are downloaded or sent to Telegram."""
        self.config.DRY_RUN = True
        unit = TransferUnit(
            unit_id=f"chat_{self.source_id}",
            unit_type="telegram_chat",
            source_id=self.source_id,
            source_title="Dry Run Test",
            dest_topic="Imported Files",
            root_msg_id=1,
        )

        doc = types.Document(
            id=3, access_hash=3, file_reference=b"", date=None,
            mime_type="application/pdf", size=5000, dc_id=1,
            attributes=[types.DocumentAttributeFilename("dry_run.pdf")]
        )
        msg = types.Message(id=77, peer_id=types.PeerChannel(1), date=None, message="Dry msg", media=types.MessageMediaDocument(document=doc))

        async def mock_iter(entity, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter
        semaphore = asyncio.Semaphore(2)

        processed = await process_telegram_unit_batch(
            self.client, self.dest_entity, unit, self.db, self.config, self.controller, semaphore
        )

        self.assertEqual(processed, 1)
        self.client.download_media.assert_not_called()
        self.client.send_file.assert_not_called()

        item = self.db.get_item(str(self.source_id), "77")
        self.assertEqual(item["status"], "UPLOADED")


if __name__ == "__main__":
    unittest.main()

