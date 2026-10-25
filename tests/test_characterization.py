#!/usr/bin/env python3
"""
Characterization tests for current Telegram Organizer baseline behavior.
Must pass before and after all phase implementations.
Uses mocked Telethon only (no real network or credentials).
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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
    redact_secrets,
)


class TestCharacterizationBaseline(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.db = Database(self.db_path)
        self.config = Config()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_database_initial_schema_and_meta(self):
        """Database meta table and schema initialization."""
        self.db.set_meta("test_key", "test_val")
        self.assertEqual(self.db.get_meta("test_key"), "test_val")

    def test_secret_redaction_characterization(self):
        """Redacts all sensitive tokens from text."""
        with patch.object(Config, "API_HASH", "secret_hash_value"), \
             patch.object(Config, "PHONE", "+19998887777"), \
             patch.object(Config, "DASHBOARD_PASSWORD", "super_secret_pw"), \
             patch.object(Config, "SESSION_STRING", "1BVts_session_secret"):

            sample = "Auth failed with hash secret_hash_value and phone +19998887777 (pass super_secret_pw session 1BVts_session_secret)"
            redacted = redact_secrets(sample)
            self.assertNotIn("secret_hash_value", redacted)
            self.assertNotIn("+19998887777", redacted)
            self.assertNotIn("super_secret_pw", redacted)
            self.assertNotIn("1BVts_session_secret", redacted)
            self.assertIn("[REDACTED]", redacted)

    def test_topic_normalization(self):
        """Normalizes titles for comparison by stripping emojis and lowercasing."""
        self.assertEqual(normalize_title("🏅 PHARMACOLOGY"), "pharmacology")
        self.assertEqual(normalize_title("  📚 NOTES & GUIDES  "), "notes guides")

    def test_process_lock_mutual_exclusion(self):
        """File lock acquires and rejects duplicate processes."""
        lock_path = os.path.join(self.test_dir, "organizer.lock")
        lock_a = ProcessLock(lock_path)
        lock_b = ProcessLock(lock_path)

        self.assertTrue(lock_a.acquire())
        self.assertFalse(lock_b.acquire())

        lock_a.release()
        self.assertTrue(lock_b.acquire())
        lock_b.release()


class TestCharacterizationAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.db = Database(self.db_path)
        self.config = Config()
        self.config.TMP_DIR = os.path.join(self.test_dir, "tmp")
        self.config.DRY_RUN = True
        os.makedirs(self.config.TMP_DIR, exist_ok=True)
        self.controller = WorkerController(self.config, self.db)

        self.source_id = -1002671181760
        self.dest_id = -1004335027746

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

    async def test_dry_run_zero_writes(self):
        """DRY_RUN=true never calls send_file on Telegram client."""
        unit = TransferUnit(
            unit_id=f"chat_{self.source_id}",
            unit_type="telegram_chat",
            source_id=self.source_id,
            source_title="Test Unit",
            dest_topic="Imported Files",
            root_msg_id=1,
        )

        doc = types.Document(
            id=10, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=1024, dc_id=1,
            attributes=[types.DocumentAttributeFilename("doc.pdf")]
        )
        msg = types.Message(id=1, peer_id=types.PeerChannel(1), date=None, message="Test Doc", media=types.MessageMediaDocument(document=doc))

        async def mock_iter(entity, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter
        sem = asyncio.Semaphore(1)

        processed = await process_telegram_unit_batch(
            self.client, self.dest_entity, unit, self.db, self.config, self.controller, sem
        )

        self.assertEqual(processed, 1)
        self.client.send_file.assert_not_called()
        self.client.download_media.assert_not_called()


if __name__ == "__main__":
    unittest.main()
