#!/usr/bin/env python3
"""
Unit and integration tests for Telegram Organizer restricted forwarding handling.
Covers:
1. Forwarding restriction detection and download fallback
2. Content protection denial handling (MANUAL_DOWNLOAD_REQUIRED)
3. Successful download & re-upload via send_file, topic routing, temp file cleanup
4. Retry logic on upload failures and preserving retryable state (FAILED)
5. Duplicate prevention for UPLOADED messages
6. Source vs. destination group separation safety checks
7. Text-only message separation (TEXT_ONLY)
8. Manual downloads directory workflow and mapping
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from telethon.errors import ChatForwardsRestrictedError, FloodWaitError
from telethon.tl import types

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from organizer import (
    Database,
    find_manual_download,
    get_message_filename,
    process_manual_downloads,
    process_source_chats,
    resolve_msg_id_from_file,
    verify_not_source,
)


class TestRestrictedForwarding(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_organizer.db")
        self.db = Database(self.db_path)
        self.source_id = -1002671181760  # Delta 6 full stack
        self.dest_id = -1003910839448    # Delta 6
        self.dest_topic = "Imported Files"
        self.root_msg_id = 3

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_verify_not_source_safeguard(self):
        """Requirement 14: Ensure source and destination are separate; never upload to source."""
        # When target is different, it passes
        verify_not_source(self.dest_id, self.source_id)
        target_peer = types.PeerChannel(channel_id=3910839448)
        verify_not_source(target_peer, self.source_id)

        # When target matches source, it raises ValueError
        source_peer = types.PeerChannel(channel_id=2671181760)
        with self.assertRaises(ValueError) as ctx:
            verify_not_source(source_peer, self.source_id)
        self.assertIn("SECURITY ABORT", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            verify_not_source(self.source_id, self.source_id)
        self.assertIn("SECURITY ABORT", str(ctx.exception))

    def test_duplicate_prevention(self):
        """Requirement 10 & 11: UPLOADED messages are considered done and not re-uploaded."""
        chat_key = str(self.source_id)
        msg_id = "253"

        # Initially not done
        self.assertFalse(self.db.is_item_done(chat_key, msg_id))

        # Mark as PENDING or FAILED -> Still not done (retryable!)
        self.db.mark_item(chat_key, msg_id, self.dest_topic, "PENDING")
        self.assertFalse(self.db.is_item_done(chat_key, msg_id))

        self.db.mark_item(chat_key, msg_id, self.dest_topic, "FAILED", error="Network error")
        self.assertFalse(self.db.is_item_done(chat_key, msg_id))

        self.db.mark_item(chat_key, msg_id, self.dest_topic, "MANUAL_DOWNLOAD_REQUIRED")
        self.assertFalse(self.db.is_item_done(chat_key, msg_id))

        # Once UPLOADED -> Is done (avoids duplicate upload)
        self.db.mark_item(chat_key, msg_id, self.dest_topic, "UPLOADED", filename="test.pdf")
        self.assertTrue(self.db.is_item_done(chat_key, msg_id))

    def test_legacy_restricted_migration(self):
        """Requirement 9: Legacy 'restricted' items are migrated to 'PENDING'."""
        chat_key = str(self.source_id)
        # Directly insert a legacy 'restricted' row
        with self.db.conn:
            self.db.conn.execute("""
                INSERT INTO items (source, item_key, topic, status, error, retries, updated_at)
                VALUES (?, '999', 'Imported Files', 'restricted', 'ChatForwardsRestrictedError', 0, '2026-10-24T00:00:00')
            """, (chat_key,))

        # Run schema migration
        self.db._migrate_schema()
        item = self.db.get_item(chat_key, "999")
        self.assertEqual(item["status"], "PENDING")
        self.assertIsNone(item["error"])

    def test_manual_download_resolution(self):
        """Requirement 13: Mapping by filename pattern and mapping.txt."""
        manual_dir = os.path.join(self.test_dir, "manual_downloads")
        os.makedirs(manual_dir, exist_ok=True)

        # 1. Filename pattern: 253_notes.pdf, msg_254.mp4
        self.assertEqual(resolve_msg_id_from_file(manual_dir, "253_notes.pdf"), 253)
        self.assertEqual(resolve_msg_id_from_file(manual_dir, "msg_254.mp4"), 254)
        self.assertEqual(resolve_msg_id_from_file(manual_dir, "msg-255-lecture.pdf"), 255)
        self.assertEqual(resolve_msg_id_from_file(manual_dir, "256.pdf"), 256)
        self.assertIsNone(resolve_msg_id_from_file(manual_dir, "arbitrary_document.pdf"))

        # 2. mapping.txt support
        with open(os.path.join(manual_dir, "mapping.txt"), "w") as f:
            f.write("257: arbitrary_document.pdf\n# comment\n258,another.zip\n")

        self.assertEqual(resolve_msg_id_from_file(manual_dir, "arbitrary_document.pdf"), 257)
        self.assertEqual(resolve_msg_id_from_file(manual_dir, "another.zip"), 258)

        # 3. find_manual_download helper
        test_file = os.path.join(manual_dir, "253_notes.pdf")
        with open(test_file, "w") as f:
            f.write("dummy pdf content")

        found = find_manual_download(manual_dir, 253)
        self.assertEqual(found, test_file)
        self.assertIsNone(find_manual_download(manual_dir, 999))

    def test_text_only_message_separation(self):
        """Requirement 6: Text-only messages are recorded separately and not treated as media."""
        doc = types.Document(
            id=1, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=100, dc_id=1,
            attributes=[types.DocumentAttributeFilename("doc.pdf")]
        )
        media_msg = types.Message(id=1, peer_id=types.PeerChannel(1), date=None, message="Media caption", media=types.MessageMediaDocument(document=doc))
        text_msg = types.Message(id=2, peer_id=types.PeerChannel(1), date=None, message="Text only announcement", media=None)

        self.assertEqual(get_message_filename(media_msg), "doc.pdf")
        self.assertIsNone(get_message_filename(text_msg))


class TestRestrictedProcessingPipeline(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.db = Database(self.db_path)
        self.source_id = -1002671181760
        self.dest_id = -1003910839448
        self.dest_topic = "Imported Files"
        self.root_msg_id = 3

        # Mock client
        self.client = MagicMock()
        self.client.__call__ = AsyncMock()
        self.client.download_media = AsyncMock()
        self.client.send_file = AsyncMock()
        self.client.get_entity = AsyncMock()
        self.client.get_input_entity = AsyncMock()

        # Group entity setup
        self.group_entity = MagicMock()
        self.group_entity.id = self.dest_id
        self.group_entity.title = "Delta 6"

        self.source_entity = MagicMock()
        self.source_entity.id = self.source_id
        self.source_entity.title = "Delta 6 full stack"

        self.client.get_entity.side_effect = lambda target: self.source_entity if target == self.source_id else self.group_entity
        self.client.get_input_entity.side_effect = lambda target: MagicMock()

    async def asyncTearDown(self):
        shutil.rmtree(self.test_dir)

    async def test_restricted_forwarding_falls_back_to_download_and_upload(self):
        """
        Requirements 1, 3, 4, 5, 12:
        When forwarding fails with ChatForwardsRestrictedError:
        - Downloader attempts download
        - Sends file to destination topic using send_file (not forward_messages)
        - Preserves filename and caption
        - Removes temporary file
        - Marks status as UPLOADED
        """
        # Create a mock message with media
        doc = types.Document(
            id=1, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=1024, dc_id=1,
            attributes=[types.DocumentAttributeFilename("lecture_01.pdf")]
        )
        msg = types.Message(
            id=253, peer_id=types.PeerChannel(abs(self.source_id)),
            date=None, message="Lecture 01 Notes", media=types.MessageMediaDocument(document=doc)
        )

        async def mock_iter(*args, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter

        # ForwardMessagesRequest raises ChatForwardsRestrictedError
        self.client.__call__.side_effect = ChatForwardsRestrictedError(request=MagicMock())

        # Download media creates a real temp file
        temp_created_files = []
        async def mock_download(m, file=None):
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "wb") as f:
                f.write(b"PDF content bytes")
            temp_created_files.append(file)
            return file

        self.client.download_media.side_effect = mock_download
        self.client.send_file.return_value = MagicMock()

        topics = {self.dest_topic: {"topic_id": self.root_msg_id, "root_msg_id": self.root_msg_id}}

        with patch("organizer.CHAT_TO_TOPIC", {self.source_id: self.dest_topic}), \
             patch("organizer.TOPIC_NAMES", [self.dest_topic]), \
             patch("organizer.TEMP_DIR", os.path.join(self.test_dir, "temp")), \
             patch("organizer.MANUAL_DOWNLOADS_DIR", os.path.join(self.test_dir, "manual")):

            await process_source_chats(self.client, self.group_entity, topics, self.db, dry_run=False)

        # 1. Download was called
        self.client.download_media.assert_called_once()

        # 2. send_file was called with reply_to=root_msg_id and caption
        self.client.send_file.assert_called_once()
        args, kwargs = self.client.send_file.call_args
        self.assertEqual(kwargs["reply_to"], self.root_msg_id)
        self.assertEqual(kwargs["caption"], "Lecture 01 Notes")

        # 3. Status in DB is UPLOADED
        item = self.db.get_item(str(self.source_id), "253")
        self.assertEqual(item["status"], "UPLOADED")
        self.assertEqual(item["filename"], "lecture_01.pdf")
        self.assertEqual(item["caption"], "Lecture 01 Notes")

        # 4. Temp file was deleted after successful upload
        for f in temp_created_files:
            self.assertFalse(os.path.exists(f))

    async def test_download_denied_content_protection(self):
        """
        Requirement 7:
        When Telegram denies download due to content protection,
        record MANUAL_DOWNLOAD_REQUIRED and continue without crashing.
        """
        doc = types.Document(
            id=2, access_hash=2, file_reference=b"", date=None,
            mime_type="video/mp4", size=2048, dc_id=1,
            attributes=[types.DocumentAttributeFilename("protected_video.mp4")]
        )
        msg = types.Message(
            id=254, peer_id=types.PeerChannel(abs(self.source_id)),
            date=None, message="Protected Video", media=types.MessageMediaDocument(document=doc)
        )

        async def mock_iter(*args, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter

        # Forwarding restricted
        self.client.__call__.side_effect = ChatForwardsRestrictedError(request=MagicMock())
        # Downloading denied by content protection
        self.client.download_media.side_effect = PermissionError("Cannot download: content protection restricted")

        topics = {self.dest_topic: {"topic_id": self.root_msg_id, "root_msg_id": self.root_msg_id}}

        with patch("organizer.CHAT_TO_TOPIC", {self.source_id: self.dest_topic}), \
             patch("organizer.TOPIC_NAMES", [self.dest_topic]), \
             patch("organizer.TEMP_DIR", os.path.join(self.test_dir, "temp")), \
             patch("organizer.MANUAL_DOWNLOADS_DIR", os.path.join(self.test_dir, "manual")):

            await process_source_chats(self.client, self.group_entity, topics, self.db, dry_run=False)

        # Did NOT call send_file
        self.client.send_file.assert_not_called()

        # Status in DB is MANUAL_DOWNLOAD_REQUIRED
        item = self.db.get_item(str(self.source_id), "254")
        self.assertEqual(item["status"], "MANUAL_DOWNLOAD_REQUIRED")
        self.assertIn("content protection", item["error"].lower())

    async def test_manual_download_upload_workflow(self):
        """
        Requirement 13:
        When a file is placed in manual_downloads matching msg ID,
        it is uploaded to the destination topic and marked UPLOADED.
        The original manual file is preserved.
        """
        manual_dir = os.path.join(self.test_dir, "manual_downloads")
        os.makedirs(manual_dir, exist_ok=True)
        manual_file = os.path.join(manual_dir, "255_notes.pdf")
        with open(manual_file, "w") as f:
            f.write("manually saved content")

        # Message in DB had MANUAL_DOWNLOAD_REQUIRED
        self.db.mark_item(str(self.source_id), "255", self.dest_topic, "MANUAL_DOWNLOAD_REQUIRED", filename="255_notes.pdf", caption="React Session 5")

        topics = {self.dest_topic: {"topic_id": self.root_msg_id, "root_msg_id": self.root_msg_id}}

        with patch("organizer.MANUAL_DOWNLOADS_DIR", manual_dir):
            uploaded = await process_manual_downloads(
                self.client, self.group_entity, topics, self.db, self.source_id, self.dest_topic, dry_run=False
            )

        self.assertEqual(uploaded, 1)
        self.client.send_file.assert_called_once()
        args, kwargs = self.client.send_file.call_args
        self.assertEqual(args[1], manual_file)
        self.assertEqual(kwargs["reply_to"], self.root_msg_id)

        # DB marked UPLOADED
        item = self.db.get_item(str(self.source_id), "255")
        self.assertEqual(item["status"], "UPLOADED")

        # Original manual file preserved
        self.assertTrue(os.path.exists(manual_file))

    async def test_upload_retries_and_failed_state(self):
        """
        Requirement 9 & 15:
        When upload fails repeatedly, retry up to 3 times, mark FAILED,
        keep message retryable (not permanently skipped).
        """
        doc = types.Document(
            id=3, access_hash=3, file_reference=b"", date=None,
            mime_type="application/pdf", size=500, dc_id=1,
            attributes=[types.DocumentAttributeFilename("retry_doc.pdf")]
        )
        msg = types.Message(
            id=256, peer_id=types.PeerChannel(abs(self.source_id)),
            date=None, message="Retry Document", media=types.MessageMediaDocument(document=doc)
        )

        async def mock_iter(*args, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter
        self.client.__call__.side_effect = ChatForwardsRestrictedError(request=MagicMock())

        async def mock_download(m, file=None):
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "wb") as f:
                f.write(b"data")
            return file

        self.client.download_media.side_effect = mock_download
        # send_file always fails
        self.client.send_file.side_effect = ConnectionResetError("Connection lost")

        topics = {self.dest_topic: {"topic_id": self.root_msg_id, "root_msg_id": self.root_msg_id}}

        with patch("organizer.CHAT_TO_TOPIC", {self.source_id: self.dest_topic}), \
             patch("organizer.TOPIC_NAMES", [self.dest_topic]), \
             patch("organizer.TEMP_DIR", os.path.join(self.test_dir, "temp")), \
             patch("organizer.MANUAL_DOWNLOADS_DIR", os.path.join(self.test_dir, "manual")), \
             patch("asyncio.sleep", AsyncMock()):

            await process_source_chats(self.client, self.group_entity, topics, self.db, dry_run=False)

        # 1. send_file called 4 times (1 initial + 3 retries)
        self.assertEqual(self.client.send_file.call_count, 4)

        # 2. Marked as FAILED (not permanently done/skipped!)
        item = self.db.get_item(str(self.source_id), "256")
        self.assertEqual(item["status"], "FAILED")
        self.assertEqual(item["retries"], 4)
        self.assertFalse(self.db.is_item_done(str(self.source_id), "256"))

    async def test_text_only_message_recording(self):
        """
        Requirement 6:
        Text-only messages are recorded as TEXT_ONLY and never passed to download_media.
        """
        msg = types.Message(
            id=257, peer_id=types.PeerChannel(abs(self.source_id)),
            date=None, message="Important text announcement", media=None
        )

        async def mock_iter(*args, **kwargs):
            yield msg

        self.client.iter_messages = mock_iter
        topics = {self.dest_topic: {"topic_id": self.root_msg_id, "root_msg_id": self.root_msg_id}}

        with patch("organizer.CHAT_TO_TOPIC", {self.source_id: self.dest_topic}), \
             patch("organizer.TOPIC_NAMES", [self.dest_topic]), \
             patch("organizer.TEMP_DIR", os.path.join(self.test_dir, "temp")), \
             patch("organizer.MANUAL_DOWNLOADS_DIR", os.path.join(self.test_dir, "manual")):

            await process_source_chats(self.client, self.group_entity, topics, self.db, dry_run=False)

        # download_media and send_file were NOT called
        self.client.download_media.assert_not_called()
        self.client.send_file.assert_not_called()

        # Recorded as TEXT_ONLY in DB
        item = self.db.get_item(str(self.source_id), "257")
        self.assertEqual(item["status"], "TEXT_ONLY")
        self.assertEqual(item["caption"], "Important text announcement")


if __name__ == "__main__":
    unittest.main()
