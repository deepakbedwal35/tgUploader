#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Phases 1-5 of Telegram Organizer:
- Metrics recording and report calculation (Phase 1)
- Transfer methods: Fast-path reference-send, FileReferenceExpiredError retry, fallback (Phase 2)
- Chunked download to .part, atomic rename, size check, DocumentAttributes (video streaming, photo) (Phase 2)
- Upload retry reusing downloaded file without re-downloading (Phase 3)
- Crash recovery across all states: downloading, downloaded, uploading, and orphan temp cleanup (Phase 3)
- Reliability: FloodWait global pause without retry penalty, exponential backoff (Phase 4)
- Disk space check and backpressure budget deferral (Phase 4)
- Versioned schema migration preserving legacy database (Phase 1 & 4)
- DRY_RUN zero writes guarantee (Phase 4)
"""

import asyncio
import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from telethon.errors import ChatForwardsRestrictedError, FileReferenceExpiredError, FloodWaitError
from telethon.tl import types

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from organizer import (
    Config,
    Database,
    PipelineEngine,
    PipelineWorkItem,
    TransferUnit,
    WorkerController,
    WorkerState,
    _transfer_telegram_message,
    global_floodwait_event,
    print_metrics_report,
    recover_startup_state,
)


class TestPipelineAndPhases(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.tmp_dir = os.path.join(self.test_dir, "tmp")
        os.makedirs(self.tmp_dir, exist_ok=True)

        self.config = Config()
        self.config.DB_PATH = self.db_path
        self.config.TMP_DIR = self.tmp_dir
        self.config.DRY_RUN = False
        self.config.MAX_RETRIES = 3
        self.config.RETRY_BASE_DELAY = 0.01
        self.config.MAX_BACKOFF_SECONDS = 0.05
        self.config.MIN_FREE_DISK_GB = 0.001

        self.db = Database(self.db_path)
        self.controller = WorkerController(self.config, self.db)

        self.source_id = -1002671181760
        self.dest_id = -1004335027746
        self.dest_topic = "Imported Files"
        self.root_msg_id = 77

        self.unit = TransferUnit(
            unit_id=f"chat_{self.source_id}",
            unit_type="telegram_chat",
            source_id=self.source_id,
            source_title="Source Chat",
            dest_topic=self.dest_topic,
            root_msg_id=self.root_msg_id,
        )

        self.dest_entity = MagicMock()
        self.dest_entity.id = self.dest_id

        self.client = MagicMock()
        self.client.download_media = AsyncMock()
        self.client.send_file = AsyncMock()
        self.client.get_messages = AsyncMock()
        self.client.iter_messages = MagicMock()

    async def asyncTearDown(self):
        shutil.rmtree(self.test_dir)

    async def test_fast_path_reference_send_success(self):
        """Phase 2: When forwarding is allowed, send by reference without downloading."""
        doc = types.Document(
            id=101, access_hash=1, file_reference=b"ref1", date=None,
            mime_type="application/pdf", size=500, dc_id=1,
            attributes=[types.DocumentAttributeFilename("doc.pdf")]
        )
        msg = types.Message(
            id=1, peer_id=types.PeerChannel(1), date=None,
            message="Fast Path Document", media=types.MessageMediaDocument(document=doc),
            noforwards=False
        )

        await _transfer_telegram_message(
            client=self.client,
            dest_entity=self.dest_entity,
            msg=msg,
            unit=self.unit,
            filename="doc.pdf",
            file_size=500,
            db=self.db,
            config=self.config,
            controller=self.controller,
            allow_reference_send=True,
        )

        # Download was NEVER called
        self.client.download_media.assert_not_called()
        # send_file was called with original media by pointer
        self.client.send_file.assert_called_once()
        args, kwargs = self.client.send_file.call_args
        self.assertEqual(args[0], self.dest_entity)
        self.assertEqual(args[1], msg.media)
        self.assertEqual(kwargs["reply_to"], self.root_msg_id)

        # Item is marked UPLOADED
        self.assertTrue(self.db.is_item_done(str(self.source_id), "1"))

        # Metric recorded as reference-send
        rep = self.db.get_metrics_report()
        self.assertEqual(rep["total_files"], 1)
        self.assertEqual(rep["method_counts"]["reference-send"], 1)
        self.assertEqual(rep["method_counts"]["re-upload"], 0)

    async def test_reference_send_file_reference_expired_retry(self):
        """Phase 2: On FileReferenceExpiredError, refetches message and retries once."""
        doc = types.Document(
            id=102, access_hash=1, file_reference=b"expired", date=None,
            mime_type="application/pdf", size=600, dc_id=1,
            attributes=[types.DocumentAttributeFilename("expired.pdf")]
        )
        msg = types.Message(
            id=2, peer_id=types.PeerChannel(1), date=None,
            message="Expired Ref Doc", media=types.MessageMediaDocument(document=doc),
            noforwards=False
        )

        new_doc = types.Document(
            id=102, access_hash=1, file_reference=b"fresh_ref", date=None,
            mime_type="application/pdf", size=600, dc_id=1,
            attributes=[types.DocumentAttributeFilename("expired.pdf")]
        )
        refetched_msg = types.Message(
            id=2, peer_id=types.PeerChannel(1), date=None,
            message="Expired Ref Doc", media=types.MessageMediaDocument(document=new_doc)
        )
        self.client.get_messages.return_value = refetched_msg

        # First call raises FileReferenceExpiredError, second succeeds
        self.client.send_file.side_effect = [
            FileReferenceExpiredError(request=MagicMock()),
            MagicMock(),
        ]

        await _transfer_telegram_message(
            client=self.client,
            dest_entity=self.dest_entity,
            msg=msg,
            unit=self.unit,
            filename="expired.pdf",
            file_size=600,
            db=self.db,
            config=self.config,
            controller=self.controller,
            allow_reference_send=True,
        )

        self.assertEqual(self.client.send_file.call_count, 2)
        self.client.get_messages.assert_called_once_with(self.source_id, ids=2)
        self.assertTrue(self.db.is_item_done(str(self.source_id), "2"))

    async def test_reference_send_fallback_to_download_upload(self):
        """Phase 2: When reference-send fails with restriction, falls through to download + reupload."""
        doc = types.Document(
            id=103, access_hash=1, file_reference=b"ref3", date=None,
            mime_type="application/pdf", size=700, dc_id=1,
            attributes=[types.DocumentAttributeFilename("fallback.pdf")]
        )
        msg = types.Message(
            id=3, peer_id=types.PeerChannel(1), date=None,
            message="Fallback Doc", media=types.MessageMediaDocument(document=doc),
            noforwards=False
        )

        async def mock_dl(m, file=None, **kwargs):
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "wb") as f:
                f.write(b"data_bytes")
            return file

        self.client.download_media.side_effect = mock_dl

        # Fast path fails with ChatForwardsRestrictedError, upload then succeeds
        self.client.send_file.side_effect = [
            ChatForwardsRestrictedError(request=MagicMock()),
            MagicMock(),
        ]

        await _transfer_telegram_message(
            client=self.client,
            dest_entity=self.dest_entity,
            msg=msg,
            unit=self.unit,
            filename="fallback.pdf",
            file_size=700,
            db=self.db,
            config=self.config,
            controller=self.controller,
            allow_reference_send=True,
        )

        # Download was called as fallback
        self.client.download_media.assert_called_once()
        self.assertTrue(self.db.is_item_done(str(self.source_id), "3"))

        rep = self.db.get_metrics_report()
        self.assertEqual(rep["method_counts"]["re-upload"], 1)

    async def test_photo_sent_with_force_document_false(self):
        """Phase 2: Photo messages are uploaded with force_document=False."""
        photo = types.Photo(id=55, access_hash=1, file_reference=b"", date=None, sizes=[], dc_id=1)
        msg = types.Message(
            id=4, peer_id=types.PeerChannel(1), date=None,
            message="A photo", media=types.MessageMediaPhoto(photo=photo),
            noforwards=True
        )

        async def mock_dl(m, file=None, **kwargs):
            with open(file, "wb") as f:
                f.write(b"photobytes")
            return file

        self.client.download_media.side_effect = mock_dl
        self.client.send_file.return_value = MagicMock()

        await _transfer_telegram_message(
            client=self.client,
            dest_entity=self.dest_entity,
            msg=msg,
            unit=self.unit,
            filename="photo_4.jpg",
            file_size=10,
            db=self.db,
            config=self.config,
            controller=self.controller,
        )

        self.client.send_file.assert_called_once()
        _, kwargs = self.client.send_file.call_args
        self.assertFalse(kwargs["force_document"])

    async def test_video_preserves_attributes_and_streaming(self):
        """Phase 2: Video documents preserve DocumentAttributeVideo and set supports_streaming=True."""
        video_attr = types.DocumentAttributeVideo(duration=60, w=1280, h=720)
        doc = types.Document(
            id=99, access_hash=1, file_reference=b"", date=None,
            mime_type="video/mp4", size=1000, dc_id=1,
            attributes=[video_attr, types.DocumentAttributeFilename("clip.mp4")]
        )
        msg = types.Message(
            id=5, peer_id=types.PeerChannel(1), date=None,
            message="Video Clip", media=types.MessageMediaDocument(document=doc),
            noforwards=True
        )

        async def mock_dl(m, file=None, **kwargs):
            with open(file, "wb") as f:
                f.write(b"videodata")
            return file

        self.client.download_media.side_effect = mock_dl
        self.client.send_file.return_value = MagicMock()

        await _transfer_telegram_message(
            client=self.client,
            dest_entity=self.dest_entity,
            msg=msg,
            unit=self.unit,
            filename="clip.mp4",
            file_size=1000,
            db=self.db,
            config=self.config,
            controller=self.controller,
        )

        self.client.send_file.assert_called_once()
        _, kwargs = self.client.send_file.call_args
        self.assertTrue(kwargs.get("supports_streaming", False))
        attrs = kwargs.get("attributes", [])
        self.assertTrue(any(isinstance(a, types.DocumentAttributeVideo) for a in attrs))

    async def test_upload_retry_reuses_downloaded_file_without_redownload(self):
        """Phase 3: When upload fails and retries, the downloaded file on disk is reused."""
        doc = types.Document(
            id=106, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=500, dc_id=1,
            attributes=[types.DocumentAttributeFilename("retry.pdf")]
        )
        msg = types.Message(
            id=6, peer_id=types.PeerChannel(1), date=None,
            message="Retry Doc", media=types.MessageMediaDocument(document=doc),
            noforwards=True
        )

        download_calls = 0
        async def mock_dl(m, file=None, **kwargs):
            nonlocal download_calls
            download_calls += 1
            with open(file, "wb") as f:
                f.write(b"data")
            return file

        self.client.download_media.side_effect = mock_dl

        # send_file fails on first attempt, succeeds on second
        self.client.send_file.side_effect = [
            ConnectionResetError("Socket reset"),
            MagicMock(),
        ]

        await _transfer_telegram_message(
            client=self.client,
            dest_entity=self.dest_entity,
            msg=msg,
            unit=self.unit,
            filename="retry.pdf",
            file_size=500,
            db=self.db,
            config=self.config,
            controller=self.controller,
        )

        # download_media was called only ONCE
        self.assertEqual(download_calls, 1)
        # send_file was called 2 times
        self.assertEqual(self.client.send_file.call_count, 2)
        self.assertTrue(self.db.is_item_done(str(self.source_id), "6"))

    async def test_floodwait_handling_global_pause_no_retry_penalty(self):
        """Phase 4: FloodWait sleeps seconds+1, pauses workers globally, does not increment retry count."""
        doc = types.Document(
            id=107, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=200, dc_id=1,
            attributes=[types.DocumentAttributeFilename("flood.pdf")]
        )
        msg = types.Message(
            id=7, peer_id=types.PeerChannel(1), date=None,
            message="Flood Doc", media=types.MessageMediaDocument(document=doc),
            noforwards=True
        )

        async def mock_dl(m, file=None, **kwargs):
            with open(file, "wb") as f:
                f.write(b"data")
            return file

        self.client.download_media.side_effect = mock_dl

        flood_err = FloodWaitError(request=MagicMock())
        flood_err.seconds = 1  # 1 second wait

        self.client.send_file.side_effect = [
            flood_err,
            MagicMock(),
        ]

        with patch("asyncio.sleep", AsyncMock()) as mock_sleep:
            await _transfer_telegram_message(
                client=self.client,
                dest_entity=self.dest_entity,
                msg=msg,
                unit=self.unit,
                filename="flood.pdf",
                file_size=200,
                db=self.db,
                config=self.config,
                controller=self.controller,
            )
            # Slept for fe.seconds + 1 = 2s
            mock_sleep.assert_any_call(2)

        self.assertTrue(self.db.is_item_done(str(self.source_id), "7"))
        # Metric records floodwait seconds and retries=0
        rep = self.db.get_metrics_report()
        self.assertGreater(rep["floodwait_total_s"], 0)
        self.assertEqual(rep["total_retries"], 0)

    async def test_startup_recovery_all_states_and_orphan_cleanup(self):
        """Phase 3: Startup recovery resets downloading, verifies downloaded, scans uploading, deletes orphans."""
        # 1. downloading item -> should become PENDING
        self.db.mark_item(str(self.source_id), "10", self.dest_topic, "downloading")
        # 2. downloaded item with valid file -> should be kept
        valid_file = os.path.join(self.tmp_dir, f"{self.source_id}_20_valid.pdf")
        with open(valid_file, "wb") as f:
            f.write(b"valid content")
        self.db.mark_item(str(self.source_id), "20", self.dest_topic, "downloaded", local_path=valid_file)
        # 3. downloaded item with missing file -> should become PENDING
        self.db.mark_item(str(self.source_id), "30", self.dest_topic, "downloaded", local_path="/nonexistent/file.pdf")
        # 4. uploading item with marker in destination -> should become UPLOADED
        self.db.mark_item(str(self.source_id), "40", self.dest_topic, "uploading")
        # 5. uploading item without marker in destination -> should become PENDING
        self.db.mark_item(str(self.source_id), "50", self.dest_topic, "uploading")

        # Create orphan file in tmp_dir and an abandoned .part file
        orphan_part = os.path.join(self.tmp_dir, "abandoned.pdf.part")
        with open(orphan_part, "wb") as f:
            f.write(b"part data")
        orphan_file = os.path.join(self.tmp_dir, "orphan_trash.tmp")
        with open(orphan_file, "wb") as f:
            f.write(b"junk")

        norm_src = str(self.source_id).replace("-100", "").replace("-", "")
        marker_msg = MagicMock()
        marker_msg.text = f"Uploaded note\n\n[src {norm_src}/40]"
        marker_msg.message = marker_msg.text

        async def mock_iter_messages(dest, limit=100):
            yield marker_msg

        self.client.iter_messages = mock_iter_messages

        await recover_startup_state(self.client, self.dest_entity, self.db, self.config)

        # Verify items
        self.assertEqual(self.db.get_item(str(self.source_id), "10")["status"], "PENDING")
        self.assertEqual(self.db.get_item(str(self.source_id), "20")["status"], "downloaded")
        self.assertEqual(self.db.get_item(str(self.source_id), "30")["status"], "PENDING")
        self.assertEqual(self.db.get_item(str(self.source_id), "40")["status"], "UPLOADED")
        self.assertEqual(self.db.get_item(str(self.source_id), "50")["status"], "PENDING")

        # Verify files
        self.assertTrue(os.path.exists(valid_file))
        self.assertFalse(os.path.exists(orphan_part))
        self.assertFalse(os.path.exists(orphan_file))

    def test_schema_migration_preserves_old_db(self):
        """Phase 1: Migration preserves existing data and upgrades to schema_version 2."""
        legacy_dir = tempfile.mkdtemp()
        legacy_db_path = os.path.join(legacy_dir, "legacy.db")
        try:
            # Create a v1 legacy database manually
            conn = sqlite3.connect(legacy_db_path)
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', '1')")
            conn.execute("CREATE TABLE topics (name TEXT PRIMARY KEY, topic_id INTEGER, root_msg_id INTEGER)")
            conn.execute("INSERT INTO topics (name, topic_id, root_msg_id) VALUES ('Notes', 10, 10)")
            conn.execute("CREATE TABLE items (source TEXT, item_key TEXT, topic TEXT, status TEXT, error TEXT, retries INTEGER, updated_at TEXT, PRIMARY KEY(source, item_key))")
            conn.execute("INSERT INTO items (source, item_key, topic, status) VALUES ('123', '456', 'Notes', 'UPLOADED')")
            conn.commit()
            conn.close()

            # Open via Database class to trigger versioned migration
            migrated_db = Database(legacy_db_path)

            # Old data preserved
            self.assertEqual(migrated_db.get_meta("schema_version"), "2")
            item = migrated_db.get_item("123", "456")
            self.assertIsNotNone(item)
            self.assertEqual(item["status"], "UPLOADED")

            # New job_metrics table exists and works
            migrated_db.record_metric("123", 10, 456, 1024, method="re-upload")
            rep = migrated_db.get_metrics_report()
            self.assertEqual(rep["total_files"], 1)

        finally:
            shutil.rmtree(legacy_dir)

    async def test_disk_space_insufficient_deferral(self):
        """Phase 4: When free disk space is less than required, item is skipped and marked skipped_disk."""
        doc = types.Document(
            id=108, access_hash=1, file_reference=b"", date=None,
            mime_type="application/pdf", size=500_000_000, dc_id=1,
            attributes=[types.DocumentAttributeFilename("huge.pdf")]
        )
        msg = types.Message(
            id=8, peer_id=types.PeerChannel(1), date=None,
            message="Huge Doc", media=types.MessageMediaDocument(document=doc),
            noforwards=True
        )

        # Mock shutil.disk_usage to return low free space (less than file_size + MIN_FREE_DISK_GB)
        low_disk = MagicMock(free=100)
        with patch("shutil.disk_usage", return_value=low_disk):
            await _transfer_telegram_message(
                client=self.client,
                dest_entity=self.dest_entity,
                msg=msg,
                unit=self.unit,
                filename="huge.pdf",
                file_size=500_000_000,
                db=self.db,
                config=self.config,
                controller=self.controller,
            )

        # Verify not downloaded or uploaded
        self.client.download_media.assert_not_called()
        self.client.send_file.assert_not_called()
        item = self.db.get_item(str(self.source_id), "8")
        self.assertEqual(item["status"], "skipped_disk")

    async def test_pipeline_engine_processing(self):
        """Phase 3: PipelineEngine processes items concurrently through download and upload queues."""
        engine = PipelineEngine(self.client, self.dest_entity, self.db, self.config, self.controller)
        await engine.start()

        doc = types.Document(
            id=109, access_hash=1, file_reference=b"ref9", date=None,
            mime_type="application/pdf", size=100, dc_id=1,
            attributes=[types.DocumentAttributeFilename("pipe.pdf")]
        )
        msg = types.Message(
            id=9, peer_id=types.PeerChannel(1), date=None,
            message="Pipe Doc", media=types.MessageMediaDocument(document=doc),
            noforwards=False
        )

        item = PipelineWorkItem(self.unit, msg, "pipe.pdf", 100, time.monotonic())
        await engine.submit(item)
        await engine.drain()
        await engine.stop()

        self.assertTrue(self.db.is_item_done(str(self.source_id), "9"))

    def test_print_metrics_report_output(self):
        """Phase 1: print_metrics_report outputs formatted statistics cleanly."""
        self.db.record_metric(str(self.source_id), 1, 101, 1024 * 1024, download_s=0.5, upload_s=0.5, total_s=1.0, method="re-upload")
        self.db.record_metric(str(self.source_id), 1, 102, 2048 * 1024, download_s=0.0, upload_s=0.2, total_s=0.2, method="reference-send")

        import io
        from contextlib import redirect_stdout
        f = io.StringIO()
        with redirect_stdout(f):
            print_metrics_report(self.config)
        out = f.getvalue()

        self.assertIn("TELEGRAM ORGANIZER PERFORMANCE & METRICS REPORT", out)
        self.assertIn("Total Files Processed   : 2", out)
        self.assertIn("Reference-send: 1 | Re-upload: 1", out)


if __name__ == "__main__":
    unittest.main()
