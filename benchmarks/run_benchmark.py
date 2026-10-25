#!/usr/bin/env python3
"""
Benchmark Runner for Telegram Organizer (Phase 5).
Evaluates pipeline configurations on ~20 mixed file samples:
- Sequential (1 dl / 1 up, ref-send off)
- Pipelined 2dl / 1up (ref-send off)
- Pipelined 3dl / 1up (ref-send off)
- Fast-path reference-send ON vs OFF

Outputs:
- benchmarks/baseline.json
- benchmarks/recommended.env
"""

import argparse
import asyncio
import io
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from typing import Any, Dict, List

# Add parent dir to sys.path
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from organizer import (
    Config,
    Database,
    PipelineEngine,
    PipelineWorkItem,
    TransferUnit,
    WorkerController,
    _transfer_telegram_message,
)
from telethon.tl import types


class MockBenchmarkClient:
    """Mock Telethon client simulating network transfer latencies and sizes."""

    def __init__(self, latency_per_mb: float = 0.05, sim_floodwait: bool = False):
        self.latency_per_mb = latency_per_mb
        self.sim_floodwait = sim_floodwait
        self.calls = 0

    async def download_media(self, msg, file=None, progress_callback=None, **kwargs):
        size = getattr(getattr(msg, "file", None), "size", None) or (1024 * 1024)
        dl_time = (size / (1024 * 1024)) * self.latency_per_mb
        await asyncio.sleep(dl_time)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, "wb") as f:
            f.write(b"0" * min(size, 4096))
        return file

    async def send_file(self, entity, file, caption=None, reply_to=None, force_document=False, progress_callback=None, **kwargs):
        self.calls += 1
        if isinstance(file, str) and os.path.exists(file):
            size = os.path.getsize(file)
        else:
            size = 1024 * 1024
        up_time = (size / (1024 * 1024)) * self.latency_per_mb
        await asyncio.sleep(up_time)
        return MagicMockMessage(self.calls)


class MagicMockMessage:
    def __init__(self, msg_id: int):
        self.id = msg_id


def generate_synthetic_sample(count: int = 20) -> List[types.Message]:
    """Generates ~20 mixed file items: documents, videos, audio, and photos."""
    messages = []
    specs = [
        ("PDF Document (Small)", 512 * 1024, "application/pdf", "doc.pdf", False),
        ("PDF Document (Medium)", 5 * 1024 * 1024, "application/pdf", "book.pdf", False),
        ("High-Res Photo", 2 * 1024 * 1024, "image/jpeg", "photo.jpg", True),
        ("Short Video Clip", 15 * 1024 * 1024, "video/mp4", "lecture.mp4", False),
        ("Audio Lecture", 8 * 1024 * 1024, "audio/mpeg", "audio.mp3", False),
    ]

    for i in range(1, count + 1):
        name, size, mime, ext, is_photo = specs[(i - 1) % len(specs)]
        if is_photo:
            photo = types.Photo(id=1000 + i, access_hash=1, file_reference=b"ref", date=None, sizes=[], dc_id=1)
            media = types.MessageMediaPhoto(photo=photo)
        else:
            attrs = [types.DocumentAttributeFilename(f"sample_{i}_{ext}")]
            if "video" in mime:
                attrs.append(types.DocumentAttributeVideo(duration=120, w=1920, h=1080))
            doc = types.Document(
                id=2000 + i, access_hash=1, file_reference=b"ref", date=None,
                mime_type=mime, size=size, dc_id=1, attributes=attrs
            )
            media = types.MessageMediaDocument(document=doc)

        msg = types.Message(
            id=i, peer_id=types.PeerChannel(1), date=None,
            message=f"Sample Item {i} - {name}", media=media,
            noforwards=False
        )
        messages.append(msg)

    return messages


async def run_scenario(
    name: str,
    dl_concurrency: int,
    up_concurrency: int,
    allow_ref_send: bool,
    sample_messages: List[types.Message],
) -> Dict[str, Any]:
    """Runs a single benchmark scenario and returns metrics."""
    test_dir = tempfile.mkdtemp(prefix="tg_bench_")
    db_path = os.path.join(test_dir, "bench.db")
    tmp_dir = os.path.join(test_dir, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    config = Config()
    config.DB_PATH = db_path
    config.TMP_DIR = tmp_dir
    config.DOWNLOAD_CONCURRENCY = dl_concurrency
    config.UPLOAD_CONCURRENCY = up_concurrency
    config.QUEUE_MAX = 4
    config.DOWNLOAD_AHEAD_MAX_GB = 2.0
    config.MIN_FREE_DISK_GB = 0.5
    config.MAX_RETRIES = 3
    config.DRY_RUN = False

    db = Database(db_path)
    controller = WorkerController(config, db)
    client = MockBenchmarkClient(latency_per_mb=0.01)

    unit = TransferUnit(
        unit_id="bench_unit",
        unit_type="telegram_chat",
        source_id=-1002671181760,
        source_title="Benchmark Source",
        dest_topic="Imported Files",
        root_msg_id=1,
    )
    dest_entity = types.PeerChannel(3910839448)

    t0 = time.monotonic()
    semaphore = asyncio.Semaphore(dl_concurrency)

    async def process_item(msg):
        filename = f"media_{msg.id}"
        file_size = getattr(getattr(msg, "file", None), "size", 0) or getattr(getattr(getattr(msg, "media", None), "document", None), "size", 0) or (1024 * 1024)
        async with semaphore:
            await _transfer_telegram_message(
                client=client,
                dest_entity=dest_entity,
                msg=msg,
                unit=unit,
                filename=filename,
                file_size=file_size,
                db=db,
                config=config,
                controller=controller,
                allow_reference_send=allow_ref_send,
            )

    tasks = [process_item(msg) for msg in sample_messages]
    await asyncio.gather(*tasks)

    elapsed_s = max(time.monotonic() - t0, 0.001)
    rep = db.get_metrics_report()
    total_bytes = sum(getattr(getattr(m, "file", None), "size", 0) or getattr(getattr(getattr(m, "media", None), "document", None), "size", 0) or (1024 * 1024) for m in sample_messages)
    overall_mb_s = (total_bytes / (1024 * 1024)) / elapsed_s
    files_per_min = round((len(sample_messages) / elapsed_s) * 60, 2)

    shutil.rmtree(test_dir)

    return {
        "scenario": name,
        "dl_concurrency": dl_concurrency,
        "up_concurrency": up_concurrency,
        "reference_send_enabled": allow_ref_send,
        "files_processed": len(sample_messages),
        "total_megabytes": round(total_bytes / (1024 * 1024), 2),
        "elapsed_seconds": round(elapsed_s, 3),
        "files_per_minute": files_per_min,
        "overall_mb_s": round(overall_mb_s, 2),
        "median_download_mb_s": rep["download_mb_s"]["median"],
        "p90_download_mb_s": rep["download_mb_s"]["p90"],
        "median_upload_mb_s": rep["upload_mb_s"]["median"],
        "p90_upload_mb_s": rep["upload_mb_s"]["p90"],
        "method_counts": rep["method_counts"],
    }


async def main_async():
    parser = argparse.ArgumentParser(description="Telegram Organizer Pipeline Benchmark")
    parser.add_argument("--sample", type=int, default=20, help="Number of sample files to process (default: 20)")
    parser.add_argument("--output", type=str, default=os.path.join(_ROOT, "benchmarks", "baseline.json"), help="Output baseline JSON path")
    args = parser.parse_args()

    print("\n" + "=" * 75)
    print(" TELEGRAM ORGANIZER PIPELINE BENCHMARK")
    print(f" Sample Size: {args.sample} mixed items (photos, docs, audio, videos)")
    print("=" * 75)

    sample = generate_synthetic_sample(args.sample)

    scenarios = [
        ("Sequential (1dl / 1up, Ref-Send OFF)", 1, 1, False),
        ("Pipelined (2dl / 1up, Ref-Send OFF)", 2, 1, False),
        ("Pipelined (3dl / 1up, Ref-Send OFF)", 3, 1, False),
        ("Fast Path (2dl / 1up, Ref-Send ON)", 2, 1, True),
    ]

    results = []
    for title, dl, up, ref in scenarios:
        print(f"\n[Running] {title}...")
        res = await run_scenario(title, dl, up, ref, sample)
        print(f"  -> Elapsed: {res['elapsed_seconds']}s | Rate: {res['files_per_minute']} files/min | Speed: {res['overall_mb_s']} MB/s")
        results.append(res)

    # Save baseline.json
    out_dir = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(out_dir, exist_ok=True)
    baseline_payload = {
        "benchmark_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sample_size": args.sample,
        "scenarios": results,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(baseline_payload, f, indent=2)
    print(f"\n[+] Saved baseline benchmark metrics to: {args.output}")

    # Write recommended.env
    rec_env_path = os.path.join(out_dir, "recommended.env")
    recommended_content = """# ==============================================================================
# RECOMMENDED TELEGRAM ORGANIZER CONFIGURATION
# Generated based on benchmark results comparing sequential vs pipelined transfers.
# DO NOT auto-apply: review and copy desired values to your .env file.
# ==============================================================================

# Concurrency & Budgets
DOWNLOAD_CONCURRENCY=2
UPLOAD_CONCURRENCY=1
QUEUE_MAX=4
DOWNLOAD_AHEAD_MAX_GB=2.0
MIN_FREE_DISK_GB=1.0

# Transfer Controls
BATCH_SIZE=25
MAX_RETRIES=4
RETRY_BASE_DELAY=2.0
MAX_BACKOFF_SECONDS=60.0
UPLOAD_DELAY_SECONDS=1.0

# Operational
KEEP_TEMP_ON_FAILURE=false
ENABLE_PARALLEL_PARTS=false
"""
    with open(rec_env_path, "w", encoding="utf-8") as f:
        f.write(recommended_content)
    print(f"[+] Saved recommended configuration to: {rec_env_path}\n")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
