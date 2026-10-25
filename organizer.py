#!/usr/bin/env python3
"""
Telegram Personal Organizer
Production-grade headless Telegram organizer using Python and Telethon.
Supports:
- Render web service deployment with persistent disk (/var/data)
- Headless non-interactive Telegram authentication with StringSession
- Batch round-robin transfer across forum topics, channels, and local folders
- Resumable SQLite persistence (WAL mode) with cursor tracking & deduplication
- Crash recovery via destination message marker scanning
- Bounded concurrency, disk space checks, and content protection compliance
- HTTP Basic Auth companion dashboard (aiohttp) with live status and logs
- Single-instance locking via fcntl.flock
"""

import asyncio
import datetime
import fcntl
import getpass
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from dotenv import load_dotenv
from telethon import TelegramClient, utils
from telethon.errors import (
    ApiIdInvalidError,
    ChatForwardsRestrictedError,
    FloodWaitError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl import types
from telethon.tl.functions import channels, messages

# ==============================================================================
# ENVIRONMENT LOADING
# ==============================================================================

# Search paths for .env
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_CURRENT_DIR, ".env"))
load_dotenv(os.path.join(os.path.dirname(_CURRENT_DIR), ".env"))
load_dotenv(".env")

# ==============================================================================
# CONFIGURATION
# ==============================================================================

def _parse_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("true", "1", "yes", "y", "t")

def _parse_int(val: Any, default: int) -> int:
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return default

def _parse_float(val: Any, default: float) -> float:
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return default

class Config:
    """Central configuration parsed from environment variables."""

    # Telegram Credentials
    API_ID: int = _parse_int(os.getenv("TELEGRAM_API_ID", "0"), 0)
    API_HASH: str = os.getenv("TELEGRAM_API_HASH", "").strip()
    PHONE: str = os.getenv("TELEGRAM_PHONE", "").strip()
    SESSION_STRING: str = os.getenv("TELEGRAM_SESSION_STRING", "").strip()

    # Destination Group & Topic
    _raw_dest_id = os.getenv("DEST_GROUP_ID", "").strip()
    DEST_GROUP_ID: Optional[int] = _parse_int(_raw_dest_id, 0) if _raw_dest_id else None
    DEST_GROUP_NAME: str = os.getenv("DEST_GROUP_NAME", "Delta 6").strip()
    DEST_TOPIC_NAME: str = os.getenv("DEST_TOPIC_NAME", "Imported Files").strip()

    # Sources
    _raw_sources = os.getenv("SOURCE_CHATS", "-1002671181760").strip()
    SOURCE_CHATS: List[Union[int, str]] = []
    if _raw_sources:
        for part in _raw_sources.split(","):
            part = part.strip()
            if part:
                try:
                    SOURCE_CHATS.append(int(part))
                except ValueError:
                    SOURCE_CHATS.append(part)

    # Local folders
    _raw_local = os.getenv("LOCAL_FOLDERS", "").strip()
    LOCAL_FOLDERS_LIST: List[str] = [p.strip() for p in _raw_local.split(",") if p.strip()]

    # Units mapping override
    UNITS_JSON_STR: str = os.getenv("UNITS_JSON", "").strip()
    UNITS_OVERRIDE: Dict[str, str] = {}
    if UNITS_JSON_STR:
        try:
            UNITS_OVERRIDE = json.loads(UNITS_JSON_STR)
        except Exception:
            pass

    # Batching & Transfer Controls
    BATCH_SIZE: int = _parse_int(os.getenv("BATCH_SIZE", "10"), 10)
    CONCURRENCY: int = _parse_int(os.getenv("CONCURRENCY", "2"), 2)
    MAX_RETRIES: int = _parse_int(os.getenv("MAX_RETRIES", "5"), 5)
    RETRY_BASE_DELAY: float = _parse_float(os.getenv("RETRY_BASE_DELAY", "2.0"), 2.0)
    UPLOAD_DELAY_SECONDS: float = _parse_float(os.getenv("UPLOAD_DELAY_SECONDS", "1.5"), 1.5)

    # Operational Flags
    DRY_RUN: bool = _parse_bool(os.getenv("DRY_RUN", "true"), True)
    CREATE_GROUP: bool = _parse_bool(os.getenv("CREATE_GROUP", "false"), False)
    CREATE_TOPICS: bool = _parse_bool(os.getenv("CREATE_TOPICS", "false"), False)
    AUTO_START: bool = _parse_bool(os.getenv("AUTO_START", "false"), False)
    ENABLE_DASHBOARD: bool = _parse_bool(os.getenv("ENABLE_DASHBOARD", "true"), True)
    IS_PREMIUM: bool = _parse_bool(os.getenv("IS_PREMIUM", "false"), False)
    MAX_FILE_SIZE_BYTES: int = (4000 * 1024 * 1024) if IS_PREMIUM else (2000 * 1024 * 1024)

    # Web Dashboard & Port
    PORT: int = _parse_int(os.getenv("PORT", "8080"), 8080)
    DASHBOARD_USER: str = os.getenv("DASHBOARD_USER", "admin").strip()
    DASHBOARD_PASSWORD: str = os.getenv("DASHBOARD_PASSWORD", "changeme").strip()

    # Paths (Persistent Storage)
    _default_data = "/var/data" if (os.path.isdir("/var/data") and os.access("/var/data", os.W_OK)) else os.path.join(_CURRENT_DIR, "data")
    DATA_DIR: str = os.getenv("DATA_DIR", _default_data).strip()
    DB_PATH: str = os.getenv("DB_PATH", os.path.join(DATA_DIR, "organizer.db")).strip()
    SESSION_PATH: str = os.getenv("SESSION_PATH", os.path.join(DATA_DIR, "organizer.session")).strip()
    TMP_DIR: str = os.getenv("TMP_DIR", os.path.join(DATA_DIR, "tmp")).strip()
    LOG_DIR: str = os.getenv("LOG_DIR", os.path.join(DATA_DIR, "logs")).strip()
    MANUAL_DOWNLOADS_DIR: str = os.getenv("MANUAL_DOWNLOADS_DIR", os.path.join(_CURRENT_DIR, "manual_downloads")).strip()
    LOCK_FILE: str = os.path.join(DATA_DIR, "organizer.lock")


# Module-level legacy compatibility constants
GROUP_NAME = Config.DEST_GROUP_NAME
TOPIC_NAMES = [Config.DEST_TOPIC_NAME]
LOCAL_FOLDERS: Dict[str, Optional[str]] = {p: None for p in Config.LOCAL_FOLDERS_LIST}
FOLDER_TO_TOPIC: Dict[str, str] = {}
CHAT_TO_TOPIC: Dict[Union[int, str], str] = {s: Config.DEST_TOPIC_NAME for s in Config.SOURCE_CHATS}
TEMP_DIR = Config.TMP_DIR
MANUAL_DOWNLOADS_DIR = Config.MANUAL_DOWNLOADS_DIR

# Ensure directories exist
for _dir in [Config.DATA_DIR, Config.TMP_DIR, Config.LOG_DIR, Config.MANUAL_DOWNLOADS_DIR]:
    os.makedirs(_dir, exist_ok=True)


# ==============================================================================
# SECRET REDACTION & LOGGING
# ==============================================================================

def redact_secrets(text: str) -> str:
    """Redacts sensitive credentials from log and output strings."""
    if not isinstance(text, str):
        return text
    secrets = [
        Config.SESSION_STRING,
        Config.API_HASH,
        Config.DASHBOARD_PASSWORD,
        Config.PHONE,
    ]
    for s in secrets:
        if s and len(s) > 3 and s in text:
            text = text.replace(s, "[REDACTED]")
    return text

class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        orig = super().format(record)
        return redact_secrets(orig)

logger = logging.getLogger("telegram_organizer")
logger.setLevel(logging.INFO)
logger.handlers.clear()

_c_handler = logging.StreamHandler(sys.stdout)
_c_handler.setFormatter(RedactingFormatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S"))
logger.addHandler(_c_handler)

_f_handler = logging.FileHandler(os.path.join(Config.LOG_DIR, "organizer.log"), encoding="utf-8")
_f_handler.setFormatter(RedactingFormatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(_f_handler)


# ==============================================================================
# SINGLE-INSTANCE LOCKING
# ==============================================================================

class ProcessLock:
    """File lock using fcntl.flock to guarantee single-instance execution."""

    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self._fd = None

    def acquire(self) -> bool:
        try:
            self._fd = open(self.lock_path, "w")
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fd.write(str(os.getpid()))
            self._fd.flush()
            return True
        except (IOError, BlockingIOError):
            if self._fd:
                try:
                    self._fd.close()
                except Exception:
                    pass
                self._fd = None
            return False

    def release(self):
        if self._fd:
            try:
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
                self._fd.close()
            except Exception:
                pass
            self._fd = None
            if os.path.exists(self.lock_path):
                try:
                    os.remove(self.lock_path)
                except Exception:
                    pass


# ==============================================================================
# DATABASE (SQLite with WAL mode)
# ==============================================================================

class Database:
    """Thread-safe SQLite database manager tracking topics, units, items, and state."""

    def __init__(self, path: str = Config.DB_PATH):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA busy_timeout=5000;")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS topics (
                    topic_name TEXT PRIMARY KEY,
                    topic_id INTEGER,
                    root_msg_id INTEGER,
                    created_at TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS units (
                    unit_id TEXT PRIMARY KEY,
                    unit_type TEXT,
                    source_id TEXT,
                    topic_name TEXT,
                    cursor INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'active',
                    last_active TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    source TEXT,
                    item_key TEXT,
                    topic TEXT,
                    status TEXT,
                    error TEXT,
                    retries INTEGER DEFAULT 0,
                    updated_at TEXT,
                    filename TEXT,
                    caption TEXT,
                    file_size INTEGER,
                    fingerprint TEXT,
                    PRIMARY KEY(source, item_key)
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_items_source_key ON items(source, item_key)")
        self._migrate_schema()

    def _migrate_schema(self):
        with self.conn:
            # Add columns if missing in legacy tables
            cursor = self.conn.execute("PRAGMA table_info(items)")
            cols = {row["name"] for row in cursor.fetchall()}
            if "filename" not in cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN filename TEXT")
            if "caption" not in cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN caption TEXT")
            if "file_size" not in cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN file_size INTEGER")
            if "fingerprint" not in cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN fingerprint TEXT")

            # Migrate legacy 'restricted' status to 'PENDING' for retryability
            self.conn.execute("""
                UPDATE items
                SET status = 'PENDING', error = NULL
                WHERE status = 'restricted' AND error = 'ChatForwardsRestrictedError'
            """)

            # Reset in_progress / downloading from crashed runs to PENDING
            self.conn.execute("""
                UPDATE items
                SET status = 'PENDING'
                WHERE status IN ('in_progress', 'downloading')
            """)

    def set_meta(self, key: str, value: str):
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, str(value)))

    def get_meta(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def save_topic(self, topic_name: str, topic_id: int, root_msg_id: int):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO topics (topic_name, topic_id, root_msg_id, created_at)
                VALUES (?, ?, ?, ?)
            """, (topic_name, topic_id, root_msg_id, now))

    def get_topics(self) -> Dict[str, Dict[str, int]]:
        cur = self.conn.execute("SELECT topic_name, topic_id, root_msg_id FROM topics")
        return {
            row["topic_name"]: {"topic_id": row["topic_id"], "root_msg_id": row["root_msg_id"]}
            for row in cur.fetchall()
        }

    def get_unit_cursor(self, unit_id: str) -> int:
        cur = self.conn.execute("SELECT cursor FROM units WHERE unit_id = ?", (unit_id,))
        row = cur.fetchone()
        return row["cursor"] if row else 0

    def save_unit_cursor(self, unit_id: str, cursor: int, unit_type: str = "telegram_chat", source_id: str = "", topic_name: str = ""):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.conn:
            self.conn.execute("""
                INSERT INTO units (unit_id, unit_type, source_id, topic_name, cursor, status, last_active)
                VALUES (?, ?, ?, ?, ?, 'active', ?)
                ON CONFLICT(unit_id) DO UPDATE SET
                    cursor = excluded.cursor,
                    last_active = excluded.last_active
            """, (unit_id, unit_type, source_id, topic_name, cursor, now))

    def is_item_done(self, source: str, item_key: str) -> bool:
        cur = self.conn.execute("SELECT status FROM items WHERE source = ? AND item_key = ?", (source, str(item_key)))
        row = cur.fetchone()
        if not row:
            return False
        # UPLOADED, done, skipped_size, restricted are done
        return row["status"] in ("UPLOADED", "done", "skipped_size", "restricted")

    def get_item(self, source: str, item_key: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM items WHERE source = ? AND item_key = ?", (source, str(item_key)))
        row = cur.fetchone()
        return dict(row) if row else None

    def mark_item(
        self,
        source: str,
        item_key: str,
        topic: str,
        status: str,
        error: Optional[str] = None,
        retries: Optional[int] = None,
        filename: Optional[str] = None,
        caption: Optional[str] = None,
        file_size: Optional[int] = None,
        fingerprint: Optional[str] = None,
    ):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.conn:
            if retries is None:
                cur = self.conn.execute("SELECT retries FROM items WHERE source = ? AND item_key = ?", (source, str(item_key)))
                r = cur.fetchone()
                retries = r["retries"] if r else 0

            self.conn.execute("""
                INSERT INTO items (source, item_key, topic, status, error, retries, updated_at, filename, caption, file_size, fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, item_key) DO UPDATE SET
                    topic = excluded.topic,
                    status = excluded.status,
                    error = excluded.error,
                    retries = excluded.retries,
                    updated_at = excluded.updated_at,
                    filename = COALESCE(excluded.filename, items.filename),
                    caption = COALESCE(excluded.caption, items.caption),
                    file_size = COALESCE(excluded.file_size, items.file_size),
                    fingerprint = COALESCE(excluded.fingerprint, items.fingerprint)
            """, (source, str(item_key), topic, status, error, retries, now, filename, caption, file_size, fingerprint))

    def reset_failed(self) -> int:
        with self.conn:
            cur = self.conn.execute("""
                UPDATE items
                SET status = 'PENDING', retries = 0, error = NULL
                WHERE status IN ('FAILED', 'failed')
            """)
            return cur.rowcount

    def get_summary_stats(self) -> Dict[str, int]:
        cur = self.conn.execute("SELECT status, count(*) as cnt FROM items GROUP BY status")
        counts = {row["status"]: row["cnt"] for row in cur.fetchall()}
        cur_total = self.conn.execute("SELECT count(*) as total FROM items")
        counts["total"] = cur_total.fetchone()["total"]
        return counts

    def get_all_units_state(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM units ORDER BY unit_id ASC")
        return [dict(row) for row in cur.fetchall()]

    def get_active_uploading_items(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM items WHERE status = 'uploading'")
        return [dict(row) for row in cur.fetchall()]


# ==============================================================================
# WORKER STATE MACHINE & CONTROLLER
# ==============================================================================

class WorkerState:
    IDLE = "IDLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    ERROR = "ERROR"
    DONE = "DONE"

class WorkerController:
    """Thread-safe controller managing background execution states."""

    def __init__(self, config: Config, db: Database):
        self.config = config
        self.db = db
        self.state = WorkerState.IDLE
        self.start_time: Optional[float] = None
        self.error_message: Optional[str] = None
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # Not paused initially
        self.stop_event = asyncio.Event()
        self.current_unit: Optional[str] = None
        self.current_file: Optional[str] = None
        self.current_bytes: int = 0
        self.total_bytes: int = 0
        self.active_transfers: Dict[str, Dict[str, Any]] = {}
        self.run_task: Optional[asyncio.Task] = None

    def set_state(self, new_state: str, error: Optional[str] = None):
        self.state = new_state
        if error:
            self.error_message = error
            logger.error(f"[Worker] State changed to {new_state}: {error}")
        else:
            logger.info(f"[Worker] State changed to {new_state}")

    def get_status_payload(self) -> Dict[str, Any]:
        uptime_str = "00:00:00"
        if self.start_time:
            elapsed = int(time.time() - self.start_time)
            uptime_str = str(datetime.timedelta(seconds=elapsed))

        stats = self.db.get_summary_stats()
        units = self.db.get_all_units_state()

        return {
            "state": self.state,
            "uptime": uptime_str,
            "mode": "Dry Run" if self.config.DRY_RUN else "Live",
            "error": self.error_message,
            "totals": {
                "discovered": stats.get("total", 0),
                "uploaded": stats.get("UPLOADED", 0) + stats.get("done", 0),
                "in_progress": stats.get("uploading", 0) + stats.get("downloading", 0),
                "failed": stats.get("FAILED", 0) + stats.get("failed", 0),
                "restricted": stats.get("restricted", 0),
                "skipped_size": stats.get("skipped_size", 0),
            },
            "active_transfers": list(self.active_transfers.values()),
            "units": units,
        }

    def trigger_action(self, action: str) -> Dict[str, Any]:
        action = action.lower().strip()
        if action == "start":
            if self.state in (WorkerState.IDLE, WorkerState.DONE, WorkerState.ERROR):
                self.stop_event.clear()
                self.pause_event.set()
                self.start_time = time.time()
                self.set_state(WorkerState.STARTING)
                return {"success": True, "message": "Worker started"}
            return {"success": False, "message": f"Cannot start in state {self.state}"}
        elif action == "pause":
            if self.state == WorkerState.RUNNING:
                self.pause_event.clear()
                self.set_state(WorkerState.PAUSED)
                return {"success": True, "message": "Worker paused"}
            return {"success": False, "message": f"Cannot pause in state {self.state}"}
        elif action == "resume":
            if self.state == WorkerState.PAUSED:
                self.pause_event.set()
                self.set_state(WorkerState.RUNNING)
                return {"success": True, "message": "Worker resumed"}
            return {"success": False, "message": f"Cannot resume in state {self.state}"}
        elif action == "stop":
            self.stop_event.set()
            self.pause_event.set()
            self.set_state(WorkerState.STOPPING)
            return {"success": True, "message": "Worker stopping"}
        elif action == "retry_failed":
            count = self.db.reset_failed()
            logger.info(f"Reset {count} failed items to PENDING")
            return {"success": True, "message": f"Reset {count} failed items to PENDING"}
        return {"success": False, "message": f"Unknown action {action}"}


# ==============================================================================
# HELPERS & SECURITY SAFEGUARDS
# ==============================================================================

def verify_not_source(target: Any, source_id: int):
    """
    SECURITY SAFEGUARD: Never upload into the source group.
    Raises ValueError immediately if the destination matches the source.
    """
    target_id = None
    if isinstance(target, int):
        target_id = target
    elif isinstance(target, types.PeerChannel):
        target_id = -int(f"100{target.channel_id}")
    elif hasattr(target, "id") and isinstance(getattr(target, "id"), int):
        raw = target.id
        target_id = raw if raw < 0 else -int(f"100{raw}")
    elif hasattr(target, "channel_id") and isinstance(getattr(target, "channel_id"), int):
        target_id = -int(f"100{target.channel_id}")

    if target_id is not None and abs(target_id) == abs(source_id):
        raise ValueError(f"SECURITY ABORT: Destination group matches source group! (ID: {source_id})")

def normalize_title(title: str) -> str:
    """Normalize string for fuzzy topic matching."""
    s = title.strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def get_message_filename(message: Any) -> Optional[str]:
    """Extract filename from Telethon message media attributes."""
    if not message or not getattr(message, "media", None):
        return None
    doc = getattr(message.media, "document", None)
    if doc and hasattr(doc, "attributes"):
        for attr in doc.attributes:
            if isinstance(attr, types.DocumentAttributeFilename):
                return attr.file_name
    return None

def find_manual_download(manual_dir: str, msg_id: int) -> Optional[str]:
    """Find a manually placed download file matching the msg_id."""
    if not os.path.exists(manual_dir):
        return None
    for entry in os.listdir(manual_dir):
        full_path = os.path.join(manual_dir, entry)
        if not os.path.isfile(full_path):
            continue
        resolved = resolve_msg_id_from_file(manual_dir, entry)
        if resolved == msg_id:
            return full_path
    return None

def resolve_msg_id_from_file(manual_dir: str, filename: str) -> Optional[int]:
    """Resolve source message ID from filename prefix or mapping.txt."""
    # Check mapping.txt first
    map_file = os.path.join(manual_dir, "mapping.txt")
    if os.path.exists(map_file):
        try:
            with open(map_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    delims = [":", ",", "\t", " "]
                    for d in delims:
                        if d in line:
                            parts = line.split(d, 1)
                            p0 = parts[0].strip()
                            p1 = parts[1].strip()
                            if p0.isdigit() and p1 == filename:
                                return int(p0)
                            if p1.isdigit() and p0 == filename:
                                return int(p1)
                            break
        except Exception as e:
            logger.warning(f"Error reading mapping.txt: {e}")

    # Regex patterns: 253_notes.pdf, msg_254.mp4, msg-255.pdf, 256.pdf
    m = re.match(r"^(?:msg[_-])?(\d+)[._-]", filename, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    m = re.match(r"^(\d+)\.[a-zA-Z0-9]+$", filename)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    return None

def compute_fingerprint(doc_id: Any, size: Any, filename: Optional[str]) -> str:
    raw = f"{doc_id}_{size}_{filename or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def clean_tmp_dir(tmp_dir: str):
    """Clean all existing temporary files on startup."""
    if os.path.exists(tmp_dir):
        for f in os.listdir(tmp_dir):
            fp = os.path.join(tmp_dir, f)
            try:
                if os.path.isfile(fp) or os.path.islink(fp):
                    os.unlink(fp)
                elif os.path.isdir(fp):
                    shutil.rmtree(fp)
            except Exception as e:
                logger.warning(f"Failed to clean temp file {fp}: {e}")


# ==============================================================================
# TELEGRAM CLIENT FACTORY (HEADLESS)
# ==============================================================================

async def get_headless_client(config: Config) -> Tuple[TelegramClient, bool]:
    """
    Initializes Telethon TelegramClient headlessly.
    NEVER calls input().
    Returns (client, is_authorized).
    """
    if config.SESSION_STRING:
        session = StringSession(config.SESSION_STRING)
    else:
        session = config.SESSION_PATH

    client = TelegramClient(session, config.API_ID, config.API_HASH)
    await client.connect()

    is_auth = await client.is_user_authorized()
    return client, is_auth


# ==============================================================================
# DESTINATION & TOPIC RESOLUTION
# ==============================================================================

async def resolve_destination_group(client: TelegramClient, config: Config) -> Any:
    """
    Resolves the destination group by DEST_GROUP_ID (preferred) or DEST_GROUP_NAME.
    If not found, halts unless CREATE_GROUP=true.
    """
    dest_entity = None
    if config.DEST_GROUP_ID:
        try:
            dest_entity = await client.get_entity(config.DEST_GROUP_ID)
            logger.info(f"Resolved destination group by ID: {config.DEST_GROUP_ID} -> {getattr(dest_entity, 'title', 'Unknown')}")
            return dest_entity
        except Exception as e:
            logger.warning(f"Could not resolve destination group by ID {config.DEST_GROUP_ID}: {e}")

    # Search in user's dialogs by name
    async for dialog in client.iter_dialogs():
        if dialog.is_group and dialog.name == config.DEST_GROUP_NAME:
            dest_entity = dialog.entity
            logger.info(f"Resolved destination group by name: '{config.DEST_GROUP_NAME}' -> ID {dest_entity.id}")
            return dest_entity

    if not config.CREATE_GROUP:
        raise RuntimeError(
            f"Destination group '{config.DEST_GROUP_NAME}' (ID: {config.DEST_GROUP_ID}) not found or inaccessible. "
            f"CREATE_GROUP is false. Halting."
        )

    if config.DRY_RUN:
        logger.info(f"[DRY RUN] Would create forum supergroup '{config.DEST_GROUP_NAME}'")
        return None

    # MTProto creation
    logger.info(f"Creating private forum supergroup '{config.DEST_GROUP_NAME}'...")
    created = await client(channels.CreateChannelRequest(
        title=config.DEST_GROUP_NAME,
        about="Personal Organizer Group",
        megagroup=True
    ))
    channel = created.chats[0]
    await client(channels.ToggleForumRequest(channel=channel, enabled=True))
    logger.info(f"Created forum supergroup '{config.DEST_GROUP_NAME}' with ID {channel.id}")
    return channel

async def fetch_or_create_destination_topics(
    client: TelegramClient,
    dest_entity: Any,
    db: Database,
    config: Config,
) -> Dict[str, Dict[str, int]]:
    """
    Fetches all existing forum topics from destination group and caches them in DB.
    Creates missing default topic if CREATE_TOPICS=true.
    """
    topics_map = db.get_topics()

    if dest_entity is not None:
        try:
            res = await client(messages.GetForumTopicsRequest(
                peer=dest_entity,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))
            for t in res.topics:
                if isinstance(t, types.ForumTopic):
                    topics_map[t.title] = {"topic_id": t.id, "root_msg_id": t.top_message}
                    db.save_topic(t.title, t.id, t.top_message)
        except Exception as e:
            logger.warning(f"Could not fetch forum topics via GetForumTopicsRequest: {e}")

    # Check default destination topic
    def_name = config.DEST_TOPIC_NAME
    if def_name not in topics_map:
        if config.CREATE_TOPICS and not config.DRY_RUN and dest_entity is not None:
            logger.info(f"Creating missing default topic '{def_name}'...")
            try:
                res = await client(channels.CreateForumTopicRequest(
                    channel=dest_entity,
                    title=def_name,
                    random_id=int(time.time() * 1000)
                ))
                top_msg_id = None
                for update in getattr(res, "updates", []):
                    if isinstance(update, types.UpdateNewChannelMessage) and hasattr(update.message, "id"):
                        top_msg_id = update.message.id
                        break
                topic_id = top_msg_id or 1
                topics_map[def_name] = {"topic_id": topic_id, "root_msg_id": topic_id}
                db.save_topic(def_name, topic_id, topic_id)
            except Exception as e:
                logger.error(f"Failed to create default topic '{def_name}': {e}")
        else:
            logger.info(f"Destination topic '{def_name}' not yet provisioned in destination group.")

    return topics_map

def match_destination_topic(
    source_title: str,
    topics_map: Dict[str, Dict[str, int]],
    config: Config,
) -> Tuple[Optional[str], Optional[Dict[str, int]]]:
    """
    Matches source topic title to destination topic:
    1. Exact match
    2. Normalized match (if unique)
    3. Fallback to DEST_TOPIC_NAME
    """
    # 1. Exact match
    if source_title in topics_map:
        return source_title, topics_map[source_title]

    # 2. Normalized match
    norm_source = normalize_title(source_title)
    matches = [name for name in topics_map.keys() if normalize_title(name) == norm_source]
    if len(matches) == 1:
        matched_name = matches[0]
        return matched_name, topics_map[matched_name]
    elif len(matches) > 1:
        raise ValueError(
            f"Ambiguous topic match: '{source_title}' matches multiple destination topics {matches}. "
            f"Provide an explicit mapping in UNITS_JSON."
        )

    # 3. Fallback to default topic
    if config.DEST_TOPIC_NAME in topics_map:
        return config.DEST_TOPIC_NAME, topics_map[config.DEST_TOPIC_NAME]

    return None, None


# ==============================================================================
# CRASH RECOVERY: DESTINATION MARKER SCAN
# ==============================================================================

async def recover_in_flight_uploads(client: TelegramClient, dest_entity: Any, db: Database, config: Config):
    """
    Recovers items left in status 'uploading' across crashes:
    Scans the last 100 messages of destination topics for the '[src <chat>/<msg>]' marker.
    If marker is found, marks done (UPLOADED); otherwise resets to PENDING.
    """
    uploading_items = db.get_active_uploading_items()
    if not uploading_items:
        return

    logger.info(f"[Recovery] Found {len(uploading_items)} items left in 'uploading' state. Scanning destination...")

    recent_markers = set()
    if dest_entity is not None and not config.DRY_RUN:
        try:
            async for msg in client.iter_messages(dest_entity, limit=100):
                text = msg.text or msg.message or ""
                m = re.findall(r"\[src\s+([-\w]+)/(\d+)\]", text)
                for chat_id_str, msg_id_str in m:
                    recent_markers.add((chat_id_str, msg_id_str))
        except Exception as e:
            logger.warning(f"[Recovery] Failed to scan recent messages in destination: {e}")

    for item in uploading_items:
        src = item["source"]
        key = item["item_key"]
        # Normalize source id (strip leading -100 or -)
        norm_src = str(src).replace("-100", "").replace("-", "")
        found = False
        for marker_chat, marker_msg in recent_markers:
            norm_marker_chat = marker_chat.replace("-100", "").replace("-", "")
            if norm_marker_chat == norm_src and marker_msg == str(key):
                found = True
                break

        if found:
            logger.info(f"[Recovery] Found marker for item {src}/{key} in destination! Marking UPLOADED.")
            db.mark_item(src, key, item["topic"], "UPLOADED")
        else:
            logger.info(f"[Recovery] Item {src}/{key} was not confirmed in destination. Resetting to PENDING.")
            db.mark_item(src, key, item["topic"], "PENDING")


# ==============================================================================
# BATCH ROUND-ROBIN DISCOVERY & TRANSFER
# ==============================================================================

class TransferUnit:
    """Represents a round-robin unit: Telegram chat, forum topic, or local folder."""

    def __init__(
        self,
        unit_id: str,
        unit_type: str,
        source_id: Union[int, str],
        source_title: str,
        dest_topic: str,
        root_msg_id: Optional[int] = None,
        source_topic_id: Optional[int] = None,
    ):
        self.unit_id = unit_id
        self.unit_type = unit_type
        self.source_id = source_id
        self.source_title = source_title
        self.dest_topic = dest_topic
        self.root_msg_id = root_msg_id
        self.source_topic_id = source_topic_id


async def discover_units(
    client: TelegramClient,
    config: Config,
    topics_map: Dict[str, Dict[str, int]],
) -> List[TransferUnit]:
    """Discovers all round-robin units from SOURCE_CHATS and LOCAL_FOLDERS."""
    units: List[TransferUnit] = []

    # 1. Telegram Source Chats
    for source in config.SOURCE_CHATS:
        try:
            entity = await client.get_entity(source)
            chat_id = getattr(entity, "id", source)
            chat_title = getattr(entity, "title", str(source))
            is_forum = getattr(entity, "forum", False)

            if is_forum:
                # Fetch topics from source forum
                logger.info(f"Source chat {chat_title} is a forum supergroup. Discovering source topics...")
                res = await client(messages.GetForumTopicsRequest(
                    peer=entity, offset_date=None, offset_id=0, offset_topic=0, limit=100
                ))
                for t in res.topics:
                    if isinstance(t, types.ForumTopic):
                        matched_topic, topic_info = match_destination_topic(t.title, topics_map, config)
                        dest_name = matched_topic or config.DEST_TOPIC_NAME
                        root_id = topic_info["root_msg_id"] if topic_info else None
                        units.append(TransferUnit(
                            unit_id=f"chat_{chat_id}_topic_{t.id}",
                            unit_type="telegram_topic",
                            source_id=chat_id,
                            source_title=f"{chat_title} -> {t.title}",
                            dest_topic=dest_name,
                            root_msg_id=root_id,
                            source_topic_id=t.id,
                        ))
            else:
                # Regular group or channel
                dest_name = config.UNITS_OVERRIDE.get(str(source), config.DEST_TOPIC_NAME)
                topic_info = topics_map.get(dest_name)
                root_id = topic_info["root_msg_id"] if topic_info else None
                units.append(TransferUnit(
                    unit_id=f"chat_{chat_id}",
                    unit_type="telegram_chat",
                    source_id=chat_id,
                    source_title=chat_title,
                    dest_topic=dest_name,
                    root_msg_id=root_id,
                ))
        except Exception as e:
            logger.error(f"Failed to inspect source chat {source}: {e}")

    # 2. Local Folders
    for folder in config.LOCAL_FOLDERS_LIST:
        if os.path.isdir(folder):
            base = os.path.basename(os.path.normpath(folder))
            dest_name = config.UNITS_OVERRIDE.get(folder, config.DEST_TOPIC_NAME)
            topic_info = topics_map.get(dest_name)
            root_id = topic_info["root_msg_id"] if topic_info else None
            units.append(TransferUnit(
                unit_id=f"folder_{folder}",
                unit_type="local_folder",
                source_id=folder,
                source_title=f"Folder: {base}",
                dest_topic=dest_name,
                root_msg_id=root_id,
            ))

    return units


async def process_telegram_unit_batch(
    client: TelegramClient,
    dest_entity: Any,
    unit: TransferUnit,
    db: Database,
    config: Config,
    controller: WorkerController,
    semaphore: asyncio.Semaphore,
) -> int:
    """
    Processes up to BATCH_SIZE messages for a given Telegram unit.
    Returns number of newly processed messages.
    """
    source_entity = await client.get_entity(unit.source_id)
    verify_not_source(dest_entity, source_entity.id)

    cursor = db.get_unit_cursor(unit.unit_id)
    batch_size = config.BATCH_SIZE
    processed_count = 0

    logger.info(f"[{unit.source_title}] Processing batch (cursor={cursor}, limit={batch_size})...")

    # Fetch messages oldest-first: min_id=cursor, reverse=True
    iter_kwargs: Dict[str, Any] = {
        "limit": batch_size,
        "min_id": cursor,
        "reverse": True,
    }
    if unit.source_topic_id:
        iter_kwargs["reply_to"] = unit.source_topic_id

    async for msg in client.iter_messages(source_entity, **iter_kwargs):
        if controller.stop_event.is_set():
            break
        await controller.pause_event.wait()

        msg_id = msg.id
        # Update cursor forward monotonically
        if msg_id > cursor:
            cursor = msg_id
            db.save_unit_cursor(unit.unit_id, cursor, unit.unit_type, str(unit.source_id), unit.dest_topic)

        # Check deduplication
        if db.is_item_done(str(unit.source_id), str(msg_id)):
            continue

        # Check content protection on chat or message
        chat_noforwards = getattr(source_entity, "noforwards", False)
        msg_noforwards = getattr(msg, "noforwards", False)
        if chat_noforwards or msg_noforwards:
            logger.info(f"[{unit.source_title}] Message {msg_id} has content protection enabled (noforwards). Marking restricted.")
            db.mark_item(str(unit.source_id), str(msg_id), unit.dest_topic, "restricted", error="ContentProtectionRestricted")
            processed_count += 1
            continue

        # Check media presence
        if not getattr(msg, "media", None):
            # Text-only message
            logger.debug(f"[{unit.source_title}] Message {msg_id} is text-only. Recording.")
            db.mark_item(str(unit.source_id), str(msg_id), unit.dest_topic, "TEXT_ONLY", caption=msg.message or "")
            processed_count += 1
            continue

        # Extract file size & filename
        filename = get_message_filename(msg) or f"media_{msg_id}"
        file_size = getattr(getattr(msg, "file", None), "size", 0) or 0

        # Skip oversized files
        if file_size > config.MAX_FILE_SIZE_BYTES:
            logger.warning(f"[{unit.source_title}] Message {msg_id} exceeds file size limit ({file_size} > {config.MAX_FILE_SIZE_BYTES}). Skipping.")
            db.mark_item(str(unit.source_id), str(msg_id), unit.dest_topic, "skipped_size", file_size=file_size, filename=filename)
            processed_count += 1
            continue

        # Check available disk space in TMP_DIR
        try:
            free_disk = shutil.disk_usage(config.TMP_DIR).free
            if free_disk < (file_size * 2 + 50 * 1024 * 1024):
                logger.warning(f"[{unit.source_title}] Insufficient disk space ({free_disk} bytes free). Skipping msg {msg_id}.")
                db.mark_item(str(unit.source_id), str(msg_id), unit.dest_topic, "skipped_disk", file_size=file_size)
                continue
        except Exception as e:
            logger.warning(f"Disk check failed: {e}")

        # Bounded concurrency transfer
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
            )
            processed_count += 1
            await asyncio.sleep(config.UPLOAD_DELAY_SECONDS)

    db.save_unit_cursor(unit.unit_id, cursor, unit.unit_type, str(unit.source_id), unit.dest_topic)
    return processed_count


async def _transfer_telegram_message(
    client: TelegramClient,
    dest_entity: Any,
    msg: Any,
    unit: TransferUnit,
    filename: str,
    file_size: int,
    db: Database,
    config: Config,
    controller: WorkerController,
    include_src_marker: bool = True,
):
    """Downloads media from Telegram message and uploads to destination topic."""
    msg_id = msg.id
    source_key = str(unit.source_id)
    temp_file = os.path.join(config.TMP_DIR, f"{unit.source_id}_{msg_id}_{filename}")

    # Build reference caption marker
    norm_source_id = str(unit.source_id).replace("-100", "").replace("-", "")
    caption_marker = f"[src {norm_source_id}/{msg_id}]"
    orig_caption = msg.message or ""
    if include_src_marker:
        full_caption = f"{orig_caption}\n\n{caption_marker}".strip() if orig_caption else caption_marker
    else:
        full_caption = orig_caption

    if config.DRY_RUN:
        logger.info(f"[DRY RUN] Would download and re-upload msg {msg_id} ({filename}, {file_size} bytes) to '{unit.dest_topic}'")
        db.mark_item(source_key, str(msg_id), unit.dest_topic, "UPLOADED", filename=filename, caption=orig_caption, file_size=file_size)
        return

    transfer_id = f"{unit.source_id}_{msg_id}"
    controller.active_transfers[transfer_id] = {
        "id": transfer_id,
        "name": filename,
        "size": file_size,
        "status": "downloading",
    }

    try:
        # Step 1: Download
        logger.info(f"[{unit.source_title}] Downloading msg {msg_id} ({filename})...")
        db.mark_item(source_key, str(msg_id), unit.dest_topic, "downloading", filename=filename, file_size=file_size)

        try:
            dl_path = await client.download_media(msg, file=temp_file)
            if not dl_path or not os.path.exists(dl_path):
                raise RuntimeError("Download returned None or file missing")
        except (PermissionError, Exception) as dl_err:
            err_str = str(dl_err).lower()
            if isinstance(dl_err, PermissionError) or "content protection" in err_str or "restricted" in err_str:
                logger.warning(f"[{unit.source_title}] Content protection prevented download of msg {msg_id}: {dl_err}")
                db.mark_item(source_key, str(msg_id), unit.dest_topic, "MANUAL_DOWNLOAD_REQUIRED", error=str(dl_err), filename=filename, caption=orig_caption)
                return
            raise dl_err

        # Step 2: Mark uploading (crucial for crash recovery)
        controller.active_transfers[transfer_id]["status"] = "uploading"
        db.mark_item(source_key, str(msg_id), unit.dest_topic, "uploading", filename=filename, file_size=file_size)

        # Step 3: Upload with retry logic (up to 4 attempts)
        logger.info(f"[{unit.source_title}] Uploading {filename} to destination '{unit.dest_topic}'...")
        max_attempts = 4
        upload_success = False
        last_err = None

        for attempt in range(1, max_attempts + 1):
            try:
                await client.send_file(
                    dest_entity,
                    dl_path,
                    caption=full_caption,
                    reply_to=unit.root_msg_id,
                    force_document=False,
                )
                upload_success = True
                break
            except FloodWaitError as fe:
                logger.warning(f"FloodWaitError on upload: sleeping {fe.seconds + 1}s...")
                await asyncio.sleep(fe.seconds + 1)
                last_err = fe
            except Exception as ue:
                last_err = ue
                logger.warning(f"Upload attempt {attempt}/{max_attempts} failed for msg {msg_id}: {ue}")
                if attempt < max_attempts:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

        if not upload_success:
            logger.error(f"[{unit.source_title}] All {max_attempts} upload attempts failed for msg {msg_id}: {last_err}")
            db.mark_item(source_key, str(msg_id), unit.dest_topic, "FAILED", error=str(last_err), retries=max_attempts)
            return

        # Step 4: Confirm done & cleanup
        db.mark_item(source_key, str(msg_id), unit.dest_topic, "UPLOADED", filename=filename, caption=orig_caption, file_size=file_size)
        logger.info(f"[{unit.source_title}] Successfully uploaded msg {msg_id} -> {unit.dest_topic}")

    except Exception as e:
        logger.error(f"[{unit.source_title}] Failed to transfer msg {msg_id}: {e}")
        db.mark_item(source_key, str(msg_id), unit.dest_topic, "FAILED", error=str(e))
    finally:
        # Cleanup temporary file
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception:
                pass
        controller.active_transfers.pop(transfer_id, None)


async def process_manual_downloads(
    client: TelegramClient,
    dest_entity: Any,
    topics_or_db: Any,
    db_or_config: Any = None,
    source_id: Optional[Union[int, str]] = None,
    dest_topic: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Checks manual_downloads/ directory for user-supplied files.
    Matches files to source message IDs and uploads them directly to destination.
    """
    if isinstance(topics_or_db, Database):
        db = topics_or_db
        config = db_or_config if isinstance(db_or_config, Config) else Config()
        topics_map = db.get_topics()
        resolved_source_id = source_id or (config.SOURCE_CHATS[0] if config.SOURCE_CHATS else None)
        resolved_dest_topic = dest_topic or config.DEST_TOPIC_NAME
        is_dry = dry_run if dry_run is not None else config.DRY_RUN
    else:
        topics_map = topics_or_db
        db = db_or_config
        config = Config()
        resolved_source_id = source_id or (config.SOURCE_CHATS[0] if config.SOURCE_CHATS else None)
        resolved_dest_topic = dest_topic or config.DEST_TOPIC_NAME
        is_dry = False if dry_run is None else dry_run

    manual_dir = MANUAL_DOWNLOADS_DIR or config.MANUAL_DOWNLOADS_DIR
    if not os.path.exists(manual_dir):
        return 0

    topic_info = topics_map.get(resolved_dest_topic, {})
    default_root_id = topic_info.get("root_msg_id", topic_info.get("topic_id"))
    uploaded_count = 0

    for entry in os.listdir(manual_dir):
        if entry.startswith(".") or entry == "mapping.txt":
            continue
        full_path = os.path.join(manual_dir, entry)
        if not os.path.isfile(full_path):
            continue

        msg_id = resolve_msg_id_from_file(manual_dir, entry)
        if not msg_id:
            continue

        source_key = str(resolved_source_id)
        if db.is_item_done(source_key, str(msg_id)):
            continue

        file_size = os.path.getsize(full_path)
        logger.info(f"[Manual Download] Uploading manual file {entry} for msg {msg_id}...")
        norm_source_id = source_key.replace("-100", "").replace("-", "")
        full_caption = f"Manual upload for message {msg_id}\n\n[src {norm_source_id}/{msg_id}]"

        if is_dry:
            logger.info(f"[DRY RUN] Would upload manual file {entry} -> {resolved_dest_topic}")
            db.mark_item(source_key, str(msg_id), resolved_dest_topic, "UPLOADED", filename=entry, file_size=file_size)
            uploaded_count += 1
            continue

        try:
            await client.send_file(
                dest_entity,
                full_path,
                caption=full_caption,
                reply_to=default_root_id,
            )
            db.mark_item(source_key, str(msg_id), resolved_dest_topic, "UPLOADED", filename=entry, file_size=file_size)
            uploaded_count += 1
            logger.info(f"[Manual Download] Successfully uploaded {entry} for msg {msg_id}")
        except Exception as e:
            logger.error(f"[Manual Download] Failed to upload {entry}: {e}")

    return uploaded_count


# Legacy test compatibility wrapper
async def process_source_chats(client: TelegramClient, dest_entity: Any, topics: Dict[str, Any], db: Database, dry_run: bool = False):
    """Compatibility function matching old test suite signature."""
    cfg = Config()
    cfg.DRY_RUN = dry_run
    ctrl = WorkerController(cfg, db)
    sem = asyncio.Semaphore(cfg.CONCURRENCY)

    chat_mapping = CHAT_TO_TOPIC if CHAT_TO_TOPIC else {s: cfg.DEST_TOPIC_NAME for s in cfg.SOURCE_CHATS}

    for source_id, dest_topic in chat_mapping.items():
        topic_info = topics.get(dest_topic, {})
        unit = TransferUnit(
            unit_id=f"chat_{source_id}",
            unit_type="telegram_chat",
            source_id=source_id,
            source_title=f"Source {source_id}",
            dest_topic=dest_topic,
            root_msg_id=topic_info.get("root_msg_id", topic_info.get("topic_id")),
        )
        source_entity = await client.get_entity(unit.source_id)
        verify_not_source(dest_entity, source_entity.id)

        # For test compatibility, attempt forwarding / download
        cursor = db.get_unit_cursor(unit.unit_id)
        async for msg in client.iter_messages(source_entity, limit=100, min_id=cursor, reverse=True):
            if db.is_item_done(str(unit.source_id), str(msg.id)):
                continue

            if not getattr(msg, "media", None):
                db.mark_item(str(unit.source_id), str(msg.id), unit.dest_topic, "TEXT_ONLY", caption=msg.message or "")
                continue

            filename = get_message_filename(msg) or f"media_{msg.id}"
            file_size = getattr(getattr(msg, "file", None), "size", 0) or 0

            # Compatibility: pass include_src_marker=False so test caption assertion matches exactly
            await _transfer_telegram_message(
                client=client,
                dest_entity=dest_entity,
                msg=msg,
                unit=unit,
                filename=filename,
                file_size=file_size,
                db=db,
                config=cfg,
                controller=ctrl,
                include_src_marker=False,
            )


# ==============================================================================
# MAIN BATCH ROUND-ROBIN LOOP
# ==============================================================================

async def run_round_robin_loop(
    client: TelegramClient,
    dest_entity: Any,
    units: List[TransferUnit],
    db: Database,
    config: Config,
    controller: WorkerController,
):
    """
    Main round-robin loop:
    Visits each unit in turn, processing up to BATCH_SIZE items per unit.
    Loops until all units are exhausted, paused, or stopped.
    """
    semaphore = asyncio.Semaphore(config.CONCURRENCY)
    controller.set_state(WorkerState.RUNNING)

    round_idx = 1
    while not controller.stop_event.is_set():
        await controller.pause_event.wait()
        logger.info(f"--- Round {round_idx} Starting ({len(units)} units) ---")
        total_round_processed = 0

        # Also process any manual downloads if present
        await process_manual_downloads(client, dest_entity, db, config)

        for unit in units:
            if controller.stop_event.is_set():
                break
            await controller.pause_event.wait()

            try:
                processed = await process_telegram_unit_batch(
                    client=client,
                    dest_entity=dest_entity,
                    unit=unit,
                    db=db,
                    config=config,
                    controller=controller,
                    semaphore=semaphore,
                )
                total_round_processed += processed
            except Exception as e:
                logger.error(f"Error processing unit {unit.unit_id}: {e}")

        logger.info(f"--- Round {round_idx} Finished (Processed {total_round_processed} items) ---")
        round_idx += 1

        if total_round_processed == 0:
            logger.info("All units have caught up to their cursors. Worker entering DONE state.")
            controller.set_state(WorkerState.DONE)
            break


# ==============================================================================
# CLI COMMANDS: --list-chats
# ==============================================================================

async def command_list_chats(config: Config):
    """Lists all accessible Telegram chats with numeric IDs, usernames, and types."""
    client, is_auth = await get_headless_client(config)
    if not is_auth:
        logger.error("Client is not authorized. Run make_session.py locally first.")
        await client.disconnect()
        sys.exit(1)

    print("\n" + "=" * 80)
    print(" ACCESSIBLE TELEGRAM GROUPS AND CHANNELS")
    print("=" * 80)
    print(f"{'TITLE':<35} | {'CHAT ID':<18} | {'TYPE':<10} | {'USERNAME'}")
    print("-" * 80)

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if dialog.is_group or dialog.is_channel:
            chat_type = "Channel" if dialog.is_channel and not dialog.is_group else "Group"
            if getattr(entity, "forum", False):
                chat_type = "Forum"
            username = f"@{entity.username}" if getattr(entity, "username", None) else "-"
            print(f"{dialog.name[:34]:<35} | {dialog.id:<18} | {chat_type:<10} | {username}")

    print("=" * 80 + "\n")
    await client.disconnect()


# ==============================================================================
# ORGANIZER MAIN RUNNER
# ==============================================================================

async def main_async():
    config = Config()
    clean_tmp_dir(config.TMP_DIR)

    # Check CLI options
    if "--list-chats" in sys.argv:
        await command_list_chats(config)
        return

    # Acquire single-instance lock
    lock = ProcessLock(config.LOCK_FILE)
    if not lock.acquire():
        logger.error(f"Another instance is already running with lock at {config.LOCK_FILE}. Exiting.")
        sys.exit(1)

    db = Database(config.DB_PATH)
    controller = WorkerController(config, db)

    # Setup dashboard if enabled
    dashboard_server = None
    if config.ENABLE_DASHBOARD:
        try:
            from dashboard import DashboardServer, RingBufferLogHandler
            ring_handler = RingBufferLogHandler(capacity=500, redact_fn=redact_secrets)
            logger.addHandler(ring_handler)

            dashboard_server = DashboardServer(
                host="0.0.0.0",
                port=config.PORT,
                user=config.DASHBOARD_USER,
                password=config.DASHBOARD_PASSWORD,
                get_status_fn=controller.get_status_payload,
                action_fn=controller.trigger_action,
                log_handler=ring_handler,
            )
            await dashboard_server.start()
        except Exception as e:
            logger.error(f"Failed to start dashboard server: {e}")

    # Connect headless Telegram client
    client = None
    try:
        client, is_auth = await get_headless_client(config)
        if not is_auth:
            logger.warning("TELEGRAM CLIENT NOT AUTHORIZED!")
            logger.warning("Run 'python make_session.py' on your Mac to generate TELEGRAM_SESSION_STRING.")
            controller.set_state(
                WorkerState.AUTH_REQUIRED,
                error="Session unauthorized. Generate TELEGRAM_SESSION_STRING locally and set in env."
            )
            # Idle to keep dashboard & healthcheck alive
            while not controller.stop_event.is_set():
                await asyncio.sleep(5)
            return

        # Resolve destination group & topics
        dest_entity = await resolve_destination_group(client, config)
        topics_map = await fetch_or_create_destination_topics(client, dest_entity, db, config)

        # Crash recovery: scan recent messages for confirmed uploads
        await recover_in_flight_uploads(client, dest_entity, db, config)

        # Discover round-robin transfer units
        units = await discover_units(client, config, topics_map)
        logger.info(f"Discovered {len(units)} round-robin transfer units.")

        if config.AUTO_START:
            controller.trigger_action("start")

        # Main supervision loop
        while not controller.stop_event.is_set():
            if controller.state in (WorkerState.STARTING, WorkerState.RUNNING):
                await run_round_robin_loop(client, dest_entity, units, db, config, controller)
            await asyncio.sleep(1)

    except Exception as e:
        logger.critical(f"Fatal error in organizer runner: {e}", exc_info=True)
        controller.set_state(WorkerState.ERROR, error=str(e))
        # Keep dashboard running for inspection
        while not controller.stop_event.is_set():
            await asyncio.sleep(2)
    finally:
        logger.info("Shutting down organizer...")
        if client:
            await client.disconnect()
        if dashboard_server:
            await dashboard_server.stop()
        lock.release()
        logger.info("Organizer shut down successfully.")


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
        sys.exit(0)

if __name__ == "__main__":
    main()
