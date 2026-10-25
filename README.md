# Telegram Personal Organizer

Production-grade, headless Telegram organizer built with Python and Telethon. Designed for deployment on **Render** with a persistent disk (`/var/data`), batch round-robin processing, companion web dashboard (`aiohttp`), automated crash recovery, and content protection compliance.

---

## Key Features

- **Batch Round-Robin Processing**: Cycles across multiple Telegram channels, forum topics, and local folders in bounded batches (`BATCH_SIZE`, default 10) oldest-first.
- **Headless Non-Interactive Operation**: Runs 24/7 on servers without interactive prompts (`input()`), using a cryptographically signed `TELEGRAM_SESSION_STRING`.
- **Resumable SQLite Persistence (WAL Mode)**: Saves per-unit message cursors, transfer status (`PENDING`, `uploading`, `UPLOADED`, `FAILED`, `restricted`, `skipped_size`), and file fingerprints in `organizer.db`.
- **Crash Recovery & Deduplication**: Scans recent destination messages for `[src <chat_id>/<msg_id>]` markers on startup, resolving interrupted uploads without duplicates.
- **Content Protection Compliance**: Detects `noforwards` flags and restricted chats; skips without bypassing security policies and records status for manual placement.
- **Interactive Web Dashboard**: Embedded dark-theme web dashboard with live metrics, ring-buffered redacted logs, and controls (`Start`, `Pause`, `Resume`, `Stop`, `Retry Failed`).
- **Render Ready**: Complete `render.yaml` blueprint with persistent disk mount, health check `/healthz`, and secret isolation.

---

## 1. Secrets & Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `TELEGRAM_API_ID` | **Yes** | - | Telegram API ID from [my.telegram.org](https://my.telegram.org) |
| `TELEGRAM_API_HASH` | **Yes** | - | Telegram API Hash from [my.telegram.org](https://my.telegram.org) |
| `TELEGRAM_PHONE` | **Yes** | - | Telegram account phone number (e.g., `+1234567890`) |
| `TELEGRAM_SESSION_STRING` | **Yes (Server)** | - | Telethon StringSession generated via `make_session.py` |
| `DASHBOARD_PASSWORD` | **Yes** | `changeme` | HTTP Basic Auth password for dashboard access |
| `DASHBOARD_USER` | No | `admin` | HTTP Basic Auth username |
| `DEST_GROUP_ID` | **Yes** | `-1003910839448` | Numeric destination supergroup ID |
| `DEST_GROUP_NAME` | No | `Delta 6` | Destination supergroup title |
| `DEST_TOPIC_NAME` | No | `Imported Files` | Default destination topic name |
| `SOURCE_CHATS` | **Yes** | `-1002671181760` | Comma-separated source chat IDs / usernames |
| `BATCH_SIZE` | No | `10` | Messages processed per unit per round |
| `CONCURRENCY` | No | `2` | Maximum concurrent transfers |
| `DRY_RUN` | No | `true` | When `true`, simulates transfers with zero Telegram writes |
| `CREATE_GROUP` | No | `false` | When `false`, halts if destination group is missing |
| `CREATE_TOPICS` | No | `false` | When `false`, halts if destination topic is missing |
| `AUTO_START` | No | `false` | If `true`, worker starts automatically on boot |
| `DATA_DIR` | No | `/var/data` or `./data` | Directory for DB, logs, and temp files |

---

## 2. Generating `TELEGRAM_SESSION_STRING` (Run Once on Mac)

Render runs headlessly and **cannot prompt for login codes or 2FA passwords**. You must generate a `TELEGRAM_SESSION_STRING` on your local computer once:

1. In your local terminal:
   ```bash
   python make_session.py
   ```
2. Enter your `API_ID`, `API_HASH`, and `PHONE` when prompted (or fill in your local `.env`).
3. Enter the login confirmation code sent by Telegram to your device.
4. If 2FA is enabled, enter your 2FA password.
5. The script will output a long string starting with `1BVts...`.
6. Copy this string — this is your `TELEGRAM_SESSION_STRING`.

> [!CAUTION]
> **Never commit your session string or `.env` to Git.**
> Do not run the script locally while Render is running with the same session string to avoid Telegram's `AUTH_KEY_DUPLICATED` error.

---

## 3. Render Deployment Guide

### Step 1: Push Code to GitHub
Ensure `.gitignore` is in place:
```bash
git status
# Confirm .env, *.session, *.db, and data/ are ignored!
git add .
git commit -m "Configure Telegram Organizer for Render"
git push origin main
```

### Step 2: Deploy on Render via Blueprint
1. Open [Render Dashboard](https://dashboard.render.com).
2. Click **New +** -> **Blueprint**.
3. Connect your repository. Render will detect `render.yaml`.
4. In the Blueprint environment setup, fill in the secret variables (`sync: false`):
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `TELEGRAM_PHONE`
   - `TELEGRAM_SESSION_STRING` (pasted from Step 2)
   - `DASHBOARD_PASSWORD` (set a strong password)
5. Click **Apply**. Render will:
   - Provision a `web` service on Python 3.11.
   - Attach a 10 GB persistent disk mounted at `/var/data`.
   - Install dependencies from `requirements.txt`.
   - Start the organizer via `python main.py`.

---

## 4. First-Run Order & Verification

1. **Verify Health Endpoint**:
   Visit `https://<your-render-service>.onrender.com/healthz`.
   It should return:
   ```json
   {"status": "ok", "state": "IDLE"}
   ```
2. **Access Dashboard**:
   Open `https://<your-render-service>.onrender.com` in your browser.
   Log in with:
   - **Username**: `admin`
   - **Password**: Your `DASHBOARD_PASSWORD`
3. **Dry-Run Test**:
   - `DRY_RUN` is `true` by default.
   - Click **Start** in the dashboard.
   - Watch the live log viewer and verify that source chats, topics, and messages are discovered without error.
4. **Live First Batch**:
   - In Render Dashboard -> Environment Variables:
     - Set `DRY_RUN=false`
     - Keep `BATCH_SIZE=10`
   - The service will automatically redeploy.
   - Log into the dashboard and click **Start**.
   - Check Telegram to confirm that the first 10 files appear in the `Imported Files` topic of `Delta 6` with `[src ...]` captions.
5. **Full Run**:
   - Let the worker continue round-robin processing. When all units are caught up, the worker enters the `DONE` state and idles safely.

---

## 5. Recovery & Troubleshooting

### Crash Recovery
- If the Render instance restarts, the worker recovers automatically:
  - SQLite WAL mode prevents database corruption.
  - In-flight items left in `uploading` are checked against the last 100 destination messages for the `[src chat/msg]` marker.
  - Confirmed items are marked `UPLOADED`; unconfirmed items are reset to `PENDING` to prevent duplicates.
  - Cursors resume from the exact last processed message ID.

### Handling `AUTH_REQUIRED`
- If Telegram revokes the session, the worker transitions to `AUTH_REQUIRED` and displays a warning on the dashboard without crash-looping.
- Re-run `python make_session.py` on your Mac, copy the new string, and update `TELEGRAM_SESSION_STRING` in Render.

### Session Revocation
- If your credentials or session are ever exposed:
  1. Open Telegram on your phone.
  2. Go to **Settings** -> **Devices**.
  3. Find the Telethon session and tap **Terminate Session**.

### `AUTH_KEY_DUPLICATED` Error
- Occurs if the same session string is running on two machines simultaneously.
- Make sure you stop any local `main.py` processes before starting the Render service.

### Insufficient Disk Space
- The organizer checks `shutil.disk_usage(TMP_DIR).free` before downloading any file.
- If disk space is low, it marks the item as `skipped_disk` and pauses until disk space is freed.
- Temporary download files are immediately removed after successful upload.

---

## 6. Running Tests

To run the complete test suite locally:
```bash
pytest
```
All 26 tests cover configuration validation, secret redaction, topic routing, cursor tracking, crash recovery, deduplication, content protection, and dashboard security.

