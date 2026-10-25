#!/usr/bin/env python3
"""
Dashboard for Telegram Personal Organizer
aiohttp web server providing:
- HTTP Basic Auth protected dashboard (DASHBOARD_USER / DASHBOARD_PASSWORD)
- Public /healthz endpoint for Render health checks
- Real-time status (/api/status), control actions (/api/action/*), and live logs (/api/logs)
- In-memory ring buffer log handler with secret redaction
"""

import base64
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional
from aiohttp import web

# ==============================================================================
# LOG RING BUFFER & REDACTION
# ==============================================================================

class RingBufferLogHandler(logging.Handler):
    """Stores the last N log records in memory with secret redaction."""

    def __init__(self, capacity: int = 500, redact_fn: Optional[Callable[[str], str]] = None):
        super().__init__()
        self.capacity = capacity
        self.buffer: List[Dict[str, Any]] = []
        self.redact_fn = redact_fn or (lambda x: x)

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            redacted_msg = self.redact_fn(msg)
            entry = {
                "timestamp": record.created,
                "time_str": time.strftime("%H:%M:%S", time.localtime(record.created)),
                "level": record.levelname,
                "message": redacted_msg,
            }
            self.buffer.append(entry)
            if len(self.buffer) > self.capacity:
                self.buffer.pop(0)
        except Exception:
            self.handleError(record)

    def get_logs(self, min_level: Optional[str] = None) -> List[Dict[str, Any]]:
        if not min_level or min_level.upper() == "ALL":
            return list(self.buffer)
        level_val = getattr(logging, min_level.upper(), logging.INFO)
        return [
            entry for entry in self.buffer
            if getattr(logging, entry["level"], logging.INFO) >= level_val
        ]


# ==============================================================================
# EMBEDDED DASHBOARD HTML/JS/CSS
# ==============================================================================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Telegram Organizer | Dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --card-border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #38bdf8;
      --success: #22c55e;
      --warning: #f59e0b;
      --danger: #ef4444;
      --purple: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      padding: 16px;
      line-height: 1.5;
    }
    .container { max-width: 1200px; margin: 0 auto; display: flex; flex-direction: column; gap: 16px; }
    header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      background: var(--card-bg);
      padding: 16px 20px;
      border-radius: 12px;
      border: 1px solid var(--card-border);
      gap: 12px;
    }
    h1 { font-size: 1.3rem; display: flex; align-items: center; gap: 8px; font-weight: 700; }
    .badge {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .badge-RUNNING { background: rgba(34, 197, 94, 0.2); color: var(--success); border: 1px solid var(--success); }
    .badge-IDLE { background: rgba(148, 163, 184, 0.2); color: var(--text-muted); border: 1px solid var(--text-muted); }
    .badge-PAUSED { background: rgba(245, 158, 11, 0.2); color: var(--warning); border: 1px solid var(--warning); }
    .badge-AUTH_REQUIRED { background: rgba(239, 68, 68, 0.2); color: var(--danger); border: 1px solid var(--danger); animation: pulse 2s infinite; }
    .badge-ERROR { background: rgba(239, 68, 68, 0.2); color: var(--danger); border: 1px solid var(--danger); }
    .badge-DONE { background: rgba(56, 189, 248, 0.2); color: var(--primary); border: 1px solid var(--primary); }
    .badge-STARTING, .badge-STOPPING { background: rgba(168, 85, 247, 0.2); color: var(--purple); border: 1px solid var(--purple); }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }

    .btn-group { display: flex; flex-wrap: wrap; gap: 8px; }
    button {
      background: var(--card-bg);
      color: var(--text);
      border: 1px solid var(--card-border);
      padding: 7px 14px;
      border-radius: 8px;
      font-size: 0.85rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
    }
    button:hover { background: #334155; border-color: var(--primary); }
    button.btn-primary { background: #0284c7; border-color: #0284c7; }
    button.btn-primary:hover { background: #0369a1; }
    button.btn-warning { background: #d97706; border-color: #d97706; }
    button.btn-danger { background: #b91c1c; border-color: #b91c1c; }

    .grid-stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 14px 16px;
    }
    .card-title { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
    .card-val { font-size: 1.4rem; font-weight: 700; margin-top: 4px; }
    .val-success { color: var(--success); }
    .val-warning { color: var(--warning); }
    .val-danger { color: var(--danger); }
    .val-primary { color: var(--primary); }

    .section-title { font-size: 0.95rem; font-weight: 700; margin-bottom: 10px; color: var(--text-muted); }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--card-border); }
    th { color: var(--text-muted); font-weight: 600; background: rgba(0,0,0,0.1); }
    tr:hover td { background: rgba(255,255,255,0.02); }

    .progress-bar { width: 100%; background: #334155; height: 6px; border-radius: 3px; overflow: hidden; margin-top: 4px; }
    .progress-fill { height: 100%; background: var(--primary); width: 0%; transition: width 0.3s; }

    #logViewer {
      background: #090d16;
      border: 1px solid var(--card-border);
      border-radius: 8px;
      height: 320px;
      overflow-y: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.78rem;
      padding: 10px;
      white-space: pre-wrap;
      word-break: break-all;
    }
    .log-line { margin-bottom: 3px; }
    .log-INFO { color: #cbd5e1; }
    .log-WARNING { color: #fde047; }
    .log-ERROR { color: #f87171; }
    .log-DEBUG { color: #64748b; }

    .alert-banner {
      padding: 12px 16px;
      border-radius: 8px;
      font-size: 0.85rem;
      display: none;
    }
    .alert-banner.show { display: block; }
    .alert-danger { background: rgba(239,68,68,0.2); border: 1px solid var(--danger); color: #fca5a5; }

    @media (max-width: 768px) {
      header { flex-direction: column; align-items: flex-start; }
      .btn-group { width: 100%; }
      button { flex: 1 1 auto; text-align: center; }
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>Telegram Organizer <span id="modeBadge" class="badge badge-DONE">DRY RUN</span></h1>
        <div style="font-size: 0.8rem; color: var(--text-muted); margin-top: 4px;">
          State: <span id="stateBadge" class="badge badge-IDLE">IDLE</span> | Uptime: <span id="uptime">0s</span>
        </div>
      </div>
      <div class="btn-group">
        <button id="btnStart" class="btn-primary" onclick="sendAction('start')">▶ Start</button>
        <button id="btnPause" class="btn-warning" onclick="sendAction('pause')">⏸ Pause</button>
        <button id="btnResume" class="btn-primary" onclick="sendAction('resume')">⏯ Resume</button>
        <button id="btnStop" class="btn-danger" onclick="sendAction('stop')">⏹ Stop</button>
        <button id="btnRetry" onclick="sendAction('retry_failed')">🔄 Retry Failed</button>
      </div>
    </header>

    <div id="errorBanner" class="alert-banner alert-danger"></div>

    <div class="grid-stats">
      <div class="card">
        <div class="card-title">Completed</div>
        <div class="card-val val-success" id="statDone">0</div>
      </div>
      <div class="card">
        <div class="card-title">Pending</div>
        <div class="card-val" id="statPending">0</div>
      </div>
      <div class="card">
        <div class="card-title">Failed</div>
        <div class="card-val val-danger" id="statFailed">0</div>
      </div>
      <div class="card">
        <div class="card-title">Restricted</div>
        <div class="card-val val-warning" id="statRestricted">0</div>
      </div>
      <div class="card">
        <div class="card-title">Skipped</div>
        <div class="card-val" id="statSkipped">0</div>
      </div>
      <div class="card">
        <div class="card-title">Transferred</div>
        <div class="card-val val-primary" id="statBytes">0 MB</div>
      </div>
      <div class="card">
        <div class="card-title">Speed / ETA</div>
        <div class="card-val" style="font-size: 1rem;" id="statSpeedEta">0 KB/s | --</div>
      </div>
    </div>

    <!-- Active Transfers -->
    <div class="card">
      <div class="section-title">Active In-Flight Transfers</div>
      <div style="overflow-x: auto;">
        <table>
          <thead>
            <tr>
              <th>File Name</th>
              <th>Unit</th>
              <th>Stage</th>
              <th>Progress</th>
              <th>Speed</th>
            </tr>
          </thead>
          <tbody id="activeTransfersBody">
            <tr><td colspan="5" style="color: var(--text-muted); text-align: center;">No active transfers</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Units Table -->
    <div class="card">
      <div class="section-title">Round-Robin Units (Source ➔ Destination Topic)</div>
      <div style="overflow-x: auto;">
        <table>
          <thead>
            <tr>
              <th>Source Unit</th>
              <th>Target Topic</th>
              <th>Cursor (Msg ID)</th>
              <th>Progress (Done / Total)</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="unitsTableBody">
            <tr><td colspan="5" style="color: var(--text-muted); text-align: center;">Scanning units...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Live Logs -->
    <div class="card">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
        <div class="section-title" style="margin-bottom: 0;">Live Activity Logs</div>
        <div style="display: flex; gap: 8px; align-items: center;">
          <label style="font-size: 0.75rem; color: var(--text-muted);">
            <input type="checkbox" id="autoScroll" checked> Auto-scroll
          </label>
          <select id="logLevelSelect" onchange="fetchLogs()" style="background: var(--bg); color: var(--text); border: 1px solid var(--card-border); padding: 4px 8px; border-radius: 6px; font-size: 0.75rem;">
            <option value="ALL">All Levels</option>
            <option value="INFO" selected>INFO+</option>
            <option value="WARNING">WARNING+</option>
            <option value="ERROR">ERROR only</option>
          </select>
        </div>
      </div>
      <div id="logViewer"></div>
    </div>
  </div>

  <script>
    function formatBytes(bytes) {
      if (!bytes || bytes === 0) return '0 MB';
      const k = 1024;
      const dm = 1;
      const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
    }

    function formatTime(sec) {
      if (!sec || isNaN(sec) || sec <= 0) return '--';
      if (sec < 60) return Math.round(sec) + 's';
      const m = Math.floor(sec / 60);
      const s = Math.round(sec % 60);
      return m + 'm ' + s + 's';
    }

    async function sendAction(action) {
      try {
        const res = await fetch('/api/action/' + action, { method: 'POST' });
        const data = await res.json();
        if (data.error) alert(data.error);
        fetchStatus();
      } catch (e) {
        console.error(e);
      }
    }

    async function fetchStatus() {
      try {
        const res = await fetch('/api/status');
        if (!res.ok) return;
        const s = await res.json();

        // Header
        const stateBadge = document.getElementById('stateBadge');
        stateBadge.textContent = s.state || 'IDLE';
        stateBadge.className = 'badge badge-' + (s.state || 'IDLE');

        const modeBadge = document.getElementById('modeBadge');
        modeBadge.textContent = s.dry_run ? 'DRY RUN' : 'LIVE MODE';
        modeBadge.className = 'badge ' + (s.dry_run ? 'badge-PAUSED' : 'badge-RUNNING');

        document.getElementById('uptime').textContent = formatTime(s.uptime);

        // Error banner
        const errBanner = document.getElementById('errorBanner');
        if (s.last_error) {
          errBanner.textContent = 'Last Error: ' + s.last_error;
          errBanner.classList.add('show');
        } else {
          errBanner.classList.remove('show');
        }

        // Stats
        document.getElementById('statDone').textContent = s.totals.done || 0;
        document.getElementById('statPending').textContent = s.totals.pending || 0;
        document.getElementById('statFailed').textContent = s.totals.failed || 0;
        document.getElementById('statRestricted').textContent = s.totals.restricted || 0;
        document.getElementById('statSkipped').textContent = (s.totals.skipped_size || 0) + (s.totals.skipped_disk || 0);
        document.getElementById('statBytes').textContent = formatBytes(s.bytes_transferred || 0);

        const speedStr = formatBytes(s.speed_bytes_sec || 0) + '/s';
        const etaStr = formatTime(s.eta_seconds);
        document.getElementById('statSpeedEta').textContent = speedStr + ' | ' + etaStr;

        // Active transfers
        const activeTbody = document.getElementById('activeTransfersBody');
        if (s.active_transfers && s.active_transfers.length > 0) {
          activeTbody.innerHTML = s.active_transfers.map(t => `
            <tr>
              <td><strong>${t.filename || '-'}</strong></td>
              <td>${t.unit || '-'}</td>
              <td><span class="badge badge-STARTING">${t.stage}</span></td>
              <td style="min-width: 120px;">
                ${t.progress_pct || 0}%
                <div class="progress-bar"><div class="progress-fill" style="width: ${t.progress_pct || 0}%;"></div></div>
              </td>
              <td>${formatBytes(t.speed || 0)}/s</td>
            </tr>
          `).join('');
        } else {
          activeTbody.innerHTML = '<tr><td colspan="5" style="color: var(--text-muted); text-align: center;">No active transfers</td></tr>';
        }

        // Units table
        const unitsTbody = document.getElementById('unitsTableBody');
        if (s.units && s.units.length > 0) {
          unitsTbody.innerHTML = s.units.map(u => {
            const pct = u.total > 0 ? Math.round((u.done / u.total) * 100) : 0;
            return `
              <tr>
                <td><strong>${u.name}</strong></td>
                <td><span style="color: var(--primary);">${u.dest_topic}</span></td>
                <td>${u.cursor || 0}</td>
                <td style="min-width: 140px;">
                  ${u.done} / ${u.total > 0 ? u.total : 'scanning...'} (${pct}%)
                  <div class="progress-bar"><div class="progress-fill" style="width: ${pct}%;"></div></div>
                </td>
                <td><span class="badge ${u.status === 'DONE' ? 'badge-DONE' : 'badge-RUNNING'}">${u.status || 'READY'}</span></td>
              </tr>
            `;
          }).join('');
        }
      } catch (e) {
        console.error(e);
      }
    }

    async function fetchLogs() {
      try {
        const level = document.getElementById('logLevelSelect').value;
        const res = await fetch('/api/logs?level=' + level);
        if (!res.ok) return;
        const logs = await res.json();
        const viewer = document.getElementById('logViewer');
        const shouldScroll = document.getElementById('autoScroll').checked;

        viewer.innerHTML = logs.map(l =>
          `<div class="log-line log-${l.level}">[${l.time_str}] [${l.level}] ${l.message}</div>`
        ).join('');

        if (shouldScroll) {
          viewer.scrollTop = viewer.scrollHeight;
        }
      } catch (e) {
        console.error(e);
      }
    }

    // Initial load & 3-second poll loop
    fetchStatus();
    fetchLogs();
    setInterval(fetchStatus, 3000);
    setInterval(fetchLogs, 3000);
  </script>
</body>
</html>
"""


# ==============================================================================
# DASHBOARD SERVER (AIOHTTP)
# ==============================================================================

class DashboardServer:
    def __init__(
        self,
        get_status_fn: Callable[[], Dict[str, Any]],
        action_fn: Callable[[str], Dict[str, Any]],
        log_handler: RingBufferLogHandler,
        user: str = "admin",
        password: str = "",
        host: str = "0.0.0.0",
        port: int = 8080,
    ):
        self.get_status_fn = get_status_fn
        self.action_fn = action_fn
        self.log_handler = log_handler
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.app = web.Application(middlewares=[self._auth_middleware])
        self._setup_routes()
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler):
        # /healthz is open for Render healthcheck without auth
        if request.path == "/healthz":
            return await handler(request)

        # Refuse to start/authenticate if password unset
        if not self.password:
            return web.Response(
                status=503,
                text="Dashboard is disabled: DASHBOARD_PASSWORD environment variable is not configured."
            )

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Basic "):
            return web.Response(
                status=401,
                headers={"WWW-Authenticate": 'Basic realm="Telegram Organizer Dashboard"'},
                text="Authentication required"
            )

        try:
            raw_credentials = base64.b64decode(auth_header[6:]).decode("utf-8")
            u, p = raw_credentials.split(":", 1)
            if u != self.user or p != self.password:
                return web.Response(
                    status=401,
                    headers={"WWW-Authenticate": 'Basic realm="Telegram Organizer Dashboard"'},
                    text="Invalid username or password"
                )
        except Exception:
            return web.Response(status=400, text="Invalid Authorization header")

        return await handler(request)

    def _setup_routes(self):
        self.app.router.add_get("/healthz", self._handle_healthz)
        self.app.router.add_get("/", self._handle_index)
        self.app.router.add_get("/api/status", self._handle_api_status)
        self.app.router.add_get("/api/logs", self._handle_api_logs)
        self.app.router.add_post("/api/action/{action}", self._handle_api_action)

    async def _handle_healthz(self, request: web.Request):
        try:
            status = self.get_status_fn()
            state = status.get("state", "UNKNOWN")
            return web.json_response({"status": "ok", "state": state})
        except Exception as e:
            return web.json_response({"status": "error", "error": str(e)}, status=500)

    async def _handle_index(self, request: web.Request):
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")

    async def _handle_api_status(self, request: web.Request):
        status = self.get_status_fn()
        return web.json_response(status)

    async def _handle_api_logs(self, request: web.Request):
        level = request.query.get("level", "ALL")
        logs = self.log_handler.get_logs(level)
        return web.json_response(logs)

    async def _handle_api_action(self, request: web.Request):
        action = request.match_info.get("action", "")
        result = self.action_fn(action)
        return web.json_response(result)

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logging.getLogger("telegram_organizer").info(
            "Dashboard server listening on http://%s:%d (Basic Auth enabled)",
            self.host, self.port
        )

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            self.site = None

