#!/usr/bin/env python3
"""
Unit and integration tests for Dashboard (aiohttp server):
- Public /healthz access without authentication
- HTTP Basic Auth protection for dashboard UI and API endpoints
- /api/status structure and metrics
- /api/logs with secret redaction
- /api/action/* endpoints (pause, resume, retry_failed)
"""

import base64
import json
import logging
import os
import unittest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard import DashboardServer, RingBufferLogHandler


class TestDashboardServer(AioHTTPTestCase):
    async def get_application(self):
        self.user = "admin"
        self.password = "secretpass123"

        # Mock status function
        self.status_data = {
            "state": "RUNNING",
            "uptime": "00:15:30",
            "mode": "Dry Run",
            "error": None,
            "totals": {
                "discovered": 50,
                "uploaded": 30,
                "in_progress": 2,
                "failed": 1,
                "restricted": 3,
                "skipped_size": 0,
            },
            "active_transfers": [],
            "units": [
                {
                    "unit_id": "chat_-1002671181760",
                    "unit_type": "telegram_chat",
                    "source_id": "-1002671181760",
                    "topic_name": "Imported Files",
                    "cursor": 253,
                    "status": "active",
                }
            ],
        }

        self.last_action = None
        def mock_action(action):
            self.last_action = action
            return {"success": True, "message": f"Action {action} performed"}

        # Ring buffer handler with redaction
        def mock_redact(msg):
            return msg.replace("secretpass123", "[REDACTED]")

        self.ring_handler = RingBufferLogHandler(capacity=100, redact_fn=mock_redact)
        test_logger = logging.getLogger("test_dashboard")
        test_logger.addHandler(self.ring_handler)
        test_logger.setLevel(logging.INFO)
        test_logger.info("System initialized with secretpass123.")

        self.server = DashboardServer(
            host="127.0.0.1",
            port=0,
            user=self.user,
            password=self.password,
            get_status_fn=lambda: self.status_data,
            action_fn=mock_action,
            log_handler=self.ring_handler,
        )
        return self.server.app

    def _auth_header(self, user="admin", password="secretpass123"):
        creds = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {creds}"}

    @unittest_run_loop
    async def test_healthz_unauthenticated(self):
        """Requirement: /healthz must be public and accessible without credentials for Render health checks."""
        resp = await self.client.request("GET", "/healthz")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["state"], "RUNNING")

    @unittest_run_loop
    async def test_auth_required_for_index_and_api(self):
        """Endpoints other than /healthz require valid HTTP Basic Auth."""
        # Index without auth
        resp = await self.client.request("GET", "/")
        self.assertEqual(resp.status, 401)
        self.assertIn("Basic", resp.headers.get("WWW-Authenticate", ""))

        # API status without auth
        resp = await self.client.request("GET", "/api/status")
        self.assertEqual(resp.status, 401)

        # Invalid credentials
        bad_headers = self._auth_header(user="admin", password="wrongpassword")
        resp = await self.client.request("GET", "/api/status", headers=bad_headers)
        self.assertEqual(resp.status, 401)

    @unittest_run_loop
    async def test_api_status_with_auth(self):
        """Authenticated GET /api/status returns valid state payload."""
        headers = self._auth_header()
        resp = await self.client.request("GET", "/api/status", headers=headers)
        self.assertEqual(resp.status, 200)
        data = await resp.json()

        self.assertEqual(data["state"], "RUNNING")
        self.assertEqual(data["totals"]["uploaded"], 30)
        self.assertEqual(len(data["units"]), 1)
        self.assertEqual(data["units"][0]["cursor"], 253)

    @unittest_run_loop
    async def test_api_logs_with_redaction(self):
        """Authenticated GET /api/logs returns logs with sensitive secrets redacted."""
        headers = self._auth_header()
        resp = await self.client.request("GET", "/api/logs", headers=headers)
        self.assertEqual(resp.status, 200)
        logs = await resp.json()

        self.assertGreaterEqual(len(logs), 1)
        msg = logs[0]["message"]
        self.assertIn("[REDACTED]", msg)
        self.assertNotIn("secretpass123", msg)

    @unittest_run_loop
    async def test_api_actions(self):
        """Authenticated POST /api/action/{action} triggers action callback."""
        headers = self._auth_header()
        for action in ["pause", "resume", "stop", "retry_failed"]:
            resp = await self.client.request("POST", f"/api/action/{action}", headers=headers)
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertTrue(data["success"])
            self.assertEqual(self.last_action, action)


if __name__ == "__main__":
    unittest.main()

