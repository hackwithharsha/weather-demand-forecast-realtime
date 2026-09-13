#!/usr/bin/env python3
"""
Alertmanager webhook receiver.

Listens on :9999/alert for POST requests from Alertmanager and logs each
alert as a structured line to stdout.  No external dependencies — pure stdlib.

Log format (one line per alert):
  ALERT  FIRING   KafkaConsumerLagHigh  group=worker-stream-demand ...  "Consumer group ..."
  ALERT  RESOLVED KafkaConsumerLagHigh  group=worker-stream-demand ...
"""

from __future__ import annotations

import json
import logging
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("webhook")

PORT = 9999


class AlertHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            log.warning("received non-JSON body: %s", body[:200])
            self._respond(400)
            return

        for alert in payload.get("alerts", []):
            status       = alert.get("status", "unknown").upper()
            labels       = alert.get("labels", {})
            annotations  = alert.get("annotations", {})
            alertname    = labels.get("alertname", "unknown")
            severity     = labels.get("severity", "")
            summary      = annotations.get("summary", "")

            # Pretty-print non-name/severity labels as key=value pairs
            extra = "  ".join(
                f"{k}={v}"
                for k, v in sorted(labels.items())
                if k not in ("alertname", "severity")
            )

            log.info(
                "ALERT  %-8s  %-30s  sev=%-8s  %s  %s",
                status,
                alertname,
                severity,
                extra,
                summary,
            )

        self._respond(200)

    def _respond(self, code: int) -> None:
        self.send_response(code)
        self.end_headers()

    # Suppress the default per-request access log line.
    def log_message(self, fmt: str, *args: object) -> None:  # noqa: ARG002
        pass


def main() -> None:
    server = HTTPServer(("0.0.0.0", PORT), AlertHandler)
    log.info("webhook receiver listening on :%d/alert", PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down")


if __name__ == "__main__":
    main()
