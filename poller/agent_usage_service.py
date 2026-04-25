#!/usr/bin/env python3
"""Expose local HTTP contract for live widget consumption."""

from __future__ import annotations

import argparse
import ipaddress
import json
import sys
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from poller.agent_usage_common import PostgresClient, load_config  # type: ignore  # noqa: E402


@dataclass
class ServiceConfig:
    host: str
    port: int


def _normalized_host(value: str) -> str:
    return str(value or "").strip().strip("[]").split("%", 1)[0].lower()


def _is_loopback_host(value: str) -> bool:
    candidate = _normalized_host(value)
    if not candidate:
        return False
    if candidate == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def _is_supported_bind_host(value: str) -> bool:
    candidate = _normalized_host(value)
    if candidate == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).version == 4 and ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=None, help="Override AGENT_USAGE_SERVICE_PORT")
    parser.add_argument("--host", default=None, help="Override AGENT_USAGE_SERVICE_HOST")
    parser.add_argument("--env-file", default=None, help="Override AGENT_USAGE_ENV_FILE")
    parser.add_argument("--history-days", type=int, default=30)
    return parser.parse_args()


class UsageRequestHandler(BaseHTTPRequestHandler):
    client: PostgresClient
    history_days: int
    service_config: ServiceConfig

    def _send_json(self, data: dict[str, Any], status_code: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False)
        body_bytes = body.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def _write_error(self, code: int, message: str) -> None:
        self._send_json({"ok": False, "error": message}, status_code=code)

    @staticmethod
    def _metric_candidates(provider: str, metric: str) -> list[str]:
        metric = metric.strip()
        if not metric:
            return []
        if metric.startswith("/"):
            return [metric]
        if provider == "claude":
            if metric in {"five_hour", "short_window"}:
                return ["five_hour"]
            if metric in {"seven_day", "week", "weekly", "long_window"}:
                return ["seven_day"]
            if metric in {"session"}:
                return ["five_hour", "seven_day"]
            if metric in {"sonnet", "sonnet_usage"}:
                return ["sonnet_usage", "sonnet", "seven_day"]
            return [metric]

        if provider == "codex":
            if metric in {"secondary_window", "week", "weekly", "long_window", "seven_day"}:
                return ["secondary_window"]
            if metric in {"primary_window", "five_hour", "short_window", "session"}:
                return ["primary_window", "secondary_window"]
            if metric in {"spark", "spark_usage"}:
                return ["spark_usage", "spark", "secondary_window"]
            if metric in {"spark_primary_window", "spark_five_hour", "spark_short_window"}:
                return ["spark_primary_window", "primary_window"]
            if metric in {"month", "monthly"}:
                return ["secondary_window"]
            return [metric]

        if provider == "cursor":
            if metric in {"monthly", "month"}:
                return ["monthly"]
            if metric in {"short_window", "auto_spend", "auto", "auto_selected"}:
                return ["auto_spend"]
            return [metric]

        return [metric]

    def _build_history_payload(
        self,
        provider: str,
        metric: str,
        days: int,
    ) -> tuple[dict[str, Any], str]:
        if metric in {"", "long_window", "short_window"}:
            windows = self.client.build_history_windows(provider=provider, days=days)
            if metric in {"long_window", "short_window"}:
                graph = windows.get(metric)
                if graph:
                    return ({"provider": provider, "days": days, "window": metric, **graph}, metric)
                return ({"provider": provider, "days": days, "window": metric, "points": []}, metric)
            return windows, metric

        metric_candidates = self._metric_candidates(provider, metric)
        payload = self.client.build_history(provider=provider, metric=metric_candidates[0], days=days)
        used_metric = metric_candidates[0]
        if payload.get("points"):
            return payload, used_metric

        for metric_key in metric_candidates[1:]:
            candidate_payload = self.client.build_history(provider=provider, metric=metric_key, days=days)
            if candidate_payload.get("points"):
                candidate_payload["requested_metric"] = metric
                candidate_payload["metric"] = metric_key
                return candidate_payload, metric_key

        return payload, used_metric

    def do_GET(self) -> None:
        if not _is_loopback_host(self.client_address[0]):
            self._write_error(403, "service is local-only")
            return

        if self.path.startswith("/health") or self.path == "/healthz":
            self._send_json({"ok": True, "service": "agent-usage-service"})
            return

        parsed = urlparse(self.path)
        if parsed.path == "/api/current":
            try:
                payload = self.client.build_current_contract(history_days=self.history_days)
            except Exception as exc:
                self._write_error(503, f"database unavailable: {exc}")
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/history":
            qs = parse_qs(parsed.query)
            provider = (qs.get("provider") or [""])[0]
            metric = (qs.get("metric") or [""])[0]
            try:
                days = int((qs.get("days") or [str(self.history_days)])[0])
            except ValueError:
                days = self.history_days

            if not provider:
                try:
                    providers = ["claude", "codex", "cursor"]
                    payload = {
                        "ok": True,
                        "items": [
                            (
                                self._build_history_payload(
                                    provider=provider,
                                    metric=metric,
                                    days=days,
                                )[0]
                                | {"provider": provider}
                            )
                            for provider in providers
                        ],
                    }
                except Exception as exc:
                    self._write_error(503, f"database unavailable: {exc}")
                    return
                self._send_json(payload)
                return

            if provider not in {"claude", "codex", "cursor"}:
                self._write_error(400, "provider must be one of: claude, codex, cursor")
                return
            try:
                payload, used_metric = self._build_history_payload(provider=provider, metric=metric, days=days)
            except Exception as exc:
                self._write_error(503, f"database unavailable: {exc}")
                return
            if metric and used_metric != metric:
                payload["requested_metric"] = metric
                payload["metric"] = used_metric
            payload = {"ok": True, **payload}
            self._send_json(payload)
            return

        if parsed.path == "/api/raw/latest":
            qs = parse_qs(parsed.query)
            provider = (qs.get("provider") or [""])[0]
            if not provider:
                self._write_error(400, "provider query param is required")
                return
            if provider not in {"claude", "codex", "cursor"}:
                self._write_error(400, "provider must be one of: claude, codex, cursor")
                return
            try:
                payload = self.client.latest_raw(provider)
            except Exception as exc:
                self._write_error(503, f"database unavailable: {exc}")
                return
            if not payload:
                self._write_error(404, f"no payload for provider={provider}")
                return
            self._send_json({"ok": True, "provider": provider, "payload": payload})
            return

        self._write_error(404, "not found")

    def log_message(self, fmt: str, *args: Any) -> None:
        pass


def main() -> int:
    args = parse_args()
    overrides = {}
    if args.env_file:
        overrides["AGENT_USAGE_ENV_FILE"] = args.env_file
    cfg = load_config(overrides)

    host = args.host or cfg.service_host
    port = args.port or cfg.service_port
    if not _is_supported_bind_host(host):
        print(
            "AGENT_USAGE_SERVICE_HOST must be a local IPv4 loopback address or localhost.",
            file=sys.stderr,
            flush=True,
        )
        return 2

    client = PostgresClient(cfg.db_dsn)
    try:
        client.ping()
    except Exception as exc:
        # Service remains up with diagnostic endpoint.
        print(f"Postgres unavailable: {exc}", flush=True)

    handler = UsageRequestHandler
    handler.client = client
    handler.history_days = args.history_days
    handler.service_config = ServiceConfig(host=host, port=port)

    server = HTTPServer((host, port), handler)
    print(f"agent-usage-service listening on http://{host}:{port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
