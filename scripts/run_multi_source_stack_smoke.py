#!/usr/bin/env python3
"""Run a full-stack multi-source smoke test against a temporary local database."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poller.agent_usage_common import PostgresClient, ProviderSnapshot  # noqa: E402


LOCAL_PG_SOCKET = "/run/user/1000/local-postgres"
LOCAL_PG_PORT = "5433"
DEFAULT_ARTIFACT_DIR = Path("/tmp/agent-usage-multi-source-smoke")


def _run(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=True, text=True, capture_output=True, env=env, cwd=cwd)


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _psql_base_cmd(dbname: str) -> list[str]:
    return [
        "psql",
        "--no-psqlrc",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        LOCAL_PG_SOCKET,
        "-p",
        LOCAL_PG_PORT,
        "-d",
        dbname,
    ]


def _createdb(name: str) -> None:
    _run(["createdb", "-h", LOCAL_PG_SOCKET, "-p", LOCAL_PG_PORT, name])


def _dropdb(name: str) -> None:
    _run(["dropdb", "-h", LOCAL_PG_SOCKET, "-p", LOCAL_PG_PORT, name])


def _http_json(url: str, timeout: float = 3.0) -> dict[str, Any]:
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)


def _wait_for_http(url: str, timeout: float = 10.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            return _http_json(url)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {url}: {last_error}")


def _metric(
    *,
    metric_key: str,
    metric_path: str,
    metric_label: str,
    percent: int,
    value: str,
    note: str,
    window_start: str,
    window_end: str,
    reset_at: str,
    provider_metric_key: str | None = None,
    metric_scope: str = "/",
    value_num: float | None = None,
    max_value: int = 100,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "metric_key": metric_key,
        "provider_metric_key": provider_metric_key or metric_key,
        "metric_path": metric_path,
        "metric_id": metric_path,
        "metric_scope": metric_scope,
        "metric_label": metric_label,
        "percent": percent,
        "value_num": value_num,
        "value": value,
        "note": note,
        "max_value": max_value,
        "window_start": window_start,
        "window_end": window_end,
        "reset_at": reset_at,
        "details": details or {},
    }


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _stamp_fetch_time(client: PostgresClient, fetch_id: int, fetched_at: str) -> None:
    client._run(  # noqa: SLF001
        """
UPDATE usage_provider_fetch
SET fetched_at = NULLIF(:'fetched_at', '')::timestamptz
WHERE id = :'fetch_id';
""",
        vars={"fetch_id": str(fetch_id), "fetched_at": fetched_at},
    )


def _seed_snapshot(
    client: PostgresClient,
    *,
    source_id: str,
    source_label: str,
    provider: str,
    account_id: str,
    organization_id: str,
    fetched_at: str,
    summary_key: str,
    history_key: str,
    history_label: str,
    plan: str,
    metrics: list[dict[str, Any]],
) -> int:
    snapshot = ProviderSnapshot(
        provider=provider,
        account_id=account_id,
        organization_id=organization_id,
        metrics=metrics,
        summary_key=summary_key,
        history_key=history_key,
        history_label=history_label,
        details=[],
        raw_payload={"source_id": source_id, "provider": provider},
        request_url=f"fixture://{source_id}",
        request_status=200,
        request_error=None,
        request_metadata={
            "source_id": source_id,
            "source_label": source_label,
            "frontend_visible": True,
            "provider": provider,
            "plan": plan,
            "summary_key": summary_key,
            "history_key": history_key,
            "history_label": history_label,
            "details": [],
        },
        success=True,
        source_id=source_id,
        source_label=source_label,
        frontend_visible=True,
    )
    fetch_id = client.persist_snapshot(snapshot)
    _stamp_fetch_time(client, fetch_id, fetched_at)
    return fetch_id


def _seed_fixture_data(client: PostgresClient) -> None:
    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=7)
    five_hour_start = now - timedelta(hours=4)
    five_hour_end = five_hour_start + timedelta(hours=5)
    note = (
        f"Started at {week_start.astimezone().strftime('%Y-%m-%d %H:%M')}\n"
        f"Resets at {week_end.astimezone().strftime('%Y-%m-%d %H:%M')}"
    )
    short_note = (
        f"Started at {five_hour_start.astimezone().strftime('%Y-%m-%d %H:%M')}\n"
        f"Resets at {five_hour_end.astimezone().strftime('%Y-%m-%d %H:%M')}"
    )

    def seed_claude(source_id: str, label: str, account_suffix: str, weekly: list[int], five_hour: list[int]) -> None:
        for idx, (week_pct, short_pct) in enumerate(zip(weekly, five_hour, strict=True)):
            fetched_at = _iso(now - timedelta(hours=(len(weekly) - idx) * 2))
            _seed_snapshot(
                client,
                source_id=source_id,
                source_label=label,
                provider="claude",
                account_id=f"org-{account_suffix}",
                organization_id=f"org-{account_suffix}",
                fetched_at=fetched_at,
                summary_key="seven_day",
                history_key="seven_day",
                history_label="This week",
                plan="pro",
                metrics=[
                    _metric(
                        metric_key="seven_day",
                        metric_path="/seven_day",
                        metric_label="This week",
                        percent=week_pct,
                        value=f"{week_pct}%",
                        note=note,
                        window_start=_iso(week_start),
                        window_end=_iso(week_end),
                        reset_at=_iso(week_end),
                    ),
                    _metric(
                        metric_key="five_hour",
                        metric_path="/five_hour",
                        metric_label="5-hour window",
                        percent=short_pct,
                        value=f"{short_pct}%",
                        note=short_note,
                        window_start=_iso(five_hour_start),
                        window_end=_iso(five_hour_end),
                        reset_at=_iso(five_hour_end),
                    ),
                ],
            )

    seed_claude("personal", "Claude Personal", "personal", [8, 13, 21], [3, 5, 7])
    seed_claude("work", "Claude Work", "work", [41, 53, 64], [12, 17, 22])

    hidden_fetch = _seed_snapshot(
        client,
        source_id="hidden",
        source_label="Claude Hidden",
        provider="claude",
        account_id="org-hidden",
        organization_id="org-hidden",
        fetched_at=_iso(now - timedelta(hours=1)),
        summary_key="seven_day",
        history_key="seven_day",
        history_label="This week",
        plan="pro",
        metrics=[
            _metric(
                metric_key="seven_day",
                metric_path="/seven_day",
                metric_label="This week",
                percent=80,
                value="80%",
                note=note,
                window_start=_iso(week_start),
                window_end=_iso(week_end),
                reset_at=_iso(week_end),
            ),
            _metric(
                metric_key="five_hour",
                metric_path="/five_hour",
                metric_label="5-hour window",
                percent=30,
                value="30%",
                note=short_note,
                window_start=_iso(five_hour_start),
                window_end=_iso(five_hour_end),
                reset_at=_iso(five_hour_end),
            ),
        ],
    )
    client._run(  # noqa: SLF001
        """
UPDATE usage_provider_fetch
SET request_metadata = jsonb_set(request_metadata, '{frontend_visible}', 'false'::jsonb, true)
WHERE id = :'fetch_id';
""",
        vars={"fetch_id": str(hidden_fetch)},
    )

    for idx, (week_pct, short_pct) in enumerate([(6, 2), (11, 5), (17, 9)], start=1):
        fetched_at = _iso(now - timedelta(hours=(4 - idx) * 3 + 1))
        _seed_snapshot(
            client,
            source_id="codex",
            source_label="Codex",
            provider="codex",
            account_id="acct-codex",
            organization_id="user-codex",
            fetched_at=fetched_at,
            summary_key="secondary_window",
            history_key="secondary_window",
            history_label="This week",
            plan="prolite",
            metrics=[
                _metric(
                    metric_key="secondary_window",
                    metric_path="/rate_limit/secondary_window",
                    metric_label="This week",
                    percent=week_pct,
                    value=f"{week_pct}%",
                    note=note,
                    window_start=_iso(week_start),
                    window_end=_iso(week_end),
                    reset_at=_iso(week_end),
                ),
                _metric(
                    metric_key="primary_window",
                    metric_path="/rate_limit/primary_window",
                    metric_label="5-hour window",
                    percent=short_pct,
                    value=f"{short_pct}%",
                    note=short_note,
                    window_start=_iso(five_hour_start),
                    window_end=_iso(five_hour_end),
                    reset_at=_iso(five_hour_end),
                ),
            ],
        )


def _write_config(path: Path, dbname: str, port: int) -> None:
    cache_dir = path.parent / "cache"
    state_file = cache_dir / "state.json"
    db_dsn = f"postgresql:///{dbname}?host={LOCAL_PG_SOCKET}&port={LOCAL_PG_PORT}"
    body = f"""
[service]
host = "127.0.0.1"
port = {port}

[poller]
default_interval_seconds = 900

[storage]
cache_dir = "{cache_dir}"
state_file = "{state_file}"
db_dsn = "{db_dsn}"

[sources.personal]
provider = "claude"
label = "Claude Personal"
frontend_visible = true
enabled = true

[sources.personal.auth]
cookie = "fixture-personal"

[sources.work]
provider = "claude"
label = "Claude Work"
frontend_visible = true
enabled = true
interval_seconds = 1800

[sources.work.auth]
cookie = "fixture-work"

[sources.hidden]
provider = "claude"
label = "Claude Hidden"
frontend_visible = false
enabled = true

[sources.hidden.auth]
cookie = "fixture-hidden"

[sources.codex]
provider = "codex"
label = "Codex"
frontend_visible = true
enabled = true

[sources.codex.auth]
authorization = "Bearer fixture"
cookie = "fixture-codex"
"""
    path.write_text(body.strip() + "\n", encoding="utf-8")


@contextmanager
def _temporary_database(prefix: str = "agent_usage_widget_smoke_") -> Any:
    dbname = f"{prefix}{int(time.time())}_{os.getpid()}"
    _createdb(dbname)
    try:
        yield dbname
    finally:
        try:
            _dropdb(dbname)
        except Exception:  # noqa: BLE001
            pass


@contextmanager
def _service_process(config_file: Path) -> Any:
    log_file = config_file.parent / "service.log"
    env = dict(os.environ)
    proc = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            str(ROOT / "poller" / "agent_usage_service.py"),
            "--config-file",
            str(config_file),
        ],
        cwd=ROOT,
        stdout=log_file.open("w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
    )
    try:
        yield proc, log_file
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _validate_service(base_url: str) -> dict[str, Any]:
    current = _http_json(f"{base_url}/api/current")
    items = current.get("agents", [])
    ids = [item.get("id") for item in items]
    _assert(ids == ["personal", "work", "codex"], f"unexpected current ids: {ids}")
    _assert([item.get("provider") for item in items] == ["claude", "claude", "codex"], "unexpected provider ordering")
    _assert([item.get("label") for item in items] == ["Claude Personal", "Claude Work", "Codex"], "unexpected labels")

    personal_hist = _http_json(f"{base_url}/api/history?source=personal&metric=/seven_day&days=30")
    work_hist = _http_json(f"{base_url}/api/history?source=work&metric=/seven_day&days=30")
    hidden_hist = _http_json(f"{base_url}/api/history?source=hidden&metric=/seven_day&days=30")
    _assert(personal_hist.get("source_id") == "personal", "personal history should stay source-scoped")
    _assert(work_hist.get("source_id") == "work", "work history should stay source-scoped")
    _assert(hidden_hist.get("source_id") == "hidden", "hidden history should be queryable by source")
    _assert(len(personal_hist.get("points", [])) >= 3, "personal history should contain seeded points")
    _assert(len(work_hist.get("points", [])) >= 3, "work history should contain seeded points")

    raw = _http_json(f"{base_url}/api/raw/latest?source=work")
    _assert(raw.get("source_id") == "work", "raw latest should resolve by source")
    _assert(raw.get("provider") == "claude", "raw latest should keep provider metadata")
    return current


def _render_artifacts(base_url: str, artifact_dir: Path) -> tuple[Path, Path]:
    panel = artifact_dir / "panel.png"
    bar = artifact_dir / "bar.png"
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "render_widget_screenshots.py"),
        "--service-url",
        f"{base_url}/api/current",
        "--state-file",
        str(artifact_dir / "missing-state.json"),
        "--panel-output",
        str(panel),
        "--bar-output",
        str(bar),
    ]
    _run(cmd, cwd=ROOT)
    _assert(panel.exists(), "panel screenshot was not rendered")
    _assert(bar.exists(), "bar screenshot was not rendered")
    return panel, bar


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-dir",
        default=str(DEFAULT_ARTIFACT_DIR),
        help="Directory for generated config, logs, and screenshots",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep the temporary database instead of dropping it on exit",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact_dir = Path(args.artifact_dir).expanduser()
    if artifact_dir.exists():
        shutil.rmtree(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    port = _pick_free_port()
    if args.keep_db:
        dbname = f"agent_usage_widget_smoke_keep_{int(time.time())}_{os.getpid()}"
        _createdb(dbname)
        db_cm = nullcontext(dbname)
    else:
        db_cm = _temporary_database()

    with db_cm as dbname:
        config_file = artifact_dir / "config.toml"
        _write_config(config_file, dbname, port)

        client = PostgresClient(f"postgresql:///{dbname}?host={LOCAL_PG_SOCKET}&port={LOCAL_PG_PORT}")
        client.ping()
        _seed_fixture_data(client)

        with _service_process(config_file) as (_proc, _log_file):
            base_url = f"http://127.0.0.1:{port}"
            _wait_for_http(f"{base_url}/health")
            current = _validate_service(base_url)
            panel, bar = _render_artifacts(base_url, artifact_dir)
            print(json.dumps(
                {
                    "ok": True,
                    "database": dbname,
                    "config_file": str(config_file),
                    "artifact_dir": str(artifact_dir),
                    "service_url": f"{base_url}/api/current",
                    "agents": [agent.get("id") for agent in current.get("agents", [])],
                    "panel_png": str(panel),
                    "bar_png": str(bar),
                },
                indent=2,
            ))
        if args.keep_db:
            print(f"kept temporary database: {dbname}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
