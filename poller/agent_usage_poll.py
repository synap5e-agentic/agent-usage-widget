#!/usr/bin/env python3
"""Poll and persist live usage snapshots for Claude, Codex, and Cursor."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import sys
from pathlib import Path as _Path

ROOT_DIR = _Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from poller.agent_usage_common import (  # type: ignore  # noqa: E402
    AppConfig,
    PostgresClient,
    ProviderSnapshot,
    SourceConfig,
    load_config,
    run_fetch,
    sync_cursor_usage_events,
    write_state_file,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-file", default=None, help="Override AGENT_USAGE_CONFIG_FILE")
    parser.add_argument("--env-file", default=None, help="Override AGENT_USAGE_ENV_FILE")
    parser.add_argument(
        "--print-state",
        action="store_true",
        help="Print contract JSON instead of only writing state file",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="Write compat JSON snapshot to path",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=30,
        help="History points window for state.json compatibility payload",
    )
    parser.add_argument(
        "--provider",
        action="append",
        choices=("claude", "codex", "cursor"),
        default=[],
        help="Limit fetches to selected providers",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Limit fetches to selected source ids from config.toml",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Poll selected sources even if their configured interval has not elapsed",
    )
    return parser.parse_args()


def _load_config(args: argparse.Namespace) -> AppConfig:
    overrides = {}
    if args.config_file:
        overrides["AGENT_USAGE_CONFIG_FILE"] = args.config_file
    if args.env_file:
        overrides["AGENT_USAGE_ENV_FILE"] = args.env_file
    if args.state_file:
        overrides["AGENT_USAGE_STATE_FILE"] = args.state_file
    return load_config(overrides)


def _parse_timestamp(raw: object) -> datetime | None:
    if raw is None:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _source_is_due(source: SourceConfig, latest_attempt: dict[str, object] | None, force: bool) -> bool:
    if force or source.interval_seconds <= 0:
        return True
    if not latest_attempt:
        return True
    fetched_at = _parse_timestamp(latest_attempt.get("fetched_at"))
    if not fetched_at:
        return True
    elapsed_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    return elapsed_seconds >= source.interval_seconds


def _should_run_source(
    source: SourceConfig,
    selected_providers: list[str],
    selected_sources: list[str],
    latest_attempt: dict[str, object] | None,
    force: bool,
) -> bool:
    if selected_sources and source.source_id not in selected_sources:
        return False
    if selected_providers and source.provider not in selected_providers:
        return False
    if not source.enabled:
        return False
    return _source_is_due(source, latest_attempt, force)


def _write_compat_state(cfg: AppConfig, client: PostgresClient, history_days: int, path: Path | None) -> None:
    path = path or cfg.state_path
    contract = client.build_compat_state(history_days=history_days, sources=cfg.sources)
    write_state_file(path, contract)
    print(f"[agent-usage-poll] wrote state file: {path}")


def main() -> int:
    args = parse_args()
    cfg = _load_config(args)
    selected_providers = args.provider or []
    selected_sources = args.source or []

    client = PostgresClient(cfg.db_dsn)
    client.ping()
    latest_attempts = {
        row.get("source_id"): row
        for row in client.latest_attempts(source_ids=[source.source_id for source in cfg.sources] or None)
        if isinstance(row, dict)
    }

    snapshots: list[ProviderSnapshot] = []
    for source in cfg.sources:
        latest_attempt = latest_attempts.get(source.source_id)
        matches_selection = (
            (not selected_sources or source.source_id in selected_sources)
            and (not selected_providers or source.provider in selected_providers)
        )
        if not _should_run_source(source, selected_providers, selected_sources, latest_attempt, args.force):
            if source.enabled and matches_selection:
                print(f"[agent-usage-poll] {source.source_id}: skipped; interval not due")
            continue

        try:
            snapshot = run_fetch(cfg, source.provider, source=source)
            fetch_id = client.persist_snapshot(snapshot)
            if source.provider == "cursor" and snapshot.success:
                sync_stats = sync_cursor_usage_events(cfg, client, fetch_id, snapshot)
                print(
                    f"[agent-usage-poll] {source.source_id} cursor usage-events:"
                    f" pages={sync_stats['pages_fetched']}"
                    f" inserted={sync_stats['inserted']}"
                    f" total={sync_stats['total_events']}"
                )
            snapshots.append(snapshot)
            print(
                f"[agent-usage-poll] {source.source_id} ({source.provider}): "
                f"status={snapshot.request_status}, success={snapshot.success}, metrics={len(snapshot.metrics)}"
            )
        except Exception as exc:
            print(f"[agent-usage-poll] {source.source_id} ({source.provider}): error={exc}", file=sys.stderr)
            continue

    contract = client.build_compat_state(history_days=args.history_days, sources=cfg.sources)
    if args.print_state:
        print(json.dumps(contract, indent=2, ensure_ascii=False))
    if not args.print_state:
        _write_compat_state(cfg, client, args.history_days, cfg.state_path if not args.state_file else Path(args.state_file))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
