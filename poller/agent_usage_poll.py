#!/usr/bin/env python3
"""Poll and persist live usage snapshots for Claude, Codex, and Cursor."""

from __future__ import annotations

import argparse
import json
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
    load_config,
    run_fetch,
    sync_cursor_usage_events,
    write_state_file,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
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
    return parser.parse_args()


def _load_config(args: argparse.Namespace) -> AppConfig:
    overrides = {}
    if args.env_file:
        overrides["AGENT_USAGE_ENV_FILE"] = args.env_file
    if args.state_file:
        overrides["AGENT_USAGE_STATE_FILE"] = args.state_file
    return load_config(overrides)


def _should_run_provider(name: str, selected: list[str], cfg: AppConfig) -> bool:
    if selected and name not in selected:
        return False
    if name == "claude":
        return cfg.claude_enabled
    if name == "codex":
        return cfg.codex_enabled
    if name == "cursor":
        return cfg.cursor_enabled
    return False


def _write_compat_state(cfg: AppConfig, client: PostgresClient, history_days: int, path: Path | None) -> None:
    path = path or cfg.state_path
    contract = client.build_compat_state(history_days=history_days)
    write_state_file(path, contract)
    print(f"[agent-usage-poll] wrote state file: {path}")


def main() -> int:
    args = parse_args()
    cfg = _load_config(args)
    providers = ("claude", "codex", "cursor")
    selected = args.provider or list(providers)

    client = PostgresClient(cfg.db_dsn)
    client.ping()

    snapshots: list[ProviderSnapshot] = []
    for provider in providers:
        if not _should_run_provider(provider, selected, cfg):
            continue

        try:
            snapshot = run_fetch(cfg, provider)
            fetch_id = client.persist_snapshot(snapshot)
            if provider == "cursor" and snapshot.success:
                sync_stats = sync_cursor_usage_events(cfg, client, fetch_id, snapshot)
                print(
                    "[agent-usage-poll] cursor usage-events:"
                    f" pages={sync_stats['pages_fetched']}"
                    f" inserted={sync_stats['inserted']}"
                    f" total={sync_stats['total_events']}"
                )
            snapshots.append(snapshot)
            print(
                f"[agent-usage-poll] {provider}: status={snapshot.request_status}, success={snapshot.success}, "
                f"metrics={len(snapshot.metrics)}"
            )
        except Exception as exc:
            print(f"[agent-usage-poll] {provider}: error={exc}", file=sys.stderr)
            continue

    contract = client.build_compat_state(history_days=args.history_days)
    if args.print_state:
        print(json.dumps(contract, indent=2, ensure_ascii=False))
    if not args.print_state:
        _write_compat_state(cfg, client, args.history_days, cfg.state_path if not args.state_file else Path(args.state_file))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
