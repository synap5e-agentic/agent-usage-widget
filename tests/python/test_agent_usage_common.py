from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "poller"))

import agent_usage_common as common  # noqa: E402


def epoch(raw: str) -> int:
    return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())


def metric(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "metric_key": "seven_day",
        "provider_metric_key": "seven_day",
        "metric_path": "/seven_day",
        "metric_label": "This week",
        "percent": 42,
        "max_value": 100,
        "window_start": "2026-04-01T00:00:00+00:00",
        "window_end": "2026-04-08T00:00:00+00:00",
        "reset_at": "2026-04-08T00:00:00+00:00",
        "details": {},
    }
    value.update(overrides)
    return value


def test_normalize_graph_points_filters_to_active_window_without_synthetic_zero() -> None:
    points = [
        {"t": epoch("2026-03-31T23:55:00+00:00"), "value": 98},
        {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 4},
        {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 8},
        {"t": epoch("2026-04-08T00:05:00+00:00"), "value": 15},
    ]

    normalized = common._normalize_graph_points(
        points,
        window_start="2026-04-01T00:00:00+00:00",
        window_end="2026-04-08T00:00:00+00:00",
    )

    assert normalized == [
        {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 4},
        {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 8},
    ]


def test_normalize_graph_points_sorts_and_collapses_duplicate_timestamps() -> None:
    normalized = common._normalize_graph_points(
        [
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 12},
            {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 4},
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 15},
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 14},
        ]
    )

    assert normalized == [
        {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 4},
        {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 15},
    ]


def test_graph_from_metric_reanchors_rolled_window_without_bridging_old_cycle() -> None:
    graph = common._graph_from_metric(
        metric(
            window_start="2026-04-08T00:00:00+00:00",
            window_end="2026-04-15T00:00:00+00:00",
            reset_at="2026-04-15T00:00:00+00:00",
        ),
        [
            {"t": epoch("2026-04-07T23:50:00+00:00"), "value": 98},
            {"t": epoch("2026-04-08T00:05:00+00:00"), "value": 1},
            {"t": epoch("2026-04-08T01:00:00+00:00"), "value": 5},
            {"t": epoch("2026-04-15T00:01:00+00:00"), "value": 7},
        ],
    )

    assert graph["window_start"] == "2026-04-08T00:00:00+00:00"
    assert graph["window_end"] == "2026-04-15T00:00:00+00:00"
    assert graph["points"] == [
        {"t": epoch("2026-04-08T00:05:00+00:00"), "value": 1},
        {"t": epoch("2026-04-08T01:00:00+00:00"), "value": 5},
    ]


def test_graph_from_metric_preserves_sparse_history_and_window_metadata() -> None:
    graph = common._graph_from_metric(
        metric(),
        [
            {"t": epoch("2026-04-03T12:00:00+00:00"), "value": "31.4"},
            {"t": epoch("2026-04-04T12:00:00+00:00"), "value": 37},
        ],
    )

    assert graph["window_start"] == "2026-04-01T00:00:00+00:00"
    assert graph["window_end"] == "2026-04-08T00:00:00+00:00"
    assert graph["points"] == [
        {"t": epoch("2026-04-03T12:00:00+00:00"), "value": 31},
        {"t": epoch("2026-04-04T12:00:00+00:00"), "value": 37},
    ]


def test_graph_from_metric_allows_empty_and_single_point_series() -> None:
    empty_graph = common._graph_from_metric(metric(), [])
    single_graph = common._graph_from_metric(
        metric(),
        [{"t": epoch("2026-04-01T00:00:00+00:00"), "value": 1}],
    )

    assert empty_graph["points"] == []
    assert single_graph["points"] == [{"t": epoch("2026-04-01T00:00:00+00:00"), "value": 1}]


def test_provider_status_marks_auth_failure_as_stale() -> None:
    status = common._provider_status(
        "claude",
        {"fetched_at": "2026-04-24T10:39:36+12:00"},
        {
            "fetched_at": "2026-04-24T12:41:24+12:00",
            "success": False,
            "http_status": 403,
            "raw_payload": {
                "error": {
                    "message": "Invalid authorization",
                    "details": {"error_code": "account_session_invalid"},
                }
            },
        },
    )

    assert status["state"] == "error"
    assert status["stale"] is True
    assert status["error_code"] == "account_session_invalid"
    assert status["label"] == "Sign-in expired"
    assert "showing data from" in status["message"].lower()


def test_claude_auth_headers_support_cookie_only_mode() -> None:
    cfg = common.load_config(
        {
            "AGENT_USAGE_CLAUDE_COOKIE": (
                "anthropic-device-id=device-123; "
                "ajs_anonymous_id=anon-456; "
                "sessionKey=session-789; "
                "lastActiveOrg=org-abc"
            ),
            "AGENT_USAGE_CLAUDE_ORGANIZATION_ID": "",
            "AGENT_USAGE_CLAUDE_ANONYMOUS_ID": "",
            "AGENT_USAGE_CLAUDE_DEVICE_ID": "",
            "AGENT_USAGE_CLAUDE_SESSION_KEY": "",
        }
    )

    url, headers = common._auth_headers(cfg, "claude")

    assert url == "https://claude.ai/api/organizations/org-abc/usage"
    assert headers["cookie"].startswith("anthropic-device-id=device-123;")
    assert headers["anthropic-anonymous-id"] == "anon-456"
    assert headers["anthropic-device-id"] == "device-123"


def test_normalize_claude_uses_org_from_cookie_or_url() -> None:
    cfg = common.load_config(
        {
            "AGENT_USAGE_CLAUDE_COOKIE": "lastActiveOrg=org-cookie",
            "AGENT_USAGE_CLAUDE_ORGANIZATION_ID": "",
        }
    )

    snapshot = common.normalize_claude(
        {"seven_day": {"utilization": 6.0, "resets_at": "2026-04-25T02:00:01+12:00"}},
        cfg,
        "https://claude.ai/api/organizations/org-cookie/usage",
        200,
        None,
    )

    assert snapshot.account_id == "org-cookie"
    assert snapshot.organization_id == "org-cookie"


def test_codex_auth_headers_support_cookie_only_mode() -> None:
    cfg = common.load_config(
        {
            "AGENT_USAGE_CODEX_AUTHORIZATION": "Bearer token-123",
            "AGENT_USAGE_CODEX_COOKIE": (
                "oai-did=device-abc; "
                "oai-session-id=session-def; "
                "__Secure-next-auth.session-token=placeholder"
            ),
            "AGENT_USAGE_CODEX_ACCOUNT_ID": "",
            "AGENT_USAGE_CODEX_DEVICE_ID": "",
            "AGENT_USAGE_CODEX_SESSION_ID": "",
        }
    )

    url, headers = common._auth_headers(cfg, "codex")

    assert url == "https://chatgpt.com/backend-api/wham/usage"
    assert headers["authorization"] == "Bearer token-123"
    assert headers["oai-device-id"] == "device-abc"
    assert headers["oai-session-id"] == "session-def"
    assert headers["cookie"].startswith("oai-did=device-abc;")


def test_normalize_codex_uses_payload_identity_without_config_account() -> None:
    cfg = common.load_config(
        {
            "AGENT_USAGE_CODEX_ACCOUNT_ID": "",
        }
    )

    snapshot = common.normalize_codex(
        {
            "account_id": "acct-live",
            "user_id": "user-live",
            "rate_limit": {
                "secondary_window": {
                    "used": 6,
                    "limit": 100,
                    "resets_at": "2026-04-25T02:00:01+12:00",
                }
            },
        },
        cfg,
        "https://chatgpt.com/backend-api/wham/usage",
        200,
        None,
    )

    assert snapshot.account_id == "acct-live"
    assert snapshot.organization_id == "user-live"


def test_currency_graph_max_uses_latest_history_value() -> None:
    graph = common._graph_from_metric(
        metric(
            metric_key="auto_spend",
            metric_path="/_aggregated_usage_events/aggregations/0",
            max_value=100,
            details={"graph_value_kind": "currency_cents", "graph_pace_line": False},
        ),
        [
            {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 1000},
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 2125},
        ],
    )

    assert graph["value_kind"] == "currency_cents"
    assert graph["pace_line"] is False
    assert graph["max_value"] == common._currency_graph_max(2125)


class RecordingClient(common.PostgresClient):
    def __init__(self, response: Any = None):
        super().__init__("postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage")
        self.response = [] if response is None else response
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def _run(self, sql: str, vars: dict[str, str] | None = None) -> str:
        self.calls.append((sql, vars))
        return json.dumps(self.response)


def test_history_points_scopes_identity_and_can_use_value_num() -> None:
    client = RecordingClient(response=[{"t": 1775001600, "value": 525}])

    points = client.history_points(
        "cursor",
        "/_aggregated_usage_events/aggregations/0",
        30,
        window_start="2026-04-01T00:00:00+00:00",
        window_end="2026-05-01T00:00:00+00:00",
        account_id="cursor_user_abc",
        organization_id="team_123",
        use_value_num=True,
    )

    sql, vars = client.calls[-1]
    assert points == [{"t": 1775001600, "value": 525}]
    assert "ROUND(COALESCE(m.value_num, 0))::bigint AS value" in sql
    assert "AND f.account_id = :'account_id'" in sql
    assert "AND f.organization_id = :'organization_id'" in sql
    assert vars is not None
    assert vars["account_id"] == "cursor_user_abc"
    assert vars["organization_id"] == "team_123"


class CursorSpendClient(common.PostgresClient):
    def __init__(self, points: list[dict[str, Any]]):
        super().__init__("postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage")
        self.points = points
        self.scope: dict[str, Any] = {}

    def cursor_usage_cumulative_points(
        self,
        cycle_end: str | None,
        model: str = "default",
        account_id: str | None = None,
        organization_id: str | None = None,
    ) -> list[dict[str, Any]]:
        self.scope = {
            "cycle_end": cycle_end,
            "model": model,
            "account_id": account_id,
            "organization_id": organization_id,
        }
        return list(self.points)


def test_cursor_auto_spend_points_append_monotonic_snapshot_and_scope_events() -> None:
    client = CursorSpendClient([{"t": epoch("2026-04-02T01:00:00+00:00"), "value": 300}])

    points = client.cursor_auto_spend_points(
        {
            "window_end": "2026-05-01T00:00:00+00:00",
            "value_num": 450,
            "details": {"graph_model": "default"},
        },
        {
            "fetched_at": "2026-04-02T02:00:00+00:00",
            "account_id": "cursor_user_abc",
            "organization_id": "team_123",
        },
    )

    assert client.scope == {
        "cycle_end": "2026-05-01T00:00:00+00:00",
        "model": "default",
        "account_id": "cursor_user_abc",
        "organization_id": "team_123",
    }
    assert points == [
        {"t": epoch("2026-04-02T01:00:00+00:00"), "value": 300},
        {"t": epoch("2026-04-02T02:00:00+00:00"), "value": 450},
    ]


def test_cursor_auto_spend_points_does_not_append_lower_snapshot_after_event_stream() -> None:
    client = CursorSpendClient([{"t": epoch("2026-04-02T01:00:00+00:00"), "value": 500}])

    points = client.cursor_auto_spend_points(
        {"window_end": "2026-05-01T00:00:00+00:00", "value_num": 450, "details": {}},
        {"fetched_at": "2026-04-02T02:00:00+00:00"},
    )

    assert points == [{"t": epoch("2026-04-02T01:00:00+00:00"), "value": 500}]


def test_normalize_cursor_derives_stable_non_legacy_identity_from_payload_or_cookie() -> None:
    cfg = common.load_config({"AGENT_USAGE_CURSOR_COOKIE": "session=secret"})
    snapshot = common.normalize_cursor(
        {
            "userId": "user_123",
            "teamUsage": {"teamId": "team_456"},
            "billingCycleEnd": "2026-05-01T00:00:00+00:00",
            "individualUsage": {
                "plan": {
                    "used": 100,
                    "limit": 2000,
                    "breakdown": {"included": 100, "bonus": 0, "total": 100},
                }
            },
        },
        cfg,
        "https://cursor.com/api/usage-summary",
        200,
        None,
    )

    assert snapshot.account_id == "user_123"
    assert snapshot.organization_id == "team_456"


def test_fingerprint_identity_is_deterministic_and_not_plain_cookie() -> None:
    first = common._fingerprint_identity("cursor_user", "session=secret")
    second = common._fingerprint_identity("cursor_user", "session=secret")

    assert first == second
    assert first.startswith("cursor_user_")
    assert "secret" not in first
