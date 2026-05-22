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


def test_collect_metric_rows_infers_new_zero_window_when_reset_timestamp_is_missing() -> None:
    rows = common._collect_metric_rows(
        "claude",
        {"five_hour": {"utilization": 0.0, "resets_at": None}},
        fetched_at="2026-04-30T01:50:10+00:00",
    )

    metric = next(row for row in rows if row["metric_key"] == "five_hour")
    assert metric["percent"] == 0
    assert metric["window_start"] == "2026-04-30T01:50:10+00:00"
    assert metric["window_end"] == "2026-04-30T06:50:10+00:00"
    assert metric["reset_at"] == "2026-04-30T06:50:10+00:00"


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


def test_pick_metric_by_selector_prefers_exact_metric_key_over_spark_alias_path() -> None:
    metrics = [
        metric(
            metric_key="spark_usage",
            provider_metric_key="secondary_window",
            metric_path="/additional_rate_limits/0/rate_limit/secondary_window",
            metric_label="Spark this week",
        ),
        metric(
            metric_key="secondary_window",
            provider_metric_key="secondary_window",
            metric_path="/rate_limit/secondary_window",
            metric_label="This week",
        ),
    ]

    picked = common._pick_metric_by_selector(metrics, "secondary_window")

    assert picked is not None
    assert picked["metric_key"] == "secondary_window"
    assert picked["metric_path"] == "/rate_limit/secondary_window"


def test_provider_default_frontend_for_codex_only_shows_current_windows() -> None:
    frontend = common._provider_default_frontend("codex")

    assert frontend["show_metrics"] == ["secondary_window", "primary_window"]
    assert frontend["show_graphs"] == ["secondary_window", "primary_window"]


def test_metric_selector_index_does_not_treat_spark_aliases_as_visible_window_metrics() -> None:
    spark_metric = metric(
        metric_key="spark_usage",
        provider_metric_key="secondary_window",
        metric_path="/additional_rate_limits/0/rate_limit/secondary_window",
        metric_label="Spark this week",
    )

    assert common._metric_selector_index(spark_metric, ["secondary_window", "primary_window"]) is None
    assert common._metric_visible(spark_metric, ["secondary_window", "primary_window"]) is False


def test_cursor_monthly_metric_uses_non_auto_event_total_when_available() -> None:
    monthly_metric = metric(
        metric_key="monthly",
        provider_metric_key="included",
        metric_path="/individualUsage/breakdown/included",
        percent=100,
        value="1577%",
        value_num=31547,
        details={
            "limit_cents": 2000,
            "total_spend_cents": 31547,
            "graph_reference_value": 100,
        },
    )

    adjusted = common._cursor_adjust_monthly_metric_from_non_auto_events(
        monthly_metric,
        [
            {"t": epoch("2026-04-05T14:10:00+00:00"), "value": 21},
            {"t": epoch("2026-04-30T10:00:00+00:00"), "value": 4832},
        ],
    )
    points = common._cursor_monthly_points_from_non_auto_events(
        adjusted,
        [
            {"t": epoch("2026-04-05T14:10:00+00:00"), "value": 21},
            {"t": epoch("2026-04-30T10:00:00+00:00"), "value": 4832},
        ],
    )

    assert adjusted["value_num"] == 4832
    assert adjusted["percent"] == 242
    assert adjusted["value"] == "242%"
    assert points[-1]["value"] == 242


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


def test_load_config_reads_toml_sources_and_defaults(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        """
[service]
host = "127.0.0.1"
port = 8786

[poller]
default_interval_seconds = 60

[frontend]
columns = 2

[sources.personal]
provider = "claude"
label = "Claude Personal"

[sources.personal.frontend]
order = 10
short_label = "CP"
show_metrics = ["seven_day", "five_hour", "extra_usage"]
show_graphs = ["seven_day", "five_hour"]
highlight_metric = "extra_usage"
highlight_window_minutes = 34

[sources.personal.auth]
cookie = "lastActiveOrg=org-personal; sessionKey=session"

[sources.work]
provider = "claude"
frontend_visible = false
interval_seconds = 1800

[sources.work.auth]
cookie = "lastActiveOrg=org-work; sessionKey=session"
""".strip(),
        encoding="utf-8",
    )

    cfg = common.load_config(
        {
            "AGENT_USAGE_CONFIG_FILE": str(config_file),
            "AGENT_USAGE_ENV_FILE": str(tmp_path / "missing.env"),
        }
    )

    assert cfg.service_port == 8786
    assert cfg.poller_default_interval_seconds == 60
    assert cfg.frontend.columns == 2
    assert [source.source_id for source in cfg.sources] == ["personal", "work"]
    assert cfg.sources[0].label == "Claude Personal"
    assert cfg.sources[0].frontend_visible is True
    assert cfg.sources[0].enabled is True
    assert cfg.sources[0].interval_seconds == 60
    assert cfg.sources[0].frontend.order == 10
    assert cfg.sources[0].frontend.short_label == "CP"
    assert cfg.sources[0].frontend.show_metrics == ("seven_day", "five_hour", "extra_usage")
    assert cfg.sources[0].frontend.show_graphs == ("seven_day", "five_hour")
    assert cfg.sources[0].frontend.highlight_metric == "extra_usage"
    assert cfg.sources[0].frontend.highlight_window_minutes == 34
    assert cfg.sources[1].label == "work"
    assert cfg.sources[1].frontend_visible is False
    assert cfg.sources[1].interval_seconds == 1800
    assert cfg.claude_cookie.startswith("lastActiveOrg=org-personal")


def test_postgres_client_parses_query_string_socket_dsn() -> None:
    client = common.PostgresClient(
        "postgresql:///agent_usage?host=/run/user/1000/local-postgres&port=5433"
    )

    assert client._parsed_dsn is not None  # noqa: SLF001
    assert client._parsed_dsn.host == "/run/user/1000/local-postgres"  # noqa: SLF001
    assert client._parsed_dsn.port == "5433"  # noqa: SLF001
    assert client._parsed_dsn.dbname == "agent_usage"  # noqa: SLF001
    assert "-h" in client._psql_cmd()  # noqa: SLF001
    assert "/run/user/1000/local-postgres" in client._psql_cmd()  # noqa: SLF001
    assert "5433" in client._psql_cmd()  # noqa: SLF001


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


def test_normalize_claude_extra_usage_as_money_metric() -> None:
    cfg = common.load_config(
        {
            "AGENT_USAGE_CLAUDE_COOKIE": "lastActiveOrg=org-cookie",
            "AGENT_USAGE_CLAUDE_ORGANIZATION_ID": "",
        }
    )

    snapshot = common.normalize_claude(
        {
            "seven_day": {"utilization": 6.0},
            "extra_usage": {
                "is_enabled": True,
                "monthly_limit": None,
                "used_credits": 1917.0,
                "utilization": None,
                "currency": "USD",
            },
        },
        cfg,
        "https://claude.ai/api/organizations/org-cookie/usage",
        200,
        None,
    )

    extra = next(metric for metric in snapshot.metrics if metric["metric_key"] == "extra_usage")
    assert extra["metric_path"] == "/extra_usage"
    assert extra["metric_label"] == "Extra usage"
    assert extra["value_num"] == 1917.0
    assert extra["value"] == "$19.17"
    assert extra["percent"] == 0
    assert extra["details"]["graph_value_kind"] == "currency_cents"
    assert extra["details"]["currency"] == "USD"


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


def test_format_period_note_uses_separate_lines() -> None:
    note = common._format_period_note(
        "2026-04-01T00:00:00+00:00",
        "2026-05-01T00:00:00+00:00",
    )

    assert "\n" in note
    first, second = note.split("\n", 1)
    assert first.startswith("Started at 2026-04-01 ")
    assert second.startswith("Resets at 2026-05-01 ")


def test_percent_graph_max_expands_above_included_cap_and_keeps_reference_value() -> None:
    graph = common._graph_from_metric(
        metric(
            metric_key="monthly",
            metric_path="/individualUsage/breakdown/included",
            percent=138,
            max_value=100,
            details={"graph_reference_value": 100},
        ),
        [
            {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 95},
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 121},
            {"t": epoch("2026-04-01T03:00:00+00:00"), "value": 138},
        ],
    )

    assert graph["value_kind"] == "percent"
    assert graph["reference_value"] == 100
    assert graph["max_value"] == 150


def test_normalize_cursor_monthly_uses_total_spend_percent_and_keeps_over_cap_money() -> None:
    snapshot = common.normalize_cursor(
        {
            "billingCycleStart": "2026-04-01T00:00:00+00:00",
            "billingCycleEnd": "2026-05-01T00:00:00+00:00",
            "membershipType": "pro",
            "individualUsage": {
                "plan": {
                    "used": 2000,
                    "limit": 2000,
                    "totalPercentUsed": 100,
                    "breakdown": {
                        "included": 2000,
                        "bonus": 760,
                        "total": 2760,
                    },
                },
            },
        },
        common.load_config({"AGENT_USAGE_CURSOR_COOKIE": "WorkosCursorSessionToken=token"}),
        "https://cursor.com/api/usage-summary",
        200,
        None,
    )

    monthly = next(metric for metric in snapshot.metrics if metric["metric_key"] == "monthly")
    over_cap = next(metric for metric in snapshot.metrics if metric["metric_key"] == "over_cap_used")

    assert monthly["percent"] == 100
    assert monthly["value"] == "138%"
    assert monthly["value_num"] == 2760
    assert monthly["max_value"] == 150
    assert monthly["details"]["graph_reference_value"] == 100
    assert over_cap["value"] == "$7.60"


def test_cursor_adjust_monthly_metric_and_points_reconstructs_over_cap_series() -> None:
    monthly_metric = metric(
        metric_key="monthly",
        metric_path="/individualUsage/breakdown/included",
        percent=100,
        value_num=2000,
        value="100%",
        max_value=100,
        details={"limit_cents": 2000, "included_cents": 2000},
    )
    over_cap_metric = metric(
        metric_key="over_cap_used",
        metric_path="/individualUsage/breakdown/bonus",
        value_num=760,
        value="$7.60",
    )

    adjusted_metric, adjusted_points = common._cursor_adjust_monthly_metric_and_points(
        monthly_metric,
        over_cap_metric,
        [
            {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 95},
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 100},
        ],
        [
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 0},
            {"t": epoch("2026-04-01T03:00:00+00:00"), "value": 760},
        ],
    )

    assert adjusted_metric["percent"] == 138
    assert adjusted_metric["value"] == "138%"
    assert adjusted_metric["value_num"] == 2760
    assert adjusted_metric["max_value"] == 150
    assert adjusted_metric["details"]["graph_reference_value"] == 100
    assert adjusted_points == [
        {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 95},
        {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 100},
        {"t": epoch("2026-04-01T03:00:00+00:00"), "value": 138},
    ]


def test_cursor_monthly_points_from_total_spend_uses_real_ramp() -> None:
    monthly_metric = metric(
        metric_key="monthly",
        metric_path="/individualUsage/breakdown/included",
        details={"limit_cents": 2000},
    )

    points = common._cursor_monthly_points_from_total_spend(
        monthly_metric,
        [
            {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 250},
            {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 1000},
            {"t": epoch("2026-04-01T03:00:00+00:00"), "value": 2000},
            {"t": epoch("2026-04-01T04:00:00+00:00"), "value": 2760},
        ],
    )

    assert points == [
        {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 12},
        {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 50},
        {"t": epoch("2026-04-01T03:00:00+00:00"), "value": 100},
        {"t": epoch("2026-04-01T04:00:00+00:00"), "value": 138},
    ]


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
    assert "source_id" not in vars
    assert vars["account_id"] == "cursor_user_abc"
    assert vars["organization_id"] == "team_123"


def test_history_points_can_scope_by_source_id() -> None:
    client = RecordingClient(response=[{"t": 1775001600, "value": 42}])

    points = client.history_points(
        "claude",
        "/seven_day",
        30,
        source_id="personal",
    )

    sql, vars = client.calls[-1]
    assert points == [{"t": 1775001600, "value": 42}]
    assert "AND f.source_id = :'source_id'" in sql
    assert vars is not None
    assert vars["source_id"] == "personal"


def test_cursor_usage_cumulative_points_deduplicates_streaming_updates() -> None:
    client = RecordingClient(response=[{"t": 1775001600, "value": 49}])

    points = client.cursor_usage_cumulative_points(
        "2026-06-05T02:10:02+00:00",
        model="default",
        source_id="cursor",
    )

    sql, vars = client.calls[-1]
    assert points == [{"t": 1775001600, "value": 49}]
    assert "WITH deduped AS" in sql
    assert "MAX(COALESCE(e.charged_cents, 0)) AS charged_cents" in sql
    assert "GROUP BY e.event_timestamp_ms, COALESCE(e.model, '')" in sql
    assert vars is not None
    assert vars["cycle_end"] == "2026-06-05T02:10:02+00:00"
    assert vars["model"] == "default"
    assert vars["source_id"] == "cursor"


class CursorSpendClient(common.PostgresClient):
    def __init__(self, points: list[dict[str, Any]]):
        super().__init__("postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage")
        self.points = points
        self.scope: dict[str, Any] = {}

    def cursor_usage_cumulative_points(
        self,
        cycle_end: str | None,
        model: str = "default",
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
        exclude_model: str | None = None,
    ) -> list[dict[str, Any]]:
        self.scope = {
            "cycle_end": cycle_end,
            "model": model,
            "source_id": source_id,
            "account_id": account_id,
            "organization_id": organization_id,
            "exclude_model": exclude_model,
        }
        return list(self.points)


class CursorSyncRateLimitedClient(common.PostgresClient):
    def __init__(self):
        super().__init__("postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage")
        self.insert_calls = 0
        self.update_calls = 0

    def latest_cursor_usage_sync_through(self, cycle_end: str, source_id: str = "") -> int:
        return 0

    def latest_cursor_usage_total_count(self, cycle_end: str, source_id: str = "") -> int:
        return 0

    def latest_cursor_usage_timestamp(self, cycle_end: str, source_id: str = "") -> int:
        return 0

    def insert_cursor_usage_events(
        self,
        provider_fetch_id: int,
        source_id: str,
        cycle_start: str,
        cycle_end: str,
        page: int,
        events: list[dict[str, Any]],
    ) -> int:
        self.insert_calls += 1
        return len(events)

    def update_cursor_usage_sync_state(
        self,
        source_id: str,
        cycle_start: str,
        cycle_end: str,
        synced_through_timestamp_ms: int,
        total_usage_events_count: int,
        last_page_fetched: int,
        last_inserted_count: int,
    ) -> None:
        self.update_calls += 1


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
        "source_id": "",
        "account_id": "cursor_user_abc",
        "organization_id": "team_123",
        "exclude_model": None,
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


def test_sync_cursor_usage_events_stops_cleanly_on_rate_limit(monkeypatch: Any) -> None:
    client = CursorSyncRateLimitedClient()
    cfg = common.load_config({"AGENT_USAGE_CURSOR_COOKIE": "WorkosCursorSessionToken=token"})
    snapshot = common.ProviderSnapshot(
        provider="cursor",
        account_id="user_123",
        organization_id="team_456",
        metrics=[],
        summary_key="monthly",
        history_key="monthly",
        history_label="This Month",
        details=[],
        raw_payload={
            "billingCycleStart": "2026-04-01T00:00:00+00:00",
            "billingCycleEnd": "2026-05-01T00:00:00+00:00",
        },
        request_url="https://cursor.com/api/usage-summary",
        request_status=200,
        request_error=None,
        request_metadata={},
        success=True,
        source_id="cursor",
        source_label="Cursor",
        frontend_visible=True,
    )

    monkeypatch.setattr(
        common,
        "fetch_cursor_usage_events_page",
        lambda *args, **kwargs: (429, {}, "rate limited"),
    )

    stats = common.sync_cursor_usage_events(cfg, client, provider_fetch_id=1, snapshot=snapshot)

    assert stats["pages_fetched"] == 0
    assert stats["inserted"] == 0
    assert client.insert_calls == 0
    assert client.update_calls == 0


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


def test_postgres_dsn_query_host_port_are_used_for_psql_command() -> None:
    client = common.PostgresClient(
        "postgresql:///agent_usage"
        "?host=/run/user/1000/local-postgres"
        "&port=5433"
        "&user=agent_usage"
        "&password=secret"
    )

    cmd = client._psql_cmd()
    assert cmd[cmd.index("-h") + 1] == "/run/user/1000/local-postgres"
    assert cmd[cmd.index("-p") + 1] == "5433"
    assert cmd[cmd.index("-d") + 1] == "agent_usage"
    assert cmd[cmd.index("-U") + 1] == "agent_usage"
    assert client._psql_env()["PGPASSWORD"] == "secret"


class CurrentContractClient(common.PostgresClient):
    def __init__(self):
        super().__init__("postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage")
        self.calls: list[tuple[str, str, bool]] = []
        self.event_calls: list[dict[str, Any]] = []

    def latest_fetch(
        self,
        source_ids: list[str] | tuple[str, ...] | None = None,
        providers: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            {
                "id": 10,
                "source_id": "cursor",
                "provider": "cursor",
                "fetched_at": "2026-04-25T04:39:27+00:00",
                "account_id": "cursor_user_abc",
                "organization_id": "team_123",
                "request_metadata": {"summary_key": "monthly", "history_key": "monthly", "plan": "Pro"},
                "request_error": "",
            }
        ]

    def latest_attempts(
        self,
        source_ids: list[str] | tuple[str, ...] | None = None,
        providers: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            {
                "id": 10,
                "source_id": "cursor",
                "provider": "cursor",
                "fetched_at": "2026-04-25T04:39:27+00:00",
                "success": True,
                "http_status": 200,
                "account_id": "cursor_user_abc",
                "organization_id": "team_123",
                "request_metadata": {},
                "request_error": "",
                "raw_payload": {},
            }
        ]

    def latest_metrics(self, fetch_id: int) -> list[dict[str, Any]]:
        assert fetch_id == 10
        return [
            {
                "metric_key": "monthly",
                "provider_metric_key": "included",
                "metric_path": "/individualUsage/breakdown/included",
                "metric_scope": "/individualUsage",
                "metric_label": "Monthly usage",
                "percent": 100,
                "value_num": 2760,
                "value_text": "138%",
                "note": "Started at 2026-04-01 00:00 | Resets at 2026-05-01 00:00",
                "max_value": 150,
                "window_start": "2026-04-01T00:00:00+00:00",
                "window_end": "2026-05-01T00:00:00+00:00",
                "reset_at": "2026-05-01T00:00:00+00:00",
                "details": {"limit_cents": 2000, "graph_reference_value": 100, "total_spend_cents": 2760},
            },
            {
                "metric_key": "total_spend",
                "provider_metric_key": "total",
                "metric_path": "/individualUsage/breakdown/total",
                "metric_scope": "/individualUsage/breakdown",
                "metric_label": "Total spend",
                "percent": 100,
                "value_num": 2760,
                "value_text": "$27.60",
                "note": "Total spend this cycle including any soft overage",
                "max_value": 2000,
                "window_start": "2026-04-01T00:00:00+00:00",
                "window_end": "2026-05-01T00:00:00+00:00",
                "reset_at": "2026-05-01T00:00:00+00:00",
                "details": {},
            },
            {
                "metric_key": "over_cap_used",
                "provider_metric_key": "bonus",
                "metric_path": "/individualUsage/breakdown/bonus",
                "metric_scope": "/individualUsage/breakdown",
                "metric_label": "Over cap used",
                "percent": 38,
                "value_num": 760,
                "value_text": "$7.60",
                "note": "Soft overage consumed above the included monthly cap",
                "max_value": 2000,
                "window_start": "2026-04-01T00:00:00+00:00",
                "window_end": "2026-05-01T00:00:00+00:00",
                "reset_at": "2026-05-01T00:00:00+00:00",
                "details": {},
            },
        ]

    def history_points(
        self,
        provider: str,
        metric_path: str,
        days: int,
        window_start: str | None = None,
        window_end: str | None = None,
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
        use_value_num: bool = False,
    ) -> list[dict[str, Any]]:
        self.calls.append((provider, metric_path, use_value_num))
        if metric_path == "/individualUsage/breakdown/included":
            return [
                {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 100},
                {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 100},
            ]
        if metric_path == "/individualUsage/breakdown/bonus":
            return [
                {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 0 if use_value_num else 0},
                {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 760 if use_value_num else 38},
            ]
        if metric_path == "/individualUsage/breakdown/total":
            return [
                {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 2000},
                {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 2760},
            ]
        return []

    def cursor_usage_cumulative_points(
        self,
        cycle_end: str | None,
        model: str = "default",
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
        exclude_model: str | None = None,
    ) -> list[dict[str, Any]]:
        self.event_calls.append(
            {
                "cycle_end": cycle_end,
                "model": model,
                "source_id": source_id,
                "account_id": account_id,
                "organization_id": organization_id,
                "exclude_model": exclude_model,
            }
        )
        if exclude_model == "default":
            return [
                {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 1450},
                {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 2760},
            ]
        return []


def test_build_current_contract_uses_service_source_total_spend_for_cursor_monthly_graph() -> None:
    client = CurrentContractClient()

    payload = client.build_current_contract(history_days=30)

    cursor = next(agent for agent in payload["agents"] if agent["id"] == "cursor")
    graph = cursor["graphs"]["long_window"]

    assert cursor["summary"]["value"] == "138%"
    assert cursor["summary"]["percent"] == 138
    assert graph["reference_value"] == 100
    assert graph["max_value"] == 150
    assert graph["points"] == [
        {"t": epoch("2026-04-01T01:00:00+00:00"), "value": 72},
        {"t": epoch("2026-04-01T02:00:00+00:00"), "value": 138},
    ]
    assert client.event_calls[-1]["exclude_model"] == "default"
    assert client.event_calls[-1]["source_id"] == "cursor"


class MultiClaudeClient(common.PostgresClient):
    def __init__(self):
        super().__init__("postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage")
        self.history_source_ids: list[str | None] = []

    def latest_fetch(
        self,
        source_ids: list[str] | tuple[str, ...] | None = None,
        providers: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            {
                "id": 1,
                "source_id": "personal",
                "provider": "claude",
                "fetched_at": "2026-04-25T01:00:00+00:00",
                "account_id": "org-personal",
                "organization_id": "org-personal",
                "request_metadata": {
                    "source_id": "personal",
                    "source_label": "Claude Personal",
                    "summary_key": "seven_day",
                    "history_key": "seven_day",
                },
                "request_error": "",
            },
            {
                "id": 2,
                "source_id": "work",
                "provider": "claude",
                "fetched_at": "2026-04-25T02:00:00+00:00",
                "account_id": "org-work",
                "organization_id": "org-work",
                "request_metadata": {
                    "source_id": "work",
                    "source_label": "Claude Work",
                    "summary_key": "seven_day",
                    "history_key": "seven_day",
                },
                "request_error": "",
            },
        ]
        if source_ids:
            rows = [row for row in rows if row["source_id"] in source_ids]
        if providers:
            rows = [row for row in rows if row["provider"] in providers]
        return rows

    def latest_attempts(
        self,
        source_ids: list[str] | tuple[str, ...] | None = None,
        providers: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            {
                **row,
                "success": True,
                "http_status": 200,
                "raw_payload": {},
            }
            for row in self.latest_fetch(source_ids=source_ids, providers=providers)
        ]

    def latest_metrics(self, fetch_id: int) -> list[dict[str, Any]]:
        percent = 21 if fetch_id == 1 else 64
        return [
            {
                "source_id": "personal" if fetch_id == 1 else "work",
                "metric_key": "seven_day",
                "provider_metric_key": "seven_day",
                "metric_path": "/seven_day",
                "metric_scope": "/",
                "metric_label": "This week",
                "percent": percent,
                "value_num": percent,
                "value_text": f"{percent}%",
                "note": "",
                "max_value": 100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {},
            }
        ]

    def history_points(
        self,
        provider: str,
        metric_path: str,
        days: int,
        window_start: str | None = None,
        window_end: str | None = None,
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
        use_value_num: bool = False,
    ) -> list[dict[str, Any]]:
        self.history_source_ids.append(source_id)
        return []


class ExtraUsagePolicyClient(MultiClaudeClient):
    def __init__(self):
        super().__init__()
        self.history_calls: list[dict[str, Any]] = []

    def latest_metrics(self, fetch_id: int) -> list[dict[str, Any]]:
        return [
            {
                "source_id": "personal",
                "metric_key": "seven_day",
                "provider_metric_key": "seven_day",
                "metric_path": "/seven_day",
                "metric_scope": "/",
                "metric_label": "This week",
                "percent": 21,
                "value_num": 21,
                "value_text": "21%",
                "note": "",
                "max_value": 100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {},
            },
            {
                "source_id": "personal",
                "metric_key": "five_hour",
                "provider_metric_key": "five_hour",
                "metric_path": "/five_hour",
                "metric_scope": "/",
                "metric_label": "5-hour window",
                "percent": 7,
                "value_num": 7,
                "value_text": "7%",
                "note": "",
                "max_value": 100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {},
            },
            {
                "source_id": "personal",
                "metric_key": "extra_usage",
                "provider_metric_key": "extra_usage",
                "metric_path": "/extra_usage",
                "metric_scope": "/",
                "metric_label": "Extra usage",
                "percent": 0,
                "value_num": 1917.0,
                "value_text": "$19.17",
                "note": "Extra usage billed in USD",
                "max_value": 2100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {"graph_value_kind": "currency_cents", "currency": "USD"},
            },
            {
                "source_id": "personal",
                "metric_key": "internal_noise",
                "provider_metric_key": "internal_noise",
                "metric_path": "/internal_noise",
                "metric_scope": "/",
                "metric_label": "Internal noise",
                "percent": 99,
                "value_num": 99,
                "value_text": "99%",
                "note": "",
                "max_value": 100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {},
            },
        ]

    def history_points(
        self,
        provider: str,
        metric_path: str,
        days: int,
        window_start: str | None = None,
        window_end: str | None = None,
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
        use_value_num: bool = False,
    ) -> list[dict[str, Any]]:
        self.history_calls.append(
            {
                "provider": provider,
                "metric_path": metric_path,
                "source_id": source_id,
                "use_value_num": use_value_num,
            }
        )
        return []

    def metric_recent_increase(
        self,
        provider: str,
        metric: str,
        window_minutes: int,
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "active": True,
            "metric": metric,
            "window_minutes": window_minutes,
            "delta": 125.0,
            "current_value": 1917.0,
            "increased_at": epoch("2026-04-25T01:55:00+00:00"),
        }


def test_build_current_contract_renders_multiple_sources_for_same_provider() -> None:
    client = MultiClaudeClient()
    sources = (
        common.SourceConfig("personal", "claude", "Claude Personal"),
        common.SourceConfig("work", "claude", "Claude Work"),
    )

    payload = client.build_current_contract(history_days=30, sources=sources)

    assert [agent["id"] for agent in payload["agents"]] == ["personal", "work"]
    assert [agent["provider"] for agent in payload["agents"]] == ["claude", "claude"]
    assert [agent["label"] for agent in payload["agents"]] == ["Claude Personal", "Claude Work"]
    assert [agent["summary"]["percent"] for agent in payload["agents"]] == [21, 64]
    assert client.history_source_ids == ["personal", "work"]


def test_build_current_contract_filters_frontend_invisible_sources() -> None:
    client = MultiClaudeClient()
    sources = (
        common.SourceConfig("personal", "claude", "Claude Personal"),
        common.SourceConfig("work", "claude", "Claude Work", frontend_visible=False),
    )

    payload = client.build_current_contract(history_days=30, sources=sources)

    assert [agent["id"] for agent in payload["agents"]] == ["personal"]


def test_build_current_contract_exposes_frontend_policy_and_extra_usage_highlight() -> None:
    client = ExtraUsagePolicyClient()
    sources = (
        common.SourceConfig(
            "personal",
            "claude",
            "Claude Personal",
            frontend=common.SourceFrontendConfig(
                order=7,
                short_label="CP",
                show_metrics=("extra_usage", "seven_day"),
                show_graphs=("seven_day",),
                highlight_metric="extra_usage",
                highlight_window_minutes=34,
            ),
        ),
    )

    payload = client.build_current_contract(
        history_days=30,
        sources=sources,
        frontend=common.GlobalFrontendConfig(columns=1),
    )

    assert payload["frontend"] == {"columns": 1}
    agent = payload["agents"][0]
    assert agent["frontend"]["order"] == 7
    assert agent["frontend"]["short_label"] == "CP"
    assert agent["frontend"]["show_metrics"] == ["extra_usage", "seven_day"]
    assert agent["frontend"]["graph_order"] == ["long_window"]
    assert agent["highlight"]["active"] is True
    assert agent["highlight"]["metric"] == "extra_usage"
    assert agent["highlight"]["window_minutes"] == 34

    metrics = {metric["metric_key"]: metric for metric in agent["metrics"]}
    assert list(metrics) == ["extra_usage", "seven_day", "five_hour", "internal_noise"]
    assert metrics["extra_usage"]["value"] == "$19.17"
    assert metrics["extra_usage"]["show_bar"] is False
    assert metrics["extra_usage"]["value_kind"] == "currency_cents"
    assert metrics["extra_usage"]["frontend"]["visible"] is True
    assert metrics["extra_usage"]["frontend"]["section"] == "secondary"
    assert metrics["extra_usage"]["frontend"]["highlight_active"] is True
    assert metrics["seven_day"]["frontend"]["section"] == "primary"
    assert metrics["five_hour"]["frontend"]["visible"] is False
    assert metrics["internal_noise"]["frontend"]["visible"] is False
    assert any(call["metric_path"] == "/extra_usage" and call["use_value_num"] for call in client.history_calls)
