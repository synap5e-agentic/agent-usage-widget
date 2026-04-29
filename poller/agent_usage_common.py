#!/usr/bin/env python3
"""Shared helpers for agent usage fetching, normalization, and persistence."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import ssl
import subprocess
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import unquote, urlparse


DEFAULT_CONFIG_FILE = Path.home() / ".config" / "agent-usage-widget" / "config.toml"
DEFAULT_ENV_FILE = Path.home() / ".config" / "agent-usage-widget" / ".env"
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "agent-usage"
DEFAULT_STATE_FILE = DEFAULT_CACHE_DIR / "state.json"
DEFAULT_DB_DSN = "postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage"
DEFAULT_SERVICE_HOST = "127.0.0.1"
DEFAULT_SERVICE_PORT = 8785
DEFAULT_POLL_INTERVAL_SECONDS = 900
SUPPORTED_PROVIDERS = ("claude", "codex", "cursor")


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    provider: str
    label: str
    frontend_visible: bool = True
    enabled: bool = True
    interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    auth: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AppConfig:
    config_file: Path
    env_file: Path
    cache_dir: Path
    state_path: Path
    db_dsn: str
    service_host: str
    service_port: int
    claude_enabled: bool
    codex_enabled: bool
    claude_organization_id: str
    claude_anonymous_id: str
    claude_device_id: str
    claude_session_key: str
    claude_cookie: str
    claude_headers_json: str
    codex_account_id: str
    codex_authorization: str
    codex_device_id: str
    codex_session_id: str
    codex_cookie: str
    codex_headers_json: str
    cursor_enabled: bool
    cursor_cookie: str
    cursor_headers_json: str
    codex_oai_session_id: str | None = None
    poller_default_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    sources: tuple[SourceConfig, ...] = ()


@dataclass
class ProviderSnapshot:
    provider: str
    account_id: str
    organization_id: str
    metrics: list[dict[str, Any]]
    summary_key: str
    history_key: str
    history_label: str
    details: list[dict[str, str]]
    raw_payload: dict[str, Any]
    request_url: str
    request_status: int
    request_error: str | None
    request_metadata: dict[str, Any] = field(default_factory=dict)
    success: bool = False
    source_id: str = ""
    source_label: str = ""
    frontend_visible: bool = True


SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip().strip('"').strip("'")
        values[key.strip()] = value

    return values


def read_toml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as fh:
            parsed = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"Invalid config TOML at {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _bool_value(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "off", "no"}


def _toml_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return _bool_value(str(value), default)


def _int_value(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _coalesce(*values: str | None) -> str:
    for value in values:
        if value is None:
            continue
        value = str(value).strip()
        if value and value != "UNSET":
            return value
    return ""


def _fingerprint_identity(prefix: str, raw: str | None) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _source_key(value: Any) -> str:
    source_id = str(value or "").strip()
    if not source_id:
        raise ValueError("source key must not be empty")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", source_id):
        raise ValueError(f"Invalid source key {source_id!r}; use letters, numbers, dot, dash, or underscore")
    return source_id


def _string_map(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    values: dict[str, str] = {}
    for key, value in raw.items():
        if value is None:
            continue
        values[str(key)] = str(value)
    return values


def _toml_table(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _source_auth_value(source: SourceConfig | None, *keys: str) -> str:
    if not source:
        return ""
    for key in keys:
        value = source.auth.get(key)
        if value is not None:
            value = str(value).strip()
            if value:
                return value
    return ""

def _to_base64_json(value: Any) -> str:
    return base64.b64encode(
        json.dumps(value, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")


def _parse_percentage(raw: Any) -> int:
    if raw is None:
        return 0
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0
    if 0 < abs(value) < 1.0:
        value *= 100
    return max(0, min(100, int(round(value))))


def _parse_unbounded_percentage(raw: Any) -> int:
    if raw is None:
        return 0
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0
    if 0 < abs(value) < 1.0:
        value *= 100
    return max(0, int(round(value)))


def _parse_header_json(raw: str, provider: str) -> dict[str, str]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid {provider} header JSON in env: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{provider} header JSON must be an object")
    return {str(k): str(v) for k, v in parsed.items()}


def _parse_timestamp(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)) or (
        isinstance(raw, str) and re.fullmatch(r"^\d+$", raw.strip())
    ):
        try:
            value = int(raw)
            if value > 10**12:
                value = int(value / 1000)
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except Exception:
            return None

    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _timestamp_to_iso_local(raw: Any) -> str:
    dt = _parse_timestamp(raw)
    if not dt:
        return ""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M")


def _timestamp_to_iso_utc(raw: Any) -> str:
    dt = _parse_timestamp(raw)
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).isoformat()


def _parse_cookie_header(raw: str | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    text = str(raw or "").strip()
    if not text:
        return parsed
    for chunk in text.split(";"):
        part = chunk.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if not key:
            continue
        parsed[key] = unquote(value.strip())
    return parsed


def _claude_organization_id(
    cfg: AppConfig,
    fetch_url: str | None = None,
    source: SourceConfig | None = None,
) -> str:
    configured_org = _coalesce(
        _source_auth_value(source, "organization_id", "org_id"),
        cfg.claude_organization_id,
    )
    if configured_org:
        return configured_org
    cookie_values = _parse_cookie_header(_coalesce(_source_auth_value(source, "cookie"), cfg.claude_cookie))
    if cookie_values.get("lastActiveOrg"):
        return cookie_values["lastActiveOrg"]
    if fetch_url:
        match = re.search(r"/organizations/([^/]+)/usage", str(fetch_url))
        if match:
            return match.group(1)
    return ""


def _timestamp_to_clock_local(raw: Any) -> str:
    dt = _parse_timestamp(raw)
    if not dt:
        return ""
    local = dt.astimezone()
    hour = local.hour % 12
    if hour == 0:
        hour = 12
    suffix = "pm" if local.hour >= 12 else "am"
    return f"{hour}:{local.minute:02d}{suffix}"


def _json_pointer(segments: list[str | int]) -> str:
    parts = []
    for segment in segments:
        encoded = str(segment).replace("~", "~0").replace("/", "~1")
        parts.append(encoded)
    return "/" + "/".join(parts) if parts else "/"


def _window_ts(raw: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        if key in raw:
            ts = _timestamp_to_iso_utc(raw.get(key))
            if ts:
                return ts
    return ""


def _safe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _metric_value_text(raw: dict[str, Any], percent: int) -> str:
    if not raw:
        return f"{percent}%"

    if "value_text" in raw:
        value = raw.get("value_text")
        if value is not None:
            return str(value)

    used = raw.get("used")
    limit = raw.get("limit")
    remaining = raw.get("remaining")
    if used is not None and limit not in (None, 0):
        return f"{used}/{limit}"
    if remaining is not None:
        return str(remaining)
    if "count" in raw and raw.get("count") is not None:
        return str(raw.get("count"))
    return f"{percent}%"


def _format_money_cents(value: Any) -> str:
    try:
        cents = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"${cents / 100.0:.2f}"


def _currency_graph_max(value: Any) -> int:
    try:
        cents = float(value or 0)
    except (TypeError, ValueError):
        cents = 0
    cents = max(0, cents)
    if cents <= 0:
        return 100
    padded = cents * 1.05
    if padded <= 1000:
        step = 100
    elif padded <= 10000:
        step = 500
    else:
        step = 2500
    return int(math.ceil(padded / step) * step)


def _percent_graph_max(value: Any, baseline: int = 100) -> int:
    try:
        percent = float(value or 0)
    except (TypeError, ValueError):
        percent = 0
    percent = max(float(baseline), percent)
    if percent <= 100:
        return 100
    step = 25 if percent <= 200 else 50
    return int(math.ceil(percent / step) * step)


def _metric_reset_at(raw: dict[str, Any]) -> str:
    for candidate in ("resets_at", "reset_at", "reset_time", "until", "expires_at", "expires"):
        if candidate in raw:
            ts = _timestamp_to_iso_local(raw.get(candidate))
            if ts:
                return ts
    return ""


def _format_period_note(window_start: Any = None, reset_at: Any = None) -> str:
    start_text = _timestamp_to_iso_local(window_start)
    reset_text = _timestamp_to_iso_local(reset_at)
    parts: list[str] = []
    if start_text:
        parts.append(f"Started at {start_text}")
    if reset_text:
        parts.append(f"Resets at {reset_text}")
    if parts:
        return "\n".join(parts)
    return "No reset in payload"


def _metric_label(provider: str, metric_key: str, raw: dict[str, Any] | None = None) -> str:
    raw = raw or {}
    for key in ("label", "name", "title", "display_name", "window_name"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    if provider == "claude":
        if metric_key == "seven_day":
            return "This week"
        if metric_key == "five_hour":
            return "5-hour window"
        if metric_key in {"sonnet", "sonnet_usage"}:
            return "Sonnet this week"
        if metric_key in {"spark", "spark_usage"}:
            return "Spark usage"

    if provider == "codex":
        if metric_key == "secondary_window":
            return "This week"
        if metric_key == "primary_window":
            return "5-hour window"
        if metric_key == "spark_usage":
            return "Spark this week"
        if metric_key == "spark_primary_window":
            return "Spark 5-hour window"

    pretty = metric_key.replace("_", " ").replace("-", " ").strip()
    return pretty[:1].upper() + pretty[1:] if pretty else metric_key


def _metric_duration_seconds(provider: str, metric_key: str, provider_metric_key: str = "") -> int | None:
    key = (metric_key or "").lower()
    provider_key = (provider_metric_key or "").lower()
    if provider == "claude":
        if key in {"seven_day", "week", "weekly", "sonnet", "sonnet_usage"}:
            return 7 * 24 * 3600
        if key in {"five_hour", "session"}:
            return 5 * 3600
        if provider_key in {"seven_day", "seven_day_sonnet"}:
            return 7 * 24 * 3600
        if provider_key == "five_hour":
            return 5 * 3600
    if provider == "codex":
        if key in {"primary_window", "spark_primary_window", "session"}:
            return 5 * 3600
        if key in {"secondary_window", "week", "weekly", "spark", "spark_usage"}:
            return 7 * 24 * 3600
        if provider_key == "primary_window":
            return 5 * 3600
        if provider_key == "secondary_window":
            return 7 * 24 * 3600
    if "day" in key:
        match = re.search(r"(\d+)", key)
        if match:
            return int(match.group(1)) * 24 * 3600
    if "hour" in key:
        match = re.search(r"(\d+)", key)
        if match:
            return int(match.group(1)) * 3600
    return None


def _metric_duration_from_raw(raw: dict[str, Any]) -> int | None:
    for key in ("limit_window_seconds", "window_seconds", "period_seconds", "duration_seconds"):
        value = raw.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return int(value)
        if isinstance(value, str) and value.strip().isdigit():
            parsed = int(value.strip())
            if parsed > 0:
                return parsed
    return None


def _path_node(root: Any, segments: list[str | int]) -> Any:
    node = root
    for segment in segments:
        if isinstance(node, dict) and isinstance(segment, str):
            if segment not in node:
                return None
            node = node.get(segment)
            continue
        if isinstance(node, list) and isinstance(segment, int):
            if segment < 0 or segment >= len(node):
                return None
            node = node[segment]
            continue
        return None
    return node


def _slug_metric_key(raw: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "_", (raw or "").strip()).strip("_").lower()
    return value or "metric"


def _is_codex_spark_window(payload: dict[str, Any], segments: list[str | int]) -> bool:
    if "additional_rate_limits" not in segments:
        return False
    idx_pos = segments.index("additional_rate_limits") + 1
    if idx_pos >= len(segments) or not isinstance(segments[idx_pos], int):
        return False
    additional_item = _path_node(payload, segments[: idx_pos + 1])
    if not isinstance(additional_item, dict):
        return False
    limit_name = str(additional_item.get("limit_name", "")).lower()
    metered = str(additional_item.get("metered_feature", "")).lower()
    return "spark" in limit_name or "spark" in metered or "bengalfox" in metered


def _canonical_metric_key(
    provider: str,
    payload: dict[str, Any],
    segments: list[str | int],
    raw: dict[str, Any],
    key_hint: str | None = None,
) -> tuple[str, str]:
    leaf = str(segments[-1]) if segments else "root"
    provider_metric_key = _slug_metric_key(
        _coalesce(
            str(raw.get("metric_key")) if raw.get("metric_key") is not None else None,
            str(raw.get("id")) if raw.get("id") is not None else None,
            str(raw.get("name")) if raw.get("name") is not None else None,
            str(raw.get("window")) if raw.get("window") is not None else None,
            key_hint,
            leaf,
        )
    )

    metric_key = provider_metric_key
    if provider == "claude":
        if provider_metric_key in {"seven_day_sonnet", "sonnet", "sonnet_usage"}:
            metric_key = "sonnet_usage"
        elif provider_metric_key in {"seven_day", "five_hour"}:
            metric_key = provider_metric_key
    elif provider == "codex":
        if provider_metric_key in {"spark", "spark_usage"}:
            metric_key = "spark_usage"
        elif provider_metric_key in {"primary_window", "secondary_window"} and _is_codex_spark_window(payload, segments):
            metric_key = "spark_usage" if provider_metric_key == "secondary_window" else "spark_primary_window"

    return metric_key, provider_metric_key


def _derive_window_bounds(
    provider: str,
    metric_key: str,
    provider_metric_key: str,
    raw: dict[str, Any],
) -> tuple[str, str]:
    start = _window_ts(raw, ("window_start", "start", "period_start", "starts_at"))
    end = _window_ts(raw, ("window_end", "end", "period_end", "ends_at"))
    reset_at = _window_ts(raw, ("resets_at", "reset_at", "reset_time", "until", "expires_at", "expires"))
    duration = _metric_duration_from_raw(raw) or _metric_duration_seconds(provider, metric_key, provider_metric_key)

    if not end and reset_at:
        end = reset_at
    if not start and end:
        if duration:
            end_dt = _parse_timestamp(end)
            if end_dt:
                start = (end_dt - timedelta(seconds=duration)).astimezone(timezone.utc).isoformat()
    if not end and start and duration:
        start_dt = _parse_timestamp(start)
        if start_dt:
            end = (start_dt + timedelta(seconds=duration)).astimezone(timezone.utc).isoformat()
    return start, end


def _looks_like_metric_payload(raw: dict[str, Any]) -> bool:
    if not raw:
        return False
    common_metric_fields = {
        "utilization",
        "used_percent",
        "used",
        "limit",
        "remaining",
        "available",
        "value",
        "count",
        "max",
        "quota",
        "cap",
        "reset_at",
        "resets_at",
        "window_start",
        "window_end",
        "limit_window_seconds",
        "used_credits",
    }
    if common_metric_fields.intersection(raw.keys()):
        return True
    if any(k.endswith("_pct") or k.endswith("_percent") for k in raw.keys()):
        return True
    return False


def _collect_metric_rows(provider: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    def walk(node: Any, segments: list[str | int], key_hint: str | None = None) -> None:
        if isinstance(node, dict):
            if _looks_like_metric_payload(node):
                path = _json_pointer(segments)
                if path not in seen_paths:
                    seen_paths.add(path)
                    metric_key, provider_metric_key = _canonical_metric_key(
                        provider=provider,
                        payload=payload,
                        segments=segments,
                        raw=node,
                        key_hint=key_hint,
                    )
                    percent = _percent_from_usage_dict(node)
                    reset_at = _window_ts(node, ("resets_at", "reset_at", "reset_time", "until", "expires_at", "expires"))
                    window_start, window_end = _derive_window_bounds(provider, metric_key, provider_metric_key, node)
                    value_num = None
                    for value_field in ("used", "value", "count", "remaining", "available"):
                        value = node.get(value_field)
                        if isinstance(value, (int, float)):
                            value_num = float(value)
                            break
                    max_value = 100
                    for limit_field in ("limit", "max", "quota", "cap"):
                        limit = node.get(limit_field)
                        if isinstance(limit, (int, float)) and limit > 0:
                            max_value = int(round(float(limit)))
                            break
                    rows.append(
                        {
                            "metric_key": metric_key,
                            "provider_metric_key": provider_metric_key,
                            "metric_path": path,
                            "metric_id": path,
                            "metric_scope": _json_pointer(segments[:-1]) if segments else "/",
                            "metric_label": _metric_label(provider, metric_key, node),
                            "percent": percent,
                            "value_num": value_num,
                            "value": _metric_value_text(node, percent),
                            "note": _format_period_note(window_start, reset_at),
                            "max_value": max_value,
                            "window_start": window_start,
                            "window_end": window_end,
                            "reset_at": reset_at,
                            "details": {
                                "path": path,
                                "scope": _json_pointer(segments[:-1]) if segments else "/",
                                "provider_metric_key": provider_metric_key,
                                "metric_key": metric_key,
                                "payload_fragment": node,
                            },
                        }
                    )
            for key, value in node.items():
                walk(value, segments + [key], key_hint=str(key))
            return

        if isinstance(node, list):
            for idx, value in enumerate(node):
                walk(value, segments + [idx], key_hint=key_hint)

    walk(payload, [])
    return rows


def _pick_summary_key(provider: str, metrics: list[dict[str, Any]]) -> str:
    if not metrics:
        return "monthly" if provider == "cursor" else "seven_day" if provider == "claude" else "secondary_window"
    
    if provider == "cursor":
        preferred = ["monthly"]
    elif provider == "claude":
        preferred = ["seven_day", "five_hour", "sonnet_usage", "primary_window", "secondary_window"]
    else:
        preferred = ["secondary_window", "primary_window", "spark_usage", "seven_day", "five_hour"]
        
    metric_keys = [str(m.get("metric_key")) for m in metrics]
    for key in preferred:
        if key in metric_keys:
            return key
    return metric_keys[0]


def _metric_path_rank(provider: str, metric_path: str) -> int:
    path = (metric_path or "").lower()
    if provider == "codex":
        if path.startswith("/rate_limit/"):
            return 0
        if path.startswith("/additional_rate_limits/"):
            return 2
    if provider == "claude" and path.count("/") <= 1:
        return 0
    return 1


def _pick_metric_by_candidates(
    metrics: list[dict[str, Any]],
    provider: str,
    keys: list[str],
) -> dict[str, Any] | None:
    for key in keys:
        matches = [metric for metric in metrics if str(metric.get("metric_key", "")).lower() == key.lower()]
        if not matches:
            continue
        return sorted(
            matches,
            key=lambda metric: (
                _metric_path_rank(provider, str(metric.get("metric_path", ""))),
                str(metric.get("metric_path", "")),
            ),
        )[0]
    return None


def _legacy_sources_from_values(values: dict[str, str], default_interval_seconds: int) -> tuple[SourceConfig, ...]:
    sources: list[SourceConfig] = []
    if _bool_value(values.get("AGENT_USAGE_ENABLE_CLAUDE", "0"), False):
        sources.append(
            SourceConfig(
                source_id="claude",
                provider="claude",
                label="Claude",
                interval_seconds=default_interval_seconds,
                auth={
                    "cookie": _coalesce(values.get("AGENT_USAGE_CLAUDE_COOKIE")),
                    "organization_id": _coalesce(values.get("AGENT_USAGE_CLAUDE_ORGANIZATION_ID")),
                    "anonymous_id": _coalesce(values.get("AGENT_USAGE_CLAUDE_ANONYMOUS_ID")),
                    "device_id": _coalesce(values.get("AGENT_USAGE_CLAUDE_DEVICE_ID")),
                    "session_key": _coalesce(values.get("AGENT_USAGE_CLAUDE_SESSION_KEY")),
                    "headers_json": _coalesce(values.get("AGENT_USAGE_CLAUDE_HEADERS_JSON")),
                },
            )
        )
    if _bool_value(values.get("AGENT_USAGE_ENABLE_CODEX", "0"), False):
        sources.append(
            SourceConfig(
                source_id="codex",
                provider="codex",
                label="Codex",
                interval_seconds=default_interval_seconds,
                auth={
                    "account_id": _coalesce(values.get("AGENT_USAGE_CODEX_ACCOUNT_ID")),
                    "authorization": _coalesce(values.get("AGENT_USAGE_CODEX_AUTHORIZATION")),
                    "device_id": _coalesce(values.get("AGENT_USAGE_CODEX_DEVICE_ID")),
                    "session_id": _coalesce(values.get("AGENT_USAGE_CODEX_SESSION_ID")),
                    "oai_session_id": _coalesce(values.get("AGENT_USAGE_CODEX_OAI_SESSION_ID")),
                    "cookie": _coalesce(values.get("AGENT_USAGE_CODEX_COOKIE")),
                    "headers_json": _coalesce(values.get("AGENT_USAGE_CODEX_HEADERS_JSON")),
                },
            )
        )
    if _bool_value(values.get("AGENT_USAGE_ENABLE_CURSOR", "0"), False):
        sources.append(
            SourceConfig(
                source_id="cursor",
                provider="cursor",
                label="Cursor",
                interval_seconds=default_interval_seconds,
                auth={
                    "cookie": _coalesce(values.get("AGENT_USAGE_CURSOR_COOKIE")),
                    "headers_json": _coalesce(values.get("AGENT_USAGE_CURSOR_HEADERS_JSON")),
                },
            )
        )
    return tuple(sources)


def _sources_from_toml(config: dict[str, Any], default_interval_seconds: int) -> tuple[SourceConfig, ...]:
    sources_table = _toml_table(config.get("sources"))
    sources: list[SourceConfig] = []
    seen: set[str] = set()
    for raw_key, raw_source in sources_table.items():
        source_id = _source_key(raw_key)
        if source_id in seen:
            raise ValueError(f"Duplicate source key {source_id!r}")
        seen.add(source_id)
        source = _toml_table(raw_source)
        provider = str(source.get("provider") or "").strip().lower()
        if provider not in SUPPORTED_PROVIDERS:
            raise ValueError(
                f"Source {source_id!r} has invalid provider {provider!r}; "
                f"expected one of: {', '.join(SUPPORTED_PROVIDERS)}"
            )
        interval_seconds = _int_value(source.get("interval_seconds"), default_interval_seconds)
        sources.append(
            SourceConfig(
                source_id=source_id,
                provider=provider,
                label=_coalesce(source.get("label"), source_id),
                frontend_visible=_toml_bool(source.get("frontend_visible"), True),
                enabled=_toml_bool(source.get("enabled"), True),
                interval_seconds=interval_seconds,
                auth=_string_map(source.get("auth")),
            )
        )
    return tuple(sources)


def _first_source_for_provider(sources: tuple[SourceConfig, ...], provider: str) -> SourceConfig | None:
    for source in sources:
        if source.provider == provider:
            return source
    return None


def load_config(overrides: dict[str, str] | None = None) -> AppConfig:
    config_file = Path(
        _coalesce(
            overrides.get("AGENT_USAGE_CONFIG_FILE") if overrides else None,
            os.environ.get("AGENT_USAGE_CONFIG_FILE"),
            str(DEFAULT_CONFIG_FILE),
        )
    ).expanduser()
    env_file = Path(
        _coalesce(
            overrides.get("AGENT_USAGE_ENV_FILE") if overrides else None,
            os.environ.get("AGENT_USAGE_ENV_FILE"),
            str(DEFAULT_ENV_FILE),
        )
    ).expanduser()
    values = {**read_env_file(env_file), **os.environ}
    if overrides:
        values.update(overrides)
    explicit_values = overrides or {}
    toml_config = read_toml_file(config_file)
    service_config = _toml_table(toml_config.get("service"))
    poller_config = _toml_table(toml_config.get("poller"))
    storage_config = _toml_table(toml_config.get("storage"))

    default_interval_seconds = _int_value(
        explicit_values.get("AGENT_USAGE_POLL_INTERVAL_SECONDS"),
        _int_value(
            poller_config.get("default_interval_seconds"),
            _int_value(values.get("AGENT_USAGE_POLL_INTERVAL_SECONDS"), DEFAULT_POLL_INTERVAL_SECONDS),
        ),
    )
    toml_sources = _sources_from_toml(toml_config, default_interval_seconds)
    sources = toml_sources or _legacy_sources_from_values(values, default_interval_seconds)

    claude_source = _first_source_for_provider(sources, "claude")
    codex_source = _first_source_for_provider(sources, "codex")
    cursor_source = _first_source_for_provider(sources, "cursor")

    cache_dir = Path(
        _coalesce(
            explicit_values.get("AGENT_USAGE_CACHE_DIR"),
            storage_config.get("cache_dir"),
            poller_config.get("cache_dir"),
            values.get("AGENT_USAGE_CACHE_DIR"),
            str(DEFAULT_CACHE_DIR),
        )
    ).expanduser()
    state_path = Path(
        _coalesce(
            explicit_values.get("AGENT_USAGE_STATE_FILE"),
            storage_config.get("state_file"),
            poller_config.get("state_file"),
            values.get("AGENT_USAGE_STATE_FILE"),
            str(cache_dir / "state.json"),
        )
    ).expanduser()

    return AppConfig(
        config_file=config_file,
        env_file=env_file,
        cache_dir=cache_dir,
        state_path=state_path,
        db_dsn=_coalesce(
            explicit_values.get("AGENT_USAGE_DB_DSN"),
            storage_config.get("db_dsn"),
            service_config.get("db_dsn"),
            values.get("AGENT_USAGE_DB_DSN"),
            DEFAULT_DB_DSN,
        ),
        service_host=_coalesce(
            explicit_values.get("AGENT_USAGE_SERVICE_HOST"),
            service_config.get("host"),
            values.get("AGENT_USAGE_SERVICE_HOST"),
            DEFAULT_SERVICE_HOST,
        ),
        service_port=int(
            _coalesce(
                explicit_values.get("AGENT_USAGE_SERVICE_PORT"),
                service_config.get("port"),
                values.get("AGENT_USAGE_SERVICE_PORT"),
                str(DEFAULT_SERVICE_PORT),
            )
        ),
        claude_enabled=bool(claude_source and claude_source.enabled),
        codex_enabled=bool(codex_source and codex_source.enabled),
        cursor_enabled=bool(cursor_source and cursor_source.enabled),
        claude_organization_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CLAUDE_ORGANIZATION_ID"),
            _source_auth_value(claude_source, "organization_id", "org_id"),
            values.get("AGENT_USAGE_CLAUDE_ORGANIZATION_ID"),
        ),
        claude_anonymous_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CLAUDE_ANONYMOUS_ID"),
            _source_auth_value(claude_source, "anonymous_id"),
            values.get("AGENT_USAGE_CLAUDE_ANONYMOUS_ID"),
        ),
        claude_device_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CLAUDE_DEVICE_ID"),
            _source_auth_value(claude_source, "device_id"),
            values.get("AGENT_USAGE_CLAUDE_DEVICE_ID"),
        ),
        claude_session_key=_coalesce(
            explicit_values.get("AGENT_USAGE_CLAUDE_SESSION_KEY"),
            _source_auth_value(claude_source, "session_key"),
            values.get("AGENT_USAGE_CLAUDE_SESSION_KEY"),
        ),
        claude_cookie=_coalesce(
            explicit_values.get("AGENT_USAGE_CLAUDE_COOKIE"),
            _source_auth_value(claude_source, "cookie"),
            values.get("AGENT_USAGE_CLAUDE_COOKIE"),
        ),
        claude_headers_json=_coalesce(
            explicit_values.get("AGENT_USAGE_CLAUDE_HEADERS_JSON"),
            _source_auth_value(claude_source, "headers_json", "headers"),
            values.get("AGENT_USAGE_CLAUDE_HEADERS_JSON"),
        ),
        codex_account_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_ACCOUNT_ID"),
            _source_auth_value(codex_source, "account_id"),
            values.get("AGENT_USAGE_CODEX_ACCOUNT_ID"),
        ),
        codex_authorization=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_AUTHORIZATION"),
            _source_auth_value(codex_source, "authorization"),
            values.get("AGENT_USAGE_CODEX_AUTHORIZATION"),
        ),
        codex_device_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_DEVICE_ID"),
            _source_auth_value(codex_source, "device_id"),
            values.get("AGENT_USAGE_CODEX_DEVICE_ID"),
        ),
        codex_session_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_SESSION_ID"),
            _source_auth_value(codex_source, "session_id"),
            values.get("AGENT_USAGE_CODEX_SESSION_ID"),
        ),
        codex_cookie=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_COOKIE"),
            _source_auth_value(codex_source, "cookie"),
            values.get("AGENT_USAGE_CODEX_COOKIE"),
        ),
        codex_headers_json=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_HEADERS_JSON"),
            _source_auth_value(codex_source, "headers_json", "headers"),
            values.get("AGENT_USAGE_CODEX_HEADERS_JSON"),
        ),
        codex_oai_session_id=_coalesce(
            explicit_values.get("AGENT_USAGE_CODEX_OAI_SESSION_ID"),
            _source_auth_value(codex_source, "oai_session_id"),
            values.get("AGENT_USAGE_CODEX_OAI_SESSION_ID"),
        )
        or None,
        cursor_cookie=_coalesce(
            explicit_values.get("AGENT_USAGE_CURSOR_COOKIE"),
            _source_auth_value(cursor_source, "cookie"),
            values.get("AGENT_USAGE_CURSOR_COOKIE"),
        ),
        cursor_headers_json=_coalesce(
            explicit_values.get("AGENT_USAGE_CURSOR_HEADERS_JSON"),
            _source_auth_value(cursor_source, "headers_json", "headers"),
            values.get("AGENT_USAGE_CURSOR_HEADERS_JSON"),
        ),
        poller_default_interval_seconds=default_interval_seconds,
        sources=sources,
    )


def _auth_headers(
    cfg: AppConfig,
    provider: str,
    source: SourceConfig | None = None,
) -> tuple[str, dict[str, str]]:
    if provider == "claude":
        cookie = _coalesce(_source_auth_value(source, "cookie"), cfg.claude_cookie)
        cookie_values = _parse_cookie_header(cookie)
        organization_id = _coalesce(
            _source_auth_value(source, "organization_id", "org_id"),
            cfg.claude_organization_id,
            cookie_values.get("lastActiveOrg"),
        )
        anonymous_id = _coalesce(
            _source_auth_value(source, "anonymous_id"),
            cfg.claude_anonymous_id,
            cookie_values.get("ajs_anonymous_id"),
        )
        device_id = _coalesce(
            _source_auth_value(source, "device_id"),
            cfg.claude_device_id,
            cookie_values.get("anthropic-device-id"),
        )
        session_key = _coalesce(
            _source_auth_value(source, "session_key"),
            cfg.claude_session_key,
            cookie_values.get("sessionKey"),
        )

        if not organization_id:
            raise ValueError(
                "Missing Claude organization config. Set "
                "sources.<name>.auth.organization_id, AGENT_USAGE_CLAUDE_ORGANIZATION_ID, "
                "or provide lastActiveOrg in the Claude cookie"
            )

        url = f"https://claude.ai/api/organizations/{organization_id}/usage"
        if not cookie:
            if not (anonymous_id and device_id and session_key):
                raise ValueError(
                    "Missing Claude auth config. Either set sources.<name>.auth.cookie "
                    "or set organization_id, anonymous_id, device_id, and session_key"
                )
            cookie = (
                f"sessionKey={session_key}; "
                f"lastActiveOrg={organization_id}; "
                f"ajs_anonymous_id={anonymous_id}; "
                f"anthropic-device-id={device_id}"
            )

        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "anthropic-client-platform": "web_claude_ai",
            "anthropic-client-sha": "dc14c30c6a5f0b1a0a5a2c4c7e323c8deb4153e6",
            "anthropic-client-version": "1.0.0",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "content-type": "application/json",
            "referer": "https://claude.ai/settings/usage",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "cookie": cookie,
        }
        if anonymous_id:
            headers["anthropic-anonymous-id"] = anonymous_id
        if device_id:
            headers["anthropic-device-id"] = device_id
        headers.update(
            _parse_header_json(
                _coalesce(_source_auth_value(source, "headers_json", "headers"), cfg.claude_headers_json),
                "Claude",
            )
        )
        return url, headers

    if provider == "codex":
        authorization = _coalesce(_source_auth_value(source, "authorization"), cfg.codex_authorization)
        cookie = _coalesce(_source_auth_value(source, "cookie"), cfg.codex_cookie)
        cookie_values = _parse_cookie_header(cookie)
        device_id = _coalesce(
            _source_auth_value(source, "device_id"),
            cfg.codex_device_id,
            cookie_values.get("oai-did"),
        )
        session_id = _coalesce(
            _source_auth_value(source, "session_id"),
            cfg.codex_session_id,
            cookie_values.get("oai-session-id"),
        )
        header_session_id = _coalesce(_source_auth_value(source, "oai_session_id"), cfg.codex_oai_session_id, session_id)

        if not (authorization and device_id and header_session_id):
            raise ValueError(
                "Missing Codex auth config. Set sources.<name>.auth.authorization, "
                "and either cookie or device_id plus session_id"
            )

        url = "https://chatgpt.com/backend-api/wham/usage"
        token = authorization.strip()
        if not re.match(r"(?i)^bearer\s+", token):
            token = f"Bearer {token}"
        if not cookie:
            cookie = (
                f"oai-did={device_id}; "
                f"oai-session-id={session_id}; "
                f"__Secure-next-auth.session-token=placeholder"
            )

        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "authorization": token,
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://chatgpt.com/codex/cloud/settings/analytics",
            "oai-device-id": device_id,
            "oai-target-page": "/backend-api/wham/usage",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "oai-session-id": header_session_id,
            "x-openai-target-path": "/backend-api/wham/usage",
            "x-openai-target-route": "/backend-api/wham/usage",
            "x-openai-target-page": "/backend-api/wham/usage",
            "cookie": cookie,
        }
        headers.update(
            _parse_header_json(
                _coalesce(_source_auth_value(source, "headers_json", "headers"), cfg.codex_headers_json),
                "Codex",
            )
        )
        return url, headers

    if provider == "cursor":
        cursor_cookie = _coalesce(_source_auth_value(source, "cookie"), cfg.cursor_cookie)
        if not cursor_cookie:
            raise ValueError(
                "Missing Cursor auth config. Set sources.<name>.auth.cookie or AGENT_USAGE_CURSOR_COOKIE"
            )

        url = "https://cursor.com/api/usage-summary"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "origin": "https://cursor.com",
            "referer": "https://cursor.com/dashboard/billing",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "cookie": cursor_cookie,
        }
        headers.update(
            _parse_header_json(
                _coalesce(_source_auth_value(source, "headers_json", "headers"), cfg.cursor_headers_json),
                "Cursor",
            )
        )
        return url, headers

    raise ValueError(f"Unknown provider {provider}")


def fetch_json(url: str, headers: dict[str, str], timeout: int = 25, data: bytes | None = None) -> tuple[int, dict[str, Any], str | None]:
    req = Request(url, headers=headers, data=data)
    try:
        with urlopen(req, timeout=timeout, context=ssl.create_default_context()) as response:
            raw = response.read().decode("utf-8", errors="replace")
            if not raw:
                return response.status, {}, None
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                return response.status, {"_raw_payload": raw}, f"Invalid JSON response: {exc}"
            if isinstance(payload, dict):
                return response.status, payload, None
            return response.status, {"_payload": payload}, "Expected JSON object payload"
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(exc)
        try:
            payload = json.loads(body)
            if isinstance(payload, dict):
                return exc.code, payload, body
            return exc.code, {"_payload": payload}, f"HTTP error {exc.code}: {body}"
        except json.JSONDecodeError:
            pass
        return exc.code, {}, body
    except URLError as exc:
        return 0, {}, str(exc)


def fetch_cursor_usage_events_page(
    cfg: AppConfig,
    start_epoch_ms: int,
    end_epoch_ms: int,
    page: int,
    page_size: int = 100,
    source: SourceConfig | None = None,
) -> tuple[int, dict[str, Any], str | None]:
    _, headers = _auth_headers(cfg, "cursor", source=source)
    headers["content-type"] = "application/json"
    headers["referer"] = "https://cursor.com/dashboard/usage"
    body = json.dumps(
        {
            "teamId": 0,
            "startDate": str(start_epoch_ms),
            "endDate": str(end_epoch_ms),
            "page": int(page),
            "pageSize": int(page_size),
        }
    ).encode("utf-8")
    return fetch_json("https://cursor.com/api/dashboard/get-filtered-usage-events", headers, data=body)


def _timestamp_to_epoch_ms(raw: Any) -> int | None:
    dt = _parse_timestamp(raw)
    if not dt:
        return None
    return int(dt.timestamp() * 1000)


def _cursor_usage_event_id(event: dict[str, Any]) -> str:
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def normalize_claude(
    payload: dict[str, Any],
    cfg: AppConfig,
    fetch_url: str,
    status: int,
    error: str | None,
    source: SourceConfig | None = None,
) -> ProviderSnapshot:
    data = _safe_json(payload)
    metrics = _collect_metric_rows("claude", data)
    source_id = source.source_id if source else "claude"
    source_label = source.label if source else "Claude"
    organization_id = _claude_organization_id(cfg, fetch_url, source=source)
    if not metrics:
        metrics.append(
            {
                "metric_key": "seven_day",
                "provider_metric_key": "seven_day",
                "metric_path": "/seven_day",
                "metric_id": "/seven_day",
                "metric_scope": "/",
                "metric_label": "Usage",
                "percent": 0,
                "value_num": 0,
                "value": "0%",
                "note": "No known metric payload",
                "max_value": 100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {},
            }
        )

    seven_pct = next((m.get("percent", 0) for m in metrics if m.get("metric_key") == "seven_day"), 0)
    five_pct = next((m.get("percent", 0) for m in metrics if m.get("metric_key") == "five_hour"), 0)

    details: list[dict[str, str]] = []

    summary_key = _pick_summary_key("claude", metrics)
    history_key = summary_key
    metadata = {
        "source_id": source_id,
        "source_label": source_label,
        "frontend_visible": source.frontend_visible if source else True,
        "provider": "claude",
        "plan": _coalesce(data.get("plan")),
        "summary_key": summary_key,
        "history_key": history_key,
        "history_label": "Last 7 days",
        "details": details,
        "five_hour_pct": five_pct,
        "seven_day_pct": seven_pct,
    }
    metadata["success_status"] = {
        "requested_at": now_iso(),
        "status": status,
        "error": error,
    }

    return ProviderSnapshot(
        provider="claude",
        account_id=organization_id,
        organization_id=organization_id,
        metrics=metrics,
        summary_key=summary_key,
        history_key=history_key,
        history_label="Last 7 days",
        details=details,
        raw_payload=data,
        request_url=fetch_url,
        request_status=status,
        request_error=error,
        request_metadata=metadata,
        success=status == 200 and not bool(error),
        source_id=source_id,
        source_label=source_label,
        frontend_visible=source.frontend_visible if source else True,
    )


def _percent_from_usage_dict(raw: Any) -> int:
    if not isinstance(raw, dict):
        return 0
    if "used_percent" in raw:
        return _parse_percentage(raw.get("used_percent"))
    if "utilization" in raw:
        return _parse_percentage(raw.get("utilization"))

    used = raw.get("used")
    limit = raw.get("limit")
    try:
        if used is None or limit in (None, 0):
            available = raw.get("available")
            remaining = raw.get("remaining")
            if remaining is not None and limit not in (None, 0):
                return _parse_percentage(((float(limit) - float(remaining)) / float(limit)) * 100)
            if available is not None and limit not in (None, 0):
                return _parse_percentage(((float(limit) - float(available)) / float(limit)) * 100)
            for key, value in raw.items():
                if key.endswith("_pct") or key.endswith("_percent"):
                    return _parse_percentage(value)
            return 0
        return _parse_percentage((float(used) / float(limit)) * 100)
    except (TypeError, ValueError, ZeroDivisionError):
        return 0


def normalize_codex(
    payload: dict[str, Any],
    cfg: AppConfig,
    fetch_url: str,
    status: int,
    error: str | None,
    source: SourceConfig | None = None,
) -> ProviderSnapshot:
    data = _safe_json(payload)
    metrics = _collect_metric_rows("codex", data)
    source_id = source.source_id if source else "codex"
    source_label = source.label if source else "Codex"
    configured_account_id = _coalesce(_source_auth_value(source, "account_id"), cfg.codex_account_id)
    if not metrics:
        metrics.append(
            {
                "metric_key": "secondary_window",
                "provider_metric_key": "secondary_window",
                "metric_path": "/secondary_window",
                "metric_id": "/secondary_window",
                "metric_scope": "/",
                "metric_label": "Usage",
                "percent": 0,
                "value_num": 0,
                "value": "0%",
                "note": "No known metric payload",
                "max_value": 100,
                "window_start": "",
                "window_end": "",
                "reset_at": "",
                "details": {},
            }
        )

    primary_pct = next((m.get("percent", 0) for m in metrics if m.get("metric_key") == "primary_window"), 0)
    secondary_pct = next((m.get("percent", 0) for m in metrics if m.get("metric_key") == "secondary_window"), 0)

    details: list[dict[str, str]] = []

    summary_key = _pick_summary_key("codex", metrics)
    history_key = summary_key
    account_id = _coalesce(data.get("account_id"), configured_account_id)
    organization_id = _coalesce(data.get("user_id"), configured_account_id, account_id)
    metadata = {
        "source_id": source_id,
        "source_label": source_label,
        "frontend_visible": source.frontend_visible if source else True,
        "provider": "codex",
        "plan": _coalesce(data.get("plan_type"), "Pro"),
        "summary_key": summary_key,
        "history_key": history_key,
        "history_label": "Last 14 days",
        "details": details,
        "primary_window_pct": primary_pct,
        "secondary_window_pct": secondary_pct,
        "account_id": account_id,
        "user_id": data.get("user_id"),
    }
    metadata["success_status"] = {
        "requested_at": now_iso(),
        "status": status,
        "error": error,
    }

    return ProviderSnapshot(
        provider="codex",
        account_id=account_id,
        organization_id=organization_id,
        metrics=metrics,
        summary_key=summary_key,
        history_key=history_key,
        history_label="Last 14 days",
        details=details,
        raw_payload=data,
        request_url=fetch_url,
        request_status=status,
        request_error=error,
        request_metadata=metadata,
        success=status == 200 and not bool(error),
        source_id=source_id,
        source_label=source_label,
        frontend_visible=source.frontend_visible if source else True,
    )


def normalize_cursor(
    payload: dict[str, Any],
    cfg: AppConfig,
    fetch_url: str,
    status: int,
    error: str | None,
    source: SourceConfig | None = None,
) -> ProviderSnapshot:
    data = _safe_json(payload)
    metrics = []
    source_id = source.source_id if source else "cursor"
    source_label = source.label if source else "Cursor"

    individual = _safe_json(data.get("individualUsage"))
    plan = _safe_json(individual.get("plan"))
    team_usage = _safe_json(data.get("teamUsage"))

    window_start = _timestamp_to_iso_utc(data.get("billingCycleStart"))
    window_end = _timestamp_to_iso_utc(data.get("billingCycleEnd"))
    reset_at = _timestamp_to_iso_utc(data.get("billingCycleEnd"))
    note = _format_period_note(window_start, reset_at)

    if plan:
        used = plan.get("used", 0)
        limit = plan.get("limit", 2000)
        total_percent_used = plan.get("totalPercentUsed", 0)
        breakdown = _safe_json(plan.get("breakdown"))
        included_cents = int(breakdown.get("included", used) or 0)
        over_cap_cents = int(breakdown.get("bonus", 0) or 0)
        total_spend_cents = int(breakdown.get("total", used) or 0)
        monthly_percent = _parse_unbounded_percentage((total_spend_cents / limit) * 100 if limit else 0)
        stored_monthly_percent = _parse_percentage((total_spend_cents / limit) * 100 if limit else 0)
        auto_message = str(data.get("autoModelSelectedDisplayMessage") or "").strip()
        api_message = str(data.get("namedModelSelectedDisplayMessage") or "").strip()

        metrics.append({
            "metric_key": "monthly",
            "provider_metric_key": "included",
            "metric_path": "/individualUsage/breakdown/included",
            "metric_id": "/individualUsage/breakdown/included",
            "metric_scope": "/individualUsage",
            "metric_label": "Monthly usage",
            "percent": stored_monthly_percent,
            "value_num": total_spend_cents,
            "value": f"{monthly_percent}%",
            "note": note,
            "max_value": _percent_graph_max(monthly_percent),
            "window_start": window_start,
            "window_end": window_end,
            "reset_at": reset_at,
            "details": {
                "used_cents": used,
                "limit_cents": limit,
                "included_cents": included_cents,
                "over_cap_cents": over_cap_cents,
                "total_spend_cents": total_spend_cents,
                "used_text": _format_money_cents(used),
                "limit_text": _format_money_cents(limit),
                "total_percent_used": float(total_percent_used or 0),
                "summary_message": auto_message,
                "graph_reference_value": 100,
            },
        })

        metrics.append({
            "metric_key": "provider_total_usage",
            "provider_metric_key": "totalPercentUsed",
            "metric_path": "/individualUsage/plan/totalPercentUsed",
            "metric_id": "/individualUsage/plan/totalPercentUsed",
            "metric_scope": "/individualUsage/plan",
            "metric_label": "Included total usage",
            "percent": _parse_percentage(total_percent_used),
            "value_num": total_percent_used,
            "value": f"{float(total_percent_used or 0):.1f}%",
            "note": auto_message or note,
            "max_value": 100,
            "window_start": window_start,
            "window_end": window_end,
            "reset_at": reset_at,
            "details": {},
        })

        metrics.append({
            "metric_key": "over_cap_used",
            "provider_metric_key": "bonus",
            "metric_path": "/individualUsage/breakdown/bonus",
            "metric_id": "/individualUsage/breakdown/bonus",
            "metric_scope": "/individualUsage/breakdown",
            "metric_label": "Over cap used",
            "percent": _parse_percentage((over_cap_cents / limit) * 100 if limit else 0),
            "value_num": over_cap_cents,
            "value": _format_money_cents(over_cap_cents),
            "note": "Soft overage consumed above the included monthly cap",
            "max_value": limit,
            "window_start": window_start,
            "window_end": window_end,
            "reset_at": reset_at,
            "details": {},
        })

        metrics.append({
            "metric_key": "total_spend",
            "provider_metric_key": "total",
            "metric_path": "/individualUsage/breakdown/total",
            "metric_id": "/individualUsage/breakdown/total",
            "metric_scope": "/individualUsage/breakdown",
            "metric_label": "Total spend",
            "percent": _parse_percentage((total_spend_cents / limit) * 100 if limit else 0),
            "value_num": total_spend_cents,
            "value": _format_money_cents(total_spend_cents),
            "note": "Total spend this cycle including any soft overage",
            "max_value": limit,
            "window_start": window_start,
            "window_end": window_end,
            "reset_at": reset_at,
            "details": {},
        })
        
        metrics.append({
            "metric_key": "auto_usage",
            "provider_metric_key": "autoPercentUsed",
            "metric_path": "/individualUsage/plan/autoPercentUsed",
            "metric_id": "/individualUsage/plan/autoPercentUsed",
            "metric_scope": "/individualUsage/plan",
            "metric_label": "Auto-selected usage",
            "percent": _parse_percentage(plan.get("autoPercentUsed", 0)),
            "value_num": plan.get("autoPercentUsed", 0),
            "value": f"{plan.get('autoPercentUsed', 0):.1f}%",
            "note": note,
            "max_value": 100,
            "window_start": window_start,
            "window_end": window_end,
            "reset_at": reset_at,
            "details": {},
        })

        metrics.append({
            "metric_key": "api_usage",
            "provider_metric_key": "apiPercentUsed",
            "metric_path": "/individualUsage/plan/apiPercentUsed",
            "metric_id": "/individualUsage/plan/apiPercentUsed",
            "metric_scope": "/individualUsage/plan",
            "metric_label": "Included API usage",
            "percent": _parse_percentage(plan.get("apiPercentUsed", 0)),
            "value_num": plan.get("apiPercentUsed", 0),
            "value": f"{plan.get('apiPercentUsed', 0):.1f}%",
            "note": api_message or note,
            "max_value": 100,
            "window_start": window_start,
            "window_end": window_end,
            "reset_at": reset_at,
            "details": {},
        })

    # Fetch model breakdown
    start_date = data.get("billingCycleStart")
    if start_date:
        try:
            start_dt = _parse_timestamp(start_date)
            if start_dt:
                start_epoch = int(start_dt.timestamp() * 1000)
                events_url = "https://cursor.com/api/dashboard/get-aggregated-usage-events"
                _, headers = _auth_headers(cfg, "cursor", source=source)
                headers["content-type"] = "application/json"
                post_data = json.dumps({"teamId": -1, "startDate": start_epoch}).encode("utf-8")
                ev_status, events_payload, _ = fetch_json(events_url, headers, data=post_data)
                
                if ev_status == 200 and isinstance(events_payload, dict):
                    data["_aggregated_usage_events"] = events_payload
                    total_cost_cents = float(events_payload.get("totalCostCents") or 0)
                    for idx, agg in enumerate(events_payload.get("aggregations", [])):
                        model = agg.get("modelIntent", "unknown")
                        model_slug = _slug_metric_key(str(model))
                        cost_cents = float(agg.get("totalCents", 0) or 0)
                        cost_dollars = cost_cents / 100.0
                        in_tokens = int(agg.get("inputTokens", 0))
                        out_tokens = int(agg.get("outputTokens", 0))
                        
                        metric_key = "auto_spend" if str(model) == "default" else f"model_{model_slug}"
                        metric_label = "Auto-selected spend" if metric_key == "auto_spend" else f"Model: {model}"
                        metric_note = (
                            "Unlimited auto-selected model spend from the billing event stream"
                            if metric_key == "auto_spend"
                            else f"In: {in_tokens:,} | Out: {out_tokens:,}"
                        )
                        metric_details = {
                            "model_slug": model_slug,
                            "cost_cents": cost_cents,
                            "input_tokens": in_tokens,
                            "output_tokens": out_tokens,
                            "cache_read_tokens": int(agg.get("cacheReadTokens", 0) or 0),
                            "cache_write_tokens": int(agg.get("cacheWriteTokens", 0) or 0),
                            "tier": agg.get("tier"),
                        }
                        if metric_key == "auto_spend":
                            metric_details.update({
                                "graph_value_kind": "currency_cents",
                                "graph_pace_line": False,
                                "graph_model": "default",
                                "graph_max_value": _currency_graph_max(cost_cents),
                            })

                        metrics.append({
                            "metric_key": metric_key,
                            "provider_metric_key": model,
                            "metric_path": f"/_aggregated_usage_events/aggregations/{idx}",
                            "metric_id": f"/_aggregated_usage_events/aggregations/{idx}",
                            "metric_scope": "/_aggregated_usage_events/aggregations",
                            "metric_label": metric_label,
                            "percent": _parse_percentage((cost_cents / total_cost_cents) * 100 if total_cost_cents > 0 else 0),
                            "value_num": cost_cents,
                            "value": _format_money_cents(cost_cents),
                            "note": metric_note,
                            "max_value": _currency_graph_max(cost_cents) if metric_key == "auto_spend" else 100,
                            "window_start": window_start,
                            "window_end": window_end,
                            "reset_at": reset_at,
                            "details": metric_details,
                        })
        except Exception as e:
            print(f"Failed to fetch model breakdown: {e}")

    if not metrics:
        metrics.append({
            "metric_key": "monthly",
            "provider_metric_key": "monthly",
            "metric_path": "/monthly",
            "metric_id": "/monthly",
            "metric_scope": "/",
            "metric_label": "Usage",
            "percent": 0,
            "value_num": 0,
            "value": "0%",
            "note": "No known metric payload",
            "max_value": 100,
            "window_start": "",
            "window_end": "",
            "reset_at": "",
            "details": {},
        })

    details: list[dict[str, str]] = []
    account_id = _coalesce(
        data.get("userId"),
        data.get("user_id"),
        data.get("accountId"),
        data.get("account_id"),
        individual.get("userId"),
        individual.get("user_id"),
        individual.get("accountId"),
        individual.get("account_id"),
        _fingerprint_identity("cursor_user", _coalesce(_source_auth_value(source, "cookie"), cfg.cursor_cookie)),
    )
    organization_id = _coalesce(
        data.get("teamId"),
        data.get("team_id"),
        data.get("organizationId"),
        data.get("organization_id"),
        team_usage.get("teamId"),
        team_usage.get("team_id"),
        team_usage.get("id"),
        team_usage.get("organizationId"),
        team_usage.get("organization_id"),
        account_id,
    )

    summary_key = "monthly"
    history_key = summary_key
    metadata = {
        "source_id": source_id,
        "source_label": source_label,
        "frontend_visible": source.frontend_visible if source else True,
        "provider": "cursor",
        "plan": str(data.get("membershipType", "Pro")).title(),
        "summary_key": summary_key,
        "history_key": history_key,
        "history_label": "This Month",
        "details": details,
        "monthly_pct": metrics[0]["percent"],
    }
    metadata["success_status"] = {
        "requested_at": now_iso(),
        "status": status,
        "error": error,
    }

    return ProviderSnapshot(
        provider="cursor",
        account_id=account_id,
        organization_id=organization_id,
        metrics=metrics,
        summary_key=summary_key,
        history_key=history_key,
        history_label="This Month",
        details=details,
        raw_payload=data,
        request_url=fetch_url,
        request_status=status,
        request_error=error,
        request_metadata=metadata,
        success=status == 200 and not bool(error),
        source_id=source_id,
        source_label=source_label,
        frontend_visible=source.frontend_visible if source else True,
    )


def run_fetch(
    cfg: AppConfig,
    provider: str,
    source: SourceConfig | None = None,
) -> ProviderSnapshot:
    fetch_url, headers = _auth_headers(cfg, provider, source=source)
    status, payload, error = fetch_json(fetch_url, headers)
    if provider == "claude":
        return normalize_claude(payload, cfg=cfg, fetch_url=fetch_url, status=status, error=error, source=source)
    if provider == "cursor":
        return normalize_cursor(payload, cfg=cfg, fetch_url=fetch_url, status=status, error=error, source=source)
    return normalize_codex(payload, cfg=cfg, fetch_url=fetch_url, status=status, error=error, source=source)


def sync_cursor_usage_events(
    cfg: AppConfig,
    client: PostgresClient,
    provider_fetch_id: int,
    snapshot: ProviderSnapshot,
    page_size: int = 100,
    max_pages: int = 100,
) -> dict[str, int]:
    if snapshot.provider != "cursor":
        return {"pages_fetched": 0, "inserted": 0, "known_through": 0, "oldest_seen": 0, "total_events": 0}

    raw = _safe_json(snapshot.raw_payload)
    cycle_start = _timestamp_to_iso_utc(raw.get("billingCycleStart"))
    cycle_end = _timestamp_to_iso_utc(raw.get("billingCycleEnd"))
    start_epoch_ms = _timestamp_to_epoch_ms(raw.get("billingCycleStart"))
    end_epoch_ms = _timestamp_to_epoch_ms(raw.get("billingCycleEnd"))
    if start_epoch_ms is None or end_epoch_ms is None or not cycle_end:
        return {"pages_fetched": 0, "inserted": 0, "known_through": 0, "oldest_seen": 0, "total_events": 0}

    source_id = _coalesce(snapshot.source_id, snapshot.provider)
    known_through = client.latest_cursor_usage_sync_through(cycle_end, source_id=source_id)
    known_total = client.latest_cursor_usage_total_count(cycle_end, source_id=source_id)
    known_latest = client.latest_cursor_usage_timestamp(cycle_end, source_id=source_id)
    pages_fetched = 0
    inserted_total = 0
    oldest_seen = 0
    total_events = 0
    seen_events = 0

    for page in range(1, max_pages + 1):
        status, payload, error = fetch_cursor_usage_events_page(
            cfg,
            start_epoch_ms=start_epoch_ms,
            end_epoch_ms=end_epoch_ms,
            page=page,
            page_size=page_size,
            source=next((candidate for candidate in cfg.sources if candidate.source_id == source_id), None),
        )
        if status != 200 or error:
            break

        events = payload.get("usageEventsDisplay")
        if not isinstance(events, list) or not events:
            break

        pages_fetched += 1
        total_events = int(payload.get("totalUsageEventsCount") or total_events or 0)
        seen_events += len(events)
        inserted_total += client.insert_cursor_usage_events(
            provider_fetch_id=provider_fetch_id,
            source_id=source_id,
            cycle_start=cycle_start,
            cycle_end=cycle_end,
            page=page,
            events=[_safe_json(event) for event in events],
        )

        timestamps = [int(event.get("timestamp")) for event in events if str(event.get("timestamp", "")).isdigit()]
        if timestamps:
            page_oldest = min(timestamps)
            oldest_seen = page_oldest if oldest_seen == 0 else min(oldest_seen, page_oldest)
            expected_new = max(0, total_events - known_total)
            if known_latest and page_oldest <= known_latest and seen_events >= (expected_new + page_size):
                break
            if not known_latest and known_through and page_oldest <= known_through:
                break

        if len(events) < page_size:
            break

    if oldest_seen:
        synced_through = min(oldest_seen, known_through) if known_through else oldest_seen
        client.update_cursor_usage_sync_state(
            source_id=source_id,
            cycle_start=cycle_start,
            cycle_end=cycle_end,
            synced_through_timestamp_ms=synced_through,
            total_usage_events_count=total_events,
            last_page_fetched=pages_fetched,
            last_inserted_count=inserted_total,
        )

    return {
        "pages_fetched": pages_fetched,
        "inserted": inserted_total,
        "known_through": known_through,
        "oldest_seen": oldest_seen,
        "total_events": total_events,
    }


def write_state_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(path)


def _build_history_label(provider: str) -> str:
    return "Last 7 days"


def _graph_metric_candidates(provider: str, window: str) -> list[str]:
    if provider == "cursor":
        return ["monthly"] if window == "long_window" else ["auto_spend"]
    if provider == "claude":
        return ["seven_day", "sonnet_usage", "sonnet"] if window == "long_window" else ["five_hour", "session"]
    return (
        ["secondary_window", "spark_usage", "spark"]
        if window == "long_window"
        else ["primary_window", "spark_primary_window", "session"]
    )


def _metric_tokens(metric: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for field in ("metric_key", "metric_path", "metric_label"):
        value = metric.get(field)
        if value:
            tokens.add(str(value).lower())

    details = _safe_json(metric.get("details"))
    payload_fragment = _safe_json(details.get("payload_fragment"))
    for key in payload_fragment.keys():
        tokens.add(str(key).lower())
    return tokens


def _metric_preference_score(metric: dict[str, Any], provider: str, window: str) -> int:
    key = str(metric.get("metric_key", "")).lower()
    path = str(metric.get("metric_path", "")).lower()
    tokens = _metric_tokens(metric)
    preferred = _graph_metric_candidates(provider, window)
    if not preferred:
        return -1
    for idx, candidate in enumerate(preferred):
        candidate_l = candidate.lower()
        if key == candidate_l:
            return 1000 - idx
        if path.endswith("/" + candidate_l) or candidate_l in tokens:
            return 900 - idx
    duration = _metric_duration_seconds(provider, key) or 0
    if window == "long_window" and duration >= 24 * 3600:
        return 700 + int(duration / 3600)
    if window == "short_window" and duration and duration < 24 * 3600:
        return 700 + int((24 * 3600 - duration) / 3600)
    return max(0, min(600, int(duration / 3600)))


def _pick_graph_metric(metrics: list[dict[str, Any]], provider: str, window: str) -> dict[str, Any] | None:
    if not metrics:
        return None
    if not _graph_metric_candidates(provider, window):
        return None
    return sorted(metrics, key=lambda metric: (_metric_preference_score(metric, provider, window), int(metric.get("percent", 0))), reverse=True)[0]


def _graph_from_metric(metric: dict[str, Any], points: list[dict[str, Any]]) -> dict[str, Any]:
    details = _safe_json(metric.get("details"))
    value_kind = str(details.get("graph_value_kind") or "percent")
    graph_max_value = details.get("graph_max_value", metric.get("max_value", 100))
    try:
        graph_max_value = int(round(float(graph_max_value)))
    except (TypeError, ValueError):
        graph_max_value = int(metric.get("max_value", 100))
    points = _normalize_graph_points(
        points,
        window_start=metric.get("window_start"),
        window_end=metric.get("window_end") or metric.get("reset_at"),
    )
    if value_kind == "currency_cents" and points:
        graph_max_value = max(graph_max_value, _currency_graph_max(points[-1]["value"]))
    if value_kind == "percent":
        peak_percent = int(metric.get("percent", 0))
        if points:
            peak_percent = max([peak_percent] + [int(point.get("value", 0)) for point in points if isinstance(point, dict)])
        graph_max_value = _percent_graph_max(max(graph_max_value, peak_percent))
    return {
        "metric_key": metric.get("metric_key"),
        "provider_metric_key": metric.get("provider_metric_key"),
        "metric_path": metric.get("metric_path"),
        "label": metric.get("metric_label"),
        "percent": int(metric.get("percent", 0)),
        "max_value": graph_max_value,
        "window_start": metric.get("window_start") or None,
        "window_end": metric.get("window_end") or None,
        "reset_at": metric.get("reset_at") or None,
        "value_kind": value_kind,
        "pace_line": bool(details.get("graph_pace_line", value_kind == "percent")),
        "reference_value": details.get("graph_reference_value"),
        "points": points,
    }


def _normalize_graph_points(
    points: list[dict[str, Any]],
    window_start: Any = None,
    window_end: Any = None,
) -> list[dict[str, Any]]:
    normalized = sorted(
        [
            {"t": int(point.get("t", 0)), "value": int(round(float(point.get("value", 0))))}
            for point in points
            if point.get("t") is not None and point.get("value") is not None
        ],
        key=lambda point: point["t"],
    )

    start_dt = _parse_timestamp(window_start) if window_start else None
    end_dt = _parse_timestamp(window_end) if window_end else None
    start_epoch = int(start_dt.timestamp()) if start_dt else None
    end_epoch = int(end_dt.timestamp()) if end_dt else None

    if start_epoch is not None:
        normalized = [point for point in normalized if point["t"] >= start_epoch]
    if end_epoch is not None:
        normalized = [point for point in normalized if point["t"] <= end_epoch]

    deduped: list[dict[str, Any]] = []
    for point in normalized:
        if deduped and point["t"] == deduped[-1]["t"]:
            if point["value"] > deduped[-1]["value"]:
                deduped[-1] = point
            continue
        deduped.append(point)

    return deduped


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return default


def _cursor_monthly_limit_cents(metric: dict[str, Any]) -> int:
    details = _safe_json(metric.get("details"))
    return _as_int(details.get("limit_cents"))


def _cursor_monthly_total_cents(
    monthly_metric: dict[str, Any],
    over_cap_metric: dict[str, Any] | None,
    monthly_percent: int | None = None,
    over_cap_cents: int | None = None,
) -> int:
    details = _safe_json(monthly_metric.get("details"))
    limit_cents = _cursor_monthly_limit_cents(monthly_metric)
    if limit_cents <= 0:
        return _as_int(monthly_metric.get("value_num"))

    total_spend_cents = _as_int(details.get("total_spend_cents"))
    if monthly_percent is None and over_cap_cents is None and total_spend_cents > 0:
        return total_spend_cents

    value_num = _as_int(monthly_metric.get("value_num"))
    if monthly_percent is None and over_cap_cents is None and value_num > limit_cents:
        return value_num

    pct = _as_int(monthly_percent if monthly_percent is not None else monthly_metric.get("percent"))
    if pct >= 100:
        included_cents = limit_cents
    else:
        included_cents = min(limit_cents, _as_int((limit_cents * max(0, pct)) / 100))

    if over_cap_cents is None:
        over_cap_cents = _as_int(over_cap_metric.get("value_num")) if over_cap_metric else 0
    return max(0, included_cents + max(0, over_cap_cents))


def _cursor_adjust_monthly_metric_and_points(
    monthly_metric: dict[str, Any],
    over_cap_metric: dict[str, Any] | None,
    monthly_points: list[dict[str, Any]],
    over_cap_points: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    limit_cents = _cursor_monthly_limit_cents(monthly_metric)
    if limit_cents <= 0:
        return monthly_metric, monthly_points

    adjusted_metric = dict(monthly_metric)
    details = _safe_json(adjusted_metric.get("details"))
    adjusted_details = dict(details)
    adjusted_details["graph_reference_value"] = 100
    adjusted_metric["details"] = adjusted_details

    current_total_cents = _cursor_monthly_total_cents(adjusted_metric, over_cap_metric)
    current_percent = _parse_unbounded_percentage((current_total_cents / limit_cents) * 100 if limit_cents else 0)
    adjusted_metric["value_num"] = current_total_cents
    adjusted_metric["percent"] = current_percent
    adjusted_metric["value"] = f"{current_percent}%"
    adjusted_metric["max_value"] = _percent_graph_max(current_percent)

    if any(_as_int(point.get("value")) > 100 for point in monthly_points):
        return adjusted_metric, monthly_points

    merged: dict[int, dict[str, int]] = {}
    for point in monthly_points:
        t = _as_int(point.get("t"))
        if t <= 0:
            continue
        merged.setdefault(t, {})["monthly_percent"] = _as_int(point.get("value"))
    for point in over_cap_points:
        t = _as_int(point.get("t"))
        if t <= 0:
            continue
        merged.setdefault(t, {})["over_cap_cents"] = _as_int(point.get("value"))

    if not merged:
        return adjusted_metric, monthly_points

    combined_points: list[dict[str, int]] = []
    last_monthly_percent = 0
    last_over_cap_cents = 0
    for t in sorted(merged):
        row = merged[t]
        if "monthly_percent" in row:
            last_monthly_percent = row["monthly_percent"]
        if "over_cap_cents" in row:
            last_over_cap_cents = row["over_cap_cents"]
        total_cents = _cursor_monthly_total_cents(
            adjusted_metric,
            over_cap_metric,
            monthly_percent=last_monthly_percent,
            over_cap_cents=last_over_cap_cents,
        )
        total_percent = _parse_unbounded_percentage((total_cents / limit_cents) * 100 if limit_cents else 0)
        combined_points.append({"t": t, "value": total_percent})

    return adjusted_metric, combined_points


def _cursor_monthly_points_from_total_spend(
    monthly_metric: dict[str, Any],
    total_spend_points: list[dict[str, Any]],
) -> list[dict[str, int]]:
    limit_cents = _cursor_monthly_limit_cents(monthly_metric)
    if limit_cents <= 0:
        return []

    points: list[dict[str, int]] = []
    for point in total_spend_points:
        t = _as_int(point.get("t"))
        cents = _as_int(point.get("value"))
        if t <= 0:
            continue
        points.append(
            {
                "t": t,
                "value": _parse_unbounded_percentage((cents / limit_cents) * 100 if limit_cents else 0),
            }
        )
    return points


def _cursor_monthly_points_from_non_auto_events(
    monthly_metric: dict[str, Any],
    event_points: list[dict[str, Any]],
) -> list[dict[str, int]]:
    limit_cents = _cursor_monthly_limit_cents(monthly_metric)
    current_total_cents = _as_int(monthly_metric.get("value_num"))
    if limit_cents <= 0 or not event_points:
        return []

    last_value = _as_int(event_points[-1].get("value"))
    scale = (float(current_total_cents) / float(last_value)) if current_total_cents > 0 and last_value > 0 else 1.0

    points: list[dict[str, int]] = []
    for point in event_points:
        t = _as_int(point.get("t"))
        cents = _as_int(point.get("value"))
        if t <= 0:
            continue
        scaled_cents = int(round(cents * scale))
        points.append(
            {
                "t": t,
                "value": _parse_unbounded_percentage((scaled_cents / limit_cents) * 100 if limit_cents else 0),
            }
        )
    return points


def _provider_status(
    provider: str,
    success_row: dict[str, Any] | None,
    attempt_row: dict[str, Any] | None,
) -> dict[str, Any]:
    last_success_at = _timestamp_to_iso_utc(success_row.get("fetched_at") if success_row else None)
    last_attempt_at = _timestamp_to_iso_utc(attempt_row.get("fetched_at") if attempt_row else None)
    status = {
        "state": "ok",
        "label": "Live",
        "message": "",
        "stale": False,
        "http_status": int(attempt_row.get("http_status") or 0) if attempt_row else 0,
        "error_code": "",
        "last_success_at": last_success_at,
        "last_attempt_at": last_attempt_at,
    }
    if not attempt_row or bool(attempt_row.get("success")):
        return status

    payload = _safe_json(attempt_row.get("raw_payload"))
    error = _safe_json(payload.get("error"))
    details = _safe_json(error.get("details"))
    error_code = _coalesce(details.get("error_code"))
    attempt_time = _timestamp_to_clock_local(attempt_row.get("fetched_at"))
    success_time = _timestamp_to_clock_local(success_row.get("fetched_at") if success_row else None)
    status["state"] = "error"
    status["stale"] = bool(success_row and success_row.get("fetched_at") != attempt_row.get("fetched_at"))
    status["error_code"] = error_code

    stale_suffix = f" Showing data from {success_time}." if success_time else ""
    if error_code == "account_session_invalid":
        status["label"] = "Sign-in expired"
        status["message"] = f"Sign-in expired{f' at {attempt_time}' if attempt_time else ''}.{stale_suffix}".strip()
        return status

    message = _coalesce(error.get("message"), attempt_row.get("request_error"))
    if status["http_status"] in {401, 403}:
        status["label"] = "Auth failed"
        detail = f" ({status['http_status']})" if status["http_status"] else ""
        status["message"] = f"Auth failed{detail}.{stale_suffix}".strip()
        return status

    status["label"] = "Fetch failed"
    suffix = f" {message}." if message else ""
    if stale_suffix:
        status["message"] = f"Latest fetch failed.{suffix}{stale_suffix}".strip()
    else:
        status["message"] = f"Latest fetch failed.{suffix}".strip()
    return status


def build_state_agent(
    snapshot: ProviderSnapshot,
    graph_points: dict[str, list[dict[str, Any]]],
    provider_status: dict[str, Any] | None = None,
    updated_at: str = "",
) -> dict[str, Any]:
    summary_metric = snapshot.metrics[0]
    for metric in snapshot.metrics:
        if metric["metric_key"] == snapshot.summary_key:
            summary_metric = metric
            break

    if snapshot.provider == "cursor":
        order = ["monthly", "auto_spend", "over_cap_used", "api_usage", "auto_usage", "included_spend", "total_spend", "provider_total_usage"]
    elif snapshot.provider == "claude":
        order = ["seven_day", "five_hour"]
    else:
        order = ["secondary_window", "primary_window"]
    order_index = {key: idx for idx, key in enumerate(order)}
    metric_rows = []
    for metric in sorted(snapshot.metrics, key=lambda metric: (order_index.get(metric.get("metric_key", ""), len(order)), metric.get("metric_label", ""))):
        metric_rows.append(
            {
                "label": metric["metric_label"],
                "metric_key": metric["metric_key"],
                "provider_metric_key": metric.get("provider_metric_key"),
                "metric_path": metric.get("metric_path", ""),
                "metric_id": metric.get("metric_id", metric.get("metric_path", "")),
                "value": metric["value"],
                "percent": int(metric["percent"]),
                "accent": (
                    "secondary"
                    if metric["metric_key"]
                    in {"primary_window", "five_hour", "session", "week", "auto_spend"}
                    else ("primary" if snapshot.provider == "claude" else "tertiary" if snapshot.provider == "codex" else "primary")
                ),
                "note": metric.get("note", ""),
                "show_bar": not (
                    _safe_json(metric.get("details")).get("graph_value_kind") == "currency_cents"
                    or metric["metric_key"] in {"over_cap_used", "total_spend", "provider_total_usage"}
                ),
            }
        )

    accent = "primary" if snapshot.provider in ("claude", "cursor") else "tertiary"
    short_label = "Cl" if snapshot.provider == "claude" else "Cx" if snapshot.provider == "codex" else "Cu"
    plan = snapshot.request_metadata.get("plan", "Pro")
    summary_label = "Monthly usage" if snapshot.provider == "cursor" else "Weekly usage"
    source_id = _coalesce(snapshot.source_id, snapshot.provider)
    label = _coalesce(snapshot.source_label, snapshot.request_metadata.get("source_label"), snapshot.provider.title())
    long_metric = _pick_graph_metric(snapshot.metrics, snapshot.provider, "long_window")
    short_metric = _pick_graph_metric(snapshot.metrics, snapshot.provider, "short_window")
    graphs: dict[str, Any] = {}
    if long_metric:
        graphs["long_window"] = _graph_from_metric(long_metric, graph_points.get(long_metric.get("metric_path") or long_metric.get("metric_key"), []))
    if short_metric:
        graphs["short_window"] = _graph_from_metric(short_metric, graph_points.get(short_metric.get("metric_path") or short_metric.get("metric_key"), []))

    return {
        "id": source_id,
        "source_id": source_id,
        "provider": snapshot.provider,
        "label": label,
        "short_label": short_label,
        "accent": accent,
        "plan": plan,
        "updated_at": updated_at,
        "status": provider_status or {"state": "ok", "label": "Live", "message": "", "stale": False},
        "summary": {
            "label": summary_label,
            "value": summary_metric["value"],
            "percent": int(summary_metric["percent"]),
            "note": summary_metric.get("note", ""),
        },
        "graphs": graphs,
        "metrics": metric_rows,
        "history": {
            "label": (graphs.get("long_window") or {}).get("label", snapshot.history_label or _build_history_label(snapshot.provider)),
            "max_value": int(summary_metric.get("max_value", 100)),
            "window_start": (graphs.get("long_window") or {}).get("window_start"),
            "window_end": (graphs.get("long_window") or {}).get("window_end"),
            "reset_at": (graphs.get("long_window") or {}).get("reset_at"),
            "points": (graphs.get("long_window") or {}).get("points", []),
        },
        "details": snapshot.details,
        "raw_provider_scope": {
            "account_id": snapshot.account_id,
            "organization_id": snapshot.organization_id,
        },
    }


def _parse_psql_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        stripped = text.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            raise
        return int(stripped)


class PostgresClient:
    @dataclass(frozen=True)
    class _ParsedDsn:
        host: str | None
        port: str | None
        user: str | None
        password: str | None
        dbname: str | None

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._parsed_dsn = self._parse_dsn(dsn)

    def _parse_dsn(self, dsn: str) -> _ParsedDsn | None:
        if not (dsn.startswith("postgresql://") or dsn.startswith("postgres://")):
            return None

        parsed = urlparse(dsn)
        if not parsed.scheme.startswith("postgres"):
            return None

        return self._ParsedDsn(
            host=parsed.hostname,
            port=str(parsed.port) if parsed.port else None,
            user=unquote(parsed.username or "") if parsed.username else None,
            password=unquote(parsed.password or "") if parsed.password else None,
            dbname=parsed.path[1:] if parsed.path else None,
        )

    def _psql_env(self) -> dict[str, str]:
        if not self._parsed_dsn or not self._parsed_dsn.password:
            return os.environ
        env = dict(os.environ)
        env["PGPASSWORD"] = self._parsed_dsn.password
        return env

    def _psql_cmd(self, vars: dict[str, str] | None = None) -> list[str]:
        cmd = [
            "psql",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            "VERBOSITY=terse",
            "-A",
            "-t",
            "-q",
        ]
        if self._parsed_dsn is not None:
            if self._parsed_dsn.host:
                cmd.extend(["-h", self._parsed_dsn.host])
            if self._parsed_dsn.port:
                cmd.extend(["-p", self._parsed_dsn.port])
            if self._parsed_dsn.dbname:
                cmd.extend(["-d", self._parsed_dsn.dbname])
            if self._parsed_dsn.user:
                cmd.extend(["-U", self._parsed_dsn.user])
        else:
            cmd.insert(1, self.dsn)

        if vars:
            for key, value in vars.items():
                cmd.extend(["-v", f"{key}={value}"])
        return cmd

    def ensure_schema(self) -> None:
        try:
            result = subprocess.run(
                self._psql_cmd() + ["-f", str(SCHEMA_SQL_PATH)],
                check=True,
                capture_output=True,
                text=True,
                env=self._psql_env(),
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout or str(exc)
            raise RuntimeError(f"psql schema bootstrap failed: {detail}") from exc
        if result.stdout:
            _parse_psql_json(result.stdout)
        migrated = self.migrate_legacy_cursor_scope()
        if migrated:
            print(
                f"[agent-usage] migrated {migrated} legacy Cursor fetch rows to the scoped identity",
                flush=True,
            )

    def _run(self, sql: str, vars: dict[str, str] | None = None) -> str:
        base_cmd = self._psql_cmd(vars)
        if vars is None:
            cmd = base_cmd + ["-c", sql]
            stdin_payload = None
        else:
            cmd = base_cmd
            stdin_payload = sql

        try:
            result = subprocess.run(
                cmd,
                input=stdin_payload,
                check=True,
                capture_output=True,
                text=True,
                env=self._psql_env(),
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout or str(exc)
            raise RuntimeError(f"psql command failed: {detail}") from exc
        return (result.stdout or "").strip()

    def ping(self) -> None:
        self.ensure_schema()
        self._run("SELECT 1;")

    def migrate_legacy_cursor_scope(self) -> int:
        sql = """
WITH real_identities AS (
  SELECT
    account_id,
    organization_id,
    MAX(fetched_at) AS latest_fetched_at
  FROM usage_provider_fetch
  WHERE provider = 'cursor'
    AND account_id <> ''
    AND organization_id <> ''
    AND NOT (account_id = 'cursor_user' AND organization_id = 'cursor_org')
  GROUP BY account_id, organization_id
),
target_identity AS (
  SELECT account_id, organization_id
  FROM real_identities
  ORDER BY latest_fetched_at DESC
  LIMIT 1
),
identity_count AS (
  SELECT COUNT(*) AS n FROM real_identities
),
updated AS (
  UPDATE usage_provider_fetch f
  SET
    account_id = t.account_id,
    organization_id = t.organization_id
  FROM target_identity t, identity_count c
  WHERE c.n = 1
    AND f.provider = 'cursor'
    AND f.account_id = 'cursor_user'
    AND f.organization_id = 'cursor_org'
  RETURNING 1
)
SELECT COUNT(*) FROM updated;
"""
        result = _parse_psql_json(self._run(sql))
        if isinstance(result, list):
            return int(result[0] or 0) if result else 0
        return int(result or 0)

    def persist_snapshot(self, snapshot: ProviderSnapshot) -> int:
        source_id = _coalesce(snapshot.source_id, snapshot.provider)
        metadata = dict(snapshot.request_metadata)
        metadata.setdefault("source_id", source_id)
        metadata.setdefault("source_label", _coalesce(snapshot.source_label, source_id))
        metadata.setdefault("frontend_visible", snapshot.frontend_visible)
        sql_lines = [
            "BEGIN;",
            r"""
INSERT INTO usage_provider_fetch
  (source_id, provider, account_id, organization_id, requested_url, http_status, request_error, raw_payload, request_metadata, success)
VALUES
  (:'source_id', :'provider', :'account_id', :'organization_id', :'request_url', :status, :'error',
   convert_from(decode(:'payload_b64', 'base64'), 'UTF8')::jsonb,
   convert_from(decode(:'metadata_b64', 'base64'), 'UTF8')::jsonb,
   :success)
RETURNING id AS fetch_id
\gset
""".strip(),
        ]

        vars = {
            "source_id": source_id,
            "provider": snapshot.provider,
            "account_id": snapshot.account_id,
            "organization_id": snapshot.organization_id,
            "request_url": snapshot.request_url,
            "status": str(snapshot.request_status),
            "error": _coalesce(snapshot.request_error),
            "payload_b64": _to_base64_json(snapshot.raw_payload),
            "metadata_b64": _to_base64_json(metadata),
            "success": ("true" if snapshot.success else "false"),
        }

        for idx, metric in enumerate(snapshot.metrics):
            reset_at = metric.get("reset_at")
            sql_lines.append(
                f"""
INSERT INTO usage_metric_snapshot
  (provider_fetch_id, source_id, provider, metric_key, provider_metric_key, metric_path, metric_scope, metric_label, percent, value_num, value_text, note, max_value, window_start, window_end, reset_at, details)
VALUES
  (:fetch_id, :'source_id_{idx}', :'provider_{idx}', :'metric_key_{idx}', :'provider_metric_key_{idx}', :'metric_path_{idx}', :'metric_scope_{idx}', :'metric_label_{idx}', :percent_{idx}, NULLIF(:'value_num_{idx}', '')::double precision, :'value_text_{idx}', :'note_{idx}', :max_value_{idx}, NULLIF(:'window_start_{idx}', '')::timestamptz, NULLIF(:'window_end_{idx}', '')::timestamptz, NULLIF(:'reset_at_{idx}', '')::timestamptz, convert_from(decode(:'details_b64_{idx}', 'base64'), 'UTF8')::jsonb);
""".strip()
            )
            vars.update(
                {
                    f"source_id_{idx}": source_id,
                    f"provider_{idx}": snapshot.provider,
                    f"metric_key_{idx}": metric["metric_key"],
                    f"provider_metric_key_{idx}": metric.get("provider_metric_key", metric["metric_key"]),
                    f"metric_path_{idx}": metric.get("metric_path", f"/{metric['metric_key']}"),
                    f"metric_scope_{idx}": metric.get("metric_scope", "/"),
                    f"metric_label_{idx}": metric["metric_label"],
                    f"percent_{idx}": str(int(metric["percent"])),
                    f"value_num_{idx}": str(metric.get("value_num", "")) if metric.get("value_num") is not None else "",
                    f"value_text_{idx}": metric["value"],
                    f"note_{idx}": _coalesce(metric.get("note")),
                    f"max_value_{idx}": str(int(metric.get("max_value", 100))),
                    f"window_start_{idx}": _coalesce(metric.get("window_start"), ""),
                    f"window_end_{idx}": _coalesce(metric.get("window_end"), ""),
                    f"reset_at_{idx}": _coalesce(reset_at, ""),
                    f"details_b64_{idx}": _to_base64_json(metric.get("details", {})),
                }
            )

        sql_lines.extend(
            [
                "COMMIT;",
                "SELECT :fetch_id;",
            ]
        )
        out = self._run("\n".join(sql_lines), vars=vars)
        parsed = _parse_psql_json(out)
        if isinstance(parsed, list):
            return int(parsed[0])
        return int(parsed)

    def insert_provider_fetch(self, snapshot: ProviderSnapshot) -> int:
        source_id = _coalesce(snapshot.source_id, snapshot.provider)
        metadata = dict(snapshot.request_metadata)
        metadata.setdefault("source_id", source_id)
        metadata.setdefault("source_label", _coalesce(snapshot.source_label, source_id))
        metadata.setdefault("frontend_visible", snapshot.frontend_visible)
        sql = """
INSERT INTO usage_provider_fetch
  (source_id, provider, account_id, organization_id, requested_url, http_status, request_error, raw_payload, request_metadata, success)
VALUES
  (:'source_id', :'provider', :'account_id', :'organization_id', :'request_url', :status, :'error',
   convert_from(decode(:'payload_b64', 'base64'), 'UTF8')::jsonb,
   convert_from(decode(:'metadata_b64', 'base64'), 'UTF8')::jsonb,
   :success)
RETURNING id;
"""
        out = self._run(
            sql,
            vars={
                "source_id": source_id,
                "provider": snapshot.provider,
                "account_id": snapshot.account_id,
                "organization_id": snapshot.organization_id,
                "request_url": snapshot.request_url,
                "status": str(snapshot.request_status),
                "error": _coalesce(snapshot.request_error),
                "payload_b64": _to_base64_json(snapshot.raw_payload),
                "metadata_b64": _to_base64_json(metadata),
                "success": ("true" if snapshot.success else "false"),
            },
        )
        parsed = _parse_psql_json(out)
        if isinstance(parsed, list):
            return int(parsed[0])
        return int(parsed)

    def insert_metric_snapshot(self, provider_fetch_id: int, snapshot: ProviderSnapshot) -> None:
        source_id = _coalesce(snapshot.source_id, snapshot.provider)
        sql = """
INSERT INTO usage_metric_snapshot
  (provider_fetch_id, source_id, provider, metric_key, provider_metric_key, metric_path, metric_scope, metric_label, percent, value_num, value_text, note, max_value, window_start, window_end, reset_at, details)
VALUES
  (:'provider_fetch_id', :'source_id', :'provider', :'metric_key', :'provider_metric_key', :'metric_path', :'metric_scope', :'metric_label', :percent, NULLIF(:'value_num', '')::double precision, :'value_text', :'note', :max_value, NULLIF(:'window_start', '')::timestamptz, NULLIF(:'window_end', '')::timestamptz, NULLIF(:'reset_at', '')::timestamptz, convert_from(decode(:'details_b64', 'base64'), 'UTF8')::jsonb)
"""
        for metric in snapshot.metrics:
            reset_at = metric.get("reset_at")
            self._run(
                sql,
                vars={
                    "provider_fetch_id": str(provider_fetch_id),
                    "source_id": source_id,
                    "provider": snapshot.provider,
                    "metric_key": metric["metric_key"],
                    "provider_metric_key": metric.get("provider_metric_key", metric["metric_key"]),
                    "metric_path": metric.get("metric_path", f"/{metric['metric_key']}"),
                    "metric_scope": metric.get("metric_scope", "/"),
                    "metric_label": metric["metric_label"],
                    "percent": str(int(metric["percent"])),
                    "value_num": str(metric.get("value_num", "")) if metric.get("value_num") is not None else "",
                    "value_text": metric["value"],
                    "note": _coalesce(metric.get("note")),
                    "max_value": str(int(metric.get("max_value", 100))),
                    "window_start": _coalesce(metric.get("window_start"), ""),
                    "window_end": _coalesce(metric.get("window_end"), ""),
                    "reset_at": _coalesce(reset_at, ""),
                    "details_b64": _to_base64_json(metric.get("details", {})),
                },
            )

    def _sql_in_filter(
        self,
        column: str,
        prefix: str,
        values: list[str] | tuple[str, ...] | None,
        vars: dict[str, str],
    ) -> str:
        cleaned = [_coalesce(value) for value in (values or []) if _coalesce(value)]
        if not cleaned:
            return ""
        placeholders: list[str] = []
        for idx, value in enumerate(cleaned):
            key = f"{prefix}_{idx}"
            vars[key] = value
            placeholders.append(f":'{key}'")
        return f"\n    AND {column} IN ({', '.join(placeholders)})"

    def latest_fetch(
        self,
        source_ids: list[str] | tuple[str, ...] | None = None,
        providers: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        vars: dict[str, str] = {}
        filter_sql = self._sql_in_filter("source_id", "source", source_ids, vars)
        filter_sql += self._sql_in_filter("provider", "provider", providers, vars)
        sql = """
SELECT COALESCE(json_agg(row_to_json(t) ORDER BY provider, source_id), '[]'::json)
FROM (
  SELECT
    f.id,
    f.source_id,
    f.provider,
    f.fetched_at,
    f.account_id,
    f.organization_id,
    f.request_metadata,
    f.request_error
  FROM (
    SELECT DISTINCT ON (source_id)
      id,
      source_id,
      provider,
      fetched_at,
      account_id,
      organization_id,
      request_metadata,
      request_error
    FROM usage_provider_fetch
    WHERE success = true
__FILTER_SQL__
    ORDER BY
      source_id,
      CASE WHEN requested_url LIKE 'legacy://%' THEN 1 ELSE 0 END,
      fetched_at DESC,
      id DESC
  ) AS f
) t;
"""
        return _parse_psql_json(self._run(sql.replace("__FILTER_SQL__", filter_sql), vars=vars or None))

    def latest_source_fetch(self, source_id: str) -> dict[str, Any] | None:
        rows = self.latest_fetch(source_ids=[source_id])
        row = rows[0] if rows else None
        return row if isinstance(row, dict) else None

    def latest_provider_fetch(self, provider: str, source_id: str | None = None) -> dict[str, Any] | None:
        if source_id:
            return self.latest_source_fetch(source_id)
        rows = self.latest_fetch(providers=[provider])
        if not rows:
            return None
        row = sorted(rows, key=lambda item: (str(item.get("fetched_at", "")), int(item.get("id", 0))))[-1]
        return row if isinstance(row, dict) else None

    def latest_attempts(
        self,
        source_ids: list[str] | tuple[str, ...] | None = None,
        providers: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        vars: dict[str, str] = {}
        filter_sql = self._sql_in_filter("source_id", "source", source_ids, vars)
        filter_sql += self._sql_in_filter("provider", "provider", providers, vars)
        sql = """
SELECT COALESCE(json_agg(row_to_json(t) ORDER BY provider, source_id), '[]'::json)
FROM (
  SELECT
    f.id,
    f.source_id,
    f.provider,
    f.fetched_at,
    f.success,
    f.http_status,
    f.account_id,
    f.organization_id,
    f.request_metadata,
    f.request_error,
    f.raw_payload
  FROM (
    SELECT DISTINCT ON (source_id)
      id,
      source_id,
      provider,
      fetched_at,
      success,
      http_status,
      account_id,
      organization_id,
      request_metadata,
      request_error,
      raw_payload,
      requested_url
    FROM usage_provider_fetch
    WHERE true
__FILTER_SQL__
    ORDER BY
      source_id,
      CASE WHEN requested_url LIKE 'legacy://%' THEN 1 ELSE 0 END,
      fetched_at DESC,
      id DESC
  ) AS f
) t;
"""
        return _parse_psql_json(self._run(sql.replace("__FILTER_SQL__", filter_sql), vars=vars or None))

    def latest_source_attempt(self, source_id: str) -> dict[str, Any] | None:
        rows = self.latest_attempts(source_ids=[source_id])
        row = rows[0] if rows else None
        return row if isinstance(row, dict) else None

    def latest_metrics(self, fetch_id: int) -> list[dict[str, Any]]:
        sql = """
SELECT COALESCE(json_agg(row_to_json(t) ORDER BY metric_key), '[]'::json)
FROM (
  SELECT
    source_id,
    metric_key,
    provider_metric_key,
    metric_path,
    metric_scope,
    metric_label,
    percent,
    value_num,
    value_text,
    note,
    max_value,
    window_start,
    window_end,
    reset_at,
    details
  FROM usage_metric_snapshot
  WHERE provider_fetch_id = :provider_fetch_id
  ORDER BY metric_key, metric_path
) t;
"""
        return _parse_psql_json(self._run(sql, vars={"provider_fetch_id": str(fetch_id)}))

    def latest_metric(
        self,
        provider: str,
        metric: str,
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
    ) -> dict[str, Any] | None:
        scope_sql = ""
        vars = {"provider": provider, "metric": metric}
        if source_id:
            scope_sql += "\n    AND f.source_id = :'source_id'"
            vars["source_id"] = source_id
        if account_id:
            scope_sql += "\n    AND f.account_id = :'account_id'"
            vars["account_id"] = account_id
        if organization_id:
            scope_sql += "\n    AND f.organization_id = :'organization_id'"
            vars["organization_id"] = organization_id
        sql = """
SELECT COALESCE(row_to_json(x), '{}'::json)
FROM (
  SELECT
    m.metric_key,
    m.source_id,
    m.provider_metric_key,
    m.metric_label,
    m.percent,
    m.value_num,
    m.value_text,
    m.note,
    m.max_value,
    m.window_start,
    m.window_end,
    m.reset_at,
    m.metric_path,
    m.details
  FROM usage_metric_snapshot m
  JOIN usage_provider_fetch f ON f.id = m.provider_fetch_id
  WHERE f.provider = :'provider'
    AND f.success = true
    AND (m.metric_path = :'metric' OR m.metric_key = :'metric')
__SCOPE_SQL__
  ORDER BY
    CASE WHEN m.metric_path = :'metric' THEN 0 ELSE 1 END,
    CASE WHEN m.metric_path LIKE '/legacy/%' THEN 1 ELSE 0 END,
    f.fetched_at DESC,
    m.created_at DESC
  LIMIT 1
) x;
"""
        result = _parse_psql_json(self._run(sql.replace("__SCOPE_SQL__", scope_sql), vars=vars))
        if isinstance(result, dict):
            return result
        if isinstance(result, list) and result:
            return result[0]
        return None

    def latest_cursor_usage_sync_through(self, cycle_end: str | None, source_id: str = "cursor") -> int:
        if not cycle_end:
            return 0
        sql = """
SELECT COALESCE((
  SELECT synced_through_timestamp_ms
  FROM cursor_usage_sync_state
  WHERE cycle_end = NULLIF(:'cycle_end', '')::timestamptz
    AND source_id = :'source_id'
), 0);
"""
        result = _parse_psql_json(self._run(sql, vars={"cycle_end": cycle_end, "source_id": source_id}))
        if isinstance(result, list):
            return int(result[0] or 0) if result else 0
        return int(result or 0)

    def latest_cursor_usage_total_count(self, cycle_end: str | None, source_id: str = "cursor") -> int:
        if not cycle_end:
            return 0
        sql = """
SELECT COALESCE((
  SELECT total_usage_events_count
  FROM cursor_usage_sync_state
  WHERE cycle_end = NULLIF(:'cycle_end', '')::timestamptz
    AND source_id = :'source_id'
), 0);
"""
        result = _parse_psql_json(self._run(sql, vars={"cycle_end": cycle_end, "source_id": source_id}))
        if isinstance(result, list):
            return int(result[0] or 0) if result else 0
        return int(result or 0)

    def latest_cursor_usage_timestamp(self, cycle_end: str | None, source_id: str = "cursor") -> int:
        if not cycle_end:
            return 0
        sql = """
SELECT COALESCE(MAX(event_timestamp_ms), 0)
FROM cursor_usage_event
WHERE cycle_end = NULLIF(:'cycle_end', '')::timestamptz
  AND source_id = :'source_id';
"""
        result = _parse_psql_json(self._run(sql, vars={"cycle_end": cycle_end, "source_id": source_id}))
        if isinstance(result, list):
            return int(result[0] or 0) if result else 0
        return int(result or 0)

    def insert_cursor_usage_events(
        self,
        provider_fetch_id: int,
        source_id: str,
        cycle_start: str | None,
        cycle_end: str | None,
        page: int,
        events: list[dict[str, Any]],
    ) -> int:
        sql = """
INSERT INTO cursor_usage_event
  (provider_fetch_id, source_id, event_id, event_timestamp, event_timestamp_ms, cycle_start, cycle_end, page, model, kind, charged_cents, is_chargeable, is_headless, is_token_based_call, raw_event)
VALUES
  (:'provider_fetch_id', :'source_id', :'event_id', NULLIF(:'event_timestamp', '')::timestamptz, :'event_timestamp_ms', NULLIF(:'cycle_start', '')::timestamptz, NULLIF(:'cycle_end', '')::timestamptz, :'page', :'model', :'kind', NULLIF(:'charged_cents', '')::double precision, :'is_chargeable'::boolean, :'is_headless'::boolean, :'is_token_based_call'::boolean, convert_from(decode(:'raw_event_b64', 'base64'), 'UTF8')::jsonb)
ON CONFLICT (source_id, event_id) DO NOTHING
RETURNING 1;
"""
        inserted = 0
        for event in events:
            event_timestamp = _timestamp_to_iso_utc(event.get("timestamp"))
            event_timestamp_ms = _timestamp_to_epoch_ms(event.get("timestamp"))
            if event_timestamp_ms is None:
                continue
            result = _parse_psql_json(
                self._run(
                    sql,
                    vars={
                        "provider_fetch_id": str(provider_fetch_id),
                        "source_id": source_id,
                        "event_id": _cursor_usage_event_id(event),
                        "event_timestamp": event_timestamp,
                        "event_timestamp_ms": str(event_timestamp_ms),
                        "cycle_start": _coalesce(cycle_start, ""),
                        "cycle_end": _coalesce(cycle_end, ""),
                        "page": str(page),
                        "model": str(event.get("model") or ""),
                        "kind": str(event.get("kind") or ""),
                        "charged_cents": str(event.get("chargedCents", "")) if event.get("chargedCents") is not None else "",
                        "is_chargeable": "true" if bool(event.get("isChargeable")) else "false",
                        "is_headless": "true" if bool(event.get("isHeadless")) else "false",
                        "is_token_based_call": "true" if bool(event.get("isTokenBasedCall")) else "false",
                        "raw_event_b64": _to_base64_json(event),
                    },
                )
            )
            if result:
                inserted += 1
        return inserted

    def update_cursor_usage_sync_state(
        self,
        source_id: str,
        cycle_start: str | None,
        cycle_end: str | None,
        synced_through_timestamp_ms: int,
        total_usage_events_count: int,
        last_page_fetched: int,
        last_inserted_count: int,
    ) -> None:
        if not cycle_end:
            return
        sql = """
INSERT INTO cursor_usage_sync_state
  (source_id, cycle_end, cycle_start, synced_through_timestamp_ms, total_usage_events_count, last_page_fetched, last_inserted_count, updated_at)
VALUES
  (:'source_id', NULLIF(:'cycle_end', '')::timestamptz, NULLIF(:'cycle_start', '')::timestamptz, :'synced_through_timestamp_ms', :'total_usage_events_count', :'last_page_fetched', :'last_inserted_count', now())
ON CONFLICT (source_id, cycle_end) DO UPDATE
SET
  cycle_start = EXCLUDED.cycle_start,
  synced_through_timestamp_ms = LEAST(cursor_usage_sync_state.synced_through_timestamp_ms, EXCLUDED.synced_through_timestamp_ms),
  total_usage_events_count = EXCLUDED.total_usage_events_count,
  last_page_fetched = EXCLUDED.last_page_fetched,
  last_inserted_count = EXCLUDED.last_inserted_count,
  updated_at = now();
"""
        self._run(
            sql,
            vars={
                "source_id": source_id,
                "cycle_end": cycle_end,
                "cycle_start": _coalesce(cycle_start, ""),
                "synced_through_timestamp_ms": str(synced_through_timestamp_ms),
                "total_usage_events_count": str(total_usage_events_count),
                "last_page_fetched": str(last_page_fetched),
                "last_inserted_count": str(last_inserted_count),
            },
        )

    def cursor_usage_cumulative_points(
        self,
        cycle_end: str | None,
        model: str = "default",
        source_id: str | None = None,
        account_id: str | None = None,
        organization_id: str | None = None,
        exclude_model: str | None = None,
    ) -> list[dict[str, Any]]:
        if not cycle_end:
            return []
        scope_sql = ""
        vars = {"cycle_end": cycle_end, "model": model}
        if source_id:
            scope_sql += "\n      AND e.source_id = :'source_id'"
            vars["source_id"] = source_id
        if account_id:
            scope_sql += "\n      AND f.account_id = :'account_id'"
            vars["account_id"] = account_id
        if organization_id:
            scope_sql += "\n      AND f.organization_id = :'organization_id'"
            vars["organization_id"] = organization_id
        if exclude_model is not None:
            scope_sql += "\n      AND COALESCE(e.model, '') <> :'exclude_model'"
            vars["exclude_model"] = exclude_model
        sql = """
SELECT COALESCE(json_agg(json_build_object('t', t, 'value', value) ORDER BY t), '[]'::json)
FROM (
  WITH raw AS (
    SELECT
      date_trunc('hour', event_timestamp) AS bucket,
      event_timestamp,
      SUM(COALESCE(charged_cents, 0)) OVER (ORDER BY event_timestamp ASC, e.id ASC) AS cumulative_cents
    FROM cursor_usage_event e
    JOIN usage_provider_fetch f ON f.id = e.provider_fetch_id
    WHERE e.cycle_end = NULLIF(:'cycle_end', '')::timestamptz
      AND e.is_chargeable = true
__MODEL_SQL__
__SCOPE_SQL__
  )
  SELECT
    EXTRACT(EPOCH FROM MAX(event_timestamp))::bigint AS t,
    ROUND(MAX(cumulative_cents))::bigint AS value
  FROM raw
  GROUP BY bucket
  ORDER BY bucket ASC
) x;
"""
        model_sql = "\n      AND e.model = :'model'" if exclude_model is None else ""
        return _parse_psql_json(self._run(sql.replace("__SCOPE_SQL__", scope_sql).replace("__MODEL_SQL__", model_sql), vars=vars))

    def cursor_auto_spend_points(
        self,
        metric_row: dict[str, Any],
        provider_row: dict[str, Any],
    ) -> list[dict[str, Any]]:
        details = _safe_json(metric_row.get("details"))
        points = self.cursor_usage_cumulative_points(
            cycle_end=_coalesce(metric_row.get("window_end") or metric_row.get("reset_at")),
            model=str(details.get("graph_model") or "default"),
            source_id=_coalesce(provider_row.get("source_id")),
            account_id=_coalesce(provider_row.get("account_id")),
            organization_id=_coalesce(provider_row.get("organization_id")),
        )

        fetched_at = _parse_timestamp(provider_row.get("fetched_at"))
        value_num = metric_row.get("value_num")
        if not fetched_at or value_num is None:
            return points

        snapshot_point = {
            "t": int(fetched_at.timestamp()),
            "value": int(round(float(value_num))),
        }
        if not points:
            return [snapshot_point]

        merged = list(points)
        last = merged[-1]
        last_t = int(last.get("t", 0))
        last_value = int(round(float(last.get("value", 0))))
        if snapshot_point["t"] == last_t:
            if snapshot_point["value"] > last_value:
                merged[-1] = snapshot_point
            return merged
        if snapshot_point["t"] > last_t and snapshot_point["value"] >= last_value:
            merged.append(snapshot_point)
        return merged

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
        window_slop_seconds = 120
        scope_sql = ""
        scope_vars: dict[str, str] = {}
        value_sql = "ROUND(COALESCE(m.value_num, 0))::bigint AS value" if use_value_num else "m.percent AS value"
        if source_id:
            scope_sql += "\n    AND f.source_id = :'source_id'"
            scope_vars["source_id"] = source_id
        if account_id:
            scope_sql += "\n    AND f.account_id = :'account_id'"
            scope_vars["account_id"] = account_id
        if organization_id:
            scope_sql += "\n    AND f.organization_id = :'organization_id'"
            scope_vars["organization_id"] = organization_id
        if window_end:
            sql = """
SELECT COALESCE(json_agg(json_build_object('t', t, 'value', value) ORDER BY t), '[]'::json)
FROM (
  SELECT
    EXTRACT(EPOCH FROM f.fetched_at)::bigint AS t,
    __VALUE_SQL__
  FROM usage_metric_snapshot m
  JOIN usage_provider_fetch f ON f.id = m.provider_fetch_id
  WHERE f.provider = :'provider'
    AND f.success = true
    AND m.metric_path = :'metric_path'
__SCOPE_SQL__
    AND (
      NULLIF(:'window_start', '')::timestamptz IS NULL
      OR f.fetched_at >= (NULLIF(:'window_start', '')::timestamptz - :'window_slop_seconds'::int * INTERVAL '1 second')
    )
    AND f.fetched_at <= (NULLIF(:'window_end', '')::timestamptz + :'window_slop_seconds'::int * INTERVAL '1 second')
  ORDER BY f.fetched_at ASC
) x;
"""
            vars = {
                "provider": provider,
                "metric_path": metric_path,
                "window_start": _coalesce(window_start, ""),
                "window_end": window_end,
                "window_slop_seconds": str(window_slop_seconds),
            }
            vars.update(scope_vars)
        elif window_start:
            sql = """
SELECT COALESCE(json_agg(json_build_object('t', t, 'value', value) ORDER BY t), '[]'::json)
FROM (
  SELECT
    EXTRACT(EPOCH FROM f.fetched_at)::bigint AS t,
    __VALUE_SQL__
  FROM usage_metric_snapshot m
  JOIN usage_provider_fetch f ON f.id = m.provider_fetch_id
  WHERE f.provider = :'provider'
    AND f.success = true
    AND m.metric_path = :'metric_path'
__SCOPE_SQL__
    AND f.fetched_at >= (NULLIF(:'window_start', '')::timestamptz - :'window_slop_seconds'::int * INTERVAL '1 second')
  ORDER BY f.fetched_at ASC
) x;
"""
            vars = {
                "provider": provider,
                "metric_path": metric_path,
                "window_start": window_start,
                "window_slop_seconds": str(window_slop_seconds),
            }
            vars.update(scope_vars)
        else:
            sql = """
SELECT COALESCE(json_agg(json_build_object('t', t, 'value', value) ORDER BY t), '[]'::json)
FROM (
  SELECT
    EXTRACT(EPOCH FROM f.fetched_at)::bigint AS t,
    __VALUE_SQL__
  FROM usage_metric_snapshot m
  JOIN usage_provider_fetch f ON f.id = m.provider_fetch_id
  WHERE f.provider = :'provider'
    AND f.success = true
    AND m.metric_path = :'metric_path'
__SCOPE_SQL__
    AND f.fetched_at >= NOW() - (:days::int * INTERVAL '1 day')
  ORDER BY f.fetched_at ASC
) x;
"""
            vars = {
                "provider": provider,
                "metric_path": metric_path,
                "days": str(days),
            }
            vars.update(scope_vars)
        sql = sql.replace("__SCOPE_SQL__", scope_sql).replace("__VALUE_SQL__", value_sql)
        return _parse_psql_json(self._run(sql, vars=vars))

    def latest_raw(self, provider: str | None = None, source_id: str | None = None) -> dict[str, Any] | None:
        if not provider and not source_id:
            return None
        vars: dict[str, str] = {}
        filter_sql = ""
        if source_id:
            filter_sql += "\n    AND f.source_id = :'source_id'"
            vars["source_id"] = source_id
        if provider:
            filter_sql += "\n    AND f.provider = :'provider'"
            vars["provider"] = provider
        sql = """
SELECT COALESCE(row_to_json(x), '{}'::json)
FROM (
  SELECT
    f.id,
    f.source_id,
    f.fetched_at,
    f.provider,
    f.http_status,
    f.account_id,
    f.organization_id,
    f.requested_url,
    f.request_metadata,
    f.raw_payload
  FROM usage_provider_fetch f
  WHERE f.success = true
__FILTER_SQL__
  ORDER BY f.fetched_at DESC
  LIMIT 1
        ) x;
"""
        result = _parse_psql_json(self._run(sql.replace("__FILTER_SQL__", filter_sql), vars=vars))
        if isinstance(result, list):
            return result[0] if result else None
        if isinstance(result, dict):
            return result
        return None

    def build_current_contract(
        self,
        history_days: int = 30,
        sources: tuple[SourceConfig, ...] | list[SourceConfig] | None = None,
    ) -> dict[str, Any]:
        visible_sources = [source for source in (sources or []) if source.enabled and source.frontend_visible]
        visible_source_ids = [source.source_id for source in visible_sources]
        source_config_by_id = {source.source_id: source for source in visible_sources}
        if sources is not None and len(sources) > 0 and not visible_source_ids:
            rows = []
            attempts = []
        else:
            source_filter = visible_source_ids if visible_source_ids else None
            rows = self.latest_fetch(source_ids=source_filter)
            attempts = self.latest_attempts(source_ids=source_filter)
        latest = {_coalesce(row.get("source_id"), row.get("provider")): row for row in rows}
        latest_attempts = {_coalesce(row.get("source_id"), row.get("provider")): row for row in attempts}
        agents = []
        latest_updated_at: datetime | None = None
        row_order = visible_source_ids or [
            _coalesce(row.get("source_id"), row.get("provider"))
            for row in sorted(
                rows,
                key=lambda row: (
                    SUPPORTED_PROVIDERS.index(str(row.get("provider"))) if str(row.get("provider")) in SUPPORTED_PROVIDERS else 99,
                    str(row.get("source_id") or row.get("provider")),
                ),
            )
        ]
        for source_id in row_order:
            row = latest.get(source_id)
            if not row:
                continue
            provider = _coalesce(row.get("provider"))
            if provider not in SUPPORTED_PROVIDERS:
                continue
            source_config = source_config_by_id.get(source_id)

            metrics = self.latest_metrics(int(row["id"]))
            metadata = _safe_json(row.get("request_metadata"))
            if not (source_config.frontend_visible if source_config else bool(metadata.get("frontend_visible", True))):
                continue
            metric_map = {metric["metric_key"]: metric for metric in metrics}
            summary_key = metadata.get("summary_key") or (
                "monthly" if provider == "cursor" else "seven_day" if provider == "claude" else "secondary_window"
            )
            summary = metric_map.get(summary_key)
            if summary is None:
                if provider == "cursor":
                    fallback_order = ["monthly"]
                elif provider == "claude":
                    fallback_order = ["seven_day", "five_hour", "secondary_window", "primary_window"]
                else:
                    fallback_order = ["secondary_window", "primary_window", "seven_day"]
                for key in fallback_order:
                    if key in metric_map:
                        summary = metric_map[key]
                        break
            if summary is None:
                continue

            history_key = metadata.get("history_key", summary_key)
            graph_points: dict[str, list[dict[str, Any]]] = {}
            for metric in metrics:
                metric_path = metric.get("metric_path")
                if not metric_path:
                    continue
                try:
                    details = _safe_json(metric.get("details"))
                    if provider == "cursor" and metric.get("metric_key") == "auto_spend":
                        graph_points[str(metric_path)] = self.cursor_auto_spend_points(metric, row)
                    else:
                        graph_points[str(metric_path)] = self.history_points(
                            provider,
                            str(metric_path),
                            history_days,
                            window_start=_coalesce(metric.get("window_start")),
                            window_end=_coalesce(metric.get("window_end") or metric.get("reset_at")),
                            source_id=source_id,
                            account_id=_coalesce(row.get("account_id")),
                            organization_id=_coalesce(row.get("organization_id")),
                            use_value_num=details.get("graph_value_kind") == "currency_cents",
                        )
                except Exception:
                    graph_points[str(metric_path)] = []

            if provider == "cursor":
                monthly_metric = metric_map.get("monthly")
                total_spend_metric = metric_map.get("total_spend")
                over_cap_metric = metric_map.get("over_cap_used")
                monthly_path = _coalesce(monthly_metric.get("metric_path")) if monthly_metric else ""
                total_spend_path = _coalesce(total_spend_metric.get("metric_path")) if total_spend_metric else ""
                over_cap_path = _coalesce(over_cap_metric.get("metric_path")) if over_cap_metric else ""
                if monthly_metric and monthly_path:
                    non_auto_event_points = self.cursor_usage_cumulative_points(
                        cycle_end=_coalesce(monthly_metric.get("window_end") or monthly_metric.get("reset_at")),
                        source_id=source_id,
                        account_id=_coalesce(row.get("account_id")),
                        organization_id=_coalesce(row.get("organization_id")),
                        exclude_model="default",
                    )
                    total_spend_points = (
                        self.history_points(
                            provider,
                            total_spend_path,
                            history_days,
                            window_start=_coalesce(total_spend_metric.get("window_start")),
                            window_end=_coalesce(total_spend_metric.get("window_end") or total_spend_metric.get("reset_at")),
                            source_id=source_id,
                            account_id=_coalesce(row.get("account_id")),
                            organization_id=_coalesce(row.get("organization_id")),
                            use_value_num=True,
                        )
                        if total_spend_metric and total_spend_path
                        else []
                    )
                    adjusted_metric, adjusted_points = _cursor_adjust_monthly_metric_and_points(
                        monthly_metric,
                        over_cap_metric,
                        graph_points.get(monthly_path, []),
                        graph_points.get(over_cap_path, []),
                    )
                    if non_auto_event_points:
                        adjusted_points = _cursor_monthly_points_from_non_auto_events(adjusted_metric, non_auto_event_points)
                    elif total_spend_points:
                        adjusted_points = _cursor_monthly_points_from_total_spend(adjusted_metric, total_spend_points)
                    metric_map["monthly"] = adjusted_metric
                    graph_points[monthly_path] = adjusted_points

            snapshot = ProviderSnapshot(
                provider=provider,
                account_id=_coalesce(row.get("account_id"), ""),
                organization_id=_coalesce(row.get("organization_id"), ""),
                metrics=[
                    {
                        "metric_key": metric.get("metric_key"),
                        "provider_metric_key": metric.get("provider_metric_key", metric.get("metric_key")),
                        "metric_path": metric.get("metric_path", ""),
                        "metric_id": metric.get("metric_path", ""),
                        "metric_scope": metric.get("metric_scope", ""),
                        "metric_label": metric.get("metric_label"),
                        "percent": int(metric.get("percent", 0)),
                        "value_num": metric.get("value_num"),
                        "value": metric.get("value") if metric.get("metric_key") == "monthly" else metric.get("value_text"),
                        "note": metric.get("note", ""),
                        "max_value": int(metric.get("max_value", 100)),
                        "window_start": metric.get("window_start", ""),
                        "window_end": metric.get("window_end", ""),
                        "reset_at": metric.get("reset_at", ""),
                        "details": metric.get("details", {}),
                    }
                    for metric in [metric_map.get(metric.get("metric_key"), metric) for metric in metrics]
                ],
                summary_key=summary_key,
                history_key=history_key,
                history_label=metadata.get("history_label", _build_history_label(provider)),
                details=metadata.get("details") if isinstance(metadata.get("details"), list) else [],
                raw_payload={},
                request_url="",
                request_status=0,
                request_error=None,
                request_metadata=metadata,
                success=True,
                source_id=source_id,
                source_label=_coalesce(source_config.label if source_config else None, metadata.get("source_label"), source_id),
                frontend_visible=source_config.frontend_visible if source_config else bool(metadata.get("frontend_visible", True)),
            )
            provider_status = _provider_status(provider, row, latest_attempts.get(source_id))
            agent = build_state_agent(
                snapshot,
                graph_points,
                provider_status=provider_status,
                updated_at=_timestamp_to_iso_utc(row.get("fetched_at")),
            )
            agents.append(agent)
            fetched_at = _parse_timestamp(row.get("fetched_at"))
            if fetched_at and (latest_updated_at is None or fetched_at > latest_updated_at):
                latest_updated_at = fetched_at

        backend = {
            "kind": "postgres",
            "label": "agent-usage-service",
            "transport": "postgres+http",
            "scope": "local",
        }
        return {
            "updated_at": latest_updated_at.astimezone(timezone.utc).isoformat() if latest_updated_at else "",
            "backend": backend,
            "agents": agents,
        }

    def build_compat_state(
        self,
        history_days: int = 30,
        sources: tuple[SourceConfig, ...] | list[SourceConfig] | None = None,
    ) -> dict[str, Any]:
        return self.build_current_contract(history_days=history_days, sources=sources)

    def build_history_windows(self, provider: str, days: int, source_id: str | None = None) -> dict[str, Any]:
        row = self.latest_provider_fetch(provider, source_id=source_id)
        source_id = _coalesce(source_id, row.get("source_id") if row else None)
        if not row:
            return {"source_id": source_id, "provider": provider, "long_window": None, "short_window": None, "days": days}

        metrics = self.latest_metrics(int(row["id"]))
        long_metric = _pick_metric_by_candidates(metrics, provider, _graph_metric_candidates(provider, "long_window"))
        short_metric = _pick_metric_by_candidates(metrics, provider, _graph_metric_candidates(provider, "short_window"))

        def build_graph(metric: dict[str, Any] | None) -> dict[str, Any] | None:
            if not metric:
                return None
            metric_path = _coalesce(metric.get("metric_path"))
            details = _safe_json(metric.get("details"))
            if provider == "cursor" and metric.get("metric_key") == "auto_spend":
                points = self.cursor_auto_spend_points(metric, row)
            elif provider == "cursor" and metric.get("metric_key") == "monthly":
                non_auto_event_points = self.cursor_usage_cumulative_points(
                    cycle_end=_coalesce(metric.get("window_end") or metric.get("reset_at")),
                    source_id=source_id,
                    account_id=_coalesce(row.get("account_id")),
                    organization_id=_coalesce(row.get("organization_id")),
                    exclude_model="default",
                )
                total_spend_metric = next((candidate for candidate in metrics if candidate.get("metric_key") == "total_spend"), None)
                over_cap_metric = next((candidate for candidate in metrics if candidate.get("metric_key") == "over_cap_used"), None)
                total_spend_path = _coalesce(total_spend_metric.get("metric_path")) if total_spend_metric else ""
                over_cap_path = _coalesce(over_cap_metric.get("metric_path")) if over_cap_metric else ""
                total_spend_points = (
                    self.history_points(
                        provider,
                        total_spend_path,
                        days,
                        window_start=_coalesce(total_spend_metric.get("window_start")),
                        window_end=_coalesce(total_spend_metric.get("window_end") or total_spend_metric.get("reset_at")),
                        source_id=source_id,
                        account_id=_coalesce(row.get("account_id")),
                        organization_id=_coalesce(row.get("organization_id")),
                        use_value_num=True,
                    )
                    if total_spend_path
                    else []
                )
                over_cap_points = (
                    self.history_points(
                        provider,
                        over_cap_path,
                        days,
                        window_start=_coalesce(over_cap_metric.get("window_start")),
                        window_end=_coalesce(over_cap_metric.get("window_end") or over_cap_metric.get("reset_at")),
                        source_id=source_id,
                        account_id=_coalesce(row.get("account_id")),
                        organization_id=_coalesce(row.get("organization_id")),
                        use_value_num=True,
                    )
                    if over_cap_path
                    else []
                )
                monthly_points = (
                    self.history_points(
                        provider,
                        metric_path,
                        days,
                        window_start=_coalesce(metric.get("window_start")),
                        window_end=_coalesce(metric.get("window_end") or metric.get("reset_at")),
                        source_id=source_id,
                        account_id=_coalesce(row.get("account_id")),
                        organization_id=_coalesce(row.get("organization_id")),
                        use_value_num=False,
                    )
                    if metric_path
                    else []
                )
                metric, points = _cursor_adjust_monthly_metric_and_points(metric, over_cap_metric, monthly_points, over_cap_points)
                if non_auto_event_points:
                    points = _cursor_monthly_points_from_non_auto_events(metric, non_auto_event_points)
                elif total_spend_points:
                    points = _cursor_monthly_points_from_total_spend(metric, total_spend_points)
            else:
                points = (
                    self.history_points(
                        provider,
                        metric_path,
                        days,
                        window_start=_coalesce(metric.get("window_start")),
                        window_end=_coalesce(metric.get("window_end") or metric.get("reset_at")),
                        source_id=source_id,
                        account_id=_coalesce(row.get("account_id")),
                        organization_id=_coalesce(row.get("organization_id")),
                        use_value_num=details.get("graph_value_kind") == "currency_cents",
                    )
                    if metric_path
                    else []
                )
            return _graph_from_metric(metric, points)

        return {
            "source_id": source_id,
            "provider": provider,
            "days": days,
            "long_window": build_graph(long_metric),
            "short_window": build_graph(short_metric),
        }

    def build_history(self, provider: str, metric: str, days: int, source_id: str | None = None) -> dict[str, Any]:
        row = self.latest_provider_fetch(provider, source_id=source_id) or {}
        source_id = _coalesce(source_id, row.get("source_id"))
        metric_row = self.latest_metric(
            provider,
            metric,
            source_id=source_id,
            account_id=_coalesce(row.get("account_id")),
            organization_id=_coalesce(row.get("organization_id")),
        ) or {}
        metric_path = metric_row.get("metric_path", metric)
        details = _safe_json(metric_row.get("details"))
        if provider == "cursor" and metric_row.get("metric_key") == "auto_spend":
            points = self.cursor_auto_spend_points(metric_row, row)
        elif provider == "cursor" and metric_row.get("metric_key") == "monthly":
            non_auto_event_points = self.cursor_usage_cumulative_points(
                cycle_end=_coalesce(metric_row.get("window_end") or metric_row.get("reset_at")),
                source_id=source_id,
                account_id=_coalesce(row.get("account_id")),
                organization_id=_coalesce(row.get("organization_id")),
                exclude_model="default",
            )
            total_spend_metric = self.latest_metric(
                provider,
                "total_spend",
                source_id=source_id,
                account_id=_coalesce(row.get("account_id")),
                organization_id=_coalesce(row.get("organization_id")),
            ) or {}
            over_cap_metric = self.latest_metric(
                provider,
                "over_cap_used",
                source_id=source_id,
                account_id=_coalesce(row.get("account_id")),
                organization_id=_coalesce(row.get("organization_id")),
            ) or {}
            total_spend_path = _coalesce(total_spend_metric.get("metric_path"))
            over_cap_path = _coalesce(over_cap_metric.get("metric_path"))
            total_spend_points = (
                self.history_points(
                    provider,
                    total_spend_path,
                    days,
                    window_start=_coalesce(total_spend_metric.get("window_start")),
                    window_end=_coalesce(total_spend_metric.get("window_end") or total_spend_metric.get("reset_at")),
                    source_id=source_id,
                    account_id=_coalesce(row.get("account_id")),
                    organization_id=_coalesce(row.get("organization_id")),
                    use_value_num=True,
                )
                if total_spend_path
                else []
            )
            over_cap_points = (
                self.history_points(
                    provider,
                    over_cap_path,
                    days,
                    window_start=_coalesce(over_cap_metric.get("window_start")),
                    window_end=_coalesce(over_cap_metric.get("window_end") or over_cap_metric.get("reset_at")),
                    source_id=source_id,
                    account_id=_coalesce(row.get("account_id")),
                    organization_id=_coalesce(row.get("organization_id")),
                    use_value_num=True,
                )
                if over_cap_path
                else []
            )
            monthly_points = (
                self.history_points(
                    provider,
                    metric_path,
                    days,
                    window_start=_coalesce(metric_row.get("window_start")),
                    window_end=_coalesce(metric_row.get("window_end") or metric_row.get("reset_at")),
                    source_id=source_id,
                    account_id=_coalesce(row.get("account_id")),
                    organization_id=_coalesce(row.get("organization_id")),
                    use_value_num=False,
                )
                if metric_path
                else []
            )
            metric_row, points = _cursor_adjust_monthly_metric_and_points(metric_row, over_cap_metric, monthly_points, over_cap_points)
            if non_auto_event_points:
                points = _cursor_monthly_points_from_non_auto_events(metric_row, non_auto_event_points)
            elif total_spend_points:
                points = _cursor_monthly_points_from_total_spend(metric_row, total_spend_points)
        else:
            points = (
                self.history_points(
                    provider,
                    metric_path,
                    days,
                    window_start=_coalesce(metric_row.get("window_start")),
                    window_end=_coalesce(metric_row.get("window_end") or metric_row.get("reset_at")),
                    source_id=source_id,
                    account_id=_coalesce(row.get("account_id")),
                    organization_id=_coalesce(row.get("organization_id")),
                    use_value_num=details.get("graph_value_kind") == "currency_cents",
                )
                if metric_path
                else []
            )
        graph = _graph_from_metric(metric_row, points)
        return {
            "source_id": source_id,
            "provider": provider,
            "metric": metric_row.get("metric_key", metric),
            "metric_path": metric_path,
            "metric_key": metric_row.get("metric_key", metric),
            "provider_metric_key": metric_row.get("provider_metric_key", metric_row.get("metric_key", metric)),
            "label": graph.get("label") or metric_row.get("metric_label", _metric_label(provider, metric, {})),
            "max_value": int(graph.get("max_value", metric_row.get("max_value", 100))),
            "window_start": graph.get("window_start"),
            "window_end": graph.get("window_end"),
            "reset_at": graph.get("reset_at"),
            "value_kind": graph.get("value_kind", "percent"),
            "pace_line": bool(graph.get("pace_line", True)),
            "days": days,
            "points": graph.get("points", []),
        }
