# agent-usage-widget

Backend, reusable QML UI, and Noctalia frontend for tracking Claude, Codex, and Cursor usage.

This repo includes a poller, a local HTTP service, a reusable QML UI layer, and a bundled Noctalia adapter. The service and QML components can be used by other frontends as well.

![Agent Usage panel](https://github.com/synap5e-agentic/agent-usage-widget/blob/readme-screenshots/docs/screenshots/panel.png?raw=1)

![Agent Usage bar widget](https://github.com/synap5e-agentic/agent-usage-widget/blob/readme-screenshots/docs/screenshots/bar.png?raw=1)

## Architecture

1. `agent-usage-poll` fetches provider usage and writes normalized snapshots to PostgreSQL.
2. `agent-usage-service` serves `/api/current`, `/api/history`, and `/api/raw/latest`.
3. `qml/` contains reusable QML components for the bar, panel, and graphs.
4. `noctalia_plugin/` contains the thin Noctalia / Quickshell adapter layer.
5. The Noctalia plugin reads only the local HTTP service; if the service is down, the UI shows that directly instead of falling back to a cached snapshot.

## Repo Layout

| Path | Purpose |
|---|---|
| `poller/` | poller, shared backend code, schema, config templates |
| `qml/` | reusable QML components |
| `noctalia_plugin/` | Noctalia / Quickshell adapter layer |
| `systemd/` | user service and timer units |
| `scripts/` | QML setup, lint, test, visual-regression helpers, and the `grab_provider_credentials.py` mitmproxy helper for capturing auth |
| `tests/python/` | backend tests |
| `qml/tests/` | QML and visual regression tests |

## Requirements

- Linux with systemd user services
- `python3`, `psql` (the PostgreSQL client)
- A PostgreSQL server reachable from this machine (recipe below)
- Noctalia / Quickshell for the UI
- A logged-in browser session for each provider you want to track

## Quick Start

The steps below get the backend running before `install.sh` enables the systemd units, so the very first poll cycle has a schema, credentials, and a database to talk to.

### 1. Clone the repo

```bash
git clone https://github.com/synap5e-agentic/agent-usage-widget.git
cd agent-usage-widget
```

All subsequent commands assume you are at the repo root.

### 2. Install PostgreSQL and create the role + database

Install and start PostgreSQL with your distro's package manager (`pacman -S postgresql`, `apt install postgresql`, `dnf install postgresql-server`, etc.) and initialize the cluster per your distro's instructions.

Generate a random password and create the role + database. Save the password — you'll paste it into `config.toml` in step 5.

```bash
PG_PASSWORD=$(openssl rand -hex 24)
sudo -u postgres psql <<SQL
CREATE ROLE agent_usage WITH LOGIN PASSWORD '$PG_PASSWORD';
CREATE DATABASE agent_usage OWNER agent_usage;
SQL
echo "Save this DSN for config.toml step 5:"
echo "postgresql://agent_usage:$PG_PASSWORD@127.0.0.1:5432/agent_usage"
```

If your PostgreSQL listens on a non-default port or host, adjust the DSN accordingly. The default expects `127.0.0.1:5432`.

### 3. Bootstrap the schema

Using the DSN you just generated:

```bash
psql "postgresql://agent_usage:$PG_PASSWORD@127.0.0.1:5432/agent_usage" < poller/schema.sql
```

This is idempotent. Re-run it any time after a `git pull` to apply schema migrations.

### 4. Write your `config.toml`

```bash
mkdir -p ~/.config/agent-usage-widget
cp poller/config.toml.example ~/.config/agent-usage-widget/config.toml
chmod 600 ~/.config/agent-usage-widget/config.toml
```

Edit it. A minimal example (paste the DSN from step 2 into `[storage].db_dsn` and leave `[sources.<id>.auth]` empty — step 5 fills it in):

```toml
[service]
host = "127.0.0.1"
port = 8785

[poller]
default_interval_seconds = 60

[storage]
db_dsn = "postgresql://agent_usage:<PG_PASSWORD>@127.0.0.1:5432/agent_usage"

[sources.personal]
provider = "claude"
label = "Claude Personal"
enabled = true

[sources.personal.auth]
# Filled in by `grab_provider_credentials.py --write personal` (step 5).
```

A few notes:

- Source table names (`personal` above) become stable `source_id` values; keep them ASCII-friendly. `label` defaults to the source id; `enabled` and `frontend_visible` default to `true`; `interval_seconds` defaults to `poller.default_interval_seconds`.
- You can configure multiple sources for the same provider (e.g. personal + work Claude) by giving each its own table name and cookie.
- The bundled `config.toml.example` ships every source with `enabled = false` and shows the full per-source frontend policy (`order`, `short_label`, `show_metrics`, `show_graphs`, `highlight_metric`, etc.) plus `[frontend].columns` for panel layout. Flip what you want on and copy over the frontend blocks you care about.

### 5. Capture provider credentials

See [Getting credentials](#getting-credentials). The fast path is `scripts/grab_provider_credentials.py --write <source_id>`, which writes captured cookies/tokens directly into the `[sources.<source_id>.auth]` table you stubbed out in step 4. A manual DevTools recipe is documented there too as a fallback — use it if you don't want to run a helper that proxies your browser, or if your environment doesn't have Chrome.

### 6. Install symlinks, the Noctalia plugin, and the systemd units

```bash
./install.sh --restart
```

This step links `agent-usage-poll` and `agent-usage-service` into `~/bin/`, links the systemd unit files into `~/.config/systemd/user/`, links the Noctalia plugin into `~/.config/noctalia/plugins/agent-usage/`, registers the plugin in `~/.config/noctalia/plugins.json`, and enables + starts the systemd timer and service. `--restart` also restarts Noctalia so the new plugin loads immediately.

### 7. Verify the backend

```bash
systemctl --user status agent-usage-poll.service agent-usage-service.service
curl -s http://127.0.0.1:8785/api/current | jq '.agents[] | {source_id, label, status: .status.state, summary: .summary.value}'
```

Each enabled source should show up with `status: "ok"` once the poller has fired at least once. If a source is `error`, the `status.message` field tells you what went wrong (most commonly an expired cookie — see [Refreshing credentials](#refreshing-credentials)).

### 8. Add the widget to your Noctalia bar

Open the Noctalia settings UI, find the bar section you want the widget in (left / center / right), and add the `agent-usage` widget. Click the widget to open the panel.

## Getting credentials

All three providers authenticate by replaying cookies (and, for Codex, an `Authorization: Bearer` token plus an `oai-session-id` request header) copied from a logged-in browser session.

### Fast path: `grab_provider_credentials.py`

`scripts/grab_provider_credentials.py` is a self-contained mitmproxy helper. It picks a free port, launches `mitmdump` with itself as the addon, opens a red-themed Chrome session pointed at the proxy, and captures the relevant traffic as you log in.

```bash
# Capture all three providers and print TOML to stdout
scripts/grab_provider_credentials.py

# Write straight into specific [sources.<id>.auth] tables (recommended)
scripts/grab_provider_credentials.py --write personal --write work --write codex --write cursor

# Capture without writing (review / copy-paste yourself)
scripts/grab_provider_credentials.py --target claude --target cursor
```

Behaviour:

- **`--write <source_id>`** is repeatable. Each named source must already exist in `config.toml` with a `provider` field; the script rewrites just that source's `[sources.<source_id>.auth]` block and leaves everything else (label, frontend policy, sibling sources) untouched. A timestamped backup is dropped next to the file (`config.toml.bak.<unix-ts>`). Two `--write` IDs mapping to the same provider are rejected (one Chrome session can hold only one account per provider — use `--reset-profile` and run again for the second account).
- **Chrome profile is persistent** under `$XDG_STATE_HOME/agent-usage-widget/grab-chrome-profile/`, so re-running picks up still-valid logins. Pass `--reset-profile` to wipe it and start fresh (useful when switching accounts, e.g. free-tier ↔ paid Codex).
- **Login redirects**: if a provider redirects you away from the usage page after login, the launcher prints a list of the original URLs — paste the one you need back into the address bar. ChatGPT in particular tends to drop the original URL on its OAuth round-trip.
- **No Chrome?** The script prints proxy details so you can route any browser through `mitmdump` manually; visit http://mitm.it/ once connected to install the CA cert.
- On capture completion the script SIGTERMs Chrome with a 15s grace period so cookies actually flush to disk before exit.

After a successful run, force an immediate poll and verify:

```bash
agent-usage-poll --force --source <source_id>
curl -s http://127.0.0.1:8785/api/current | jq '.agents[] | {source_id, status: .status.state, summary: .summary.value}'
```

### Refreshing credentials

Provider session tokens expire (Codex bearer tokens within hours, Claude/Cursor session cookies after days or weeks). The poller surfaces stale / expired state via `status.state == "error"` on each agent. To rotate, re-run the grab script with `--write`:

```bash
scripts/grab_provider_credentials.py --write <source_id>
```

The persistent Chrome profile usually means you only need to re-log-in once per provider per browser-session-expiry; the next rotation just re-captures from the already-logged-in session.

### Manual fallback (DevTools)

If you'd rather skip the proxy entirely, copy the required pieces by hand from your browser's DevTools (`F12`) → Network tab.

**Claude** — Log in to https://claude.ai/, open Settings → Usage so `/api/organizations/<org>/usage` fires, click that request, and copy the full `Cookie:` value into `[sources.<id>.auth].cookie`. The poller extracts `sessionKey`, `lastActiveOrg`, `anthropic-device-id`, and `ajs_anonymous_id` from the cookie. If your cookie is missing `lastActiveOrg`, set `organization_id = "..."` alongside the cookie (the org id appears in the request URL).

**Codex (ChatGPT)** — Log in to https://chatgpt.com/, navigate to https://chatgpt.com/codex/cloud/settings/usage so `/backend-api/wham/usage` fires, and from that request copy:

- `Authorization:` value (starts with `Bearer eyJ…`) → `[sources.<id>.auth].authorization`
- `Cookie:` value → `[sources.<id>.auth].cookie`
- `oai-session-id:` **request header** (not a cookie!) → `[sources.<id>.auth].session_id`

The poller derives `oai-did` from the cookie. `oai-session-id` lives only in the request header, so it must be set explicitly.

**Cursor** — Log in to https://cursor.com/, visit https://cursor.com/dashboard/billing so `/api/usage-summary` fires, and copy the full `Cookie:` value into `[sources.<id>.auth].cookie`.

### Legacy `.env`

The legacy `.env` configuration is still read as a fallback when no TOML sources are configured. If both files exist, `config.toml` controls service settings and sources; explicit CLI overrides still win.

## API

Default base URL: `http://127.0.0.1:8785`

- `/health` - service health
- `/api/current` - current widget contract, including frontend policy and highlight state
- `/api/history?source=<source_id>&metric=<metric>` - graph history for a configured source
- `/api/history?provider=<provider>&metric=<metric>` - legacy provider-scoped history
- `/api/raw/latest?source=<source_id>` - latest stored raw source payload
- `/api/raw/latest?provider=<provider>` - legacy latest stored raw provider payload

## Development

Set up the local Noctalia import shim once:

```bash
python3 scripts/setup_noctalia_qml_imports.py --checkout /etc/xdg/quickshell/noctalia-shell
```

Run tests:

```bash
uv run --with pytest python3 -m pytest tests/python
python3 scripts/run_qml_tests.py
python3 scripts/usage_graph_visual_regression.py
```

Run the full multi-source stack smoke against a temporary local database:

```bash
python3 scripts/run_multi_source_stack_smoke.py
```

This writes a temporary multi-source `config.toml`, seeds a throwaway Postgres database, starts the real local service, validates `/api/current`, `/api/history`, and `/api/raw/latest`, and renders panel/bar PNGs into `/tmp/agent-usage-multi-source-smoke/`.

Lint QML:

```bash
python3 scripts/lint_qml.py
```

Render the current UI from the live service payload:

```bash
python3 scripts/render_widget_screenshots.py
```

The renderer writes `/tmp/agent-usage-panel.png` and `/tmp/agent-usage-bar.png` at the README image sizes by default. It renders through the Noctalia plugin wrappers, reads the live `/api/current` payload, and mirrors local Noctalia `settings.json` / `colors.json` for theme and bar sizing.
