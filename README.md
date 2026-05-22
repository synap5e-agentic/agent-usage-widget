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
| `scripts/` | QML setup, lint, test, and visual-regression helpers |
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

### 4. Get provider credentials

See [Getting credentials](#getting-credentials) for per-provider DevTools steps. You need one cookie or token per source you intend to enable.

### 5. Write your `config.toml`

```bash
mkdir -p ~/.config/agent-usage-widget
cp poller/config.toml.example ~/.config/agent-usage-widget/config.toml
chmod 600 ~/.config/agent-usage-widget/config.toml
```

Edit it. A minimal one-source example (replace `<PG_PASSWORD>` with the value from step 2):

```toml
[service]
host = "127.0.0.1"
port = 8785

[poller]
default_interval_seconds = 60

[storage]
db_dsn = "postgresql://agent_usage:<PG_PASSWORD>@127.0.0.1:5432/agent_usage"

[frontend]
columns = 3

[sources.personal]
provider = "claude"
label = "Claude Personal"
enabled = true
frontend_visible = true

[sources.personal.frontend]
order = 10
short_label = "CP"
show_metrics = ["seven_day", "five_hour", "extra_usage"]
show_graphs = ["seven_day", "five_hour"]
highlight_metric = "extra_usage"
highlight_window_minutes = 34

[sources.personal.auth]
cookie = "sessionKey=...; lastActiveOrg=...; anthropic-device-id=...; ajs_anonymous_id=..."
```

A few notes on the schema:

- Source table names like `personal` become stable `source_id` values in the service contract; keep them ASCII-friendly.
- `label` defaults to the source id; `enabled` and `frontend_visible` default to `true`; `interval_seconds` defaults to `poller.default_interval_seconds`.
- The bundled `config.toml.example` ships every source with `enabled = false` so a fresh install doesn't poll empty credentials. Flip the ones you want on.
- You can configure multiple sources for the same provider (e.g. personal + work Claude) by giving each its own table name and cookie.
- `[frontend].columns` controls panel columns. `[sources.<name>.frontend]` controls per-source order, bar short label, visible metrics, graph selection, and the highlight metric. Claude `extra_usage` is a currency metric; the default policy shows it and highlights the bar when it increased in the last 34 minutes.

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

All three providers authenticate by replaying cookies and tokens copied from a logged-in browser session.

The fast path is `scripts/grab_provider_credentials.py`: it self-launches `mitmdump`, opens a fresh red-themed Chrome pointed at the proxy, and prints ready-to-paste TOML for each provider as soon as it captures the relevant traffic.

```bash
# all three providers
scripts/grab_provider_credentials.py

# only the ones you want
scripts/grab_provider_credentials.py --target claude --target cursor
```

Log in inside the launched Chrome profile and visit the usage page that opens; the script self-terminates once it has each requested target's cookie (and Authorization header for Codex). The Chrome profile is persisted under `$XDG_STATE_HOME/agent-usage-widget/grab-chrome-profile/` so subsequent runs reuse any still-valid logins; pass `--reset-profile` to wipe it. Chrome is sent SIGTERM with a 15s grace period on success so cookies actually flush to disk before exit. If Chrome is not installed, the script prints proxy details for routing any browser through `mitmdump` manually (visit `http://mitm.it/` once connected to install the CA).

To write captures straight into the existing config without copy-paste, pass `--write <source_id>` (repeatable). The named source must already exist in `config.toml` with a `provider` field; the script replaces its `[sources.<source_id>.auth]` table and leaves the rest untouched.

```bash
scripts/grab_provider_credentials.py --write claude_personal --write codex_main
```

A timestamped backup is dropped next to `config.toml` (`config.toml.bak.<unix-ts>`). Two `--write` source IDs that map to the same provider are rejected — capture one, close Chrome with `--reset-profile`, then capture the next.

If you'd rather copy by hand, the manual recipe per provider is below. Open Chromium / Firefox DevTools (`F12`), log in to the provider, and capture from the **Network** tab.

### Claude

1. Log in to https://claude.ai/.
2. DevTools → Network → reload the page.
3. Click any request to `claude.ai` (e.g. the usage endpoint at `/api/organizations/.../usage` if you visit Settings → Usage).
4. Under **Request Headers**, copy the entire `Cookie:` value.
5. Paste it into `[sources.<name>.auth].cookie`.

The poller extracts `sessionKey`, `lastActiveOrg`, `anthropic-device-id`, and `ajs_anonymous_id` from the cookie automatically. You can also set them individually under `[sources.<name>.auth]` as `session_key`, `organization_id`, `device_id`, `anonymous_id` if your cookie is missing one of them.

### Codex (ChatGPT)

1. Log in to https://chatgpt.com/.
2. Visit https://chatgpt.com/codex/cloud/settings/analytics so a usage request fires.
3. DevTools → Network → find the request to `/backend-api/wham/usage`.
4. Under **Request Headers**:
   - Copy the full `Authorization:` value (starts with `Bearer eyJ…`) into `[sources.<name>.auth].authorization`.
   - Copy the full `Cookie:` value into `[sources.<name>.auth].cookie`.

The poller derives `oai-did` and `oai-session-id` from the cookie. If your cookie is missing them you can supply `device_id` / `session_id` (and optionally `oai_session_id` for the header) directly under `[sources.<name>.auth]`.

### Cursor

1. Log in to https://cursor.com/.
2. Visit https://cursor.com/dashboard/billing so the usage endpoint fires.
3. DevTools → Network → find any request to `cursor.com/api/...`.
4. Under **Request Headers**, copy the entire `Cookie:` value into `[sources.<name>.auth].cookie`.

### Refreshing credentials

Provider auth here is based on browser session tokens. They expire (Codex bearer tokens within hours, Claude/Cursor session cookies after days or weeks) and have to be refreshed manually by repeating the steps above. The service surfaces stale / expired sign-in state through the `status` field on each agent, but it cannot renew credentials for you. This is the main UX limitation of the current design.

Legacy `.env` configuration is still read as a fallback when no TOML sources are configured. If both files exist, `config.toml` controls service settings and sources; explicit CLI overrides still win.

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
