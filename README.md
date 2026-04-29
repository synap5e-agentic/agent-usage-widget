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
5. `~/.cache/agent-usage/state.json` exists only as a compatibility fallback for the QML side.

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

- `python3`
- `psql`
- PostgreSQL reachable via `AGENT_USAGE_DB_DSN`
- Noctalia / Quickshell for the UI

## Quick Start

1. Install the symlinks, plugin, and systemd units:

```bash
./install.sh --restart
```

2. If the install script did not already create it, copy the sample config:

```bash
mkdir -p ~/.config/agent-usage-widget
cp poller/config.toml.example ~/.config/agent-usage-widget/config.toml
chmod 600 ~/.config/agent-usage-widget/config.toml
```

3. Configure each provider identity as a source:

```toml
[service]
host = "127.0.0.1"
port = 8785

[poller]
default_interval_seconds = 900

[sources.personal]
provider = "claude"
label = "Claude Personal"
frontend_visible = true

[sources.personal.auth]
cookie = "..."

[sources.work]
provider = "claude"
label = "Claude Work"
frontend_visible = true
interval_seconds = 1800

[sources.work.auth]
cookie = "..."

[sources.codex]
provider = "codex"
label = "Codex"
frontend_visible = true

[sources.codex.auth]
authorization = "Bearer ..."
cookie = "..."
```

`~/.config/agent-usage-widget/config.toml` is the primary runtime config. Source table names such as `personal` and `work` become stable `source_id` values in the service contract. `label` defaults to the source id, `frontend_visible` and `enabled` default to `true`, and `interval_seconds` defaults to `poller.default_interval_seconds`.

The checked-in `poller/config.toml.example` disables placeholder sources so a fresh install does not poll empty credentials. Set `enabled = true` or remove that line after filling in auth.

Legacy `.env` configuration is still read as a fallback when no TOML sources are configured. If both files exist, `config.toml` controls service settings and sources; explicit CLI overrides still win.

Known limitation: provider auth here is based on copied browser cookies and session tokens. They expire and have to be refreshed manually. The service can surface stale or expired sign-in state, but it cannot renew credentials for you yet. This is a major UX limitation of the current design.

4. Bootstrap the schema:

```bash
psql "postgresql://agent_usage:agent_usage@127.0.0.1:5433/agent_usage" < poller/schema.sql
```

5. Start the backend:

```bash
systemctl --user enable --now agent-usage-service.service
systemctl --user enable --now agent-usage-poll.timer
systemctl --user start agent-usage-poll.service
```

## API

Default base URL: `http://127.0.0.1:8785`

- `/health` - service health
- `/api/current` - current widget contract
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

Lint QML:

```bash
python3 scripts/lint_qml.py
```

Render the current UI from the live service payload:

```bash
python3 scripts/render_widget_screenshots.py
```

The renderer writes `/tmp/agent-usage-panel.png` and `/tmp/agent-usage-bar.png` at the README image sizes by default. It renders through the Noctalia plugin wrappers, reads the live `/api/current` payload when available, falls back to `~/.cache/agent-usage/state.json`, and mirrors local Noctalia `settings.json` / `colors.json` for theme and bar sizing.
