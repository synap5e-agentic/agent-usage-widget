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
| `poller/` | poller, shared backend code, schema, env template |
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
cp poller/.env.example ~/.config/agent-usage-widget/.env
chmod 600 ~/.config/agent-usage-widget/.env
```

3. Fill in only the providers you want:

```dotenv
AGENT_USAGE_ENABLE_CLAUDE=1
AGENT_USAGE_CLAUDE_COOKIE=...

AGENT_USAGE_ENABLE_CODEX=1
AGENT_USAGE_CODEX_AUTHORIZATION=...
AGENT_USAGE_CODEX_COOKIE=...

AGENT_USAGE_ENABLE_CURSOR=1
AGENT_USAGE_CURSOR_COOKIE=...
```

`poller/.env.example` is the template. Do not commit a filled live `.env`.

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
- `/api/history` - graph history for a provider or metric
- `/api/raw/latest` - latest stored raw provider payload

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
