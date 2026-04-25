#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_ID="agent-usage"
PLUGIN_DIR="$HOME/.config/noctalia/plugins/$PLUGIN_ID"
PLUGINS_JSON="$HOME/.config/noctalia/plugins.json"
CONFIG_DIR="$HOME/.config/agent-usage-widget"
ENV_FILE="$CONFIG_DIR/.env"
RESTART_NOCTALIA=0

find_noctalia_pids() {
  ps -C qs -o pid=,args= | awk '
    {
      pid = $1
      $1 = ""
      sub(/^[[:space:]]+/, "", $0)
      if ($0 ~ /(^|\/)qs -c noctalia-shell$/) {
        print pid
      }
    }
  '
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --restart)
      RESTART_NOCTALIA=1
      ;;
    *)
      echo "Usage: $0 [--restart]" >&2
      exit 2
      ;;
  esac
  shift
done

echo "Installing agent-usage-widget..."

mkdir -p "$HOME/bin" "$HOME/.config/systemd/user" "$CONFIG_DIR"

if [ ! -f "$ENV_FILE" ]; then
  cp "$SCRIPT_DIR/poller/.env.example" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "Created $ENV_FILE from the sample template. Providers stay disabled until you opt in."
fi

ln -sfv "$SCRIPT_DIR/poller/agent_usage_poll.py" "$HOME/bin/agent-usage-poll"
ln -sfv "$SCRIPT_DIR/poller/agent_usage_service.py" "$HOME/bin/agent-usage-service"

ln -sfv "$SCRIPT_DIR/systemd/agent-usage-poll.service" \
  "$HOME/.config/systemd/user/agent-usage-poll.service"
ln -sfv "$SCRIPT_DIR/systemd/agent-usage-poll.timer" \
  "$HOME/.config/systemd/user/agent-usage-poll.timer"
ln -sfv "$SCRIPT_DIR/systemd/agent-usage-service.service" \
  "$HOME/.config/systemd/user/agent-usage-service.service"

mkdir -p "$PLUGIN_DIR"
for f in manifest.json Main.qml BarWidget.qml Panel.qml UsageGraph.qml; do
  ln -sfv "$SCRIPT_DIR/noctalia_plugin/$f" "$PLUGIN_DIR/$f"
done

if [ -f "$PLUGINS_JSON" ]; then
  if python3 -c "
import json
path = '$PLUGINS_JSON'
with open(path) as f:
    data = json.load(f)
if '$PLUGIN_ID' not in data.get('states', {}):
    data.setdefault('states', {})['$PLUGIN_ID'] = {'enabled': True, 'sourceUrl': 'local'}
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)
        f.write('\n')
    print('Registered plugin in plugins.json')
else:
    print('Plugin already registered in plugins.json')
"; then
    :
  else
    echo "Warning: Failed to update plugins.json - register manually" >&2
  fi
else
  echo "Warning: $PLUGINS_JSON not found - register manually" >&2
fi

systemctl --user daemon-reload
systemctl --user enable --now agent-usage-poll.timer
systemctl --user enable --now agent-usage-service.service

if [ "$RESTART_NOCTALIA" -eq 1 ]; then
  # Use the direct qs restart path. A transient systemd-run unit is brittle
  # here because helper children can outlive qs and leave the unit stuck in
  # deactivating, even though the actual shell process is gone.
  existing_qs_pids="$(find_noctalia_pids)"
  if [ -n "$existing_qs_pids" ]; then
    kill $existing_qs_pids
    for _ in $(seq 1 20); do
      if [ -z "$(find_noctalia_pids)" ]; then
        break
      fi
      sleep 0.25
    done
    if [ -n "$(find_noctalia_pids)" ]; then
      echo "Failed to stop existing Noctalia shell. Verify with: ps -C qs -o pid=,args= | awk '/qs -c noctalia-shell/'" >&2
      exit 1
    fi
  fi
  systemctl --user stop --no-block noctalia-manual.service >/dev/null 2>&1 || true
  setsid qs -c noctalia-shell >/dev/null 2>&1 &
  for _ in $(seq 1 20); do
    noctalia_pids="$(find_noctalia_pids)"
    if [ -n "$noctalia_pids" ]; then
      echo "Restarted Noctalia:"
      printf '%s\n' "$noctalia_pids"
      exit 0
    fi
    sleep 0.5
  done
  echo "Failed to restart Noctalia. Verify with: ps -C qs -o pid=,args= | awk '/qs -c noctalia-shell/'" >&2
  exit 1
else
  echo "Done. Restart Noctalia to load the plugin."
fi
