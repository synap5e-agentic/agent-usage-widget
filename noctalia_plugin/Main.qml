import QtQuick
import Quickshell
import Quickshell.Io
import qs.Commons

Item {
  id: root
  property var pluginApi: null

  property var agents: []
  property var backend: ({})
  property var configValues: ({})
  property string updatedAt: ""
  property string _envConfigStatus: ""
  property string _rejectedServiceHost: ""
  property real currentTime: Date.now()
  property bool serviceAvailable: false
  property var _serviceRequest: null

  readonly property string homeDir: (Quickshell.env("HOME") || "")
  readonly property string defaultCacheDir: homeDir + "/.cache/agent-usage"
  readonly property string envFilePath: expandHome(Quickshell.env("AGENT_USAGE_ENV_FILE") || (homeDir + "/.config/agent-usage-widget/.env"))
  readonly property string cacheDir: expandHome(runtimeConfigValue("AGENT_USAGE_CACHE_DIR", defaultCacheDir))
  readonly property string stateFilePath: expandHome(runtimeConfigValue("AGENT_USAGE_STATE_FILE", cacheDir + "/state.json"))
  readonly property string serviceHost: {
    const configured = runtimeConfigValue("AGENT_USAGE_SERVICE_HOST", "127.0.0.1");
    return isLocalHost(configured) ? configured : "127.0.0.1";
  }
  readonly property int servicePort: {
    const configured = parseInt(runtimeConfigValue("AGENT_USAGE_SERVICE_PORT", "8785"), 10);
    return configured > 0 && configured < 65536 ? configured : 8785;
  }
  readonly property string serviceCurrentUrl: "http://" + serviceHost + ":" + servicePort + "/api/current"

  function accentColor(name) {
    if (name === "secondary") return Color.mSecondary;
    if (name === "tertiary") return Color.mTertiary;
    return Color.mPrimary;
  }

  function readFileText(view) {
    if (!view) return "";
    if (typeof view.text === "function") return view.text();
    if (typeof view.text === "string") return view.text;
    return "";
  }

  function expandHome(path) {
    const raw = String(path || "").trim();
    if (!raw) return "";
    if (raw === "~") return root.homeDir;
    if (raw.indexOf("~/") === 0) return root.homeDir + raw.slice(1);
    return raw;
  }

  function configValue(name, fallback) {
    const raw = root.configValues ? root.configValues[name] : undefined;
    if (raw === undefined || raw === null) return fallback;
    const text = String(raw).trim();
    return text ? text : fallback;
  }

  function runtimeConfigValue(name, fallback) {
    const envValue = String(Quickshell.env(name) || "").trim();
    if (envValue) return envValue;
    return configValue(name, fallback);
  }

  function isLocalHost(host) {
    const candidate = String(host || "").trim().toLowerCase();
    if (!candidate) return false;
    if (candidate === "localhost") return true;
    const parts = candidate.split(".");
    if (parts.length !== 4 || parts[0] !== "127") return false;
    for (let i = 0; i < parts.length; i++) {
      const value = parseInt(parts[i], 10);
      if (isNaN(value) || value < 0 || value > 255 || String(value) !== parts[i]) return false;
    }
    return true;
  }

  function parseEnvText(rawText) {
    const parsed = {};
    const lines = String(rawText || "").split(/\r?\n/);
    for (let i = 0; i < lines.length; i++) {
      let line = lines[i].trim();
      if (!line || line.charAt(0) === "#") continue;
      if (line.indexOf("export ") === 0) line = line.slice(7);
      const eq = line.indexOf("=");
      if (eq === -1) continue;
      const key = line.slice(0, eq).trim();
      let value = line.slice(eq + 1).trim();
      if (!key) continue;
      if (
        value.length >= 2 &&
        ((value.charAt(0) === "\"" && value.charAt(value.length - 1) === "\"") ||
         (value.charAt(0) === "'" && value.charAt(value.length - 1) === "'"))
      ) {
        value = value.slice(1, -1);
      }
      parsed[key] = value;
    }
    return parsed;
  }

  function applyEnvConfig(rawText) {
    root.configValues = parseEnvText(rawText);
    const configuredHost = runtimeConfigValue("AGENT_USAGE_SERVICE_HOST", "127.0.0.1");
    if (configuredHost && !isLocalHost(configuredHost)) {
      if (root._rejectedServiceHost !== configuredHost) {
        Logger.w("AgentUsage", "Ignoring non-local service host in config: " + configuredHost);
      }
      root._rejectedServiceHost = configuredHost;
    } else {
      root._rejectedServiceHost = "";
    }
  }

  function refreshFromConfiguredSources() {
    refreshFromService();
    if (!root.serviceAvailable) stateFile.reload();
  }

  function backendSummary() {
    if (!root.backend || !root.backend.label) return "Waiting for backend";
    return root.backend.label + (root.backend.transport ? " via " + root.backend.transport : "");
  }

  function applyStatePayload(rawText, sourceLabel) {
    if (!rawText) {
      Logger.w("AgentUsage", sourceLabel + " payload empty");
      return;
    }

    try {
      const parsed = JSON.parse(rawText);
      if (!parsed || typeof parsed !== "object") {
        Logger.w("AgentUsage", sourceLabel + " payload invalid JSON object");
        return;
      }

      root.agents = parsed.agents || [];
      root.backend = parsed.backend || ({});
      root.updatedAt = parsed.updated_at || "";
      Logger.i("AgentUsage", sourceLabel + " loaded " + root.agents.length + " agents");
      root.serviceAvailable = sourceLabel === "Service";
      root.currentTime = Date.now();
    } catch (e) {
      Logger.e("AgentUsage", sourceLabel + " failed to parse payload: " + e);
    }
  }

  function refreshFromService() {
    if (root._serviceRequest) {
      try {
        root._serviceRequest.abort();
      } catch (_ignore) {}
    }

    const xhr = new XMLHttpRequest();
    root._serviceRequest = xhr;
    xhr.onreadystatechange = function() {
      if (xhr.readyState !== XMLHttpRequest.DONE) return;

      if (xhr.status >= 200 && xhr.status < 300) {
        root.applyStatePayload(xhr.responseText, "Service");
      } else {
        root.serviceAvailable = false;
        stateFile.reload();
      }
    };
    xhr.open("GET", root.serviceCurrentUrl, true);
    xhr.send();
  }

  Timer {
    interval: 30000
    running: true
    repeat: true
    onTriggered: {
      root.currentTime = Date.now();
      envConfigFile.reload();
    }
  }

  FileView {
    id: envConfigFile
    path: root.envFilePath

    onLoaded: {
      root._envConfigStatus = "loaded";
      root.applyEnvConfig(root.readFileText(envConfigFile));
      root.refreshFromConfiguredSources();
    }

    onLoadFailed: function(error) {
      if (root._envConfigStatus !== "missing") {
        Logger.i("AgentUsage", "env file not found, using defaults: " + error);
      }
      root._envConfigStatus = "missing";
      root.configValues = ({});
      root._rejectedServiceHost = "";
      root.refreshFromConfiguredSources();
    }
  }

  FileView {
    id: stateFile
    path: root.stateFilePath

    onLoaded: {
      root.applyStatePayload(root.readFileText(stateFile), "Fallback state file");
    }

    onLoadFailed: function(error) {
      Logger.w("AgentUsage", "state.json not found: " + error);
    }
  }

  IpcHandler {
    target: "plugin:agent-usage"

    function toggle() {
      if (pluginApi) {
        pluginApi.withCurrentScreen(function(screen) {
          pluginApi.togglePanel(screen);
        });
      }
    }

    function refresh() {
      Logger.i("AgentUsage", "Manual refresh triggered via IPC");
      envConfigFile.reload();
    }
  }

  Component.onCompleted: {
    Logger.i("AgentUsage", "Plugin initialized");
    envConfigFile.reload();
  }
}
