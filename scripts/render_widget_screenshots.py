#!/usr/bin/env python3
"""Render the QML widget screenshots from the live agent-usage payload."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IMPORT_ROOT_FILE = ROOT / ".cache" / "noctalia-qml-import-root"
DEFAULT_STATE_FILE = Path.home() / ".cache" / "agent-usage" / "state.json"
DEFAULT_SERVICE_URL = "http://127.0.0.1:8785/api/current"


def _import_root() -> Path | None:
    env_root = os.environ.get("NOCTALIA_QML_IMPORT_ROOT")
    if env_root:
        path = Path(env_root).expanduser()
        return path if path.exists() else None
    if DEFAULT_IMPORT_ROOT_FILE.exists():
        configured = DEFAULT_IMPORT_ROOT_FILE.read_text(encoding="utf-8").strip()
        if configured:
            path = Path(configured).expanduser()
            return path if path.exists() else None
    return None


def _qmltestrunner() -> str | None:
    configured = os.environ.get("QML_TESTRUNNER")
    if configured:
        path = Path(configured)
        return configured if path.exists() else shutil.which(configured)
    fallback = Path("/usr/lib/qt6/bin/qmltestrunner")
    if fallback.exists():
        return str(fallback)
    for candidate in ("qmltestrunner6", "qmltestrunner"):
        discovered = shutil.which(candidate)
        if discovered:
            return discovered
    return None


def _qml_url(path: Path) -> str:
    return path.resolve().as_uri()


def _load_payload(service_url: str, state_file: Path) -> tuple[dict[str, object], str]:
    try:
        with urlopen(service_url, timeout=2) as response:
            return json.load(response), "service"
    except (URLError, TimeoutError, json.JSONDecodeError, OSError):
        pass

    if not state_file.exists():
        raise FileNotFoundError(f"Neither {service_url} nor {state_file} provided a readable payload")
    return json.loads(state_file.read_text(encoding="utf-8")), "state"


def _render_test_source(component_path: Path, payload: dict[str, object], output: Path, width: int, height: int, component_kind: str) -> str:
    payload_json = json.dumps(payload, sort_keys=True)
    component_url = _qml_url(component_path)
    object_props = """{
      width: %(width)d,
      height: %(height)d,
      agents: payload.agents || [],
      accentColorFn: function(name) {
        if (name === "secondary") return "#8ec07c";
        if (name === "tertiary") return "#83a598";
        return "#f97316";
      }
    }""" % {"width": width, "height": height}
    if component_kind == "panel":
        object_props = """{
      width: %(width)d,
      height: %(height)d,
      agents: payload.agents || [],
      backend: payload.backend || {},
      updatedAt: String(payload.updated_at || ""),
      currentTime: Date.now(),
      showCloseButton: false,
      accentColorFn: function(name) {
        if (name === "secondary") return "#8ec07c";
        if (name === "tertiary") return "#83a598";
        return "#f97316";
      }
    }""" % {"width": width, "height": height}
    return f"""
import QtQuick
import QtTest

TestCase {{
  id: testRoot
  name: "WidgetRender"
  when: windowShown
  width: {width}
  height: {height}

  property bool renderDone: false
  property bool renderOk: false
  property var payload: ({payload_json})

  function test_render_fixture() {{
    const component = Qt.createComponent("{component_url}");
    compare(component.status, Component.Ready, component.errorString());
    const item = component.createObject(testRoot, {object_props});
    verify(item !== null);
    wait(150);
    item.grabToImage(function(result) {{
      testRoot.renderOk = result.saveToFile({json.dumps(str(output))});
      testRoot.renderDone = true;
    }});
    tryCompare(testRoot, "renderDone", true, 5000);
    verify(renderOk);
    item.destroy();
  }}
}}
""".strip()


def _render(runner: str, import_root: Path, component_path: Path, payload: dict[str, object], output: Path, width: int, height: int, tmp_path: Path, component_kind: str) -> None:
    test_file = tmp_path / f"tst_render_{component_path.stem}.qml"
    test_file.write_text(_render_test_source(component_path, payload, output, width, height, component_kind) + "\n", encoding="utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["QT_QPA_PLATFORM"] = "offscreen"
    env.setdefault("QML_DISABLE_DISK_CACHE", "1")
    env.setdefault("QT_QUICK_BACKEND", "software")
    env.setdefault("QT_STYLE_OVERRIDE", "Fusion")
    cmd = [
        runner,
        "-platform",
        "offscreen",
        "-import",
        str(import_root),
        "-input",
        str(test_file),
    ]
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)
    if not output.exists():
        raise RuntimeError(f"Renderer completed without writing {output}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service-url", default=DEFAULT_SERVICE_URL, help="Primary payload URL, defaults to local /api/current")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="Fallback state.json path")
    parser.add_argument("--panel-output", default="/tmp/agent-usage-panel.png", help="Rendered panel PNG path")
    parser.add_argument("--bar-output", default="/tmp/agent-usage-bar.png", help="Rendered bar PNG path")
    parser.add_argument("--panel-width", type=int, default=1280)
    parser.add_argument("--panel-height", type=int, default=900)
    parser.add_argument("--bar-width", type=int, default=420)
    parser.add_argument("--bar-height", type=int, default=36)
    args = parser.parse_args()

    runner = _qmltestrunner()
    if runner is None:
        print("Widget render unavailable: install Qt 6 qmltestrunner or set QML_TESTRUNNER.", file=sys.stderr)
        return 2
    import_root = _import_root()
    if import_root is None:
        print(
            "Widget render unavailable: configure the local Noctalia import shim first, e.g.\n"
            "  python3 scripts/setup_noctalia_qml_imports.py --checkout /etc/xdg/quickshell/noctalia-shell",
            file=sys.stderr,
        )
        return 2

    payload, source = _load_payload(args.service_url, Path(args.state_file).expanduser())
    print(f"Using payload source: {source}")

    with tempfile.TemporaryDirectory(prefix="widget-render-") as tmp:
        tmp_path = Path(tmp)
        _render(
            runner,
            import_root,
            ROOT / "qml" / "AgentUsagePanel.qml",
            payload,
            Path(args.panel_output).expanduser(),
            args.panel_width,
            args.panel_height,
            tmp_path,
            "panel",
        )
        _render(
            runner,
            import_root,
            ROOT / "qml" / "AgentUsageBar.qml",
            payload,
            Path(args.bar_output).expanduser(),
            args.bar_width,
            args.bar_height,
            tmp_path,
            "bar",
        )

    print(f"panel: {Path(args.panel_output).expanduser()}")
    print(f"bar: {Path(args.bar_output).expanduser()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
