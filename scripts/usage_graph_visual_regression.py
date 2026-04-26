#!/usr/bin/env python3
"""Render UsageGraph.qml fixtures and compare them with checked-in baselines."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "qml" / "tests" / "visual" / "fixtures"
BASELINE_DIR = ROOT / "qml" / "tests" / "visual" / "baselines"
ACTUAL_DIR = ROOT / "qml" / "tests" / "visual" / "actual"
DIFF_DIR = ROOT / "qml" / "tests" / "visual" / "diffs"
DEFAULT_IMPORT_ROOT_FILE = ROOT / ".cache" / "noctalia-qml-import-root"


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
        return configured if Path(configured).exists() else shutil.which(configured)
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


def _render_test_source(fixture: dict[str, object], output: Path) -> str:
    width = int(fixture.get("width") or 420)
    height = int(fixture.get("height") or 180)
    return f"""
import QtQuick
import QtTest

TestCase {{
  id: testRoot
  name: "UsageGraphVisual"
  when: windowShown
  width: {width}
  height: {height}

  property bool renderDone: false
  property bool renderOk: false
  property var fixture: ({json.dumps(fixture, sort_keys=True)})

  function test_render_fixture() {{
    const component = Qt.createComponent("{_qml_url(ROOT / "qml" / "UsageGraph.qml")}");
    compare(component.status, Component.Ready, component.errorString());
    const graph = component.createObject(testRoot, {{
      width: fixture.width || {width},
      height: fixture.height || {height},
      graph: fixture.graph || {{}},
      nowMs: Number(fixture.now_ms || Date.now()),
      accentColor: fixture.accent_color || "#7aa2f7",
    }});
    verify(graph !== null);
    graph.requestPaint();
    wait(100);
    graph.grabToImage(function(result) {{
      testRoot.renderOk = result.saveToFile({json.dumps(str(output))});
      testRoot.renderDone = true;
    }});
    tryCompare(testRoot, "renderDone", true, 5000);
    verify(renderOk);
    graph.destroy();
  }}
}}
""".strip()


def _render(runner: str, import_root: Path, fixture: Path, output: Path, tmp_path: Path) -> None:
    rendered_fixture = json.loads(fixture.read_text(encoding="utf-8"))
    test_file = tmp_path / f"tst_render_{fixture.stem}.qml"
    test_file.write_text(_render_test_source(rendered_fixture, output) + "\n", encoding="utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    platform = os.environ.get("QML_TEST_PLATFORM", "minimal")
    env = os.environ.copy()
    env["QT_QPA_PLATFORM"] = platform
    env.setdefault("QML_DISABLE_DISK_CACHE", "1")
    env.setdefault("QT_QUICK_BACKEND", "software")
    env.setdefault("QT_STYLE_OVERRIDE", "Fusion")
    cmd = [
        runner,
        "-platform",
        platform,
        "-import",
        str(import_root),
        "-input",
        str(test_file),
    ]
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)
    if not output.exists():
        raise RuntimeError(f"Renderer completed without writing {output}")


def _compare_tool() -> str | None:
    return shutil.which("compare")


def _absolute_error(compare: str, baseline: Path, actual: Path, diff: Path) -> int:
    diff.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [compare, "-metric", "AE", str(baseline), str(actual), str(diff)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    metric = (proc.stderr or proc.stdout).strip().splitlines()[-1].strip()
    try:
        return int(float(metric.split()[0]))
    except ValueError as exc:
        raise RuntimeError(f"Could not parse ImageMagick compare metric: {metric!r}") from exc


def _fixtures(selected: list[str]) -> list[Path]:
    if selected:
        return [(FIXTURE_DIR / name).with_suffix(".json") for name in selected]
    return sorted(FIXTURE_DIR.glob("*.json"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixtures", nargs="*", help="Fixture names without .json, defaults to all")
    parser.add_argument("--update", action="store_true", help="Refresh baseline PNGs from current rendering")
    parser.add_argument("--max-diff-pixels", type=int, default=20, help="Allowed absolute-error pixel count")
    args = parser.parse_args()

    runner = _qmltestrunner()
    if runner is None:
        print(
            "UsageGraph visual tests unavailable: install Qt 6 qmltestrunner or set QML_TESTRUNNER.",
            file=sys.stderr,
        )
        return 2
    import_root = _import_root()
    if import_root is None:
        print(
            "UsageGraph visual tests unavailable: configure the local Noctalia import shim first, e.g.\n"
            "  python3 scripts/setup_noctalia_qml_imports.py --checkout /etc/xdg/quickshell/noctalia-shell",
            file=sys.stderr,
        )
        return 2
    compare = _compare_tool()
    if compare is None and not args.update:
        print("UsageGraph visual tests unavailable: install ImageMagick `compare`.", file=sys.stderr)
        return 2

    fixtures = _fixtures(args.fixtures)
    missing = [fixture for fixture in fixtures if not fixture.exists()]
    if missing:
        for fixture in missing:
            print(f"Missing fixture: {fixture}", file=sys.stderr)
        return 1

    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    with tempfile.TemporaryDirectory(prefix="usage-graph-render-") as tmp:
        tmp_path = Path(tmp)
        for fixture in fixtures:
            name = fixture.stem
            rendered = tmp_path / f"{name}.png"
            baseline = BASELINE_DIR / f"{name}.png"
            actual = ACTUAL_DIR / f"{name}.png"
            diff = DIFF_DIR / f"{name}.png"

            _render(runner, import_root, fixture, rendered, tmp_path)
            if args.update or not baseline.exists():
                shutil.copy2(rendered, baseline)
                print(f"baseline updated: {baseline.relative_to(ROOT)}")
                continue

            ACTUAL_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(rendered, actual)
            diff_pixels = _absolute_error(compare or "compare", baseline, actual, diff)
            if diff_pixels > args.max_diff_pixels:
                failures.append(
                    f"{name}: {diff_pixels} pixels differ "
                    f"(allowed {args.max_diff_pixels}); see {actual.relative_to(ROOT)} and {diff.relative_to(ROOT)}"
                )
            else:
                print(f"visual ok: {name} ({diff_pixels} differing pixels)")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
