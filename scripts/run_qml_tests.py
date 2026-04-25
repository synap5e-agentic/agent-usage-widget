#!/usr/bin/env python3
"""Run headless Qt Quick Test coverage for the Noctalia plugin QML files."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IMPORT_ROOT_FILE = ROOT / ".cache" / "noctalia-qml-import-root"
DEFAULT_TEST_DIR = ROOT / "noctalia_plugin" / "tests" / "qml"


def _first_existing(paths: list[str | None]) -> str | None:
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.exists() and os.access(path, os.X_OK):
            return str(path)
    return None


def _qmltestrunner() -> str | None:
    return _first_existing(
        [
            os.environ.get("QML_TESTRUNNER"),
            "/usr/lib/qt6/bin/qmltestrunner",
            shutil.which("qmltestrunner6"),
            shutil.which("qmltestrunner"),
            shutil.which("qmltestrunner-qt5"),
        ]
    )


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


def main() -> int:
    runner = _qmltestrunner()
    if runner is None:
        print(
            "QML tests unavailable: install Qt 6 qmltestrunner or set QML_TESTRUNNER.",
            file=sys.stderr,
        )
        return 2

    import_root = _import_root()
    if import_root is None:
        print(
            "QML tests unavailable: configure the local Noctalia import shim first, e.g.\n"
            "  python3 scripts/setup_noctalia_qml_imports.py --checkout /etc/xdg/quickshell/noctalia-shell",
            file=sys.stderr,
        )
        return 2

    test_input = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_TEST_DIR
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
        str(test_input),
    ]
    return subprocess.run(cmd, cwd=ROOT, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
