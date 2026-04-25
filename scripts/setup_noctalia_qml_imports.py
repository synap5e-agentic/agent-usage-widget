#!/usr/bin/env python3
"""Configure a local Noctalia QML import shim for linting."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / ".cache" / "noctalia-shell"
STUB_ROOT = ROOT / ".cache" / "noctalia-qml-lint-shim"
IMPORT_ROOT_FILE = ROOT / ".cache" / "noctalia-qml-import-root"
REQUIRED_MODULES = ("Quickshell", "Quickshell.Io", "qs.Commons", "qs.Widgets", "qs.Services.UI")


def _ensure_git() -> str:
    git = shutil.which("git")
    if git is None:
        raise SystemExit("git is required")
    return git


def _run(args: list[str], cwd: Path | None = None) -> None:
    subprocess.run(args, cwd=cwd, check=True)


def _write_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(body).strip() + "\n", encoding="utf-8")


def _reset_stub_root() -> None:
    if STUB_ROOT.exists():
        if STUB_ROOT.is_dir():
            shutil.rmtree(STUB_ROOT)
        else:
            STUB_ROOT.unlink()
    STUB_ROOT.mkdir(parents=True, exist_ok=True)


def _module_path_candidates(root: Path, module: str) -> tuple[Path, ...]:
    module_path = module.replace(".", "/")
    if module.startswith("qs."):
        return (root / module_path, root / module.removeprefix("qs.").replace(".", "/"))
    return (root / module_path,)


def _has_module(root: Path, module: str) -> bool:
    return any(candidate.exists() for candidate in _module_path_candidates(root, module))


def _resolve_import_root(root: Path, import_root: str | None) -> Path:
    candidate = (root / import_root).resolve() if import_root else root.resolve()
    if not candidate.exists():
        raise SystemExit(f"Import-root path does not exist: {candidate}")

    missing = [required for required in REQUIRED_MODULES if not _has_module(candidate, required)]
    if missing:
        print(
            f"warning: import-root {candidate} is missing optional Noctalia modules: "
            + ", ".join(missing),
            file=sys.stderr,
        )
    return candidate


def _write_stub_modules(import_root: Path) -> None:
    _reset_stub_root()
    # Keep a small pointer to the checked-out source for future local debugging.
    _write_text(STUB_ROOT / "source-root.txt", str(import_root))

    # Quickshell namespace.
    _write_text(
        STUB_ROOT / "Quickshell" / "qmldir",
        """
        module Quickshell
        singleton QuickshellCore 1.0 QuickshellCore.qml
        FileView 1.0 FileView.qml
        IpcHandler 1.0 IpcHandler.qml
        ShellScreen 1.0 ShellScreen.qml
        """,
    )
    _write_text(
        STUB_ROOT / "Quickshell" / "QuickshellCore.qml",
        """
        import QtQuick

        pragma Singleton
        QtObject {
          function env(name) {
            return name === \"HOME\" ? \"/tmp\" : name;
          }
        }
        """,
    )
    _write_text(
        STUB_ROOT / "Quickshell" / "FileView.qml",
        """
        import QtQuick

        QtObject {
          property string path: \"\"
          signal loaded()
          signal loadFailed(string error)

          function reload() {
            loaded()
          }

          property string text: \"\"
        }
        """,
    )
    _write_text(
        STUB_ROOT / "Quickshell" / "IpcHandler.qml",
        """
        import QtQuick

        QtObject {
          property string target: \"\"
        }
        """,
    )
    _write_text(
        STUB_ROOT / "Quickshell" / "ShellScreen.qml",
        """
        import QtQuick

        QtObject {
          property string name: \"\"
        }
        """,
    )
    _write_text(
        STUB_ROOT / "Quickshell" / "Io" / "qmldir",
        """
        module Quickshell.Io
        FileSystemWatcher 1.0 FileSystemWatcher.qml
        """,
    )
    _write_text(
        STUB_ROOT / "Quickshell" / "Io" / "FileSystemWatcher.qml",
        """
        import QtQuick

        QtObject {
        }
        """,
    )

    # qs.Commons namespace.
    _write_text(
        STUB_ROOT / "qs" / "Commons" / "qmldir",
        """
        module qs.Commons
        singleton Logger 1.0 Logger.qml
        singleton Settings 1.0 Settings.qml
        singleton Style 1.0 Style.qml
        singleton Color 1.0 Color.qml
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Commons" / "Logger.qml",
        """
        import QtQuick

        pragma Singleton
        QtObject {
          function i() {}
          function w() {}
          function e() {}
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Commons" / "Settings.qml",
        """
        import QtQuick

        pragma Singleton
        QtObject {
          function getBarPositionForScreen(_screenName) {
            return \"bottom\";
          }

          property var data: ({
            ui: {
              fontFixed: \"monospace\"
            },
            general: {
              animationDisabled: false
            },
          });
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Commons" / "Style.qml",
        """
        import QtQuick

        pragma Singleton
        QtObject {
          property real uiScaleRatio: 1
          property real marginXL: 16
          property real marginL: 8
          property real marginM: 6
          property real marginS: 4
          property real marginXS: 2
          property real radiusL: 10
          property real radiusM: 8
          property real baseWidgetSize: 16
          property real fontSizeXXL: 24
          property real fontSizeL: 20
          property real fontSizeS: 14
          property real fontSizeXS: 12
          property real fontSizeXXS: 10
          property int fontWeightBold: Font.Bold
          property real animationNormal: 180

          property color capsuleColor: \"#222\"
          property color capsuleBorderColor: \"#555\"
          property real capsuleBorderWidth: 1

          property var mSecondary: Qt.rgba(0.38, 0.65, 0.95, 1.0)
          property var mTertiary: Qt.rgba(0.74, 0.75, 0.30, 1.0)

          function getCapsuleHeightForScreen(_name) {
            return 28
          }

          function getBarFontSizeForScreen(_name) {
            return 11
          }

          function getBarPositionForScreen(_name) {
            return \"bottom\"
          }

          function toOdd(value) {
            const rounded = Math.round(value);
            return rounded + (rounded % 2 === 0 ? 1 : 0);
          }

          function pixelAlignCenter(outer, inner) {
            return (outer - inner) / 2;
          }
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Commons" / "Color.qml",
        """
        import QtQuick

        pragma Singleton
        QtObject {
          property var mPrimary: Qt.rgba(0.18, 0.24, 0.35, 1.0)
          property var mSecondary: Qt.rgba(0.38, 0.65, 0.95, 1.0)
          property var mTertiary: Qt.rgba(0.74, 0.75, 0.30, 1.0)
          property var mOnSurface: Qt.rgba(0.90, 0.92, 0.96, 1.0)
          property var mOnSurfaceVariant: Qt.rgba(0.75, 0.79, 0.86, 1.0)
          property var mSurfaceVariant: Qt.rgba(0.18, 0.23, 0.34, 1.0)
          property var mOutline: Qt.rgba(0.55, 0.60, 0.73, 1.0)
        }
        """,
    )

    # qs.Widgets namespace.
    _write_text(
        STUB_ROOT / "qs" / "Widgets" / "qmldir",
        """
        module qs.Widgets
        NBox 1.0 NBox.qml
        NText 1.0 NText.qml
        NIcon 1.0 NIcon.qml
        NIconButton 1.0 NIconButton.qml
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Widgets" / "NBox.qml",
        """
        import QtQuick

        Rectangle {
          color: \"transparent\"
          border.width: 0
          border.color: \"#555\"
          radius: 4
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Widgets" / "NText.qml",
        """
        import QtQuick

        Text {
          color: \"#fff\"
          property real pointSize: 11

          onPointSizeChanged: font.pointSize = pointSize

          Component.onCompleted: font.pointSize = pointSize
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Widgets" / "NIcon.qml",
        """
        import QtQuick

        Item {
          property string icon: \"\"
          property bool applyUiScale: true
          property real pointSize: 12
          property color color: \"#fff\"
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Widgets" / "NIconButton.qml",
        """
        import QtQuick

        Item {
          signal clicked()
          property string icon: \"\"
          property real baseSize: 12
        }
        """,
    )

    # qs.Services.UI namespace.
    _write_text(
        STUB_ROOT / "qs" / "Services" / "UI" / "qmldir",
        """
        module qs.Services.UI
        TooltipService 1.0 TooltipService.qml
        BarService 1.0 BarService.qml
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Services" / "UI" / "TooltipService.qml",
        """
        import QtQuick

        QtObject {
          function show(_target, _text, _direction) {}
          function hide() {}
        }
        """,
    )
    _write_text(
        STUB_ROOT / "qs" / "Services" / "UI" / "BarService.qml",
        """
        import QtQuick

        QtObject {
          function getTooltipDirection() {
            return \"top\"
          }
        }
        """,
    )


def _clone_checkout(git: str, repo: str, ref: str | None) -> Path:
    CACHE_DIR.parent.mkdir(parents=True, exist_ok=True)
    if not CACHE_DIR.exists():
        _run([git, "clone", repo, str(CACHE_DIR)])
    else:
        _run([git, "fetch", "--all", "--tags"], cwd=CACHE_DIR)

    if ref:
        _run([git, "checkout", ref], cwd=CACHE_DIR)
        _run([git, "pull", "--ff-only"], cwd=CACHE_DIR)

    return CACHE_DIR


def main() -> int:
    parser = argparse.ArgumentParser(description="Set up Noctalia QML imports for local linting")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--repo", help="Git repo URL to clone/update into .cache/noctalia-shell")
    source.add_argument("--checkout", help="Path to an existing local Noctalia checkout")
    parser.add_argument("--ref", help="Optional branch, tag, or ref to checkout after cloning/fetching")
    parser.add_argument(
        "--import-root",
        help="Path relative to the checkout root that contains qs/Commons, qs/Widgets, and qs/Services/UI",
    )
    args = parser.parse_args()
    repo = cast(str | None, args.repo)
    checkout_arg = cast(str | None, args.checkout)
    ref = cast(str | None, args.ref)
    import_root_arg = cast(str | None, args.import_root)

    if repo:
        git = _ensure_git()
        checkout = _clone_checkout(git, repo, ref)
    else:
        if checkout_arg is None:
            raise SystemExit("Either --repo or --checkout is required")
        checkout = Path(checkout_arg).expanduser().resolve()
        if not checkout.exists():
            raise SystemExit(f"Checkout path does not exist: {checkout}")

    import_root = _resolve_import_root(checkout, import_root_arg)
    _write_stub_modules(import_root)

    IMPORT_ROOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    IMPORT_ROOT_FILE.write_text(str(STUB_ROOT) + "\n")

    print(f"Configured Noctalia QML import root: {STUB_ROOT}")
    print(f"Source modules checked from: {import_root}")
    print(f"Saved to: {IMPORT_ROOT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
