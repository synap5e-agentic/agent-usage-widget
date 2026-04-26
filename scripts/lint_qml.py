#!/usr/bin/env python3
"""Syntax-check the reusable QML UI and Noctalia wrappers with the local Qt runtime."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGETS = (
    Path("qml/UsageGraph.qml"),
    Path("qml/AgentUsageBar.qml"),
    Path("qml/AgentUsagePanel.qml"),
    Path("noctalia_plugin/Main.qml"),
    Path("noctalia_plugin/BarWidget.qml"),
    Path("noctalia_plugin/Panel.qml"),
)
DEFAULT_IMPORT_ROOT_FILE = ROOT / ".cache" / "noctalia-qml-import-root"
UNAVAILABLE_PATTERNS = (
    'module "quickshell"',
    'module "qs.',
    "quickshell-coreplugin",
    "failed to create wl_display",
    'could not load the qt platform plugin "wayland"',
    "no qt platform plugin could be initialized",
)
IMPORT_RE = re.compile(r"^\s*import\s+([A-Za-z0-9_.]+)", re.MULTILINE)


def _default_targets() -> list[Path]:
    return [ROOT / target for target in DEFAULT_TARGETS]


def _resolve_targets(raw_targets: list[str]) -> list[Path]:
    if not raw_targets:
        return _default_targets()

    resolved: list[Path] = []
    for raw in raw_targets:
        path = Path(raw)
        if not path.is_absolute():
            path = (ROOT / path).resolve()
        if path.suffix != ".qml":
            continue
        resolved.append(path)
    return resolved


def _qml_runner() -> str | None:
    for candidate in ("qml6", "qml"):
        binary = shutil.which(candidate)
        if binary:
            return binary
    return None


def _configured_import_roots() -> list[Path]:
    roots: list[Path] = []
    env_root = os.environ.get("NOCTALIA_QML_IMPORT_ROOT")
    if env_root:
        roots.append(Path(env_root).expanduser())
    elif DEFAULT_IMPORT_ROOT_FILE.exists():
        configured_root = DEFAULT_IMPORT_ROOT_FILE.read_text().strip()
        if configured_root:
            roots.append(Path(configured_root).expanduser())
    return roots


def _missing_modules_for_root(root: Path, imported_modules: set[str]) -> list[str]:
    missing: list[str] = []
    for module in imported_modules:
        candidates = [root / module.replace(".", "/")]
        if module.startswith("qs."):
            candidates.append(root / module.removeprefix("qs.").replace(".", "/"))
        if not any(candidate.exists() for candidate in candidates):
            missing.append(module)
    return sorted(missing)


def _find_missing_noctalia_modules(targets: list[Path]) -> tuple[list[str], Path | None]:
    imported_modules: set[str] = set()
    for target in targets:
        try:
            text = target.read_text()
        except OSError:
            continue
        for match in IMPORT_RE.finditer(text):
            module = match.group(1)
            if module.startswith("qs."):
                imported_modules.add(module)

    if not imported_modules:
        return [], None

    roots = _configured_import_roots()
    for root in roots:
        missing = _missing_modules_for_root(root, imported_modules)
        if not missing:
            return [], root

    return sorted(imported_modules), roots[0] if roots else None


def _runner_source(target: Path) -> str:
    target_url = target.resolve().as_uri()
    return f"""import QtQuick
import QtQml

QtObject {{
    Component.onCompleted: {{
        const component = Qt.createComponent("{target_url}");
        function finish() {{
            if (component.status === Component.Ready) {{
                Qt.exit(0);
                return;
            }}
            if (component.status === Component.Error) {{
                console.error(component.errorString());
                Qt.exit(1);
            }}
        }}
        finish();
        component.statusChanged.connect(finish);
    }}
}}
"""


def lint_file(qml: str, target: Path) -> tuple[bool, str]:
    with tempfile.NamedTemporaryFile("w", suffix=".qml", delete=False) as handle:
        handle.write(_runner_source(target))
        runner_path = Path(handle.name)

    env = os.environ.copy()
    env["QT_QPA_PLATFORM"] = "offscreen"
    env.setdefault("QML_DISABLE_DISK_CACHE", "1")
    env.setdefault("QT_FORCE_STDERR_LOGGING", "1")
    import_roots = _configured_import_roots()
    if import_roots:
        joined = os.pathsep.join(str(root) for root in import_roots)
        env["QML_IMPORT_PATH"] = joined
        env["QML2_IMPORT_PATH"] = joined

    try:
        proc = subprocess.run(
            [qml, "-platform", env["QT_QPA_PLATFORM"], runner_path],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
    finally:
        runner_path.unlink(missing_ok=True)

    output = "\n".join(part.strip() for part in (proc.stdout, proc.stderr) if part.strip())
    if proc.returncode == 0:
        return True, output
    if not output:
        return False, f"{qml} exited with status {proc.returncode} without diagnostics"
    return False, output


def _is_unavailable(output: str) -> bool:
    lowered = output.lower()
    if "without diagnostics" in lowered:
        return True
    return any(pattern in lowered for pattern in UNAVAILABLE_PATTERNS)


def _parse_args() -> tuple[list[str], bool]:
    parser = argparse.ArgumentParser(description="Syntax-check QML files with qml6")
    parser.add_argument("targets", nargs="*", help="QML files to lint (defaults to qml/*.qml and noctalia_plugin/*.qml)")
    parser.add_argument(
        "--skip-unavailable",
        action="store_true",
        help="Return success instead of an availability error when the local QML runtime cannot load Quickshell",
    )
    args = parser.parse_args()
    return cast(list[str], args.targets), bool(args.skip_unavailable)


def _validate_targets(targets: list[Path]) -> int | None:
    missing = [target for target in targets if not target.exists()]
    if not missing:
        return None
    for target in missing:
        print(f"Missing QML file: {target}", file=sys.stderr)
    return 1


def _lint_targets(qml: str, targets: list[Path]) -> tuple[bool, bool]:
    failed = False
    unavailable = False
    for target in targets:
        ok, output = lint_file(qml, target)
        if ok:
            print(f"QML OK: {target.relative_to(ROOT)}")
            continue
        if _is_unavailable(output):
            unavailable = True
        else:
            failed = True
        print(f"QML FAIL: {target.relative_to(ROOT)}", file=sys.stderr)
        if output:
            print(output, file=sys.stderr)
    return failed, unavailable


def main() -> int:
    raw_targets, skip_unavailable = _parse_args()

    qml = _qml_runner()
    if qml is None:
        print("QML lint unavailable: install `qml6` (or `qml`) to validate plugin files.", file=sys.stderr)
        return 2

    targets = _resolve_targets(raw_targets)
    if not targets:
        return 0

    missing_result = _validate_targets(targets)
    if missing_result is not None:
        return missing_result

    missing_modules, import_root = _find_missing_noctalia_modules(targets)
    if missing_modules:
        configured = f"NOCTALIA_QML_IMPORT_ROOT={import_root}" if import_root else "NOCTALIA_QML_IMPORT_ROOT is unset"
        print(
            "QML lint unavailable: missing Noctalia shell imports "
            + ", ".join(missing_modules)
            + f" ({configured}).",
            file=sys.stderr,
        )
        return 0 if skip_unavailable else 2

    failed, unavailable = _lint_targets(qml, targets)
    if failed:
        return 1
    if unavailable:
        return 0 if skip_unavailable else 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
