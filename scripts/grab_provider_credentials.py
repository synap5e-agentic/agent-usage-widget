#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Capture provider credentials (Claude / Codex / Cursor) via mitmproxy.

Usage:
    grab_provider_credentials.py                         # all three providers
    grab_provider_credentials.py --target claude         # one
    grab_provider_credentials.py --target claude --target codex

The script self-launches mitmdump with this file as the addon. mitmdump
listens on a random local port; a fresh red-themed Chrome profile is
launched pointed at the proxy and the relevant provider URLs. As soon as
each requested target has its cookie (and Authorization header for Codex)
captured, the script prints ready-to-paste TOML for `config.toml` and
shuts down.

If Chrome is not installed the script falls back to printing proxy
details so the user can route any browser through it manually.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROVIDERS: dict[str, dict[str, object]] = {
    "claude": {
        "label": "Claude (claude.ai)",
        "open_urls": ["https://claude.ai/settings/usage"],
        "match_host_suffix": "claude.ai",
        "match_path_prefix": "/api/organizations/",
        "needs": ("cookie",),
        "toml_section": "claude",
    },
    "codex": {
        "label": "Codex (chatgpt.com)",
        "open_urls": ["https://chatgpt.com/codex/cloud/settings/analytics"],
        "match_host_suffix": "chatgpt.com",
        "match_path_prefix": "/backend-api/wham/",
        "needs": ("cookie", "authorization"),
        "toml_section": "codex",
    },
    "cursor": {
        "label": "Cursor (cursor.com)",
        "open_urls": ["https://cursor.com/dashboard/billing"],
        "match_host_suffix": "cursor.com",
        "match_path_prefix": "/api/usage-summary",
        "needs": ("cookie",),
        "toml_section": "cursor",
    },
}

CHROME_CANDIDATES = (
    "google-chrome-stable",
    "google-chrome",
    "chromium",
    "chromium-browser",
)

DEFAULT_TIMEOUT_SECONDS = 600


def _xdg_state_home() -> Path:
    raw = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(raw)


def _xdg_runtime_dir() -> Path:
    raw = os.environ.get("XDG_RUNTIME_DIR")
    return Path(raw) if raw else Path(tempfile.gettempdir())


def _persistent_chrome_profile() -> Path:
    path = _xdg_state_home() / "agent-usage-widget" / "grab-chrome-profile"
    path.mkdir(parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return path


# ----------------------------- launcher ----------------------------- #


def _pick_port() -> int:
    import socket

    for _ in range(20):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if 1024 < port < 65535:
            return port
    raise RuntimeError("could not bind a local port for mitmdump")


def _find_chrome() -> str | None:
    for name in CHROME_CANDIDATES:
        path = shutil.which(name)
        if path:
            return path
    return None


def _write_chrome_preferences(profile_dir: Path) -> None:
    default_dir = profile_dir / "Default"
    default_dir.mkdir(parents=True, exist_ok=True)
    prefs_path = default_dir / "Preferences"
    if prefs_path.exists():
        return  # don't overwrite Chrome's own state on subsequent runs
    prefs_path.write_text(
        json.dumps(
            {
                "browser": {"theme": {"color_variant2": 1, "user_color2": -65532}},
                "extensions": {"theme": {"id": "user_color_theme_id"}},
                "profile": {"name": "agent-usage credentials"},
            }
        )
    )


def _format_toml(provider: str, capture: dict[str, str]) -> str:
    section = PROVIDERS[provider]["toml_section"]
    lines = [f"[sources.{section}.auth]"]
    for key in ("authorization", "cookie", "organization_id"):
        if key in capture:
            value = capture[key].replace("\\", "\\\\").replace("'", "\\'")
            lines.append(f"{key} = '{value}'")
    return "\n".join(lines)


def _wait_for_listening(port: int, timeout: float) -> bool:
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            try:
                sock.connect(("127.0.0.1", port))
                return True
            except OSError:
                pass
        time.sleep(0.2)
    return False


def _print_browser_instructions(port: int, urls: list[str], cert_path: Path) -> None:
    sys.stderr.write(
        "\nChrome not found. Configure your browser manually:\n"
        f"  - HTTP/HTTPS proxy: 127.0.0.1:{port}\n"
        f"  - Trust mitmproxy CA: visit http://mitm.it through the proxy and follow the instructions,\n"
        f"    or import {cert_path}\n"
        "  - Then log in and visit:\n"
    )
    for url in urls:
        sys.stderr.write(f"      {url}\n")
    sys.stderr.write("\n")
    sys.stderr.flush()


def _launch_chrome(chrome_path: str, profile_dir: Path, port: int, urls: list[str]) -> subprocess.Popen[bytes]:
    cmd = [
        chrome_path,
        f"--proxy-server=127.0.0.1:{port}",
        "--ignore-certificate-errors",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        *urls,
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _collect_captures(capture_dir: Path, targets: list[str], timeout: float) -> dict[str, dict[str, str]]:
    deadline = time.monotonic() + timeout
    seen: dict[str, dict[str, str]] = {}
    last_status = 0.0
    while time.monotonic() < deadline:
        for target in targets:
            if target in seen:
                continue
            path = capture_dir / f"{target}.json"
            if path.exists():
                try:
                    seen[target] = json.loads(path.read_text())
                except json.JSONDecodeError:
                    continue
                sys.stderr.write(f"[grab] captured {target}\n")
                sys.stderr.flush()
        if len(seen) == len(targets):
            return seen
        if time.monotonic() - last_status > 30:
            missing = [t for t in targets if t not in seen]
            sys.stderr.write(f"[grab] waiting on: {', '.join(missing)}\n")
            sys.stderr.flush()
            last_status = time.monotonic()
        time.sleep(0.5)
    return seen


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        action="append",
        choices=sorted(PROVIDERS),
        default=[],
        help="Provider to capture; repeatable. Defaults to all providers.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Overall capture timeout in seconds (default {DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--reset-profile",
        action="store_true",
        help="Discard the persistent Chrome profile before launching (forces a fresh login).",
    )
    args = parser.parse_args()

    targets = args.target or sorted(PROVIDERS)

    port = _pick_port()
    capture_dir = Path(tempfile.mkdtemp(prefix="agent-usage-grab-", dir=_xdg_runtime_dir()))
    capture_dir.chmod(0o700)
    log_path = capture_dir / "mitm.log"
    chrome_profile = _persistent_chrome_profile()
    if args.reset_profile:
        shutil.rmtree(chrome_profile, ignore_errors=True)
        chrome_profile = _persistent_chrome_profile()
    _write_chrome_preferences(chrome_profile)

    targets_json = json.dumps({t: PROVIDERS[t] for t in targets}, default=str)

    mitm_cmd = [
        "uvx",
        "--quiet",
        "--from",
        "mitmproxy",
        "mitmdump",
        "-s",
        str(Path(__file__).resolve()),
        "--listen-host",
        "127.0.0.1",
        "--listen-port",
        str(port),
        "--set",
        f"grab_capture_dir={capture_dir}",
        "--set",
        f"grab_targets={targets_json}",
    ]

    sys.stderr.write(f"[grab] starting mitmdump on 127.0.0.1:{port}\n")
    sys.stderr.write(f"[grab] capture dir: {capture_dir}\n")
    sys.stderr.flush()

    with log_path.open("wb") as log_fp:
        mitm = subprocess.Popen(mitm_cmd, stdout=log_fp, stderr=subprocess.STDOUT)

    chrome_proc: subprocess.Popen[bytes] | None = None
    cleanup_done = False

    def cleanup(_signum: int | None = None, _frame: object | None = None) -> None:
        nonlocal cleanup_done
        if cleanup_done:
            return
        cleanup_done = True
        if chrome_proc and chrome_proc.poll() is None:
            chrome_proc.terminate()
            try:
                chrome_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                chrome_proc.kill()
        if mitm.poll() is None:
            mitm.terminate()
            try:
                mitm.wait(timeout=5)
            except subprocess.TimeoutExpired:
                mitm.kill()
        # Capture dir holds the JSON dump of live cookies/bearer tokens — wipe it.
        # Chrome profile is intentionally persistent across runs (use --reset-profile to wipe).
        shutil.rmtree(capture_dir, ignore_errors=True)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        if not _wait_for_listening(port, timeout=20):
            sys.stderr.write("[grab] mitmdump did not start listening; log follows:\n")
            sys.stderr.write(log_path.read_text(errors="replace"))
            cleanup()
            return 1

        chrome_path = _find_chrome()
        urls = [url for t in targets for url in PROVIDERS[t]["open_urls"]]  # type: ignore[index]

        if chrome_path:
            sys.stderr.write(
                f"[grab] launching {Path(chrome_path).name} with persistent profile "
                f"at {chrome_profile}\n"
            )
            sys.stderr.write("[grab] log in to each provider; the script self-terminates on capture\n")
            sys.stderr.flush()
            chrome_proc = _launch_chrome(chrome_path, chrome_profile, port, urls)
        else:
            cert_path = Path.home() / ".mitmproxy" / "mitmproxy-ca-cert.pem"
            _print_browser_instructions(port, urls, cert_path)

        captures = _collect_captures(capture_dir, targets, timeout=args.timeout)
    finally:
        cleanup()

    if not captures:
        sys.stderr.write("[grab] no captures collected before exit\n")
        return 2

    print()
    print("# Paste into ~/.config/agent-usage-widget/config.toml")
    print("# (one [sources.<id>] block per provider; section name is the source id)")
    print()
    for target in targets:
        if target in captures:
            print(_format_toml(target, captures[target]))
            print()
        else:
            print(f"# {target}: NOT CAPTURED")
            print()

    missing = [t for t in targets if t not in captures]
    return 0 if not missing else 3


if __name__ == "__main__":
    raise SystemExit(main())


# --------------------------- mitmproxy addon --------------------------- #
# This block runs only when mitmdump imports the file as an addon module.

from mitmproxy import ctx, http  # noqa: E402


class CredentialCapture:
    def __init__(self) -> None:
        self.capture_dir: Path | None = None
        self.targets: dict[str, dict[str, object]] = {}
        self.captured: set[str] = set()

    def load(self, loader) -> None:  # type: ignore[no-untyped-def]
        loader.add_option("grab_capture_dir", str, "", "directory for capture JSON files")
        loader.add_option("grab_targets", str, "{}", "JSON object describing targets")

    def configure(self, updates) -> None:  # type: ignore[no-untyped-def]
        if "grab_capture_dir" in updates and ctx.options.grab_capture_dir:
            self.capture_dir = Path(ctx.options.grab_capture_dir)
        if "grab_targets" in updates and ctx.options.grab_targets:
            self.targets = json.loads(ctx.options.grab_targets)
            ctx.log.info(f"[grab] capturing targets: {sorted(self.targets)}")

    def response(self, flow: http.HTTPFlow) -> None:
        # Only capture from authenticated endpoints that responded successfully —
        # that guarantees the cookie / bearer in the request is a live session.
        if not self.capture_dir or not self.targets:
            return
        if flow.response is None or flow.response.status_code != 200:
            return
        host = flow.request.pretty_host
        path = flow.request.path or ""
        for target_id, target in self.targets.items():
            if target_id in self.captured:
                continue
            host_suffix = str(target.get("match_host_suffix") or "")
            path_prefix = str(target.get("match_path_prefix") or "")
            if host_suffix and not host.endswith(host_suffix):
                continue
            if path_prefix and not path.startswith(path_prefix):
                continue
            needs = tuple(target.get("needs") or ())
            cap: dict[str, str] = {}
            if "cookie" in needs:
                # Rebuild from parsed cookies so HTTP/2 multi-Cookie-headers
                # serialize as the standard "; "-joined form the poller expects.
                pairs = list(flow.request.cookies.items(multi=True))
                if pairs:
                    cap["cookie"] = "; ".join(f"{name}={value}" for name, value in pairs)
            if "authorization" in needs:
                auth = flow.request.headers.get("Authorization", "")
                if auth.lower().startswith("bearer "):
                    cap["authorization"] = auth
            if target_id == "claude":
                import re

                match = re.search(r"/organizations/([^/]+)/", path)
                if match:
                    cap["organization_id"] = match.group(1)
            if not all(k in cap for k in needs):
                continue
            self.captured.add(target_id)
            out = self.capture_dir / f"{target_id}.json"
            out.write_text(json.dumps(cap, indent=2))
            ctx.log.info(f"[grab] captured {target_id} from {host}{path}")
            if self.captured >= set(self.targets):
                ctx.log.info("[grab] all targets captured, shutting down")
                ctx.master.shutdown()
            return


addons = [CredentialCapture()]
