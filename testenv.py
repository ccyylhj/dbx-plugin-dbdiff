"""Where the sidecar and the dbx CLI are on this machine.

Both test scripts are meant to be runnable as-is on the machine the plugin will
be used from, so neither can hardcode a Windows path. This mirrors the plugin's
own `cli::resolve` -- npm global layout first, then PATH -- because that is the
same CLI the plugin will pick up, and a test that drives a different one is not
testing the plugin.

`DBX_CLI_BIN` overrides, exactly as it does for the plugin.
"""

import os
import platform
import shutil
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def exe(name):
    return f"{name}.exe" if os.name == "nt" else name


def sidecar_path():
    """The sidecar `build.py` produced for this machine."""
    return os.path.join(ROOT, "backend", "target", "release", exe("dbx-plugin-dbdiff"))


def _npm_roots():
    if os.name == "nt":
        return [os.path.join(os.environ.get("APPDATA", ""), "npm", "node_modules")]
    home = os.path.expanduser("~")
    return [
        os.path.join(home, ".npm-global", "lib", "node_modules"),
        os.path.join(home, ".local", "lib", "node_modules"),
        os.path.join(home, ".npm", "lib", "node_modules"),
        # Homebrew's prefix: /opt/homebrew on Apple Silicon, /usr/local on Intel.
        "/opt/homebrew/lib/node_modules",
        "/usr/local/lib/node_modules",
        "/usr/lib/node_modules",
    ]


def _platform_package():
    os_name = {"Windows": "win32", "Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"AMD64": "x64", "x86_64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(
        platform.machine()
    )
    if os_name is None or arch is None:
        sys.exit(f"unrecognised host: {platform.system()} / {platform.machine()}")
    return f"cli-{os_name}-{arch}"


def cli_path():
    explicit = os.environ.get("DBX_CLI_BIN")
    if explicit:
        if not os.path.isfile(explicit):
            sys.exit(f"DBX_CLI_BIN points at something that is not a file: {explicit}")
        return explicit

    for root in _npm_roots():
        candidate = os.path.join(
            root, "@dbx-app", "cli", "node_modules", "@dbx-app", _platform_package(), "bin", exe("dbx")
        )
        if os.path.isfile(candidate):
            return candidate

    # The desktop app's own binary is also called `dbx.exe`, so a PATH hit can be
    # the wrong program -- the plugin skips cargo output trees for the same
    # reason, and does it before trusting PATH.
    found = shutil.which(exe("dbx"))
    if found:
        return found

    sys.exit("dbx CLI not found. `npm install -g @dbx-app/cli`, or set DBX_CLI_BIN.")
