"""Where the sidecar and the dbx CLI are on this machine.

Both test scripts are meant to be runnable as-is on the machine the plugin will be
used from, so neither can hardcode a Windows path. This mirrors the plugin's own
`cli::resolve` -- the same npm prefixes, the same login-shell fallback, the same
order -- because that is the CLI the plugin actually drives, and a test that picks
a different one is not testing the plugin.

`DBX_CLI_BIN` overrides, exactly as it does for the plugin.
"""

import os
import platform
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))

# Matches `cli::SHELL_PROBE_TIMEOUT`: a shell slower than this is stuck on
# something in an rc file.
SHELL_TIMEOUT = 10

# Marks the shell's answer, so whatever an rc file prints is ignored.
SHELL_MARKER = "__dbx_cli_lookup="


def exe(name):
    return f"{name}.exe" if os.name == "nt" else name


def sidecar_path():
    """The sidecar `build.py` produced for this machine."""
    return os.path.join(ROOT, "backend", "target", "release", exe("dbx-plugin-dbdiff"))


def _platform_packages():
    """The platform package's `<os>-<arch>` suffix, in the CLI's spelling."""
    os_name = {"Windows": "win32", "Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"AMD64": "x64", "x86_64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(
        platform.machine()
    )
    if os_name is None or arch is None:
        sys.exit(f"unrecognised host: {platform.system()} / {platform.machine()}")
    names = [f"cli-{os_name}-{arch}"]
    if os_name == "linux":
        # The Linux builds are published with an explicit libc suffix.
        names += [f"cli-{os_name}-{arch}-gnu", f"cli-{os_name}-{arch}-musl"]
    if os_name == "darwin":
        # Either macOS package runs on either Mac, and the one installed is not
        # necessarily the one this interpreter's architecture suggests.
        names.append(f"cli-{os_name}-{'x64' if arch == 'arm64' else 'arm64'}")
    return names


def _versioned(base, middle=""):
    """`<base>/<version>/<middle>/lib/node_modules`, newest first."""
    if not os.path.isdir(base):
        return []
    versions = [name for name in os.listdir(base) if os.path.isdir(os.path.join(base, name))]
    versions.sort(key=lambda name: [int(part) for part in re.findall(r"\d+", name)], reverse=True)
    return [
        os.path.join(base, version, *([middle] if middle else []), "lib", "node_modules")
        for version in versions
    ]


def _npm_roots():
    """The npm global prefixes worth looking under, as `node_modules` directories."""
    if os.name == "nt":
        return [os.path.join(os.environ.get("APPDATA", ""), "npm", "node_modules")]

    home = os.path.expanduser("~")
    roots = [
        os.path.join(home, relative, "node_modules")
        for relative in (
            ".npm-global/lib",
            ".local/lib",
            ".npm/lib",
            ".config/yarn/global",
            ".bun/install/global",
        )
    ]
    # Version managers: a known base, an unknown version directory.
    for base, middle in (
        (".nvm/versions/node", ""),
        (".asdf/installs/nodejs", ""),
        (".volta/tools/image/node", ""),
        ("Library/Application Support/fnm/node-versions", "installation"),
        (".local/share/fnm/node-versions", "installation"),
        ("Library/pnpm/global", ""),
        (".local/share/pnpm/global", ""),
    ):
        roots += _versioned(os.path.join(home, base), middle)
    for variable, relative in (("NVM_DIR", "versions/node"), ("PNPM_HOME", "global")):
        directory = os.environ.get(variable)
        if directory:
            roots += _versioned(os.path.join(directory, *relative.split("/")))
    # `/opt/homebrew` is Homebrew on Apple Silicon, `/usr/local` is Homebrew on
    # Intel and the nodejs.org installer, `/opt/local` is MacPorts.
    roots += [
        os.path.join(prefix, "node_modules")
        for prefix in ("/opt/homebrew/lib", "/usr/local/lib", "/opt/local/lib", "/usr/lib")
    ]
    roots += [entry for entry in os.environ.get("NODE_PATH", "").split(os.pathsep) if entry]
    return roots


def _native_under(node_modules, packages):
    """The native binary in one `node_modules` directory, nested or hoisted."""
    for package in packages:
        for base in (
            os.path.join(node_modules, "@dbx-app", "cli", "node_modules"),
            node_modules,
        ):
            candidate = os.path.join(base, "@dbx-app", package, "bin", exe("dbx"))
            if os.path.isfile(candidate):
                return candidate
    return None


def _is_script(path):
    """npm's `bin` entry is a `.js` file with a `#!` line, not a program."""
    try:
        with open(path, "rb") as handle:
            return handle.read(2) == b"#!"
    except OSError:
        return False


def _shim_node_modules(shim):
    """The `node_modules` directories a shim could have got its package from."""
    package = os.path.dirname(os.path.dirname(os.path.realpath(shim)))
    roots = [os.path.join(package, "node_modules")]
    current = os.path.dirname(package)
    for _ in range(6):
        roots.append(os.path.join(current, "node_modules"))
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return roots


def _cli_in_login_shell():
    """Ask the user's own login shell where `dbx` is.

    A GUI process does not inherit a terminal's PATH, and on macOS Node usually
    comes from a version manager whose setup lives in a shell profile -- so this
    is the same last resort `cli::resolve` falls back to.
    """
    shell = os.environ.get("SHELL") or ("/bin/zsh" if os.path.exists("/bin/zsh") else "/bin/sh")
    script = f"printf '{SHELL_MARKER}%s\\n' \"$(command -v dbx 2>/dev/null)\""
    args = {
        "fish": ["-l", "-i", "-c", script],
        "sh": ["-ic", script],
        "dash": ["-ic", script],
    }.get(os.path.basename(shell), ["-ilc", script])
    try:
        done = subprocess.run(
            [shell] + args, capture_output=True, text=True, encoding="utf-8", timeout=SHELL_TIMEOUT
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in (done.stdout or "").splitlines():
        answer = line.strip()
        if answer.startswith(SHELL_MARKER):
            answer = answer[len(SHELL_MARKER):].strip()
            # An absolute path to a real file, or nothing: `command -v` also
            # answers for shell functions and aliases, and prints a bare name.
            if answer and os.path.isabs(answer) and os.path.isfile(answer):
                return answer
    return None


def cli_path():
    explicit = os.environ.get("DBX_CLI_BIN")
    if explicit:
        if not os.path.isfile(explicit):
            sys.exit(f"DBX_CLI_BIN points at something that is not a file: {explicit}")
        return explicit

    packages = _platform_packages()
    for root in _npm_roots():
        found = _native_under(root, packages)
        if found:
            return found

    # A `dbx` on PATH may be npm's shim rather than a program, in which case it is
    # a pointer at the package the native binary lives in.
    pointers = []
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(directory, exe("dbx")) if directory else ""
        if candidate and os.path.isfile(candidate):
            pointers.append(candidate)
    if os.name != "nt":
        from_shell = _cli_in_login_shell()
        if from_shell:
            pointers.append(from_shell)
    for pointer in pointers:
        if not _is_script(pointer):
            return pointer
        for node_modules in _shim_node_modules(pointer):
            found = _native_under(node_modules, packages)
            if found:
                return found

    tried = _npm_roots() + [f"{pointer} (npm script)" for pointer in pointers]
    sys.exit(
        "dbx CLI not found. `npm install -g @dbx-app/cli`, or set DBX_CLI_BIN.\nLooked in:\n"
        + "\n".join(tried or ["(nowhere)"])
    )
