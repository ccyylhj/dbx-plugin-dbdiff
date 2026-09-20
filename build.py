"""Build the DB Diff plugin into an installable .dbxp package.

**One package per platform.** A `.dbxp` carries the sidecar built for exactly one
target and a manifest that names that target's binary -- DBX's own package layout
puts it at `bin/<target>/`, and the runtime resolves the manifest's `executable`
path literally. So `...-windows-x64.dbxp` and `...-darwin-arm64.dbxp` are two
artifacts of one source, not one package that runs anywhere. A plugin with a
native sidecar cannot use the `universal` target; that is only for frontend-only
packages.

A `.dbxp` is a zip containing manifest.json, an exact sha256 manifest
(checksums.json), and the plugin payload. This script builds the sidecar, stages
the payload, hashes it, and zips the result - no external packager needed.

The default target is the host's, which is also the only one that builds without
extra setup: the sidecar links against its platform's libc, so a macOS package has
to be built on macOS. `--target darwin-arm64` on Windows gets as far as cargo and
stops there.

Unsigned output only. DBX refuses it unless "允许安装未签名开发包" is on in the
plugin center's developer options.

Usage:
    python build.py                      # for this machine
    python build.py -t darwin-arm64      # on a Mac
    python build.py --list-targets
"""

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.abspath(__file__))
DIST = os.path.join(ROOT, "dist")
STAGE = os.path.join(ROOT, "build", "stage")
EXECUTABLE_STEM = "dbx-plugin-dbdiff"

# DBX's target names -> Rust's, kept in one place so `--target` is validated here
# rather than passed through to cargo and failing obscurely.
TRIPLES = {
    "windows-x64": "x86_64-pc-windows-msvc",
    "windows-arm64": "aarch64-pc-windows-msvc",
    "darwin-arm64": "aarch64-apple-darwin",
    "darwin-x64": "x86_64-apple-darwin",
    "linux-x64": "x86_64-unknown-linux-gnu",
    "linux-arm64": "aarch64-unknown-linux-gnu",
}


def host_target():
    """This machine's DBX target name, spelled the way the host spells it in
    `current_plugin_target`."""
    os_name = {"Windows": "windows", "Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"AMD64": "x64", "x86_64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(
        platform.machine()
    )
    if os_name is None or arch is None:
        sys.exit(f"unrecognised host: {platform.system()} / {platform.machine()}")
    return f"{os_name}-{arch}"


def executable_name(target):
    return f"{EXECUTABLE_STEM}.exe" if target.startswith("windows") else EXECUTABLE_STEM


def cargo_env():
    """cargo, and the environment it needs.

    The machine this was written on keeps cargo outside PATH in a non-standard
    prefix; a normal install (including any Mac) has it on PATH. Both are tried,
    and the prefix variables are set only if those directories exist.
    """
    cargo = shutil.which("cargo")
    if not cargo:
        for candidate in (r"D:\rust\cargo\bin\cargo.exe", os.path.expanduser("~/.cargo/bin/cargo")):
            if os.path.isfile(candidate):
                cargo = candidate
                break
    if not cargo:
        sys.exit("cargo not found. Put it on PATH, or install Rust and retry.")

    env = dict(os.environ)
    for name, path in (("CARGO_HOME", r"D:\rust\cargo"), ("RUSTUP_HOME", r"D:\rust\rustup")):
        if os.path.isdir(path):
            env[name] = path
    # Without this the build lands in whatever ambient CARGO_TARGET_DIR is set to
    # (this machine has one), and the packaging step then copies a stale binary.
    env["CARGO_TARGET_DIR"] = os.path.join(ROOT, "backend", "target")
    return cargo, env


def build_sidecar(target):
    """Build for `target` and return the path to the executable."""
    cargo, env = cargo_env()
    manifest = os.path.join(ROOT, "backend", "Cargo.toml")
    name = executable_name(target)

    args = [cargo, "build", "--release", "--manifest-path", manifest]
    if target == host_target():
        # No `--target` for the host: the output stays in `target/release`, which
        # is where the test scripts look for it.
        out_dir = os.path.join(ROOT, "backend", "target", "release")
    else:
        triple = TRIPLES[target]
        print(f"cross-building for {triple} -- this needs that platform's toolchain and SDK")
        args += ["--target", triple]
        out_dir = os.path.join(ROOT, "backend", "target", triple, "release")

    print("building sidecar...")
    result = subprocess.run(args, env=env)
    if result.returncode != 0:
        if target != host_target():
            sys.exit(
                f"\ncargo build failed for {target}.\n"
                f"A native sidecar links against its platform's libc, so a {target} package\n"
                f"has to be built on {target} -- cross-compiling needs that platform's SDK.\n"
                f"On a Mac: python build.py -t {target}"
            )
        sys.exit("cargo build failed")

    executable = os.path.join(out_dir, name)
    if not os.path.isfile(executable):
        sys.exit(f"cargo reported success but {executable} is not there")
    return executable


def stage_files(executable, target):
    if os.path.isdir(STAGE):
        shutil.rmtree(STAGE)
    os.makedirs(os.path.join(STAGE, "bin", target))
    shutil.copy2(os.path.join(ROOT, "manifest.json"), os.path.join(STAGE, "manifest.json"))
    shutil.copytree(os.path.join(ROOT, "assets"), os.path.join(STAGE, "assets"))
    shutil.copytree(os.path.join(ROOT, "ui"), os.path.join(STAGE, "ui"))
    shutil.copy2(executable, os.path.join(STAGE, "bin", target, os.path.basename(executable)))

    # The manifest's executable path is written here rather than kept in the
    # source, because it is the one field that differs per package. Editing it by
    # hand per build is how a Windows package ends up claiming a Mac binary.
    manifest_path = os.path.join(STAGE, "manifest.json")
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest["entrypoints"]["backend"]["executable"] = f"bin/{target}/{os.path.basename(executable)}"
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def relative_paths():
    """Every staged file except checksums.json / signature.json, with '/' separators."""
    paths = []
    for directory, _dirs, files in os.walk(STAGE):
        for name in files:
            absolute = os.path.join(directory, name)
            relative = os.path.relpath(absolute, STAGE).replace(os.sep, "/")
            if relative in ("checksums.json", "signature.json"):
                continue
            paths.append(relative)
    return sorted(paths)


def write_checksums():
    checksums = {}
    for relative in relative_paths():
        with open(os.path.join(STAGE, relative), "rb") as handle:
            checksums[relative] = hashlib.sha256(handle.read()).hexdigest()
    payload = {"algorithm": "sha256", "files": checksums}
    with open(os.path.join(STAGE, "checksums.json"), "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return checksums


def package(version, target):
    os.makedirs(DIST, exist_ok=True)
    output = os.path.join(DIST, f"dbx.demo.dbdiff-{version}-{target}.dbxp")
    entries = relative_paths() + ["checksums.json"]
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for relative in entries:
            archive.write(os.path.join(STAGE, relative), relative)
    return output


def main():
    parser = argparse.ArgumentParser(description="Build the DB Diff plugin into a .dbxp")
    parser.add_argument("-t", "--target", default=None, help="DBX target name (default: this machine's)")
    parser.add_argument("--list-targets", action="store_true", help="show the known targets and exit")
    options = parser.parse_args()

    if options.list_targets:
        for name, triple in TRIPLES.items():
            marker = "   <- this machine" if name == host_target() else ""
            print(f"  {name:<16} {triple}{marker}")
        return

    target = options.target or host_target()
    if target not in TRIPLES:
        sys.exit(f"unknown target {target!r}. Known: {', '.join(TRIPLES)}")

    with open(os.path.join(ROOT, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)

    executable = build_sidecar(target)
    stage_files(executable, target)
    checksums = write_checksums()
    output = package(manifest["version"], target)
    print(f"\n{output}")
    print(f"{len(checksums)} files hashed, {os.path.getsize(output)} bytes")
    if target != host_target():
        print(f"\nNOTE: built for {target}, not this machine ({host_target()}). Untested here.")


if __name__ == "__main__":
    main()
