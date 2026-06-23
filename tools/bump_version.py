#!/usr/bin/env python3
"""Bump the Flowfile app version across all of its manifests.

Usage: python tools/bump_version.py X.Y.Z

The root pyproject.toml ``[tool.poetry]`` version is the canonical source; this
keeps the frontend/Tauri/Cargo mirrors in lockstep (CI enforces no drift via
tools/check_version_sync.py). kernel_runtime and flowfile_wasm version
independently and are intentionally not touched here.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SEMVER = re.compile(r"^\d+\.\d+\.\d+([-+.][0-9A-Za-z.-]+)?$")


def _replace_in_section(text: str, section: str, new_version: str) -> str:
    pattern = re.compile(
        r"(^\[" + re.escape(section) + r"\][^\n]*\n.*?^version\s*=\s*)([\"'])[^\"']+\2",
        re.MULTILINE | re.DOTALL,
    )
    new_text, n = pattern.subn(rf'\g<1>"{new_version}"', text, count=1)
    if n != 1:
        raise SystemExit(f"Could not find a version under [{section}]")
    return new_text


def _replace_first_json_version(text: str, new_version: str) -> str:
    new_text, n = re.subn(r'("version"\s*:\s*")[^"]+(")', rf"\g<1>{new_version}\g<2>", text, count=1)
    if n != 1:
        raise SystemExit('Could not find a "version" key')
    return new_text


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    print(f"  updated {path.relative_to(ROOT)}")


def main(argv: list[str]) -> int:
    if len(argv) != 1 or not SEMVER.match(argv[0]):
        print("Usage: python tools/bump_version.py X.Y.Z", file=sys.stderr)
        return 2
    new_version = argv[0]
    print(f"Bumping Flowfile version -> {new_version}")

    pyproject = ROOT / "pyproject.toml"
    _write(pyproject, _replace_in_section(pyproject.read_text(encoding="utf-8"), "tool.poetry", new_version))

    cargo = ROOT / "flowfile_frontend" / "src-tauri" / "Cargo.toml"
    _write(cargo, _replace_in_section(cargo.read_text(encoding="utf-8"), "package", new_version))

    for rel in ("flowfile_frontend/package.json", "flowfile_frontend/src-tauri/tauri.conf.json"):
        path = ROOT / rel
        _write(path, _replace_first_json_version(path.read_text(encoding="utf-8"), new_version))

    print("Done. Refresh Cargo.lock (cargo update -p flowfile) and commit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
