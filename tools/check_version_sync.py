#!/usr/bin/env python3
"""Fail if the Flowfile version is out of sync across manifests (CI guard)."""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _section_version(path: Path, section: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    pattern = r"^\[" + re.escape(section) + r"\][^\n]*\n(.*?)(?=^\[|\Z)"
    section_match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    if not section_match:
        return None
    version_match = re.search(r"""^\s*version\s*=\s*["']([^"']+)["']""", section_match.group(1), re.MULTILINE)
    return version_match.group(1) if version_match else None


def _json_version(path: Path) -> str | None:
    match = re.search(r'"version"\s*:\s*"([^"]+)"', path.read_text(encoding="utf-8"))
    return match.group(1) if match else None


def _py_version(path: Path) -> str | None:
    match = re.search(r'__version__\s*=\s*"([^"]+)"', path.read_text(encoding="utf-8"))
    return match.group(1) if match else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Check the Flowfile version is in sync across manifests.")
    parser.add_argument("--expect", help="Also assert the canonical version equals this value (e.g. a release tag).")
    args = parser.parse_args()

    canonical = _section_version(ROOT / "pyproject.toml", "tool.poetry")
    versions = {
        "pyproject.toml": canonical,
        "shared/_version.py": _py_version(ROOT / "shared/_version.py"),
        "flowfile_frontend/package.json": _json_version(ROOT / "flowfile_frontend/package.json"),
        "flowfile_frontend/src-tauri/tauri.conf.json": _json_version(
            ROOT / "flowfile_frontend/src-tauri/tauri.conf.json"
        ),
        "flowfile_frontend/src-tauri/Cargo.toml": _section_version(
            ROOT / "flowfile_frontend/src-tauri/Cargo.toml", "package"
        ),
    }
    width = max(len(name) for name in versions)
    for name, ver in versions.items():
        print(f"  {name:<{width}}  {ver}")

    if not canonical or any(ver != canonical for ver in versions.values()):
        print(
            f"\nVersion drift detected (canonical {canonical!r}). Run: python tools/bump_version.py <version>",
            file=sys.stderr,
        )
        return 1
    if args.expect and args.expect != canonical:
        print(f"\nExpected version {args.expect!r} but manifests are at {canonical!r}.", file=sys.stderr)
        return 1
    suffix = f" (matches expected {args.expect})" if args.expect else ""
    print(f"\nAll versions in sync: {canonical}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
