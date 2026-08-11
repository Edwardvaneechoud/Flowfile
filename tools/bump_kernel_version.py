#!/usr/bin/env python3
"""Bump the kernel runtime version in lockstep. Usage: python tools/bump_kernel_version.py X.Y.Z

Keeps the two source-of-truth streams in sync: the image/package version in
``kernel_runtime/pyproject.toml`` and the runtime ``__version__`` in
``kernel_runtime/kernel_runtime/__init__.py`` (reported by ``/health`` and shown
in the Kernel Manager). Independent from the app version (tools/bump_version.py);
the pinned launch tags in ``flowfile_core/kernel/images.py`` move only when a new
image is actually published.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "kernel_runtime" / "pyproject.toml"
INIT = ROOT / "kernel_runtime" / "kernel_runtime" / "__init__.py"

SEMVER = re.compile(r"^\d+\.\d+\.\d+([-+.][0-9A-Za-z.-]+)?$")


def _replace_poetry_version(text: str, new_version: str) -> str:
    pattern = re.compile(
        r"(^\[tool\.poetry\][^\n]*\n(?:(?!^\[).)*?^version\s*=\s*)([\"'])[^\"']+\2",
        re.MULTILINE | re.DOTALL,
    )
    new_text, n = pattern.subn(rf'\g<1>"{new_version}"', text, count=1)
    if n != 1:
        raise SystemExit("Could not find a version under [tool.poetry] in kernel_runtime/pyproject.toml")
    return new_text


def _replace_dunder_version(text: str, new_version: str) -> str:
    new_text, n = re.subn(r'(__version__\s*=\s*")[^"]+(")', rf"\g<1>{new_version}\g<2>", text, count=1)
    if n != 1:
        raise SystemExit("Could not find a __version__ assignment in kernel_runtime/kernel_runtime/__init__.py")
    return new_text


def main(argv: list[str]) -> int:
    if len(argv) != 1 or not SEMVER.match(argv[0]):
        print("Usage: python tools/bump_kernel_version.py X.Y.Z", file=sys.stderr)
        return 2
    new_version = argv[0]
    print(f"Bumping kernel runtime version -> {new_version}")

    PYPROJECT.write_text(_replace_poetry_version(PYPROJECT.read_text(encoding="utf-8"), new_version), encoding="utf-8")
    print(f"  updated {PYPROJECT.relative_to(ROOT)}")
    INIT.write_text(_replace_dunder_version(INIT.read_text(encoding="utf-8"), new_version), encoding="utf-8")
    print(f"  updated {INIT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
