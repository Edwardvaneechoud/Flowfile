#!/usr/bin/env python3
"""Fail if flowfile_core's kernel image pins drift from the kernel_runtime version (CI guard).

kernel_runtime/pyproject.toml carries the kernel image version that CI publishes;
flowfile_core/kernel/manager.py hardcodes the exact registry tags core pulls at
runtime. The two must move together — see docs/for-developers/kernel-architecture.md,
"Shipping a kernel change".
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KERNEL_PYPROJECT = ROOT / "kernel_runtime/pyproject.toml"
MANAGER = ROOT / "flowfile_core/flowfile_core/kernel/manager.py"


def _section_version(path: Path, section: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    pattern = r"^\[" + re.escape(section) + r"\][^\n]*\n(.*?)(?=^\[|\Z)"
    section_match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    if not section_match:
        return None
    version_match = re.search(r"""^\s*version\s*=\s*["']([^"']+)["']""", section_match.group(1), re.MULTILINE)
    return version_match.group(1) if version_match else None


def main() -> int:
    kernel_version = _section_version(KERNEL_PYPROJECT, "tool.poetry")
    if not kernel_version:
        print(f"Could not read the kernel version from {KERNEL_PYPROJECT}.", file=sys.stderr)
        return 1

    pins = re.findall(r'_KERNEL_IMAGE_(BASE|ML|LITE)_DEFAULT\s*=\s*"([^"]+)"', MANAGER.read_text(encoding="utf-8"))
    if len(pins) < 3:
        print(f"Expected 3 _KERNEL_IMAGE_*_DEFAULT pins in {MANAGER}, found {len(pins)}.", file=sys.stderr)
        return 1

    print(f"  kernel_runtime/pyproject.toml   {kernel_version}")
    drifted = False
    for flavour, ref in pins:
        tag = ref.rsplit(":", 1)[-1]
        print(f"  manager.py {flavour:<4} pin             {ref}")
        if tag != kernel_version:
            drifted = True

    if drifted:
        print(
            f"\nKernel image pin drift: manager.py pins do not match kernel_runtime/pyproject.toml "
            f"({kernel_version!r}). Bump the three _KERNEL_IMAGE_*_DEFAULT pins together with the "
            'kernel version (docs/for-developers/kernel-architecture.md, "Shipping a kernel change").',
            file=sys.stderr,
        )
        return 1
    print(f"\nKernel image pins in sync: {kernel_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
