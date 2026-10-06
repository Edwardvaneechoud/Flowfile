#!/usr/bin/env python3
"""Fail a kernel image change that would ship under an already-published kernel version (CI guard).

docker-publish.yml builds flowfile-kernel-<flavour>:<version> only when that tag is
missing from Docker Hub (tools/docker_publish_matrix.py), so a change to the kernel
image inputs merged without a kernel version bump silently never ships. This compares
--base with HEAD: when an image input changed and the kernel version did not, that
version must not be on Docker Hub yet. See docs/for-developers/kernel-architecture.md,
"Shipping a kernel change".
"""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ORG = "edwardvaneechoud"
DEFAULT_IMAGE = "flowfile-kernel-base"
KERNEL_PYPROJECT = "kernel_runtime/pyproject.toml"
IMAGE_INPUTS = [
    "kernel_runtime/pyproject.toml",
    "kernel_runtime/poetry.lock",
    "kernel_runtime/Dockerfile",
    "kernel_runtime/entrypoint.sh",
    "kernel_runtime/kernel_runtime/",
]


def _section_version(text: str, section: str) -> str | None:
    pattern = r"^\[" + re.escape(section) + r"\][^\n]*\n(.*?)(?=^\[|\Z)"
    section_match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    if not section_match:
        return None
    version_match = re.search(r"""^\s*version\s*=\s*["']([^"']+)["']""", section_match.group(1), re.MULTILINE)
    return version_match.group(1) if version_match else None


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"Kernel release guard: git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def _changed_inputs(repo: Path, base: str) -> list[str]:
    output = _git(repo, "diff", "--name-only", base, "HEAD", "--", *IMAGE_INPUTS)
    return [line for line in output.splitlines() if line.strip()]


def _kernel_version_at(repo: Path, rev: str) -> str | None:
    return _section_version(_git(repo, "show", f"{rev}:{KERNEL_PYPROJECT}"), "tool.poetry")


def _tag_exists(org: str, image: str, tag: str) -> bool:
    """True if the tag exists on Docker Hub, False on 404, hard-fail otherwise."""
    url = f"https://hub.docker.com/v2/repositories/{org}/{image}/tags/{tag}"
    last_error: Exception | None = None
    for attempt in range(3):
        if attempt:
            time.sleep(2**attempt)
        try:
            with urllib.request.urlopen(url, timeout=15):
                return True
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            last_error = exc
        except urllib.error.URLError as exc:
            last_error = exc
    raise SystemExit(
        f"Kernel release guard: could not verify whether {org}/{image}:{tag} exists on Docker Hub "
        f"({last_error}); re-run the job."
    )


def _next_patch(version: str) -> str:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", version)
    return f"{match[1]}.{match[2]}.{int(match[3]) + 1}" if match else "X.Y.Z"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", required=True, help="git ref or sha to compare HEAD with")
    parser.add_argument("--org", default=os.environ.get("DOCKERHUB_ORG", "").strip() or DEFAULT_ORG)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--repo", type=Path, default=ROOT, help="repository to inspect (default: this checkout)")
    args = parser.parse_args(argv)

    base = args.base.strip()
    if base and set(base) == {"0"}:
        print(f"Kernel release guard: base {base} is the all-zeros sha (a branch's first push); nothing to compare.")
        return 0

    changed = _changed_inputs(args.repo, base)
    if not changed:
        print("Kernel release guard: no kernel image inputs changed")
        return 0

    old_version = _kernel_version_at(args.repo, base)
    new_version = _kernel_version_at(args.repo, "HEAD")
    if not new_version:
        print(f"Kernel release guard: could not read the kernel version from {KERNEL_PYPROJECT}.", file=sys.stderr)
        return 1
    if old_version != new_version:
        print(f"Kernel release guard: kernel version bumped {old_version} -> {new_version}")
        return 0

    if not _tag_exists(args.org, args.image, new_version):
        print(f"Kernel release guard: {new_version} is not published yet; it publishes on merge")
        return 0

    changed_list = "\n".join(f"  {path}" for path in changed)
    print(
        f"Kernel release guard: kernel image inputs changed, but kernel version {new_version} is already "
        f"published ({args.org}/{args.image}:{new_version}). docker-publish skips existing tags, so this "
        f"change would never ship.\nChanged since {base}:\n{changed_list}\n"
        f'Fix: run "make bump-version-kernel VERSION={_next_patch(new_version)}" and bump the three '
        "_KERNEL_IMAGE_*_DEFAULT pins in flowfile_core/flowfile_core/kernel/images.py to match "
        '(docs/for-developers/kernel-architecture.md, "Shipping a kernel change").',
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
