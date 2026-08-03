#!/usr/bin/env python3
"""Compute the docker-publish build/merge matrices (CI helper for docker-publish.yml).

Writes app_version / kernel_version / build_matrix / merge_matrix / has_work to
$GITHUB_OUTPUT. App images are included when PUBLISH_APP is true (v* tag runs and
the publish_app dispatch input); kernel images only when Docker Hub is missing
flowfile-kernel-<flavour>:<kernel_version> — or FORCE_KERNEL is true — so
published kernel version tags stay immutable and a missed publish self-heals on
the next qualifying run.

The matrices deliberately carry bare image names: DOCKERHUB_ORG comes from a
repository secret, and GitHub silently drops job outputs containing a secret
value. The org is applied inside workflow steps instead.
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PLATFORMS = [
    ("linux/amd64", "ubuntu-latest", "amd64"),
    ("linux/arm64", "ubuntu-24.04-arm", "arm64"),
]
APP_IMAGES = [
    ("flowfile-core", "./flowfile_core/Dockerfile"),
    ("flowfile-frontend", "./flowfile_frontend/Dockerfile"),
    ("flowfile-worker", "./flowfile_worker/Dockerfile"),
]
KERNEL_IMAGES = [
    ("flowfile-kernel-base", "", "false"),
    ("flowfile-kernel-ml", "ml", "false"),
    ("flowfile-kernel-lite", "", "true"),
]


def _section_version(path: Path, section: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    pattern = r"^\[" + re.escape(section) + r"\][^\n]*\n(.*?)(?=^\[|\Z)"
    section_match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    if not section_match:
        return None
    version_match = re.search(r"""^\s*version\s*=\s*["']([^"']+)["']""", section_match.group(1), re.MULTILINE)
    return version_match.group(1) if version_match else None


def _tags_for(version: str) -> list[str]:
    return [version] if "-" in version else [version, "latest"]


def _tag_exists(org: str, image: str, tag: str) -> bool:
    """True if the tag exists on Docker Hub, False on 404, hard-fail otherwise.

    Never guesses: an unexpected status or an unreachable Hub aborts the run
    instead of risking a republish over an existing tag.
    """
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
    raise SystemExit(f"Could not determine whether {image}:{tag} exists on Docker Hub ({last_error}); aborting.")


def main() -> int:
    org = os.environ["DOCKERHUB_ORG"].strip()
    publish_app = os.environ.get("PUBLISH_APP", "").strip().lower() == "true"
    force_kernel = os.environ.get("FORCE_KERNEL", "").strip().lower() == "true"

    app_version = _section_version(ROOT / "pyproject.toml", "tool.poetry")
    kernel_version = _section_version(ROOT / "kernel_runtime/pyproject.toml", "tool.poetry")
    if not app_version or not kernel_version:
        print("Could not read versions from the pyproject manifests.", file=sys.stderr)
        return 1

    build_cells: list[dict[str, str]] = []
    merge_cells: list[dict[str, str]] = []

    def add_image(image: str, dockerfile: str, context: str, extras: str, slim: str, version: str) -> None:
        for platform, runner, arch in PLATFORMS:
            build_cells.append(
                {
                    "image": image,
                    "dockerfile": dockerfile,
                    "context": context,
                    "extras": extras,
                    "slim_constraints": slim,
                    "version": version,
                    "platform": platform,
                    "runner": runner,
                    "arch": arch,
                }
            )
        merge_cells.append({"image": image, "version": version, "tags": " ".join(_tags_for(version))})

    if publish_app:
        for image, dockerfile in APP_IMAGES:
            add_image(image, dockerfile, ".", "", "false", app_version)

    for image, extras, slim in KERNEL_IMAGES:
        if force_kernel or not _tag_exists(org, image, kernel_version):
            add_image(image, "./kernel_runtime/Dockerfile", "./kernel_runtime", extras, slim, kernel_version)
        else:
            print(f"::notice::{image}:{kernel_version} already exists on Docker Hub — skipped.")

    for cell in merge_cells:
        print(f"Will publish {cell['image']} tags: {cell['tags']}")
    if not build_cells:
        message = (
            f"Nothing to publish. Kernel {kernel_version} is already on Docker Hub (bump "
            "kernel_runtime/pyproject.toml to release a kernel change); app images publish on "
            "v* tag pushes (or a dispatch with publish_app)."
        )
        print(message)
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as fh:
                fh.write(f"## Docker publish\n\n{message}\n")

    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
        fh.write(f"app_version={app_version}\n")
        fh.write(f"kernel_version={kernel_version}\n")
        fh.write(f"build_matrix={json.dumps({'include': build_cells})}\n")
        fh.write(f"merge_matrix={json.dumps({'include': merge_cells})}\n")
        fh.write(f"has_work={'true' if build_cells else 'false'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
