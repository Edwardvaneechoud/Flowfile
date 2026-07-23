"""Remove local Flowfile kernels: Docker containers, per-kernel derived images,
and (when Core is stopped) their catalog-DB records. With ``--images`` it also
removes the kernel flavour images (base/ml/lite, local builds + pulled tags) so
the Kernel Manager's image list is cleared.

Run via ``make clean_kernels`` (instances) or ``make clean_kernel_images``
(instances + flavour images). Core re-pulls/rebuilds images on demand.
"""

from __future__ import annotations

import argparse
import contextlib
import socket
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

import docker
from docker.errors import DockerException

CONTAINER_NAME_MARKER = "flowfile-kernel-"
# Every kernel image repo contains "flowfile-kernel" (base/ml/lite/derived, with
# or without a registry prefix); the frontend/worker/core images never do.
FLAVOUR_IMAGE_MARKER = "flowfile-kernel"
DERIVED_IMAGE_MARKER = "flowfile-kernel-derived"
CORE_PORT = 63578


@dataclass
class DockerCleanup:
    containers: list[str] = field(default_factory=list)
    images: list[str] = field(default_factory=list)
    images_failed: list[str] = field(default_factory=list)


@dataclass
class RecordCleanup:
    cleared: int = 0
    skipped: str | None = None


def clean_docker(client: docker.DockerClient, include_flavours: bool) -> DockerCleanup:
    result = DockerCleanup()
    for container in client.containers.list(all=True, filters={"name": CONTAINER_NAME_MARKER}):
        container.remove(force=True)
        result.containers.append(container.name)

    marker = FLAVOUR_IMAGE_MARKER if include_flavours else DERIVED_IMAGE_MARKER
    refs = sorted({tag for image in client.images.list() for tag in image.tags if marker in tag.rsplit(":", 1)[0]})
    for ref in refs:
        try:
            # Remove by reference, not image id: a tag sharing an id with a
            # flavour/base image is then only untagged, never deleted.
            client.images.remove(ref, force=True)
            result.images.append(ref)
        except DockerException:
            result.images_failed.append(ref)
    return result


def clean_records() -> RecordCleanup:
    path = _catalog_db_path()
    if path is None or not path.exists():
        return RecordCleanup(skipped="catalog DB not found")
    # kernels is a leaf table (no inbound FKs), so clearing it is self-contained.
    with contextlib.closing(sqlite3.connect(path)) as conn:
        try:
            (count,) = conn.execute("SELECT count(*) FROM kernels").fetchone()
        except sqlite3.OperationalError:
            return RecordCleanup(skipped="no kernels table")
        if count == 0:
            return RecordCleanup()
        if _core_is_running():
            # Core serves the kernel list from memory, so a DB delete wouldn't
            # take effect live and would diverge from its state until restart.
            return RecordCleanup(skipped=f"{count} kept — core is live on :{CORE_PORT} (remove in-app, or stop core)")
        cleared = conn.execute("DELETE FROM kernels").rowcount
        conn.commit()
        return RecordCleanup(cleared=cleared)


def _core_is_running() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", CORE_PORT)) == 0


def _catalog_db_path() -> Path | None:
    try:
        from sqlalchemy.engine import make_url

        from shared.storage_config import get_database_url
    except ImportError:
        return None
    url = make_url(get_database_url())
    if url.get_backend_name() != "sqlite" or not url.database:
        return None
    return Path(url.database)


def render(docker_result: DockerCleanup | None, records: RecordCleanup, include_flavours: bool) -> str:
    lines = ["Cleaning local Flowfile kernels..."]
    if docker_result is None:
        lines.append("  docker: unreachable — containers/images skipped")
    else:
        lines.append(f"  containers removed: {len(docker_result.containers)}")
        scope = "flavour + derived" if include_flavours else "derived"
        lines.append(f"  images removed ({scope}): {len(docker_result.images)}")
        lines += [f"    - {ref}" for ref in docker_result.images]
        lines += [f"    ! {ref} (could not remove)" for ref in docker_result.images_failed]
    if records.cleared:
        lines.append(f"  catalog records cleared: {records.cleared}")
    else:
        lines.append(f"  catalog records: {records.skipped or 'none'}")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Remove local Flowfile kernels (and optionally their images).")
    parser.add_argument(
        "--images",
        action="store_true",
        help="Also remove kernel flavour images (base/ml/lite, local + pulled).",
    )
    args = parser.parse_args(argv)

    try:
        client = docker.from_env()
        client.ping()
    except DockerException:
        client = None

    docker_result = clean_docker(client, args.images) if client is not None else None
    print(render(docker_result, clean_records(), args.images))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
