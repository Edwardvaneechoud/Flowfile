"""Persistence for custom-node mount directories.

Mounts are extra directories scanned for custom-node .py files alongside the
default ``storage.user_defined_nodes_directory``. They are install-wide and
stored in a single ``mounts.json`` next to the default nodes directory — no DB
involvement. In docker mode the paths are container paths; the API accepts
them as-is.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from shared import storage

logger = logging.getLogger(__name__)

MOUNTS_FILE_NAME = "mounts.json"


class MountValidationError(ValueError):
    """A mount path failed validation (not absolute, missing, nested, ...)."""


@dataclass
class NodeMount:
    path: str
    added_at: str


def mounts_file_path(base_dir: Path | None = None) -> Path:
    """The mounts file lives next to the default nodes dir so tests with a custom dir stay isolated."""
    return (base_dir if base_dir is not None else storage.user_defined_nodes_directory) / MOUNTS_FILE_NAME


def load_mounts(base_dir: Path | None = None) -> list[NodeMount]:
    """Read the persisted mounts; missing or corrupt files degrade to an empty list."""
    file_path = mounts_file_path(base_dir)
    try:
        raw = file_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as e:
        logger.warning("Cannot read custom-node mounts file %s: %s", file_path, e)
        return []
    try:
        data = json.loads(raw)
    except ValueError as e:
        logger.warning("Custom-node mounts file %s is not valid JSON: %s", file_path, e)
        return []
    if not isinstance(data, list):
        logger.warning("Custom-node mounts file %s has an unexpected shape; ignoring", file_path)
        return []
    mounts: list[NodeMount] = []
    for item in data:
        if isinstance(item, dict) and isinstance(item.get("path"), str):
            mounts.append(NodeMount(path=item["path"], added_at=str(item.get("added_at", ""))))
    return mounts


def save_mounts(mounts: list[NodeMount], base_dir: Path | None = None) -> None:
    file_path = mounts_file_path(base_dir)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps([asdict(m) for m in mounts], indent=2), encoding="utf-8")


def _normalize(path: Path) -> Path:
    return path.expanduser().resolve()


def add_mount(path_str: str, base_dir: Path | None = None) -> NodeMount:
    """Validate and persist a new mount directory.

    Raises MountValidationError when the path is not an absolute existing
    directory, is the default nodes directory, or nests with the default
    directory or an existing mount (either direction).
    """
    candidate = Path(path_str).expanduser()
    if not candidate.is_absolute():
        raise MountValidationError("Mount path must be absolute")
    if not candidate.exists():
        raise MountValidationError(f"Directory does not exist: {candidate}")
    if not candidate.is_dir():
        raise MountValidationError(f"Not a directory: {candidate}")

    resolved = _normalize(candidate)
    default_dir = _normalize(base_dir if base_dir is not None else storage.user_defined_nodes_directory)
    if resolved == default_dir:
        raise MountValidationError("This is already the default custom nodes directory")
    if resolved.is_relative_to(default_dir) or default_dir.is_relative_to(resolved):
        raise MountValidationError("Mount path may not nest with the default custom nodes directory")

    mounts = load_mounts(base_dir)
    for existing in mounts:
        existing_resolved = _normalize(Path(existing.path))
        if resolved == existing_resolved:
            raise MountValidationError(f"Already mounted: {existing.path}")
        if resolved.is_relative_to(existing_resolved) or existing_resolved.is_relative_to(resolved):
            raise MountValidationError(f"Mount path may not nest with existing mount: {existing.path}")

    mount = NodeMount(path=str(candidate), added_at=datetime.now(timezone.utc).isoformat())
    mounts.append(mount)
    save_mounts(mounts, base_dir)
    logger.info("Added custom-node mount %s", mount.path)
    return mount


def remove_mount(path_str: str, base_dir: Path | None = None) -> NodeMount | None:
    """Unregister a mount (never deletes files). Returns the removed mount, or None if not found."""
    target = _normalize(Path(path_str))
    mounts = load_mounts(base_dir)
    kept: list[NodeMount] = []
    removed: NodeMount | None = None
    for mount in mounts:
        if removed is None and _normalize(Path(mount.path)) == target:
            removed = mount
        else:
            kept.append(mount)
    if removed is not None:
        save_mounts(kept, base_dir)
        logger.info("Removed custom-node mount %s", removed.path)
    return removed
