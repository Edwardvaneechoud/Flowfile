"""On-disk store for catalog notebook content.

Cells live as one deterministic YAML file per notebook,
``<notebooks_directory>/<owner_id>/<notebook_uuid>.notebook.yaml``. The file is
the source of truth for cells (the DB row holds only metadata) and is the
artifact the project git-tracking will version. Multi-line ``source`` is dumped
as a literal block scalar so a one-line code edit is a one-line diff. The
filename is fully server-derived (owner id + validated uuid), so it carries no
user-controlled path component.
"""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from pathlib import Path

import yaml

from flowfile_core.fileExplorer.funcs import _is_contained
from flowfile_core.schemas.catalog_schema import NotebookCellModel
from shared.storage_config import storage

logger = logging.getLogger(__name__)


def _atomic_write(path: Path, content: str) -> None:
    """Write-then-rename so a crash never leaves a torn file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-", suffix=path.suffix or ".yaml")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

NOTEBOOK_FORMAT = 1
_SUFFIX = ".notebook.yaml"


class _NotebookDumper(yaml.SafeDumper):
    pass


def _represent_str(dumper: yaml.SafeDumper, data: str):
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


_NotebookDumper.add_representer(str, _represent_str)


def _dump(data: dict) -> str:
    return yaml.dump(
        data, Dumper=_NotebookDumper, default_flow_style=False, sort_keys=False, allow_unicode=True, width=4096
    )


def _notebook_path(owner_id: int, notebook_uuid: str) -> Path:
    uuid.UUID(str(notebook_uuid))  # rejects separators / .. / nulls by construction
    owner_dir = storage.notebooks_directory / str(int(owner_id))
    path = (owner_dir / f"{notebook_uuid}{_SUFFIX}").resolve()
    if not _is_contained(str(owner_dir), str(path)):
        raise ValueError(f"notebook path escapes owner directory: {path}")
    return path


def write_notebook_file(
    owner_id: int,
    notebook_uuid: str,
    *,
    name: str,
    description: str | None,
    namespace_name: str | None,
    default_kernel_id: str | None,
    cells: list[NotebookCellModel],
) -> None:
    """Atomically (over)write the notebook's content file. Deterministic: no
    timestamps, no outputs, fixed key order."""
    data = {
        "notebook_format": NOTEBOOK_FORMAT,
        "notebook_uuid": str(notebook_uuid),
        "name": name,
        "description": description,
        "default_kernel_id": default_kernel_id,
        "namespace": namespace_name,
        "cells": [
            {
                "id": c.id,
                "type": c.type,
                "metadata": dict(sorted(c.metadata.items())),
                "source": c.source,
            }
            for c in cells
        ],
    }
    _atomic_write(_notebook_path(owner_id, notebook_uuid), _dump(data))


def read_notebook_cells(owner_id: int, notebook_uuid: str) -> list[NotebookCellModel]:
    """Read cells, degrading gracefully: a missing / torn / malformed file
    yields an empty notebook (and a bad cell is skipped) rather than raising."""
    ctx = f"notebook {notebook_uuid}"
    try:
        path = _notebook_path(owner_id, notebook_uuid)
    except ValueError as exc:
        logger.warning("Invalid notebook path for %s: %s", ctx, exc)
        return []
    if not path.is_file():
        logger.warning("Notebook file missing for %s; returning empty cells", ctx)
        return []
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        logger.warning("Failed to read notebook file for %s: %s", ctx, exc)
        return []
    cells_raw = raw.get("cells") if isinstance(raw, dict) else None
    if not isinstance(cells_raw, list):
        logger.warning("Notebook file for %s has no cells list; dropping all cells", ctx)
        return []
    cells: list[NotebookCellModel] = []
    for index, item in enumerate(cells_raw):
        if not isinstance(item, dict):
            logger.warning("Skipping non-object cell at index %d in %s", index, ctx)
            continue
        try:
            cells.append(NotebookCellModel.model_validate(item))
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping invalid cell at index %d in %s: %s", index, ctx, exc)
            continue
    return cells


def delete_notebook_file(owner_id: int, notebook_uuid: str) -> None:
    """Remove the notebook's content file; idempotent (a missing file is fine)."""
    try:
        path = _notebook_path(owner_id, notebook_uuid)
    except ValueError:
        return
    path.unlink(missing_ok=True)
