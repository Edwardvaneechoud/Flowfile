"""Disk persistence for standalone notebooks (Jupyter ``.ipynb`` format).

Notebooks are flow-independent and stored per-user under
``storage.notebooks_directory / <user_id> / <notebook_id>.ipynb``. Using the
``.ipynb`` schema keeps them interoperable with Jupyter / VS Code while a
small ``metadata.flowfile`` block carries the Flowfile-specific fields.
"""

from __future__ import annotations

import json
import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path

from shared.storage_config import storage

from flowfile_core.notebook.models import Notebook, NotebookCell

# Synthetic notebook flow ids live in a high range so they never collide with
# real (small, autoincrement) FlowRegistration ids or kernel scratch flows.
_FLOW_ID_BASE = 2_000_000_000
_FLOW_ID_SPAN = 1_000_000_000


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_flow_id() -> int:
    return _FLOW_ID_BASE + secrets.randbelow(_FLOW_ID_SPAN)


def is_valid_id(notebook_id: str) -> bool:
    """Guard against path traversal — ids are server-generated UUID4 strings."""
    try:
        uuid.UUID(notebook_id)
    except (ValueError, AttributeError, TypeError):
        return False
    return True


def _user_dir(user_id: int) -> Path:
    directory = storage.notebooks_directory / str(user_id)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _path(user_id: int, notebook_id: str) -> Path:
    return _user_dir(user_id) / f"{notebook_id}.ipynb"


def _to_ipynb(nb: Notebook) -> dict:
    return {
        "cells": [
            {
                "cell_type": cell.cell_type,
                "metadata": {"id": cell.id},
                "source": cell.source.splitlines(keepends=True),
                "outputs": [],
                "execution_count": None,
            }
            for cell in nb.cells
        ],
        "metadata": {
            "flowfile": {
                "id": nb.id,
                "name": nb.name,
                "flow_id": nb.flow_id,
                "kernel_id": nb.kernel_id,
                "created_at": nb.created_at,
                "modified_at": nb.modified_at,
            },
            "kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _from_ipynb(data: dict) -> Notebook:
    meta = (data.get("metadata") or {}).get("flowfile") or {}
    cells: list[NotebookCell] = []
    for raw in data.get("cells") or []:
        source = raw.get("source", "")
        if isinstance(source, list):
            source = "".join(source)
        cell_id = (raw.get("metadata") or {}).get("id") or str(uuid.uuid4())
        cells.append(NotebookCell(id=cell_id, source=source, cell_type=raw.get("cell_type", "code")))
    return Notebook(
        id=meta["id"],
        name=meta.get("name", "Untitled notebook"),
        flow_id=meta.get("flow_id") or _new_flow_id(),
        kernel_id=meta.get("kernel_id"),
        created_at=meta.get("created_at") or _now(),
        modified_at=meta.get("modified_at") or _now(),
        cells=cells,
    )


def list_notebooks(user_id: int) -> list[Notebook]:
    directory = _user_dir(user_id)
    notebooks: list[Notebook] = []
    for path in directory.glob("*.ipynb"):
        try:
            notebooks.append(_from_ipynb(json.loads(path.read_text())))
        except (OSError, ValueError, KeyError):
            continue
    notebooks.sort(key=lambda n: n.modified_at, reverse=True)
    return notebooks


def get_notebook(user_id: int, notebook_id: str) -> Notebook | None:
    path = _path(user_id, notebook_id)
    if not path.exists():
        return None
    try:
        return _from_ipynb(json.loads(path.read_text()))
    except (OSError, ValueError, KeyError):
        return None


def create_notebook(user_id: int, name: str, kernel_id: str | None) -> Notebook:
    notebook = Notebook(
        id=str(uuid.uuid4()),
        name=name or "Untitled notebook",
        flow_id=_new_flow_id(),
        kernel_id=kernel_id,
        created_at=_now(),
        modified_at=_now(),
        cells=[NotebookCell(id=str(uuid.uuid4()), source="")],
    )
    _write(user_id, notebook)
    return notebook


def save_notebook(
    user_id: int,
    notebook: Notebook,
    *,
    name: str | None = None,
    kernel_id: str | None = None,
    cells: list[NotebookCell] | None = None,
) -> Notebook:
    if name is not None:
        notebook.name = name
    if kernel_id is not None:
        notebook.kernel_id = kernel_id
    if cells is not None:
        notebook.cells = cells
    notebook.modified_at = _now()
    _write(user_id, notebook)
    return notebook


def delete_notebook(user_id: int, notebook_id: str) -> bool:
    path = _path(user_id, notebook_id)
    if not path.exists():
        return False
    path.unlink()
    return True


def _write(user_id: int, notebook: Notebook) -> None:
    path = _path(user_id, notebook.id)
    path.write_text(json.dumps(_to_ipynb(notebook), indent=1))
