from __future__ import annotations

from pydantic import BaseModel, Field


class NotebookCell(BaseModel):
    """A single code cell in a standalone notebook."""

    id: str
    source: str = ""
    cell_type: str = "code"


class NotebookSummary(BaseModel):
    """Lightweight notebook metadata for list views."""

    id: str
    name: str
    # Synthetic, notebook-scoped flow id used as the kernel namespace key so
    # each notebook keeps its own isolated set of variables/imports across
    # cells (see flowfile_core/notebook/store.py).
    flow_id: int
    kernel_id: str | None = None
    created_at: str
    modified_at: str


class Notebook(NotebookSummary):
    """A full notebook including its cells."""

    cells: list[NotebookCell] = Field(default_factory=list)


class NotebookCreate(BaseModel):
    name: str = "Untitled notebook"
    kernel_id: str | None = None


class NotebookUpdate(BaseModel):
    name: str | None = None
    kernel_id: str | None = None
    cells: list[NotebookCell] | None = None
