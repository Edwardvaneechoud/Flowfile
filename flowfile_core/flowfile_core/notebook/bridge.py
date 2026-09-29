"""The seam between a notebook push (core) and whatever runs the cells: the clean-run request, result and runner.

A push sends every cell to a runner that builds them on a fresh session graph under notebook build mode and
returns the save-format payload relabelled onto the canvas ids. No runner is installed in production yet, so
push and plan answer 503 (``NO_RUNNER_DETAIL``); tests install one with :func:`set_clean_runner`.
"""

from __future__ import annotations

from typing import Any, Protocol

from fastapi import HTTPException, status
from pydantic import BaseModel, Field

NO_RUNNER_DETAIL = "no notebook session runner"


class CleanRunRequest(BaseModel):
    """What a push sends to the runner.

    ``cells`` are ``(cell_id, code)`` in notebook order; ``provenance`` maps a cell to the
    ``(node_type, canvas_id)`` pairs it rendered; ``ceiling`` is the id above which new nodes are
    numbered; ``snapshot`` is the seed payload ``{flowfile_data, parameters, names, schemas}``
    that ``fl.canvas_node`` adopts from.
    """

    cells: list[tuple[str, str]]
    provenance: dict[str, list[tuple[str, int]]] = Field(default_factory=dict)
    ceiling: int = 0
    snapshot: dict[str, Any] = Field(default_factory=dict)


class CleanRunResult(BaseModel):
    """The relabelled clean-run payload, or ``error`` when a cell failed (the other fields are then empty)."""

    flowfile_data: dict[str, Any] = Field(default_factory=dict)
    node_ids_by_cell: dict[str, list[int]] = Field(default_factory=dict)
    names: dict[int, str] = Field(default_factory=dict)
    refusals: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None


class CleanRunner(Protocol):
    def clean_run(self, user_id: int, flow_id: int, request: CleanRunRequest) -> CleanRunResult: ...


_runner: CleanRunner | None = None


def set_clean_runner(runner: CleanRunner | None) -> None:
    """Install (or, with ``None``, remove) the runner every push goes through."""
    global _runner
    _runner = runner


def get_clean_runner() -> CleanRunner:
    """The installed runner; 503 when none is."""
    if _runner is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=NO_RUNNER_DETAIL)
    return _runner


def result_from_payload(payload: dict[str, Any]) -> CleanRunResult:
    """A ``flowfile_frame.notebook_cells.clean_run`` return value as a :class:`CleanRunResult`."""
    refusals = list(payload.get("refusals") or [])
    if not payload.get("ok"):
        cell_id = payload.get("cell_id")
        error = payload.get("error") or "the clean run failed"
        return CleanRunResult(refusals=refusals, error=f"Cell {cell_id} failed:\n{error}" if cell_id else error)
    return CleanRunResult(
        flowfile_data=payload["flowfile_data"],
        node_ids_by_cell=payload.get("cells") or {},
        names={int(k): v for k, v in (payload.get("names") or {}).items()},
        refusals=refusals,
    )
