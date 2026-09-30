"""The seam between a notebook push (core) and whatever runs the cells: the clean-run request, result and runner.

A push sends every cell to the installed runner, which builds them on a fresh session graph under notebook
build mode and returns the save-format payload relabelled onto the canvas ids, or the failing cell with its
line and kind. Core's runner is :class:`~flowfile_core.notebook.runner.NotebookRunner`, which interprets the
cells and executes none of them; ``main.py`` installs it at import through
:func:`~flowfile_core.notebook.runner.install_notebook_runner`.
"""

from __future__ import annotations

from typing import Any, Literal, Protocol

from fastapi import HTTPException, status
from pydantic import BaseModel, Field

NO_RUNNER_DETAIL = "no notebook session runner"

FailureKind = Literal["needs_kernel", "refused", "error"]


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
    """The relabelled clean-run payload, or ``error`` when the run failed (the payload fields are then empty).

    On a failure ``error`` is the message to show, ``cell_id`` the failing cell (``None`` when no cell is
    to blame), ``line`` the 1-based line within it and ``kind`` ``"needs_kernel"`` (outside what core
    interprets), ``"refused"`` (a notebook refusal or a size bound) or ``"error"`` (the call ran and
    raised). ``traceback`` is the underlying exception's traceback, for logs and tests; it is never
    serialised.
    """

    flowfile_data: dict[str, Any] = Field(default_factory=dict)
    node_ids_by_cell: dict[str, list[int]] = Field(default_factory=dict)
    names: dict[int, str] = Field(default_factory=dict)
    refusals: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None
    cell_id: str | None = None
    line: int | None = None
    kind: FailureKind | None = None
    traceback: str | None = Field(default=None, exclude=True)


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
    """A ``flowfile_frame.notebook_cells.clean_run`` return value as a :class:`CleanRunResult`.

    A failure keeps the cell's message (``format_exception_only`` text for an exception), cell id, line,
    kind and traceback as separate fields; a success keeps the run's warnings (nodes placed unchecked).
    """
    refusals = list(payload.get("refusals") or [])
    if not payload.get("ok"):
        message = payload.get("message") or payload.get("error") or "the clean run failed"
        return CleanRunResult(
            refusals=refusals,
            error=message.rstrip(),
            cell_id=payload.get("cell_id"),
            line=payload.get("line"),
            kind=payload.get("kind") or "error",
            traceback=payload.get("traceback"),
        )
    return CleanRunResult(
        flowfile_data=payload["flowfile_data"],
        node_ids_by_cell=payload.get("cells") or {},
        names={int(k): v for k, v in (payload.get("names") or {}).items()},
        refusals=refusals,
        warnings=list(payload.get("warnings") or []),
    )
