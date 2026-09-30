"""The notebook's clean runner in core: every push and plan interprets the cells and executes none of them.

:class:`NotebookRunner` is the production :class:`~flowfile_core.notebook.bridge.CleanRunner`. ``main.py``
installs it once, at import, through :func:`install_notebook_runner`, the function tests call too; no
setting or environment variable switches it. A clean run is one call on the calling thread, as the
requesting user, holding ``notebook.RUN_LOCK``: it enters a sync on the canvas snapshot, runs every cell
through a fresh executor and ends the mode. A request over a size bound (``allowlist.BOUNDS``) or naming a
malformed cell id is refused before any cell is parsed. ``flowfile_frame`` is imported on the first run.
"""

from __future__ import annotations

import copy
import re
import traceback
from collections.abc import Callable
from typing import Any, ClassVar

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_graph import EDIT_LOCK_TIMEOUT_SECONDS
from flowfile_core.notebook.allowlist import BOUNDS
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult, result_from_payload, set_clean_runner
from flowfile_core.notebook.interpret import CellInterpreter

_CELL_ID = re.compile(rf"[\w.:\-]{{1,{BOUNDS['cell_id_length']}}}")


def _refused(message: str, cell_id: str | None = None) -> CleanRunResult:
    return CleanRunResult(error=message, cell_id=cell_id, kind="refused")


def request_refusal(request: CleanRunRequest) -> CleanRunResult | None:
    """A ``refused`` result when ``request`` is over a size bound or names a malformed cell id, else ``None``.

    Cell ids end up in cell filenames and error details, so they are letters, digits and ``_ . : -``,
    at most ``cell_id_length`` long; a malformed id is not echoed back.
    """
    if len(request.cells) > BOUNDS["cells_per_request"]:
        return _refused(f"The notebook has more than {BOUNDS['cells_per_request']} cells")
    total = 0
    for cell_id, code in request.cells:
        if not _CELL_ID.fullmatch(cell_id):
            return _refused(f"A cell id is 1 to {BOUNDS['cell_id_length']} letters, digits or the characters _ . : -")
        try:
            size = len(code.encode("utf-8"))
        except UnicodeEncodeError:
            return _refused("The cell holds text that is not valid Unicode", cell_id)
        if size > BOUNDS["bytes_per_cell"]:
            return _refused(f"The cell is larger than {BOUNDS['bytes_per_cell']} bytes", cell_id)
        total += size
    if total > BOUNDS["bytes_per_request"]:
        return _refused(f"The notebook is larger than {BOUNDS['bytes_per_request']} bytes")
    return None


class NotebookRunner:
    """The production clean runner: interprets a push's cells as the requesting user, executing none of them.

    ``executor`` builds the cell executor for one run (``notebook_cells.CellExecutor``); here it is
    :class:`CellInterpreter`, built fresh per run because its bounds count per run. The runner holds no
    state and owns no threads.
    """

    executor: ClassVar[Callable[[], Any]] = CellInterpreter

    def clean_run(self, user_id: int, flow_id: int, request: CleanRunRequest) -> CleanRunResult:
        """Clean-run ``request``'s cells on a sync of its snapshot, as ``user_id``; a failure comes back in the result.

        ``user_id`` is required and must be a positive ``int``. The snapshot is copied first, so the
        caller's copy (the live payload a push reconciles against) is never touched. Everything happens
        in this call, on this thread, under ``notebook.RUN_LOCK``, and the mode always ends before the
        lock is released. Waiting longer than ``EDIT_LOCK_TIMEOUT_SECONDS`` for another sync's lock gives
        an ``error`` result, as does an exception outside the cells (logged).
        """
        if not isinstance(user_id, int) or isinstance(user_id, bool) or user_id < 1:
            raise TypeError(f"A clean run needs the requesting user's id, a positive int; got {user_id!r}")
        refusal = request_refusal(request)
        if refusal is not None:
            return refusal
        from flowfile_frame import notebook
        from flowfile_frame.notebook_cells import clean_run, enter_snapshot_session

        snapshot = copy.deepcopy(request.snapshot)
        cells = [tuple(cell) for cell in request.cells]
        provenance = {cell_id: [tuple(entry) for entry in entries] for cell_id, entries in request.provenance.items()}
        if not notebook.RUN_LOCK.acquire(timeout=EDIT_LOCK_TIMEOUT_SECONDS):
            return CleanRunResult(error="Another notebook sync is in progress; try again", kind="error")
        try:
            try:
                if snapshot.get("flowfile_data"):
                    enter_snapshot_session(snapshot, user_id=user_id)
                payload = clean_run(cells, request.ceiling, provenance, user_id=user_id, executor=self.executor())
                return result_from_payload(payload)
            except Exception as exc:
                logger.exception(f"notebook clean run of flow {flow_id} failed")
                return CleanRunResult(
                    error=f"{type(exc).__name__}: {exc}", kind="error", traceback=traceback.format_exc()
                )
            finally:
                notebook.exit()
        finally:
            notebook.RUN_LOCK.release()


def install_notebook_runner() -> None:
    """Install :class:`NotebookRunner` as the runner every push and plan goes through."""
    set_clean_runner(NotebookRunner())
