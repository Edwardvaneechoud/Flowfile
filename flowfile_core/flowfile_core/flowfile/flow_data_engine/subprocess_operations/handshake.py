"""Check each compute backend before core sends it work, and say what it offers.

``verify`` checks any backend through its own interface: ``capabilities()`` compared with this process's
``local_capabilities()``, then each probe plan run on it as an ordinary job. The ``Verdict`` is cached per backend
instance and kept as the last of its id (``last_verdict``). Placement (``verdict_for``) and the gate in front of every
worker call (``ensure_handshake``) read it, so a backend is checked on its first use, after a call could not reach it
(``reset_handshake``), and on each use while its verdict refuses it. Concurrent calls share one check.

These fields refuse a backend whole, and the gate raises ``VersionMismatch`` before any work is sent:

- ``polars_version`` and ``polars_runtime``: plan bytes are not stable across Polars versions, and the index width makes
  ``pl.len()`` and row indexes ``UInt32`` on one side and ``UInt64`` on the other.
- ``protocol_version``: the worker contract, so two releases that speak one contract work together.

Every other difference only narrows ``offered``, so placement refuses just the nodes that need the missing piece:

- ``python_version`` is listed in ``differences``, but the ``python`` probe decides whether core's Python functions
  run there (``placement.PYTHON_FUNCTIONS``).
- ``plugins``: a ``PLUGIN_PACKAGES`` package core has is offered where the backend has it in core's version and, for
  an expression plugin, its probe loads it from the path core's plans carry.
- ``app_version``, ``platform`` and ``plugin_libs`` are reported, not compared.
"""

from __future__ import annotations

import copy
import threading
from typing import TYPE_CHECKING
from uuid import uuid4

import polars as pl
import requests
from polars_grouper import graph_solver
from pydantic import BaseModel, ValidationError

from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable, VersionMismatch
from shared._version import get_version
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities, local_capabilities

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.flowfile.execution.protocol import ComputeBackend

_COMPARED_FIELDS = ("protocol_version", "polars_version", "polars_runtime", "python_version")
_REFUSING_FIELDS = frozenset({"protocol_version", "polars_version", "polars_runtime"})
_NOT_INSTALLED = "not installed"

_lock = threading.Lock()
_verdicts: dict[int, tuple[ComputeBackend, Verdict]] = {}
_by_id: dict[str, ComputeBackend] = {}
_by_url: dict[str, WorkerBackend] = {}
_stand_ins: dict[str, WorkerBackend] = {}
_flights: dict[int, tuple[ComputeBackend, _Flight]] = {}


class Verdict(BaseModel):
    """What one check of a backend found.

    Attributes:
        id: The backend's id.
        ok: Its capabilities were read and no refusing field differs from core's.
        core: This process's capabilities.
        backend: The capabilities the backend reported; ``None`` when they could not be read.
        differences: Each compared field that differs, both sides named.
        offered: ``backend`` with ``plugins`` cut to what it offers; ``None`` unless ``ok``.
        absent: What it does not offer (a plugin name or ``placement.PYTHON_FUNCTIONS``) mapped to why, in words that
            follow "which".
        error: Why the backend is refused whole; ``None`` when it is not.
        error_kind: The refusing error's class name (``ComputeUnavailable``, ``VersionMismatch``, …), so the refusal
            can be told without its message, which may name the worker's address; ``None`` when not refused.
    """

    id: str
    ok: bool
    core: Capabilities
    backend: Capabilities | None = None
    differences: list[str] = []
    offered: Capabilities | None = None
    absent: dict[str, str] = {}
    error: str | None = None
    error_kind: str | None = None


class _Flight:
    """The one check of a backend that calls finding it unchecked share: the lock the check runs under, how many
    checks have ended, and the last one's outcome, its verdict or the ``ComputeError`` it raised."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.ended = 0
        self.outcome: Verdict | ComputeError | None = None


def fetch_worker_capabilities(worker_url: str) -> Capabilities:
    """``GET /capabilities`` from the worker at *worker_url*, signed with the internal token like every worker call.

    Raises:
        ComputeUnavailable: the worker cannot be reached, or its answer is no ``Capabilities`` and carries no
            ``app_version``, so the URL is no Flowfile worker.
        VersionMismatch: the worker answers but has no ``/capabilities`` route, so it predates core; or its answer
            carries an ``app_version`` but is no ``Capabilities``, so it is of another release.
    """
    # imported here: subprocess_operations imports this module
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
        _WORKER_TIMEOUT,
        _worker_session,
    )

    try:
        response = _worker_session.get("/capabilities", worker_url=worker_url, gated=False, timeout=_WORKER_TIMEOUT)
    except (requests.ConnectionError, requests.Timeout) as e:
        raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {e}") from e
    if response.status_code == 404:
        raise VersionMismatch(
            f"The worker at {worker_url} does not report its capabilities (GET /capabilities answered 404), "
            "so it is an older release than core"
        )
    response.raise_for_status()
    return capabilities_from_answer(worker_url, response)


def capabilities_from_answer(
    worker_url: str, response: requests.Response, *, backend_id: str | None = None
) -> Capabilities:
    """The ``Capabilities`` a 2xx answer to ``GET /capabilities`` from *worker_url* carries.

    An answer that does not validate is a worker of another release when it carries an ``app_version``, else a
    server that is no Flowfile worker. Either error is raised from pydantic's ``ValidationError``.

    Raises:
        VersionMismatch: the answer carries an ``app_version`` but is no ``Capabilities``. The message names both app
            versions, both contract numbers when they differ, and each field this release could not read.
        ComputeUnavailable: the answer carries no ``app_version``; its ``backend_id`` is *backend_id*.
    """
    try:
        answer = response.json()
    except ValueError:
        answer = None
    try:
        return Capabilities.model_validate(answer)
    except ValidationError as e:
        app_version = answer.get("app_version") if isinstance(answer, dict) else None
        if isinstance(app_version, str):
            unread = "; ".join(f"{'.'.join(map(str, error['loc']))}: {error['msg']}" for error in e.errors())
            protocol_version = answer.get("protocol_version")
            contract = (
                f"protocol_version: core {PROTOCOL_VERSION}, worker {protocol_version}; "
                if isinstance(protocol_version, int) and protocol_version != PROTOCOL_VERSION
                else ""
            )
            raise VersionMismatch(
                f"The worker at {worker_url} does not match core ({contract}app_version: core {get_version()}, worker "
                f"{app_version}): its capabilities answer is not one this release reads ({unread})"
            ) from e
        raise ComputeUnavailable(
            f"The server at {worker_url} is not a Flowfile worker: its capabilities answer carries no app_version",
            backend_id=backend_id,
        ) from e


def capability_differences(core: Capabilities, backend: Capabilities) -> list[str]:
    """Each compared field where *backend* differs from *core*, e.g. ``polars_version: core 1.43.2, backend 0.20.31``.

    The fields are the three that refuse a backend whole and ``python_version``, in that order; ``app_version`` is
    not compared.
    """
    differences = []
    for field in _COMPARED_FIELDS:
        ours, theirs = getattr(core, field), getattr(backend, field)
        if ours != theirs:
            differences.append(f"{field}: core {ours}, backend {theirs}")
    return differences


def _probe_plans() -> dict[str, pl.LazyFrame]:
    """One empty-frame plan per expression plugin, each calling one cheap expression of it, and one calling a trivial
    Python function, under ``placement.PYTHON_FUNCTIONS``."""
    import polars_ds  # imported here: core does not need it at startup

    from flowfile_core.flowfile.execution.placement import PYTHON_FUNCTIONS  # imported here: placement imports a lot

    return {
        "polars_grouper": pl.LazyFrame(schema={"a": pl.String, "b": pl.String}).select(
            graph_solver(pl.col("a"), pl.col("b"))
        ),
        "polars_ds": pl.LazyFrame(schema={"x": pl.Int32}).select(polars_ds.gcd(pl.col("x"), 1)),
        PYTHON_FUNCTIONS: pl.LazyFrame(schema={"x": pl.Int32}).select(
            pl.col("x").map_batches(lambda s: s, return_dtype=pl.Int32)
        ),
    }


def verify(backend: ComputeBackend) -> Verdict:
    """Check *backend* now and answer the verdict, which is kept as its last.

    A refused verdict is answered, not raised. While the probes run, the comparison's verdict stands for the backend,
    so the probe jobs' own calls pass the gate.

    Raises:
        ComputeError: The backend's capabilities could not be read (``ComputeUnavailable`` when it cannot be
            reached), or it could not be reached during a probe; the verdict kept refuses it with the error.
    """
    from flowfile_core.flowfile.execution.placement import (  # imported here: placement imports a lot
        PLUGIN_MISSING,
        PYTHON_FUNCTIONS,
    )

    core = local_capabilities()
    try:
        reported = backend.capabilities()
    except ComputeError as e:
        _record(backend, Verdict(id=backend.id, ok=False, core=core, error=str(e), error_kind=type(e).__name__))
        raise
    differences = capability_differences(core, reported)
    refusing = [difference for difference in differences if difference.split(":", 1)[0] in _REFUSING_FIELDS]
    if refusing:
        error = f'Backend "{backend.id}" does not match core ({"; ".join(refusing)})'
        verdict = Verdict(
            id=backend.id,
            ok=False,
            core=core,
            backend=reported,
            differences=differences,
            error=error,
            error_kind=VersionMismatch.__name__,
        )
        _record(backend, verdict)
        return verdict
    absent: dict[str, str] = {}
    for name, version in core.plugins.items():
        theirs = reported.plugins.get(name)
        if theirs is None:
            absent[name] = PLUGIN_MISSING
        elif theirs != version:
            absent[name] = f"version {theirs}, core has {version}"
    verdict = Verdict(
        id=backend.id,
        ok=True,
        core=core,
        backend=reported,
        differences=differences,
        offered=_offered(core, reported, absent),
        absent=absent,
    )
    _record(backend, verdict)
    absent = dict(absent)
    try:
        for name, plan in _probe_plans().items():
            if name in absent or (name != PYTHON_FUNCTIONS and name not in core.plugins):
                continue
            try:
                _run_probe(backend, name, plan)
            except ComputeUnavailable:
                raise
            except Exception as e:
                if name == PYTHON_FUNCTIONS:
                    absent[name] = (
                        f"core's Python functions do not run on this backend (Python: core {core.python_version}, "
                        f"backend {reported.python_version}): {e}"
                    )
                else:
                    absent[name] = (
                        f"fails to load (core "
                        f"{core.plugin_libs.get(name, _NOT_INSTALLED)}, backend "
                        f"{reported.plugin_libs.get(name, _NOT_INSTALLED)}): {e}"
                    )
    except ComputeUnavailable as e:
        refused = {"ok": False, "offered": None, "error": str(e), "error_kind": type(e).__name__}
        _record(backend, verdict.model_copy(update=refused))
        raise
    except BaseException:
        _drop(backend)
        raise
    verdict = verdict.model_copy(update={"absent": absent, "offered": _offered(core, reported, absent)})
    _record(backend, verdict)
    return verdict


def _offered(core: Capabilities, reported: Capabilities, absent: dict[str, str]) -> Capabilities:
    """*reported* with ``plugins`` cut to the packages it has in core's version and does not lack (*absent*)."""
    plugins = {name: version for name, version in reported.plugins.items() if core.plugins.get(name) == version}
    return reported.model_copy(update={"plugins": {name: v for name, v in plugins.items() if name not in absent}})


def _run_probe(backend: ComputeBackend, name: str, plan: pl.LazyFrame) -> None:
    """Run *plan* on *backend* as an ordinary job and read its first row; the result is released either way."""
    from flowfile_core.flowfile.execution.protocol import SubmitHints  # imported here: protocol imports the engine

    hints = SubmitHints(must_materialise=False, wide=False, fanout=1, preview_rows=1)
    result = backend.submit(plan, [], hints, f"handshake-{name}-{uuid4().hex}", flow_id=-1, node_id=-1)
    try:
        if result.sample is None:
            backend.sample(result.ref, 1)
    finally:
        backend.release(result.ref)


def verdict_for(backend: ComputeBackend) -> Verdict:
    """The verdict cached for *backend* when it passed, else a fresh one (``verify``).

    Calls that find no passing verdict at once share one check: the first runs it, and each call that waited for it
    answers its outcome, the verdict or a copy of the error it raised, instead of checking again.

    Raises:
        ComputeError: As ``verify``.
    """
    verdict = _cached(backend)
    if verdict is not None and verdict.ok:
        return verdict
    flight = _flight(backend)
    seen = flight.ended
    with flight.lock:
        if flight.ended != seen:
            outcome = flight.outcome
            if isinstance(outcome, ComputeError):
                raise copy.copy(outcome) from outcome
            return outcome
        try:
            outcome = verify(backend)
        except ComputeError as e:
            flight.outcome, flight.ended = e, flight.ended + 1
            raise
        flight.outcome, flight.ended = outcome, flight.ended + 1
        return outcome


def ensure_verdict(backend: ComputeBackend) -> Verdict:
    """``verdict_for`` *backend*, raising for a backend refused whole.

    Raises:
        VersionMismatch: The verdict refuses the backend; the message names every field that refuses it.
        ComputeError: As ``verify``.
    """
    verdict = verdict_for(backend)
    if not verdict.ok:
        raise VersionMismatch(verdict.error)
    return verdict


def cached_verdict(backend: ComputeBackend) -> Verdict | None:
    """The verdict cached for *backend* by its last check, without checking it; ``None`` when it has none."""
    return _cached(backend)


def last_verdict(backend_id: str) -> Verdict | None:
    """The verdict of the backend last checked under *backend_id* in this process; ``None`` when none was."""
    with _lock:
        backend = _by_id.get(backend_id)
    return None if backend is None else _cached(backend)


def owns(backend: WorkerBackend) -> None:
    """Make *backend* the one a URL-only gate call on its URL is checked by.

    Such a call signs with the process token, so a backend that does too keeps the URL from one with a token of its
    own. Otherwise the last one made wins.
    """
    with _lock:
        current = _by_url.get(backend.url)
        if current is None or backend.token is None or current.token is not None:
            _by_url[backend.url] = backend
            _stand_ins.pop(backend.url, None)


def ensure_handshake(worker_url: str, *, backend_id: str | None = None) -> None:
    """Refuse the worker at *worker_url* before a call is sent to it, by the verdict of the backend it is.

    That backend is the one last checked under *backend_id* when it runs on *worker_url*, else the URL's owner
    (``owns``), else a stand-in ``WorkerBackend`` that signs with the process token. A cached passing verdict, or the
    one that stands while its own check runs (``verify``), passes at once; otherwise the backend is checked now.

    Raises:
        ComputeUnavailable: the worker cannot be reached. It names *backend_id* when given, no backend for a
            stand-in, else the backend checked.
        VersionMismatch: the worker is refused whole; the message names every field that refuses it.
    """
    with _lock:
        backend = _by_id.get(backend_id) if backend_id is not None else None
        if getattr(backend, "url", None) != worker_url:
            backend = _by_url.get(worker_url)
        stand_in = backend is None
        if stand_in:
            backend = _stand_ins.get(worker_url)
    if backend is None:
        from flowfile_core.flowfile.execution.backends.worker import WorkerBackend  # imported here: it imports this

        made = WorkerBackend(worker_url)
        with _lock:
            if _by_url.get(worker_url) is made:
                del _by_url[worker_url]
            backend = _stand_ins.setdefault(worker_url, made)
    try:
        ensure_verdict(backend)
    except ComputeUnavailable as e:
        if backend_id is not None or stand_in:
            e.backend_id = backend_id
        raise


def reset_handshake(worker_url: str | None = None) -> None:
    """Make the next use of each backend on *worker_url* check it again; every backend's when it is ``None``."""
    with _lock:
        if worker_url is None:
            _verdicts.clear()
            return
        for key, (backend, _) in list(_verdicts.items()):
            if getattr(backend, "url", None) == worker_url:
                del _verdicts[key]


def _flight(backend: ComputeBackend) -> _Flight:
    """The flight *backend*'s checks share, made on its first check."""
    with _lock:
        entry = _flights.get(id(backend))
        if entry is None or entry[0] is not backend:
            entry = _flights[id(backend)] = (backend, _Flight())
        return entry[1]


def _cached(backend: ComputeBackend) -> Verdict | None:
    with _lock:
        entry = _verdicts.get(id(backend))
    return entry[1] if entry is not None and entry[0] is backend else None


def _record(backend: ComputeBackend, verdict: Verdict) -> None:
    with _lock:
        _verdicts[id(backend)] = (backend, verdict)
        if _stand_ins.get(getattr(backend, "url", None)) is not backend:
            _by_id[backend.id] = backend


def _drop(backend: ComputeBackend) -> None:
    with _lock:
        if _verdicts.get(id(backend), (None,))[0] is backend:
            del _verdicts[id(backend)]
