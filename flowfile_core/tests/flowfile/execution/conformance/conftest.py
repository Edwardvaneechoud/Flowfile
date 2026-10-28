"""The backends every conformance test runs over: the worker embedded in this process and the session worker.

``backend`` is each backend as core configures it: the embedded worker under its own id, ``local``, whose jobs every
test runs in this process, as a library host does (the root conftest sets the worker's in-process mode for the
session), and the worker backend on the session worker. ``unreachable`` and ``on_other_versions`` are the same kind of
backend with its compute out of reach, or on other versions than core; a backend whose compute is this process can be
neither, so those tests skip for it. ``unreadable`` is ``backend`` as core on another host sees it: the leaf of a
stored result names a file core cannot open (``UnreadableLocation``). ``minted_elsewhere`` holds results every other
backend minted, for ``backend`` to refuse.
"""

import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef, SubmitHints
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from shared.compute_protocol import local_capabilities
from tests.conftest import closed_url


def _worker_url() -> str:
    return registry.backend(registry.WORKER_BACKEND_ID).url


def _session_worker() -> WorkerBackend:
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")
    return WorkerBackend(url=_worker_url())


def _worker_out_of_reach() -> WorkerBackend:
    return WorkerBackend(url=closed_url())


def _worker_on_other_polars(monkeypatch: pytest.MonkeyPatch) -> WorkerBackend:
    reported = local_capabilities().model_copy(update={"polars_version": "0.0.0"})
    monkeypatch.setattr(WorkerBackend, "capabilities", lambda self: reported)
    return WorkerBackend(url=_worker_url())


class UnreadableLocation:
    """A backend whose stored results sit where core cannot open them, as on a compute on another host.

    Every call is the wrapped backend's, except ``plan_for`` of a ``"materialised"`` reference: it runs the backend's
    own, so a foreign or released reference still raises, then answers ``scan_ipc`` of ``<root>/<result id>.arrow``,
    which never exists. Reading that leaf in core raises ``FileNotFoundError``; ``sample`` and ``count`` still answer.
    """

    def __init__(self, backend: ComputeBackend, root: Path) -> None:
        self._backend = backend
        self._root = root

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        leaf = self._backend.plan_for(ref)
        if ref.state != "materialised":
            return leaf
        return pl.scan_ipc(self._root / f"{ref.result_id}.arrow")


BACKENDS: dict[str, Callable[[], ComputeBackend]] = {
    "embedded": EmbeddedWorkerBackend,
    "worker": _session_worker,
}
OUT_OF_REACH: dict[str, Callable[[], ComputeBackend]] = {"worker": _worker_out_of_reach}
ON_OTHER_VERSIONS: dict[str, Callable[[pytest.MonkeyPatch], ComputeBackend]] = {"worker": _worker_on_other_polars}


@pytest.fixture(params=list(BACKENDS))
def backend_name(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def backend(backend_name: str) -> ComputeBackend:
    return BACKENDS[backend_name]()


@pytest.fixture
def unreadable(backend: ComputeBackend, tmp_path: Path) -> UnreadableLocation:
    """``backend`` itself behind ``UnreadableLocation``, so what a test submits through ``backend`` it reads here."""
    return UnreadableLocation(backend, tmp_path / "unreadable")


@pytest.fixture
def unreachable(backend_name: str) -> ComputeBackend:
    if backend_name not in OUT_OF_REACH:
        pytest.skip(f"the {backend_name} backend computes in this process, which is always in reach")
    return OUT_OF_REACH[backend_name]()


@pytest.fixture
def on_other_versions(backend_name: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[ComputeBackend]:
    if backend_name not in ON_OTHER_VERSIONS:
        pytest.skip(f"the {backend_name} backend computes in this process, which runs core's versions")
    handshake.reset_handshake()
    yield ON_OTHER_VERSIONS[backend_name](monkeypatch)
    handshake.reset_handshake()


@pytest.fixture
def minted_elsewhere(backend_name: str) -> Iterator[list[tuple[ComputeBackend, ResultRef]]]:
    """From every other backend in ``BACKENDS``, a result it submitted deferred and one it submitted as
    ``must_materialise``, each beside the backend that minted it.

    The minting backend releases them when the test ends; one whose compute is not running skips the test.
    """
    minted = []
    for name, make in BACKENDS.items():
        if name == backend_name:
            continue
        minter = make()
        for must_materialise in (False, True):
            hints = SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=None)
            plan = pl.LazyFrame({"id": [0, 1, 2]}).filter(pl.col("id") >= 0)
            result = minter.submit(plan, [], hints, f"conformance-{uuid.uuid4().hex}", flow_id=-1, node_id=-1)
            minted.append((minter, result.ref))
    yield minted
    for minter, ref in minted:
        minter.release(ref)
