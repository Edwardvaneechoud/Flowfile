"""The backends every conformance test runs over: core's own process and the session worker.

``backend`` is each backend as core configures it. ``unreachable`` and ``on_other_versions`` are the same kind of
backend with its compute out of reach, or on other versions than core; a backend whose compute is core's own process
can be neither, so those tests skip for it. ``minted_elsewhere`` holds results every other backend minted, for
``backend`` to refuse.
"""

import socket
import uuid
from collections.abc import Callable, Iterator

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef, SubmitHints
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from shared.compute_protocol import local_capabilities


def _worker_url() -> str:
    return registry.backend(registry.WORKER_BACKEND_ID).url


def _session_worker() -> WorkerBackend:
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")
    return WorkerBackend(url=_worker_url())


def _worker_out_of_reach() -> WorkerBackend:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return WorkerBackend(url=f"http://127.0.0.1:{port}")


def _worker_on_other_polars(monkeypatch: pytest.MonkeyPatch) -> WorkerBackend:
    reported = local_capabilities().model_copy(update={"polars_version": "0.0.0"})
    monkeypatch.setattr(handshake, "fetch_worker_capabilities", lambda worker_url: reported)
    return WorkerBackend(url=_worker_url())


BACKENDS: dict[str, Callable[[], ComputeBackend]] = {"local": LocalBackend, "worker": _session_worker}
OUT_OF_REACH: dict[str, Callable[[], ComputeBackend]] = {"worker": _worker_out_of_reach}
ON_OTHER_VERSIONS: dict[str, Callable[[pytest.MonkeyPatch], ComputeBackend]] = {"worker": _worker_on_other_polars}


@pytest.fixture(params=list(BACKENDS))
def backend_name(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def backend(backend_name: str) -> ComputeBackend:
    return BACKENDS[backend_name]()


@pytest.fixture
def unreachable(backend_name: str) -> ComputeBackend:
    if backend_name not in OUT_OF_REACH:
        pytest.skip(f"the {backend_name} backend computes in core's own process, which is always in reach")
    return OUT_OF_REACH[backend_name]()


@pytest.fixture
def on_other_versions(backend_name: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[ComputeBackend]:
    if backend_name not in ON_OTHER_VERSIONS:
        pytest.skip(f"the {backend_name} backend computes in core's own process, which runs core's versions")
    handshake.reset_handshake()
    yield ON_OTHER_VERSIONS[backend_name](monkeypatch)
    handshake.reset_handshake()


@pytest.fixture
def minted_elsewhere(backend_name: str) -> Iterator[list[tuple[ComputeBackend, ResultRef]]]:
    """A deferred and a stored result from every other backend in ``BACKENDS``, each beside the backend that minted it.

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
