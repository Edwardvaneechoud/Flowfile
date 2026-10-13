"""A result one backend holds crosses to another only through ``export`` and ``import_uri``.

Node A runs on one backend and node B on the other: A's result is exported to a parquet file at a temporary URI, the
other backend imports that URI, and B's plan, composed on the imported result, runs there. Both directions run, with A
held deferred and as a required result. The protocol flag comes from the environment, as for the conformance package:
with it on the worker exports in a child of its own and holds an import as a result of its own; with it off the worker
backend sinks and holds them in core over the shared disk.
"""

import uuid
from collections.abc import Iterator

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ForeignRef
from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef, SubmitHints

CUSTOMERS = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6, 7],
        "name": ["ada", "grace", "edsger", "barbara", "donald", "frances", "alan"],
        "region": ["north", "south", "north", "south", "north", "east", "east"],
        "amount": [12.5, 8.0, 30.0, 15.0, 4.5, 22.0, 9.5],
    }
)
A_ROWS = CUSTOMERS.filter(pl.col("amount") > 10)
EXPECTED = A_ROWS.group_by("region").agg(total=pl.col("amount").sum()).sort("region")
STATES = pytest.mark.parametrize("must_materialise", [False, True], ids=["deferred", "required"])

Held = list[tuple[ComputeBackend, ResultRef]]


@pytest.fixture
def local() -> LocalBackend:
    return LocalBackend()


@pytest.fixture
def worker() -> WorkerBackend:
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")
    return WorkerBackend(url=registry.backend(registry.WORKER_BACKEND_ID).url)


@pytest.fixture
def kept() -> Iterator[Held]:
    """Every result a test made, beside the backend holding it, released when the test ends."""
    held: Held = []
    yield held
    for backend, ref in held:
        backend.release(ref)


def node_a(backend: ComputeBackend, kept: Held, must_materialise: bool) -> ResultRef:
    hints = SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=None)
    plan = CUSTOMERS.lazy().filter(pl.col("amount") > 10)
    ref = backend.submit(plan, [], hints, f"chain-a-{uuid.uuid4().hex}", flow_id=-1, node_id=-1).ref
    kept.append((backend, ref))
    return ref


def cross(source: ComputeBackend, target: ComputeBackend, ref_a: ResultRef, kept: Held, tmp_path) -> ResultRef:
    """A's result exported by ``source`` to a temporary URI and imported by ``target``; the edge ``target`` holds."""
    uri = f"file://{tmp_path}/edge.parquet"

    assert source.export(ref_a, uri, None) == uri

    assert pl.read_parquet(uri).sort("id").equals(A_ROWS)
    edge = target.import_uri(uri, ref_a.schema, source.count(ref_a), None)
    kept.append((target, edge))
    assert (edge.backend_id, edge.state, edge.row_count) == (target.id, "deferred", A_ROWS.height)
    assert [(c.column_name, c.data_type) for c in edge.schema] == [(c.column_name, c.data_type) for c in ref_a.schema]
    return edge


def node_b(backend: ComputeBackend, edge: ResultRef, kept: Held) -> ResultRef:
    plan = backend.plan_for(edge).group_by("region").agg(total=pl.col("amount").sum()).sort("region")
    hints = SubmitHints(must_materialise=False, wide=True, fanout=1, preview_rows=100)
    ref = backend.submit(plan, [edge], hints, f"chain-b-{uuid.uuid4().hex}", flow_id=-1, node_id=-1).ref
    kept.append((backend, ref))
    return ref


def assert_node_b_is_correct(backend: ComputeBackend, ref_b: ResultRef) -> None:
    assert pl.read_ipc_stream(backend.sample(ref_b, 10)).equals(EXPECTED)
    assert backend.count(ref_b) == EXPECTED.height


def assert_nothing_crosses_implicitly(source: ComputeBackend, target: ComputeBackend, ref_a, edge) -> None:
    with pytest.raises(ForeignRef):
        target.plan_for(ref_a)
    with pytest.raises(ForeignRef):
        source.plan_for(edge)


@STATES
def test_a_local_result_crosses_to_the_worker_through_export(local, worker, kept, must_materialise, tmp_path):
    ref_a = node_a(local, kept, must_materialise)

    edge = cross(local, worker, ref_a, kept, tmp_path)
    ref_b = node_b(worker, edge, kept)

    assert ref_b.backend_id == worker.id
    assert_node_b_is_correct(worker, ref_b)
    assert_nothing_crosses_implicitly(local, worker, ref_a, edge)


@STATES
def test_a_worker_result_crosses_to_core_through_export(local, worker, kept, must_materialise, tmp_path):
    ref_a = node_a(worker, kept, must_materialise)

    edge = cross(worker, local, ref_a, kept, tmp_path)
    ref_b = node_b(local, edge, kept)

    assert ref_b.backend_id == local.id
    assert_node_b_is_correct(local, ref_b)
    assert_nothing_crosses_implicitly(worker, local, ref_a, edge)
