"""A random split over the worker's task routes stores each split and leaves no shuffle behind."""

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.payloads import RandomSplitPayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import results_exists
from flowfile_core.schemas import input_schema


@pytest.fixture
def backend(running_worker, protocol_off) -> WorkerBackend:
    return WorkerBackend(url=registry.worker_url())


def test_each_split_is_stored_and_the_shuffle_task_is_cleared(backend):
    node = input_schema.NodeRandomSplit(flow_id=1, node_id=1, seed=7)
    split = RandomSplitPayload(plan=pl.LazyFrame({"n": list(range(100))}), settings=node, flow_id=1, node_id=1)

    refs = backend.run_op(OperationKind.random_split, op_payload(split)).refs
    try:
        assert list(refs) == ["train", "test"]
        assert [(ref.state, ref.row_count) for ref in refs.values()] == [("materialised", 80), ("materialised", 20)]
        assert not results_exists(split.job_id, worker_url=backend.url)
        held = pl.concat([backend.plan_for(ref).collect() for ref in refs.values()])
        assert sorted(held["n"].to_list()) == list(range(100))
    finally:
        for ref in refs.values():
            backend.release(ref)
    assert not any(backend.exists(ref) for ref in refs.values())
