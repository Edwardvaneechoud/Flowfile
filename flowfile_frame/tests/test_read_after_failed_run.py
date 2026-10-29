"""A read of a node whose last run failed answers that run's error, never the frame built before the run.

A frame's graph holds the result each node was built with. A Development run on an unreachable worker, which sends
every node to it in either flag state, fails the source with ``ComputeUnavailable`` and skips the filter below it;
both reads then raise the run's error rather than compose the frame in this process. The protocol flag is read from
the environment; the expectations hold in either state.
"""

import socket

import pytest

import flowfile_frame as ff
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.errors import ComputeUnavailable


@pytest.fixture
def closed_worker_url(monkeypatch) -> str:
    """Point the registry's worker backend at a port nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", url)
    return url


def test_reads_after_a_run_on_an_unreachable_worker_raise_its_error(closed_worker_url):
    source = ff.from_dict({"a": [1, 2, 3]})
    filtered = source.filter(ff.col("a") > 1)
    graph = filtered.flow_graph
    graph.flow_settings.execution_location = "remote"
    graph.flow_settings.execution_mode = "Development"

    run_info = graph.run_graph()

    assert run_info.success is False
    results = {r.node_id: r for r in run_info.node_step_result}
    assert results[source.node_id].success is False
    assert results[source.node_id].compute_error == "ComputeUnavailable"
    assert closed_worker_url in results[source.node_id].error
    assert filtered.node_id not in results
    for node_id in (source.node_id, filtered.node_id):
        node = graph.get_node(node_id)
        for _ in range(2):
            with pytest.raises(ComputeUnavailable):
                node.get_resulting_data()
