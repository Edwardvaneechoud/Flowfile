"""``V1_OPS``, the one table both worker backends run ``/v1/ops/<kind>`` through, against what the worker serves.

A kind in the table with no route would fail at the worker, and a route with no entry would leave a kind the worker
serves unreachable from core, so the table names exactly the kinds mounted as ``POST /v1/ops/<kind>``. Each kind's
value also names the route's body on ``flowfile_worker.service``, which the embedded worker calls by that name with
the table's query parameters, and a kind outside the table is refused with ``OperationUnsupported`` before anything
runs.
"""

import inspect
from types import SimpleNamespace

import pytest

from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend, _route_body
from flowfile_core.flowfile.execution.backends.worker import V1_OPS
from flowfile_core.flowfile.execution.errors import OperationUnsupported
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_worker import service
from flowfile_worker.protocol_routes import router

pytestmark = pytest.mark.core

KINDS = sorted(V1_OPS, key=lambda kind: kind.value)


def _mounted_ops() -> set[str]:
    prefix = f"{router.prefix}/ops/"
    return {route.path.removeprefix(prefix) for route in router.routes if route.path.startswith(prefix)}


def test_the_table_names_exactly_the_mounted_op_routes():
    assert {kind.value for kind in V1_OPS} == _mounted_ops()


@pytest.mark.parametrize("kind", KINDS, ids=[kind.value for kind in KINDS])
def test_every_kind_in_the_table_names_a_service_body_the_embedded_worker_calls(kind):
    parameters = list(inspect.signature(getattr(service, kind.value)).parameters)

    assert parameters[1] == "job_id"
    assert set(V1_OPS[kind].params(SimpleNamespace(flow_id=1, node_id=1))) <= set(parameters[2:])
    assert callable(_route_body(kind))


def test_the_embedded_worker_refuses_a_kind_it_has_no_body_for(monkeypatch):
    monkeypatch.delitem(V1_OPS, OperationKind.random_split)
    with pytest.raises(OperationUnsupported, match="random_split"):
        EmbeddedWorkerBackend().run_op(OperationKind.random_split, {"payload": None})
