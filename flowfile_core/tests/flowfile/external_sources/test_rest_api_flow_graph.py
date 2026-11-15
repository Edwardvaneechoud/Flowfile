"""End-to-end tests for the REST API reader against a real local HTTP server.

Unlike the unit tests (which mock ``httpx.MockTransport`` / the worker fetcher),
these exercise the full pipeline over real sockets:

    core ``add_rest_api_reader`` -> ``ExternalRestApiFetcher`` ->
    worker ``/store_rest_api_read_result`` -> ``start_generic_process`` ->
    ``fetch_rest_api`` (real HTTP) -> Arrow IPC -> core ``LazyFrame``.

The worker is booted by the session-scoped ``flowfile_worker`` fixture in
``flowfile_core/tests/conftest.py``; the ``_require_worker`` guard below skips
this module when no worker is reachable (e.g. ``SKIP_WORKER_TESTS=1``). The
external API is a ``ThreadingHTTPServer`` served in-process on an ephemeral port,
so the tests are hermetic — no outbound network, no extra dependency.
"""

from __future__ import annotations

import os
import platform

import pytest
import requests
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.routes.routes import flow_file_handler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.transform_schema import SelectInput
from shared.compute_protocol import WorkerLocation
from test_utils.rest_api_server import KEYSET_RECORDS, SECURE_TOKEN, start_api_server
from tests.conftest import protocol_states

# Windows has no 0.0.0.0 loopback alias for connects; the worker binds 127.0.0.1 there.
WORKER_HOST = os.environ.get(
    "FLOWFILE_WORKER_HOST", "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
)
WORKER_PORT = int(os.environ.get("FLOWFILE_WORKER_PORT", 63579))


@pytest.fixture(scope="module")
def api_server():
    with start_api_server() as server:
        yield server


@pytest.fixture(scope="module")
def local_api_server(api_server) -> str:
    """The base URL of the in-process API server."""
    return api_server.base_url


def _is_worker_running() -> bool:
    try:
        return requests.get(f"http://{WORKER_HOST}:{WORKER_PORT}/docs", timeout=5).ok
    except requests.exceptions.RequestException:
        return False


@pytest.fixture(autouse=True)
def _require_worker():
    """Skip the whole module unless a live worker is reachable.

    The REST reader always offloads the fetch to the worker (even for local
    execution), so without one there is nothing meaningful to exercise.
    """
    if not _is_worker_running():
        pytest.skip("flowfile_worker is not running")


# --- helpers ------------------------------------------------------------------


def _graph(flow_id: int = 1, execution_mode: str = "Performance") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="rest_e2e", path=".", execution_location="remote", execution_mode=execution_mode
        )
    )
    return handler.get_flow(flow_id)


def _make_node(graph: FlowGraph, node_id: int = 1, **settings) -> input_schema.NodeRestApiReader:
    return input_schema.NodeRestApiReader(
        flow_id=graph.flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(**settings),
    )


def _add_reader(graph: FlowGraph, node: input_schema.NodeRestApiReader) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node.node_id, node_type="rest_api_reader")
    )
    graph.add_rest_api_reader(node)


def _run(graph: FlowGraph) -> None:
    run_info = graph.run_graph()
    if not run_info.success:
        errors = [f"node {s.node_id}: {s.error}" for s in run_info.node_step_result if not s.success]
        raise AssertionError("Graph execution failed:\n" + "\n".join(errors))


def _collect(graph: FlowGraph, node_id: int = 1):
    result = graph.get_node(node_id).get_resulting_data()
    return result.collect() if hasattr(result, "collect") else result.data_frame.collect()


# --- e2e: materialise real data through core -> worker -> Arrow IPC -----------


def test_e2e_top_level_array(local_api_server):
    graph = _graph()
    _add_reader(graph, _make_node(graph, url=f"{local_api_server}/items"))
    _run(graph)

    df = _collect(graph)
    assert df.height == 3
    assert df.columns == ["id", "name"]
    assert sorted(df["id"].to_list()) == [1, 2, 3]


def test_e2e_record_path_flattens_nested(local_api_server):
    graph = _graph()
    _add_reader(graph, _make_node(graph, url=f"{local_api_server}/wrapped", record_path="data.items"))
    _run(graph)

    df = _collect(graph)
    assert df.height == 2
    assert "nested.city" in df.columns
    assert sorted(df["nested.city"].to_list()) == ["ams", "rtm"]


def test_e2e_offset_pagination(local_api_server):
    graph = _graph()
    _add_reader(
        graph,
        _make_node(
            graph,
            url=f"{local_api_server}/offset",
            pagination=input_schema.RestApiPaginationSettings(pagination_type="offset", page_size=2),
        ),
    )
    _run(graph)

    df = _collect(graph)
    assert df.height == 5  # walked 3 pages (2 + 2 + short 1) over real requests
    assert sorted(df["id"].to_list()) == [0, 1, 2, 3, 4]


def test_e2e_page_pagination(local_api_server):
    graph = _graph()
    _add_reader(
        graph,
        _make_node(
            graph,
            url=f"{local_api_server}/page",
            pagination=input_schema.RestApiPaginationSettings(pagination_type="page", start_page=1),
        ),
    )
    _run(graph)

    df = _collect(graph)
    assert sorted(df["id"].to_list()) == [1, 2, 3]  # stopped on the empty page


def test_e2e_cursor_pagination(local_api_server):
    graph = _graph()
    _add_reader(
        graph,
        _make_node(
            graph,
            url=f"{local_api_server}/cursor",
            record_path="rows",
            pagination=input_schema.RestApiPaginationSettings(
                pagination_type="cursor", cursor_param="cursor", cursor_location="body", cursor_response_path="next"
            ),
        ),
    )
    _run(graph)

    df = _collect(graph)
    assert sorted(df["id"].to_list()) == [1, 2, 3]


def test_e2e_keyset_cursor_in_json_body(api_server):
    api_server.reset()
    graph = _graph()
    _add_reader(
        graph,
        _make_node(
            graph,
            url=f"{api_server.base_url}/keyset",
            method="POST",
            json_body={"app_id": "app-1", "entry_id": "entry-1", "limit": 3},
            record_path="data",
            pagination=input_schema.RestApiPaginationSettings(
                pagination_type="cursor",
                cursor_param="data_id",
                cursor_request_location="body",
                cursor_response_path="data.-1._id",
            ),
        ),
    )
    _run(graph)

    df = _collect(graph)
    assert df["_id"].to_list() == [r["_id"] for r in KEYSET_RECORDS]
    assert api_server.request_count == 4
    assert [r.body.get("data_id") for r in api_server.requests] == [None, "id03", "id06", "id07"]


def test_e2e_bearer_auth_credential_reaches_server(local_api_server):
    """An inline bearer secret is encrypted on the core, decrypted on the worker,
    and applied to the request — proven by the protected route succeeding."""
    graph = _graph()
    _add_reader(
        graph,
        _make_node(
            graph,
            url=f"{local_api_server}/secure",
            auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret=SECURE_TOKEN),
        ),
    )
    _run(graph)

    df = _collect(graph)
    assert df.height == 1
    assert df["name"].to_list() == ["secret"]


def test_e2e_retry_on_429(local_api_server):
    graph = _graph()
    _add_reader(graph, _make_node(graph, url=f"{local_api_server}/flaky", max_retries=2))
    _run(graph)

    df = _collect(graph)
    assert df.height == 1


def test_e2e_downstream_select(local_api_server):
    """REST output flows through a downstream select node."""
    graph = _graph()
    _add_reader(graph, _make_node(graph, url=f"{local_api_server}/items", node_id=1))

    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="select"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id, node_id=2, select_input=[SelectInput(old_name="name")], keep_missing=False
        )
    )
    _run(graph)

    df = _collect(graph, node_id=2)
    assert df.columns == ["name"]
    assert sorted(df["name"].to_list()) == ["a", "b", "c"]


def test_e2e_read_api_public_function(local_api_server):
    """``flowfile_frame.read_api`` works end-to-end through the worker."""
    import flowfile_frame as ff

    ff_frame = ff.read_api(f"{local_api_server}/items")
    df = ff_frame.collect()
    assert df.height == 3
    assert sorted(df["id"].to_list()) == [1, 2, 3]


# --- re-run / re-fetch contract -----------------------------------------------


def test_performance_mode_refetches_on_every_run(api_server):
    """In Performance mode (the default), each run re-executes the source node,
    so the worker hits the API again — no stale reuse across runs."""
    api_server.reset()
    graph = _graph(execution_mode="Performance")
    _add_reader(graph, _make_node(graph, url=f"{api_server.base_url}/items"))

    _run(graph)
    _run(graph)

    assert api_server.request_count == 2  # one real fetch per run


def test_development_mode_reuses_result_without_refetch(api_server):
    """In Development mode, a re-run with unchanged settings is skipped and the
    prior (staged) result is reused — so the API is NOT hit again. Changing a
    setting (here the URL) invalidates the node and forces a fresh fetch."""
    api_server.reset()
    graph = _graph(execution_mode="Development")
    node = _make_node(graph, url=f"{api_server.base_url}/items")
    _add_reader(graph, node)

    _run(graph)
    _run(graph)
    assert api_server.request_count == 1  # second run reused the result, no re-fetch

    # Mutating the settings hash invalidates the node -> it runs again.
    node.rest_api_settings.url = f"{api_server.base_url}/page"
    graph.add_rest_api_reader(node)
    _run(graph)
    assert api_server.request_count == 2


# --- /rest_api/sample route (real worker round-trip) --------------------------


def _auth_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


@pytest.fixture()
def api_client() -> TestClient:
    return _auth_client()


def _register_rest_node_in_global_handler(flow_id: int, node_id: int, url: str, **settings) -> None:
    """Register a flow + a rest_api_reader node on the route-facing handler."""
    if flow_file_handler.get_flow(flow_id):
        flow_file_handler.delete_flow(flow_id)
    flow_file_handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="rest_sample", path=".", execution_location="remote"), user_id=1
    )
    flow = flow_file_handler.get_flow(flow_id)
    node = input_schema.NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(url=url, **settings),
    )
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type="rest_api_reader"))
    flow.add_rest_api_reader(node)


def test_sample_route_infers_and_caches_fields(api_client, local_api_server):
    flow_id, node_id = 7001, 1
    _register_rest_node_in_global_handler(flow_id, node_id, f"{local_api_server}/items")

    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(url=f"{local_api_server}/items"),
    ).model_dump()
    resp = api_client.post("/rest_api/sample", json=payload, params={"sample_size": 50})
    assert resp.status_code == 200, resp.text

    fields = resp.json()["fields"]
    assert {f["name"] for f in fields} == {"id", "name"}

    # The route caches the inferred fields onto the live node for downstream
    # schema prediction without another fetch.
    cached = flow_file_handler.get_flow(flow_id).get_node(node_id).setting_input.fields
    assert cached is not None
    assert {f.name for f in cached} == {"id", "name"}
    flow_file_handler.delete_flow(flow_id)


def test_sample_route_unknown_flow_returns_404(api_client, local_api_server):
    payload = input_schema.NodeRestApiReader(
        flow_id=987654,
        node_id=1,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(url=f"{local_api_server}/items"),
    ).model_dump()

    resp = api_client.post("/rest_api/sample", json=payload)
    assert resp.status_code == 404


def test_sample_route_failed_fetch_returns_422(api_client, local_api_server):
    flow_id, node_id = 7002, 1
    # The server 404s on /nope; a non-retryable 4xx makes the worker fetch fail fast.
    bad_url = f"{local_api_server}/nope"
    _register_rest_node_in_global_handler(flow_id, node_id, bad_url, max_retries=0)

    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(url=bad_url, max_retries=0),
    ).model_dump()
    resp = api_client.post("/rest_api/sample", json=payload)
    assert resp.status_code == 422
    flow_file_handler.delete_flow(flow_id)


protocol_flag = protocol_states("flag_off", "flag_on")


def test_sample_route_fetches_one_page(api_client, api_server, protocol_flag):
    flow_id, node_id = 7003, 1
    url = f"{api_server.base_url}/offset"
    pagination = input_schema.RestApiPaginationSettings(pagination_type="offset", page_size=2)
    _register_rest_node_in_global_handler(flow_id, node_id, url, pagination=pagination)
    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(url=url, pagination=pagination),
    ).model_dump()
    api_server.reset()

    resp = api_client.post("/rest_api/sample", json=payload, params={"sample_size": 3})

    assert resp.status_code == 200, resp.text
    assert {f["name"] for f in resp.json()["fields"]} == {"id"}
    assert api_server.request_count == 1
    flow_file_handler.delete_flow(flow_id)


def test_sample_route_suggests_record_and_cursor_paths(api_client, local_api_server, monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    flow_id, node_id = 7005, 1
    url = f"{local_api_server}/wrapped"
    _register_rest_node_in_global_handler(flow_id, node_id, url)
    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id, node_id=node_id, user_id=1, rest_api_settings=input_schema.RestApiSettings(url=url)
    ).model_dump()

    resp = api_client.post("/rest_api/sample", json=payload)

    assert resp.status_code == 200, resp.text
    assert resp.json()["suggested_paths"] == {"record_paths": ["data.items"], "cursor_paths": ["data.items.-1.id"]}
    flow_file_handler.delete_flow(flow_id)


def test_sample_route_returns_the_request_it_sent_and_the_shape_of_the_response(
    api_client, local_api_server, monkeypatch
):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    flow_id, node_id = 7006, 1
    url = f"{local_api_server}/wrapped"
    auth = input_schema.RestApiAuthSettings(auth_type="bearer", secret=SECURE_TOKEN)
    _register_rest_node_in_global_handler(flow_id, node_id, url, auth=auth)
    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(url=url, auth=auth, record_path="data.items"),
    ).model_dump()
    payload["rest_api_settings"]["auth"]["secret"] = SECURE_TOKEN

    resp = api_client.post("/rest_api/sample", json=payload)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    (sent,) = body["trace"]
    assert (sent["method"], sent["url"], sent["status"], sent["records"]) == ("GET", url, 200, 2)
    assert sent["headers"] == {"Authorization": "Bearer REDACTED"}
    items = body["response_tree"]["children"][0]["children"][0]
    assert (items["path"], items["kind"], items["length"], items["records"]) == ("data.items", "list", 2, True)
    assert '"city": "ams"' in body["response_text"] and body["response_truncated"] is False
    assert SECURE_TOKEN not in resp.text
    flow_file_handler.delete_flow(flow_id)


def test_a_failed_sample_names_the_request_and_the_answer_without_the_secret(
    api_client, local_api_server, protocol_flag
):
    flow_id, node_id = 7007, 1
    url = f"{local_api_server}/secure"
    auth = input_schema.RestApiAuthSettings(
        auth_type="api_key", api_key_name="api_key", api_key_location="query", secret="wrong-key-value"
    )
    _register_rest_node_in_global_handler(flow_id, node_id, url, auth=auth)
    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id, node_id=node_id, user_id=1, rest_api_settings=input_schema.RestApiSettings(url=url, auth=auth)
    ).model_dump()
    payload["rest_api_settings"]["auth"]["secret"] = "wrong-key-value"

    resp = api_client.post("/rest_api/sample", json=payload)

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert f"GET {url}?api_key=REDACTED answered 401 Unauthorized" in detail
    assert '{"error": "unauthorized"}' in detail
    assert "wrong-key-value" not in resp.text
    flow_file_handler.delete_flow(flow_id)


def test_with_the_protocol_flag_the_sample_route_reads_over_v1_and_releases_the_read(
    api_client, local_api_server, monkeypatch
):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)

    def _no_fetcher(*_args, **_kwargs):
        pytest.fail("the sample was read through the task routes' fetcher")

    monkeypatch.setattr("flowfile_core.routes.routes.ExternalRestApiFetcher", _no_fetcher)
    reads, samples = [], []
    real_run_op, real_sample = WorkerBackend.run_op, WorkerBackend.sample

    def _recording_run_op(self, op, payload):
        result = real_run_op(self, op, payload)
        reads.append((self, op, result))
        return result

    def _recording_sample(self, ref, n):
        samples.append((ref, n))
        return real_sample(self, ref, n)

    monkeypatch.setattr(WorkerBackend, "run_op", _recording_run_op)
    monkeypatch.setattr(WorkerBackend, "sample", _recording_sample)
    flow_id, node_id = 7004, 1
    url = f"{local_api_server}/items"
    _register_rest_node_in_global_handler(flow_id, node_id, url)
    payload = input_schema.NodeRestApiReader(
        flow_id=flow_id, node_id=node_id, user_id=1, rest_api_settings=input_schema.RestApiSettings(url=url)
    ).model_dump()

    resp = api_client.post("/rest_api/sample", json=payload, params={"sample_size": 2})

    assert resp.status_code == 200, resp.text
    assert {f["name"] for f in resp.json()["fields"]} == {"id", "name"}
    [(backend, op, result)] = reads
    assert op is OperationKind.rest_api_read
    ref = result.refs["result"]
    assert WorkerLocation.model_validate_json(ref.location).kind == "file"
    assert ref.row_count == 2
    assert samples == [(ref, 2)]
    assert not backend.exists(ref)
    flow_file_handler.delete_flow(flow_id)
