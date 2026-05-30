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

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.routes.routes import flow_file_handler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.transform_schema import SelectInput

WORKER_HOST = os.environ.get("FLOWFILE_WORKER_HOST", "0.0.0.0")
WORKER_PORT = int(os.environ.get("FLOWFILE_WORKER_PORT", 63579))

# A bearer token the in-process server requires on its /secure route. Routed
# end-to-end as an inline plaintext secret (encrypted on the core, decrypted on
# the worker), so a successful /secure fetch proves cross-process decryption.
SECURE_TOKEN = "e2e-secret-token"


# --- in-process "real" API server --------------------------------------------


class _ApiHandler(BaseHTTPRequestHandler):
    """Serves canned JSON for each REST feature, recording what it received.

    Runs in the pytest process; the worker subprocess reaches it over TCP, so
    anything stored on ``self.server`` is visible to the test assertions.
    """

    def log_message(self, *_args):  # noqa: D401 - silence per-request logging
        pass

    def _send(self, status: int, payload, headers: dict[str, str] | None = None):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        parts = urlsplit(self.path)
        path = parts.path
        params = {k: v[0] for k, v in parse_qs(parts.query).items()}
        srv = self.server
        srv.request_count += 1  # counts every inbound request (re-run / re-fetch checks)

        if path == "/items":
            self._send(200, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}])
        elif path == "/wrapped":
            self._send(
                200,
                {"data": {"items": [{"id": 1, "nested": {"city": "ams"}}, {"id": 2, "nested": {"city": "rtm"}}]}},
            )
        elif path == "/offset":
            rows = [{"id": i} for i in range(5)]
            offset = int(params.get("offset", "0"))
            limit = int(params.get("limit", "100"))
            self._send(200, rows[offset : offset + limit])
        elif path == "/page":
            page = int(params.get("page", "1"))
            self._send(200, [{"id": page}] if page <= 3 else [])
        elif path == "/cursor":
            cursor = params.get("cursor")
            nxt = {None: ("c2", 1), "c2": ("c3", 2), "c3": (None, 3)}
            next_cursor, rid = nxt.get(cursor, (None, 99))
            self._send(200, {"rows": [{"id": rid}], "next": next_cursor})
        elif path == "/secure":
            srv.seen_authorization = self.headers.get("Authorization")
            if srv.seen_authorization == f"Bearer {SECURE_TOKEN}":
                self._send(200, [{"id": 1, "name": "secret"}])
            else:
                self._send(401, {"error": "unauthorized"})
        elif path == "/flaky":
            srv.flaky_calls += 1
            if srv.flaky_calls == 1:
                self._send(429, {"error": "slow down"}, headers={"Retry-After": "0"})
            else:
                self._send(200, [{"id": 1}])
        else:
            self._send(404, {"error": "not found"})


@pytest.fixture(scope="module")
def api_server():
    """A real HTTP server on an ephemeral 127.0.0.1 port serving canned JSON.

    Yields the server so tests can read ``request_count`` (how many requests the
    worker actually made) and a ``base_url`` to point nodes at.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ApiHandler)
    server.seen_authorization = None
    server.flaky_calls = 0
    server.request_count = 0
    host, port = server.server_address
    server.base_url = f"http://{host}:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


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
    api_server.request_count = 0
    graph = _graph(execution_mode="Performance")
    _add_reader(graph, _make_node(graph, url=f"{api_server.base_url}/items"))

    _run(graph)
    _run(graph)

    assert api_server.request_count == 2  # one real fetch per run


def test_development_mode_reuses_result_without_refetch(api_server):
    """In Development mode, a re-run with unchanged settings is skipped and the
    prior (staged) result is reused — so the API is NOT hit again. Changing a
    setting (here the URL) invalidates the node and forces a fresh fetch."""
    api_server.request_count = 0
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
        schemas.FlowSettings(flow_id=flow_id, name="rest_sample", path=".", execution_location="remote")
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
