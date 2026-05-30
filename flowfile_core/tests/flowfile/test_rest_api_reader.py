"""Core-side tests for the REST API reader node.

Covers schema inference from a sample, credential resolution (stored secret by
name, or inline plaintext), worker-settings construction, and node execution.
All fetching is offloaded to the worker, so execution is tested with a mocked
``ExternalRestApiFetcher`` — the core never makes an HTTP call.
"""

from __future__ import annotations

import polars as pl

from flowfile_core.flowfile import flow_graph as flow_graph_module
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.sources.external_sources.rest_api_source import (
    build_rest_api_worker_settings,
    infer_schema_from_sample,
    resolve_auth_secret_encrypted,
)
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.secret_manager.secret_manager import decrypt_secret


# --- helpers ------------------------------------------------------------------


def _graph(execution_location: str = "remote", flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="rest_test", path=".", execution_location=execution_location)
    )
    return handler.get_flow(flow_id)


def _make_node(node_id: int = 1, flow_id: int = 1, **settings) -> input_schema.NodeRestApiReader:
    return input_schema.NodeRestApiReader(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        rest_api_settings=input_schema.RestApiSettings(**settings),
    )


def _add_reader(graph: FlowGraph, node: input_schema.NodeRestApiReader) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node.node_id, node_type="rest_api_reader")
    )
    graph.add_rest_api_reader(node)


class _FakeFetcher:
    """Stands in for ExternalRestApiFetcher so the core never hits the worker/network."""

    last: dict = {}
    result = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    def __init__(self, settings, wait_on_completion: bool = False):
        _FakeFetcher.last = {"settings": settings, "wait": wait_on_completion}

    def get_result(self):
        return _FakeFetcher.result


# --- schema inference ---------------------------------------------------------


def test_infer_schema_from_sample_maps_dtypes():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "score": [1.5, 2.5]})
    minimal = {c.column_name: c.get_minimal_field_info().data_type for c in infer_schema_from_sample(df)}
    assert minimal == {"id": "Int64", "name": "String", "score": "Float64"}


# --- credential resolution ----------------------------------------------------


def test_resolve_secret_from_stored_secret(monkeypatch):
    """A ``secret_name`` is resolved via the user's secret store."""
    monkeypatch.setattr(
        "flowfile_core.secret_manager.secret_manager.get_encrypted_secret",
        lambda current_user_id, secret_name: f"ENC::{secret_name}::{current_user_id}",
    )
    auth = input_schema.RestApiAuthSettings(auth_type="bearer", secret_name="my_token")
    assert resolve_auth_secret_encrypted(auth, 7) == "ENC::my_token::7"


def test_resolve_secret_from_inline_plaintext():
    """An inline ``secret`` (programmatic use) is encrypted with the master key."""
    auth = input_schema.RestApiAuthSettings(auth_type="bearer", secret="plain-token")
    enc = resolve_auth_secret_encrypted(auth, 1)
    assert enc is not None and enc != "plain-token"
    assert decrypt_secret(enc).get_secret_value() == "plain-token"


def test_resolve_secret_prefers_stored_over_inline(monkeypatch):
    monkeypatch.setattr(
        "flowfile_core.secret_manager.secret_manager.get_encrypted_secret",
        lambda current_user_id, secret_name: "STORED",
    )
    auth = input_schema.RestApiAuthSettings(auth_type="bearer", secret_name="ref", secret="inline")
    assert resolve_auth_secret_encrypted(auth, 1) == "STORED"


def test_resolve_secret_none_when_unset():
    auth = input_schema.RestApiAuthSettings(auth_type="none")
    assert resolve_auth_secret_encrypted(auth, 1) is None


# --- worker-settings construction --------------------------------------------


def test_build_worker_settings_routes_secret_by_auth_type():
    node = _make_node(
        url="https://x/api", auth=input_schema.RestApiAuthSettings(auth_type="api_key", api_key_name="X-Key")
    )
    ws = build_rest_api_worker_settings(node, "ENCTOKEN")
    assert ws.auth.api_key_encrypted == "ENCTOKEN"
    assert ws.auth.bearer_token_encrypted is None
    assert ws.auth.basic_password_encrypted is None
    assert ws.url == "https://x/api"


def test_build_worker_settings_maps_pagination_and_sample():
    node = _make_node(
        url="https://x/api",
        pagination=input_schema.RestApiPaginationSettings(pagination_type="offset", page_size=25),
    )
    ws = build_rest_api_worker_settings(node, None, sample_size=5)
    assert ws.pagination.pagination_type.value == "offset"
    assert ws.pagination.page_size == 25
    assert ws.sample_size == 5


# --- add_rest_api_reader ------------------------------------------------------


def test_add_reader_clears_inline_plaintext_and_registers_node():
    graph = _graph()
    node = _make_node(url="https://x/api", auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret="tok"))
    _add_reader(graph, node)
    assert node.rest_api_settings.auth.secret is None, "inline plaintext must be cleared"
    registered = graph.get_node(1)
    assert registered.node_type == "rest_api_reader"
    assert registered.name == "rest_api_reader"


# --- execution (worker-only, mocked fetcher) ---------------------------------


def test_execution_uses_worker_fetcher_and_stamps_schema(monkeypatch):
    monkeypatch.setattr(flow_graph_module, "ExternalRestApiFetcher", _FakeFetcher)
    _FakeFetcher.result = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    graph = _graph(execution_location="local")  # even local must offload to the worker
    node = _make_node(
        url="https://x/api",
        auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret="tok"),
        pagination=input_schema.RestApiPaginationSettings(pagination_type="offset", page_size=2),
    )
    _add_reader(graph, node)

    df = graph.get_node(1).get_resulting_data().data_frame.collect()

    # The fetch went through the (faked) worker fetcher, not an in-core HTTP call.
    assert _FakeFetcher.last["settings"].url == "https://x/api"
    assert _FakeFetcher.last["settings"].auth.bearer_token_encrypted is not None
    assert df.height == 3
    assert sorted(df["id"].to_list()) == [1, 2, 3]
    # The fetched schema is stamped onto the node for downstream introspection.
    assert node.fields is not None
    assert {f.name for f in node.fields} == {"id", "name"}


def test_execution_empty_result_returns_empty_frame(monkeypatch):
    monkeypatch.setattr(flow_graph_module, "ExternalRestApiFetcher", _FakeFetcher)
    _FakeFetcher.result = pl.LazyFrame({})

    graph = _graph(execution_location="local")
    node = _make_node(url="https://x/api", record_path="items")
    _add_reader(graph, node)

    df = graph.get_node(1).get_resulting_data().data_frame.collect()
    assert df.height == 0


# --- save / load round-trip (.flowfile YAML) ---------------------------------


def test_rest_api_node_yaml_round_trip(tmp_path):
    """A saved flow with a rest_api_reader node must load back (regression:
    the node type was missing from the YAML settings-class registry)."""
    graph = _graph(flow_id=4242)
    node = _make_node(
        flow_id=4242,
        url="https://api.example.com/items",
        record_path="data",
        auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret_name="my_token"),
        pagination=input_schema.RestApiPaginationSettings(pagination_type="offset", page_size=50),
    )
    _add_reader(graph, node)

    yaml_path = tmp_path / "rest_flow.yaml"
    graph.save_flow(str(yaml_path))

    loaded = open_flow(yaml_path)
    rest_node = next((n for n in loaded.nodes if n.node_type == "rest_api_reader"), None)
    assert rest_node is not None, "rest_api_reader node should survive the round-trip"
    s = rest_node.setting_input.rest_api_settings
    assert s.url == "https://api.example.com/items"
    assert s.record_path == "data"
    assert s.auth.secret_name == "my_token"
    assert s.pagination.pagination_type == "offset"


def test_build_worker_settings_routes_bearer_and_basic():
    """The encrypted token lands in the auth field matching ``auth_type``."""
    node_b = _make_node(url="https://x/api", auth=input_schema.RestApiAuthSettings(auth_type="bearer"))
    ws_b = build_rest_api_worker_settings(node_b, "ENC")
    assert ws_b.auth.bearer_token_encrypted == "ENC"
    assert ws_b.auth.api_key_encrypted is None
    assert ws_b.auth.basic_password_encrypted is None

    node_basic = _make_node(
        url="https://x/api", auth=input_schema.RestApiAuthSettings(auth_type="basic", basic_username="u")
    )
    ws_basic = build_rest_api_worker_settings(node_basic, "ENC")
    assert ws_basic.auth.basic_password_encrypted == "ENC"
    assert ws_basic.auth.basic_username == "u"
    assert ws_basic.auth.bearer_token_encrypted is None


def test_execution_aligns_fetched_frame_to_cached_schema(monkeypatch):
    """When the node has sampled fields, the fetched frame is aligned to them:
    a missing column is added as a typed null and column order follows the schema."""
    monkeypatch.setattr(flow_graph_module, "ExternalRestApiFetcher", _FakeFetcher)
    _FakeFetcher.result = pl.LazyFrame({"id": [1, 2], "name": ["a", "b"]})

    graph = _graph(execution_location="local")
    node = _make_node(url="https://x/api")
    node.fields = [
        input_schema.MinimalFieldInfo(name="id", data_type="Int64"),
        input_schema.MinimalFieldInfo(name="name", data_type="String"),
        input_schema.MinimalFieldInfo(name="extra", data_type="String"),
    ]
    _add_reader(graph, node)

    df = graph.get_node(1).get_resulting_data().data_frame.collect()
    assert df.columns == ["id", "name", "extra"]  # cached schema order, missing col added
    assert df["extra"].null_count() == 2


def test_read_api_builds_graph_and_executes_via_worker(monkeypatch):
    """``flowfile_frame.read_api`` coerces auth/pagination, builds the node, and
    materialises through the (faked) worker fetcher — no real HTTP, no live worker."""
    import flowfile_frame as ff

    monkeypatch.setattr(flow_graph_module, "ExternalRestApiFetcher", _FakeFetcher)
    _FakeFetcher.result = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    frame = ff.read_api(
        "https://x/api",
        auth={"auth_type": "bearer", "secret": "tok"},
        pagination={"pagination_type": "offset", "page_size": 2},
    )
    df = frame.collect()

    assert df.height == 3
    assert sorted(df["id"].to_list()) == [1, 2, 3]
    # auth dict coerced + secret encrypted + routed by auth_type to the worker settings
    settings = _FakeFetcher.last["settings"]
    assert settings.url == "https://x/api"
    assert settings.auth.bearer_token_encrypted is not None
    assert settings.pagination.pagination_type.value == "offset"
