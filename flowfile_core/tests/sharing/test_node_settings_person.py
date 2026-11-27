"""On a server a flow open in a person's editor session works as that person.

Node settings carry a ``user_id``, and a file node uses the workspace of the person it names. Settings reach an open
flow through several routes; whatever ``user_id`` a request's settings name, the node carries the person whose session
holds the flow. ``test_every_route_that_takes_node_settings_is_tried`` is built from the live app, so a new route
that names a flow and takes node settings or free-form JSON has a case in ``PLANTS`` or a line in
``NOT_NODE_SETTINGS`` saying why it has none.
"""

import inspect
import typing

import polars as pl
import pytest
from pydantic import BaseModel

from flowfile_core import flow_file_handler
from flowfile_core.ai import diff as ai_diff
from flowfile_core.configs import node_store
from flowfile_core.configs.settings import FEATURE_FLAG_AI
from flowfile_core.fileExplorer import workspace
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from tests.sharing.test_flow_session_isolation import FLOW_ID_ROUTES, _locations
from tests.test_history_routes import CUSTOM_NODE_TYPE, HistoryFixedColumn

REFUSED = "is not in your workspace or the shared folder, so this flow cannot use it."

NOT_NODE_SETTINGS = {
    ("POST", "/analysis_data/compute"): "a chart's query over a node's result; places no node",
    ("POST", "/editor/notebook/push/"): "cells the server interprets into settings; the JSON maps cells to node ids",
    ("POST", "/notebook/plan"): "previews a push; places no node",
    (
        "POST",
        "/notebook/session/node_run",
    ): "a kernel's callback; the nodes run on a graph built for the kernel's owner",
    ("POST", "/notebook/session/lookup"): "a kernel's callback; answers catalog metadata as the kernel's owner",
    ("POST", "/rest_api/sample"): "fetches a sample from a URL; places no node and names no file",
    ("POST", "/kernels/{kernel_id}/execute"): "code for a kernel the route has owner-checked; places no node",
    ("POST", "/kernels/{kernel_id}/execute_cell"): "code for a kernel the route has owner-checked; places no node",
    ("POST", "/raw_logs"): "a worker's or kernel's log lines; places no node",
}


def _carries_settings(annotation, seen: set | None = None) -> bool:
    """Whether a request body of this type can hold node settings: a node's model, or free-form JSON."""
    seen = set() if seen is None else seen
    if annotation is typing.Any or annotation is dict or typing.get_origin(annotation) is dict:
        return True
    if inspect.isclass(annotation):
        if annotation in seen:
            return False
        seen.add(annotation)
        if issubclass(annotation, input_schema.NodeBase):
            return True
        if issubclass(annotation, BaseModel):
            return any(_carries_settings(field.annotation, seen) for field in annotation.model_fields.values())
    return any(_carries_settings(arg, seen) for arg in typing.get_args(annotation))


def _settings_routes() -> set[tuple[str, str]]:
    return {
        key
        for key, route in FLOW_ID_ROUTES.items()
        if any(
            kind == "body" and _carries_settings(param.field_info.annotation)
            for kind, param in _locations(route.dependant)
        )
    }


@pytest.fixture
def bob(client_for):
    return client_for("bob")


@pytest.fixture
def theirs(users):
    """A file in alice's workspace, as CSV and as Parquet."""
    folder = workspace.workspace_of(users["alice"].id)
    pl.DataFrame({"secret_col": [41, 42]}).write_csv(folder / "private.csv")
    pl.DataFrame({"secret_col": [41, 42]}).write_parquet(folder / "private.parquet")
    return folder / "private.csv"


@pytest.fixture
def flow_id(bob, users):
    own = workspace.workspace_of(users["bob"].id)
    created = bob.post(
        "/editor/create_flow/", params={"flow_path": str(own / "mine.yaml"), "register_in_catalog": False}
    )
    assert created.status_code == 200, created.text
    flow_file_handler.get_flow(created.json()).flow_settings.execution_location = "local"
    yield created.json()
    flow_file_handler.delete_flow(created.json())


@pytest.fixture
def ai_enabled():
    original = FEATURE_FLAG_AI.value
    FEATURE_FLAG_AI.set(True)
    yield
    FEATURE_FLAG_AI.set(original)


@pytest.fixture
def custom_node_type():
    saved = dict(node_store.CUSTOM_NODE_STORE), dict(node_store.node_dict), list(node_store.nodes_list)
    node_store.add_to_custom_node_store(HistoryFixedColumn)
    yield CUSTOM_NODE_TYPE
    for live, kept in zip((node_store.CUSTOM_NODE_STORE, node_store.node_dict), saved[:2], strict=True):
        live.clear()
        live.update(kept)
    node_store.nodes_list[:] = saved[2]


def _add_node(client, flow_id: int, node_id: int, node_type: str) -> None:
    added = client.post(
        "/editor/add_node/",
        params={"flow_id": flow_id, "node_id": node_id, "node_type": node_type, "pos_x": 0, "pos_y": 0},
    )
    assert added.status_code == 200, added.text


def _read_settings(flow_id: int, node_id: int, path, user_id: int) -> dict:
    return {
        "flow_id": flow_id,
        "node_id": node_id,
        "user_id": user_id,
        "received_file": {"path": str(path), "name": path.name, "file_type": "csv"},
    }


def _update_settings(client, flow_id, path, user_id):
    _add_node(client, flow_id, 1, "read")
    return client.post(
        "/update_settings/", params={"node_type": "read"}, json=_read_settings(flow_id, 1, path, user_id)
    )


def _add_input(client, flow_id, path, user_id):
    _add_node(client, flow_id, 1, "manual_input")
    body = {"flow_id": flow_id, "node_id": 1, "file_ref": str(path.with_suffix(".parquet")), "user_id": user_id}
    return client.post("/transform/add_input/", json=body)


def _manual_input(client, flow_id, path, user_id):
    _add_node(client, flow_id, 1, "manual_input")
    body = input_schema.NodeManualInput(
        flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]), user_id=user_id
    )
    return client.post("/transform/manual_input", json=body.model_dump(mode="json"))


def _apply_operations(client, flow_id, path, user_id):
    _add_node(client, flow_id, 1, "read")
    operation = {"op": "update_settings", "node_type": "read", "settings": _read_settings(flow_id, 1, path, user_id)}
    return client.post(
        "/editor/apply_operations/", json={"flow_id": flow_id, "label": "Batch", "operations": [operation]}
    )


def _copy_node(client, flow_id, path, user_id):
    _add_node(client, flow_id, 2, "read")
    placed = client.post(
        "/update_settings/", params={"node_type": "read"}, json=_read_settings(flow_id, 2, path, user_id)
    )
    assert placed.status_code == 200, placed.text
    promise = {"flow_id": flow_id, "node_id": 1, "node_type": "read", "user_id": user_id}
    return client.post(
        "/editor/copy_node", params={"node_id_to_copy_from": 2, "flow_id_to_copy_from": flow_id}, json=promise
    )


def _custom_node(client, flow_id, path, user_id):
    _add_node(client, flow_id, 1, CUSTOM_NODE_TYPE)
    body = {
        "flow_id": flow_id,
        "node_id": 1,
        "user_id": user_id,
        "settings": {"main_section": {"standard_input": str(path)}},
    }
    return client.post(
        "/user_defined_components/update_user_defined_node", params={"node_type": CUSTOM_NODE_TYPE}, json=body
    )


def _staged(client, flow_id, node_type: str, settings: dict):
    payload = {"node_type": node_type, "settings": settings, "insertion_context": {}}
    entry = {"tool_name": ai_diff._ADD_PREFIX + node_type, "staged_node_payload": payload}
    staged = client.post("/ai/diff/stage", json={"session_id": "person", "flow_id": flow_id, "staged_results": [entry]})
    assert staged.status_code == 200, staged.text
    return client.post(f"/ai/diff/{staged.json()['diff_id']}/accept", json={"flow_id": flow_id})


def _staged_read(client, flow_id, path, user_id):
    return _staged(client, flow_id, "read", _read_settings(flow_id, 1, path, user_id))


PLANTS = {
    ("POST", "/update_settings/"): [_update_settings],
    ("POST", "/transform/add_input/"): [_add_input],
    ("POST", "/transform/manual_input"): [_manual_input],
    ("POST", "/editor/apply_operations/"): [_apply_operations],
    ("POST", "/editor/copy_node"): [_copy_node],
    ("POST", "/user_defined_components/update_user_defined_node"): [_custom_node],
    ("POST", "/ai/diff/stage"): [_staged_read],
}
CASES = [(key, plant) for key, plants in PLANTS.items() for plant in plants]


def test_every_route_that_takes_node_settings_is_tried():
    found = _settings_routes()
    unknown = found - set(PLANTS) - set(NOT_NODE_SETTINGS)
    assert not unknown, f"add a case to PLANTS, or a reason to NOT_NODE_SETTINGS, for {sorted(unknown)}"
    stale = (set(PLANTS) | set(NOT_NODE_SETTINGS)) - found
    assert not stale, f"no longer a route that names a flow and takes settings: {sorted(stale)}"


@pytest.mark.parametrize(("key", "plant"), CASES, ids=[f"{key[1]} {plant.__name__}" for key, plant in CASES])
def test_a_node_carries_the_person_of_its_session_whoever_the_request_names(
    key, plant, bob, users, theirs, flow_id, ai_enabled, custom_node_type
):
    response = plant(bob, flow_id, theirs, users["alice"].id)
    assert response.status_code == 200, response.text
    node = flow_file_handler.get_flow(flow_id).get_node(1)
    assert node is not None
    assert node.setting_input.user_id == users["bob"].id
    shown = bob.get("/node", params={"flow_id": flow_id, "node_id": 1})
    assert shown.status_code == 200, shown.text
    assert "secret_col" not in shown.text
    for route in ("/flow_data/v2", "/node/downstream_node_ids"):
        bob.get(route, params={"flow_id": flow_id, "node_id": 1})
    assert "secret_col" not in bob.get("/node", params={"flow_id": flow_id, "node_id": 1}).text


def test_a_data_source_added_for_another_person_holds_none_of_their_rows(bob, users, theirs, flow_id):
    assert _add_input(bob, flow_id, theirs, users["alice"].id).status_code == 200
    run = flow_file_handler.get_flow(flow_id).run_graph()
    [result] = [r for r in run.node_step_result if r.node_id == 1]
    assert result.success is False and REFUSED in result.error


def test_an_undo_brings_nodes_back_for_the_person_of_the_session(bob, users, theirs, flow_id):
    assert _manual_input(bob, flow_id, theirs, users["alice"].id).status_code == 200
    assert bob.post("/editor/undo/", params={"flow_id": flow_id}).status_code == 200
    assert bob.post("/editor/redo/", params={"flow_id": flow_id}).status_code == 200
    node = flow_file_handler.get_flow(flow_id).get_node(1)
    assert node.setting_input.user_id == users["bob"].id


class TestOutsideASession:
    def _graph(self) -> FlowGraph:
        handler = FlowfileHandler()
        handler.register_flow(schemas.FlowSettings(flow_id=1, name="headless", path=".", execution_location="local"))
        return handler.get_flow(1)

    def test_a_flow_no_session_holds_keeps_the_user_its_settings_carry(self, users):
        graph = self._graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=1,
                node_id=1,
                raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
                user_id=users["alice"].id,
            )
        )
        assert graph.get_node(1).setting_input.user_id == users["alice"].id

    def test_on_a_desktop_a_session_leaves_the_settings_as_sent(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", "electron")
        handler = FlowfileHandler()
        handler.register_flow(schemas.FlowSettings(flow_id=1, name="desktop", path=".", execution_location="local"), 1)
        graph = handler.get_flow(1)
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]), user_id=7
            )
        )
        assert graph.get_node(1).setting_input.user_id == 7


def test_a_predicted_schema_for_a_staged_node_is_read_as_the_person_of_the_session(bob, users, theirs, flow_id):
    from flowfile_core.ai.tools.predictor import predict_schema_via_mirror

    settings = input_schema.NodeRead.model_validate(_read_settings(flow_id, 1, theirs, users["alice"].id))
    predicted = predict_schema_via_mirror("read", settings, {}, flow=flow_file_handler.get_flow(flow_id)) or []
    assert "secret_col" not in [column.column_name for column in predicted]
