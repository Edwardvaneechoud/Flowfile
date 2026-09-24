"""Route-level invariants of the undo/redo history.

I2 atomicity: every editor mutation route (and ``/editor/apply_operations/``) records exactly one undo
step when the graph changed and none otherwise; a failing request leaves the graph and both stacks untouched.
I3 history algebra: seeded random gesture sequences; undo-all returns the initial graph, redo-all the final one.
I4 dirty: dirty holds exactly when the persisted snapshot differs from the saved one, also across undo/redo.
I5 concurrency: parallel undo/redo/mutations leave a restorable, consistent history.
"""

import random
import threading

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.configs import node_store
from flowfile_core.flowfile import flow_graph as flow_graph_module
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas, transform_schema
from tests.flowfile.history_graphs import edge_bookkeeping_errors, in_scope


def _client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    api_client = TestClient(main.app)
    api_client.headers = {"Authorization": f"Bearer {token}"}
    return api_client


client = _client()


class HistoryFixedColumn(CustomNodeBase):
    node_name: str = "History Fixed Column"
    node_category: str = "Testing"
    title: str = "History Fixed Column"
    intro: str = "Adds a column with a fixed value."

    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Configuration", standard_input=TextInput(label="Fixed Value")),
    )

    def process(self, *inputs):
        return inputs[0]


CUSTOM_NODE_TYPE = "history_fixed_column"


def new_flow(flow_id: int, path: str = ".") -> "Api":
    if flow_file_handler.get_flow(flow_id) is not None:
        flow_file_handler.delete_flow(flow_id)
    flow_file_handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name=f"history_routes_{flow_id}",
            path=path,
            execution_mode="Development",
            execution_location="local",
        )
    )
    return Api(flow_id)


def _connection(from_id: int, to_id: int, input_class: str = "input-0", handle: str = "output-0") -> dict:
    connection = input_schema.NodeConnection.create_from_simple_input(from_id, to_id, output_handle=handle)
    connection.input_connection.connection_class = input_class
    return connection.model_dump(mode="json")


def _manual_payload(flow_id: int, node_id: int, rows: list[dict]) -> dict:
    return input_schema.NodeManualInput(
        flow_id=flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(rows)
    ).model_dump(mode="json")


def _filter_payload(flow_id: int, node_id: int, depending_on_id: int, value: str) -> dict:
    return input_schema.NodeFilter(
        flow_id=flow_id,
        node_id=node_id,
        depending_on_id=depending_on_id,
        filter_input=transform_schema.FilterInput(
            mode="basic", basic_filter=transform_schema.BasicFilter(field="a", operator="equals", value=value)
        ),
    ).model_dump(mode="json")


def _sample_payload(flow_id: int, node_id: int, depending_on_id: int, size: int) -> dict:
    return input_schema.NodeSample(
        flow_id=flow_id, node_id=node_id, depending_on_id=depending_on_id, sample_size=size
    ).model_dump(mode="json")


def _insert_on_edge(node_id: int, from_id: int, to_id: int, input_class="input-0", handle="output-0") -> dict:
    return {"op": "insert_on_edge", "node_id": node_id, "connection": _connection(from_id, to_id, input_class, handle)}


class Api:
    def __init__(self, flow_id: int, http: TestClient | None = None):
        self.flow_id = flow_id
        self.http = http or client

    @property
    def flow(self):
        return flow_file_handler.get_flow(self.flow_id)

    def snapshot(self) -> dict:
        return in_scope(self.flow)

    def history(self) -> dict:
        return self.http.get("/editor/history_status/", params={"flow_id": self.flow_id}).json()

    def add_node(self, node_id: int, node_type: str, x: float = 0, y: float = 0):
        return self.http.post(
            "/editor/add_node/",
            params={"flow_id": self.flow_id, "node_id": node_id, "node_type": node_type, "pos_x": x, "pos_y": y},
        )

    def settings(self, node_type: str, payload: dict):
        return self.http.post("/update_settings/", params={"node_type": node_type}, json=payload)

    def custom_settings(self, node_id: int, value: str, pos: tuple[float, float] = (0, 0)):
        payload = {
            "flow_id": self.flow_id,
            "node_id": node_id,
            "pos_x": pos[0],
            "pos_y": pos[1],
            "settings": {"main_section": {"standard_input": value}},
        }
        return self.http.post(
            "/user_defined_components/update_user_defined_node", params={"node_type": CUSTOM_NODE_TYPE}, json=payload
        )

    def connect(self, from_id, to_id, input_class="input-0", handle="output-0"):
        return self.http.post(
            "/editor/connect_node/",
            params={"flow_id": self.flow_id},
            json=_connection(from_id, to_id, input_class, handle),
        )

    def delete_connection(self, from_id, to_id, input_class="input-0", handle="output-0"):
        return self.http.post(
            "/editor/delete_connection/",
            params={"flow_id": self.flow_id},
            json=_connection(from_id, to_id, input_class, handle),
        )

    def delete_node(self, node_id: int):
        return self.http.post("/editor/delete_node/", params={"flow_id": self.flow_id, "node_id": node_id})

    def layout(self, positions: dict[int, tuple[float, float]], record_history: bool = True, comments=None):
        body = {
            "node_positions": [{"node_id": n, "pos_x": x, "pos_y": y} for n, (x, y) in positions.items()],
            "comment_bounds": comments or [],
            "record_history": record_history,
        }
        return self.http.post("/editor/update_layout/", params={"flow_id": self.flow_id}, json=body)

    def copy(self, source_id: int, new_id: int, node_type: str, x: float = 0, y: float = 0):
        node_promise = input_schema.NodePromise(
            flow_id=self.flow_id, node_id=new_id, node_type=node_type, pos_x=x, pos_y=y
        ).model_dump(mode="json")
        return self.http.post(
            "/editor/copy_node",
            params={"node_id_to_copy_from": source_id, "flow_id_to_copy_from": self.flow_id},
            json=node_promise,
        )

    def create_group(self, node_ids: list[int], name: str = "G"):
        return self.http.post(
            "/editor/create_group/", params={"flow_id": self.flow_id}, json={"node_ids": node_ids, "name": name}
        )

    def update_group(self, group_id: int, **body):
        return self.http.post(
            "/editor/update_group/", params={"flow_id": self.flow_id, "group_id": group_id}, json=body
        )

    def delete_group(self, group_id: int):
        return self.http.post("/editor/delete_group/", params={"flow_id": self.flow_id, "group_id": group_id})

    def group_add(self, group_id: int, node_ids: list[int]):
        return self.http.post(
            "/editor/group/add_nodes/",
            params={"flow_id": self.flow_id, "group_id": group_id},
            json={"node_ids": node_ids},
        )

    def group_remove(self, node_ids: list[int]):
        return self.http.post(
            "/editor/group/remove_nodes/", params={"flow_id": self.flow_id}, json={"node_ids": node_ids}
        )

    def create_comment(self, text: str, x: float = 0, y: float = 0):
        return self.http.post(
            "/editor/create_comment/",
            params={"flow_id": self.flow_id},
            json={"text": text, "x_position": x, "y_position": y},
        )

    def update_comment(self, comment_id: int, text: str):
        return self.http.post(
            "/editor/update_comment/", params={"flow_id": self.flow_id, "comment_id": comment_id}, json={"text": text}
        )

    def delete_comment(self, comment_id: int):
        return self.http.post("/editor/delete_comment/", params={"flow_id": self.flow_id, "comment_id": comment_id})

    def description(self, node_id: int, text: str):
        return self.http.post("/node/description/", params={"flow_id": self.flow_id, "node_id": node_id}, json=text)

    def reference(self, node_id: int, ref: str):
        return self.http.post("/node/reference/", params={"flow_id": self.flow_id, "node_id": node_id}, json=ref)

    def apply(self, operations: list[dict], label: str = "Batch"):
        return self.http.post(
            "/editor/apply_operations/", json={"flow_id": self.flow_id, "label": label, "operations": operations}
        )

    def standard_layout(self):
        return self.http.post("/flow/apply_standard_layout/", params={"flow_id": self.flow_id})

    def undo(self):
        return self.http.post("/editor/undo/", params={"flow_id": self.flow_id})

    def redo(self):
        return self.http.post("/editor/redo/", params={"flow_id": self.flow_id})

    def flow_settings(self) -> dict:
        return self.http.get("/flow_settings", params={"flow_id": self.flow_id}).json()


def seed_two_sources_and_filter(api: Api) -> None:
    """1: manual_input, 2: manual_input, 3: filter(1). History is cleared afterwards."""
    fid = api.flow_id
    assert api.add_node(1, "manual_input", 0, 0).status_code == 200
    assert api.settings("manual_input", _manual_payload(fid, 1, [{"a": 1}, {"a": 2}])).status_code == 200
    assert api.add_node(2, "manual_input", 0, 200).status_code == 200
    assert api.settings("manual_input", _manual_payload(fid, 2, [{"a": 3}])).status_code == 200
    assert api.add_node(3, "filter", 200, 0).status_code == 200
    assert api.connect(1, 3).status_code == 200
    assert api.settings("filter", _filter_payload(fid, 3, 1, "1")).status_code == 200
    api.flow._history_manager.clear()


def expect_one_step(api: Api, response, before_snapshot, before_history) -> dict:
    assert response.status_code == 200, response.text
    after = api.history()
    assert api.snapshot() != before_snapshot, "gesture did not change the graph"
    assert after["undo_count"] == before_history["undo_count"] + 1, (before_history, after)
    assert after["redo_count"] == 0
    return after


def expect_no_step(api: Api, response, before_snapshot, before_history, status: int = 200) -> None:
    assert response.status_code == status, response.text
    assert api.snapshot() == before_snapshot
    after = api.history()
    assert (after["undo_count"], after["redo_count"]) == (before_history["undo_count"], before_history["redo_count"])


@pytest.fixture
def custom_node_type():
    saved_store = dict(node_store.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    node_store.add_to_custom_node_store(HistoryFixedColumn)
    yield CUSTOM_NODE_TYPE
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_store)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list


# Every mutating route of the editor route modules must be classified: graph mutations in GESTURE_ROUTES,
# everything else in NON_GRAPH_ROUTES.
GESTURE_ROUTES = {
    "/editor/add_node/",
    "/editor/copy_node",
    "/editor/delete_node/",
    "/editor/delete_connection/",
    "/editor/connect_node/",
    "/editor/create_group/",
    "/editor/update_group/",
    "/editor/delete_group/",
    "/editor/group/add_nodes/",
    "/editor/group/remove_nodes/",
    "/editor/update_layout/",
    "/editor/create_comment/",
    "/editor/update_comment/",
    "/editor/delete_comment/",
    "/editor/apply_operations/",
    "/update_settings/",
    "/node/description/",
    "/node/reference/",
    "/user_defined_components/update_user_defined_node",
    "/flow/apply_standard_layout/",
    "/transform/manual_input",
    "/transform/add_input/",
}
NON_GRAPH_ROUTES = {
    "/editor/undo/",
    "/editor/redo/",
    "/editor/history_clear/",
    "/editor/create_flow/",
    "/editor/close_flow/",
    "/editor/rename_flow/",
    "/editor/code_to_polars/exported",
    "/editor/code_to_flowframe/exported",
    "/editor/code_to_project/save",
    "/files/create_directory",
    "/flow/register/",
    "/node/trigger_fetch_data",
    "/flow/run/",
    "/flow/cancel/",
    "/db_connection_lib",
    "/rest_api/sample",
    "/dynamic_rename/preview",
    "/save_flow",
    "/save_flow_to_catalog",
    "/overwrite_flow_in_catalog",
    "/flow_settings",
    "/analysis_data/compute",
    "/analysis_data/fields",
    "/custom_functions/formula_chain_check",
    "/custom_functions/formula_chain_instant_result",
    "/validate_db_settings",
    "/db_schemas",
    "/db_tables",
    "/templates/{template_id}/create",
    "/user_defined_components/save-custom-node",
    "/user_defined_components/preview-custom-node",
    "/user_defined_components/dry-run",
    "/user_defined_components/delete-custom-node/{file_name}",
    "/user_defined_components/rescan",
    "/user_defined_components/upload-icon",
    "/user_defined_components/delete-icon/{file_name}",
}
EDITOR_ROUTE_MODULES = {"flowfile_core.routes.routes", "flowfile_core.routes.user_defined_components"}


def test_every_mutating_editor_route_is_classified():
    mutating = {
        route.path
        for route in main.app.routes
        if getattr(getattr(route, "endpoint", None), "__module__", None) in EDITOR_ROUTE_MODULES
        and set(getattr(route, "methods", None) or ()) - {"GET", "HEAD", "OPTIONS"}
    }
    unclassified = mutating - GESTURE_ROUTES - NON_GRAPH_ROUTES
    assert not unclassified, f"classify these routes as graph gestures or not: {sorted(unclassified)}"
    assert GESTURE_ROUTES <= mutating, sorted(GESTURE_ROUTES - mutating)
    assert not GESTURE_ROUTES & NON_GRAPH_ROUTES


class TestOneStepPerGesture:
    def test_each_route_records_exactly_one_step(self, custom_node_type):
        api = new_flow(9201)
        seed_two_sources_and_filter(api)
        fid = api.flow_id

        def step(call):
            before, history = api.snapshot(), api.history()
            response = call()
            expect_one_step(api, response, before, history)
            return response

        step(lambda: api.add_node(4, "sample", 400, 0))
        step(lambda: api.connect(3, 4))
        step(lambda: api.settings("sample", _sample_payload(fid, 4, 3, 5)))
        step(lambda: api.layout({4: (450, 50)}))
        step(lambda: api.copy(3, 5, "filter", 200, 300))
        step(lambda: api.delete_connection(3, 4))
        step(lambda: api.delete_node(5))
        group_id = step(lambda: api.create_group([1, 2], "Sources")).json()["group"]["id"]
        step(lambda: api.update_group(group_id, name="Inputs"))
        step(lambda: api.group_add(group_id, [3]))
        step(lambda: api.group_remove([3]))
        comment_id = step(lambda: api.create_comment("hello", 10, 10)).json()["comment"]["id"]
        step(lambda: api.update_comment(comment_id, "hello world"))
        step(lambda: api.delete_comment(comment_id))
        step(lambda: api.description(3, "keeps rows with a=1"))
        step(lambda: api.reference(3, "only_ones"))
        step(lambda: api.delete_group(group_id))
        step(lambda: api.standard_layout())
        step(lambda: api.add_node(6, CUSTOM_NODE_TYPE, 600, 0))
        step(lambda: api.custom_settings(6, "abc", (600, 0)))
        step(
            lambda: api.apply(
                [
                    {"op": "add_node", "node_id": 7, "node_type": "sample", "pos_x": 10, "pos_y": 500},
                    {"op": "connect", "connection": _connection(3, 7)},
                    {"op": "update_settings", "node_type": "sample", "settings": _sample_payload(fid, 7, 3, 2)},
                    {"op": "update_layout", "layout": {"node_positions": [{"node_id": 7, "pos_x": 20, "pos_y": 520}]}},
                ],
                label="Drop sample on filter",
            )
        )
        assert api.history()["undo_description"] == "Drop sample on filter"
        comment_id = step(lambda: api.create_comment("tidy me")).json()["comment"]["id"]
        step(
            lambda: api.apply(
                [
                    {"op": "add_node", "node_id": 8, "node_type": "sample", "pos_x": 10, "pos_y": 400},
                    _insert_on_edge(8, 3, 7),
                    {"op": "delete_comment", "comment_id": comment_id},
                ],
                label="Splice and tidy",
            )
        )

    def test_repeating_an_identical_request_records_nothing(self, custom_node_type):
        api = new_flow(9202)
        seed_two_sources_and_filter(api)
        assert api.layout({3: (220, 20)}).status_code == 200
        assert api.description(3, "same").status_code == 200
        assert api.reference(3, "same_ref").status_code == 200
        created = api.create_group([1], "G").json()
        comment_id = api.create_comment("c").json()["comment"]["id"]
        assert api.add_node(6, CUSTOM_NODE_TYPE).status_code == 200
        assert api.custom_settings(6, "v").status_code == 200

        live_filter = api.flow.get_node(3).setting_input.model_dump(mode="json")
        repeats = [
            lambda: api.layout({3: (220, 20)}),
            lambda: api.layout({}),
            lambda: api.settings("filter", live_filter),
            lambda: api.description(3, "same"),
            lambda: api.reference(3, "same_ref"),
            lambda: api.update_group(created["group"]["id"], name="G"),
            lambda: api.group_add(created["group"]["id"], [1]),
            lambda: api.update_comment(comment_id, "c"),
            lambda: api.custom_settings(6, "v"),
            lambda: api.apply([{"op": "update_layout", "layout": {"node_positions": []}}]),
        ]
        for call in repeats:
            before, history = api.snapshot(), api.history()
            expect_no_step(api, call(), before, history)

    def test_record_history_false_applies_without_a_step(self):
        api = new_flow(9203)
        seed_two_sources_and_filter(api)
        before_history = api.history()
        response = api.layout({3: (999, 999)}, record_history=False)
        assert response.status_code == 200
        assert api.flow.get_node(3).setting_input.pos_x == 999
        assert api.history()["undo_count"] == before_history["undo_count"]


class TestFailedRequestsChangeNothing:
    @pytest.fixture
    def api_with_redo(self):
        api = new_flow(9210)
        seed_two_sources_and_filter(api)
        assert api.add_node(4, "sample", 400, 0).status_code == 200
        assert api.connect(3, 4).status_code == 200
        assert api.layout({4: (500, 10)}).status_code == 200
        assert api.undo().json()["success"] is True
        history = api.history()
        assert history["redo_count"] == 1
        return api

    def test_cycle_connect_is_rejected_atomically(self, api_with_redo):
        api = api_with_redo
        before, history = api.snapshot(), api.history()
        expect_no_step(api, api.connect(4, 3), before, history, status=422)

    def test_stale_delete_connection_is_rejected_atomically(self, api_with_redo):
        api = api_with_redo
        before, history = api.snapshot(), api.history()
        expect_no_step(api, api.delete_connection(2, 4), before, history, status=422)

    def test_delete_missing_node_is_404_and_atomic(self, api_with_redo):
        api = api_with_redo
        before, history = api.snapshot(), api.history()
        expect_no_step(api, api.delete_node(12345), before, history, status=404)

    def test_failing_batch_rolls_back_every_operation(self, api_with_redo):
        api = api_with_redo
        before, history = api.snapshot(), api.history()
        response = api.apply(
            [
                {"op": "add_node", "node_id": 20, "node_type": "sample", "pos_x": 0, "pos_y": 0},
                {"op": "connect", "connection": _connection(3, 20)},
                {"op": "connect", "connection": _connection(20, 3)},
            ]
        )
        expect_no_step(api, response, before, history, status=422)
        assert response.json()["detail"].startswith("Operation 2 (connect): ")
        assert api.flow.get_node(20) is None


class TestBatchOperations:
    @staticmethod
    def _rows(api: Api, node_id: int) -> list[dict]:
        return api.flow.get_node(node_id).get_resulting_data().collect().to_dicts()

    def test_insert_on_edge_keeps_positional_inputs_and_undoes_in_one_step(self):
        api = new_flow(9250)
        fid = api.flow_id
        for node_id, value in ((1, 1), (2, 2)):
            assert api.add_node(node_id, "manual_input").status_code == 200
            assert api.settings("manual_input", _manual_payload(fid, node_id, [{"a": value}])).status_code == 200
        polars_code = input_schema.NodePolarsCode(
            flow_id=fid,
            node_id=3,
            depending_on_ids=[1, 2],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="input_df_1"),
        )
        union = input_schema.NodeUnion(
            flow_id=fid, node_id=4, depending_on_ids=[1, 2], union_input=transform_schema.UnionInput(mode="relaxed")
        )
        for node_type, settings in (("polars_code", polars_code), ("union", union)):
            assert api.add_node(settings.node_id, node_type).status_code == 200
            assert api.connect(1, settings.node_id).status_code == 200
            assert api.connect(2, settings.node_id).status_code == 200
            assert api.settings(node_type, settings.model_dump(mode="json")).status_code == 200
        assert api.flow.run_graph().success is True
        results = {node_id: self._rows(api, node_id) for node_id in (3, 4)}
        before, history = api.snapshot(), api.history()

        response = api.apply(
            [
                {"op": "add_node", "node_id": 5, "node_type": "sample", "pos_x": 0, "pos_y": 0},
                _insert_on_edge(5, 1, 3),
                {"op": "add_node", "node_id": 6, "node_type": "sample", "pos_x": 0, "pos_y": 100},
                _insert_on_edge(6, 1, 4),
            ],
            label="Drop samples on edges",
        )

        expect_one_step(api, response, before, history)
        assert [n.node_id for n in api.flow.get_node(3).main_input] == [5, 2]
        assert [n.node_id for n in api.flow.get_node(4).main_input] == [6, 2]
        assert edge_bookkeeping_errors(api.flow) == []
        assert api.flow.run_graph().success is True
        assert {node_id: self._rows(api, node_id) for node_id in (3, 4)} == results

        assert api.undo().json()["success"] is True
        assert api.snapshot() == before

    def test_insert_on_a_missing_edge_rolls_back_the_batch(self):
        api = new_flow(9251)
        seed_two_sources_and_filter(api)
        before, history = api.snapshot(), api.history()

        response = api.apply(
            [
                {"op": "add_node", "node_id": 7, "node_type": "sample", "pos_x": 0, "pos_y": 0},
                _insert_on_edge(7, 1, 3, handle="output-1"),
            ]
        )

        expect_no_step(api, response, before, history, status=422)
        assert response.json()["detail"].startswith("Operation 1 (insert_on_edge): ")
        assert api.flow.get_node(7) is None

    def test_deleting_nodes_and_comments_together_is_one_step(self):
        api = new_flow(9252)
        seed_two_sources_and_filter(api)
        comment_id = api.create_comment("note").json()["comment"]["id"]
        before, history = api.snapshot(), api.history()

        response = api.apply(
            [{"op": "delete_node", "node_id": 3}, {"op": "delete_comment", "comment_id": comment_id}],
            label="Delete 1 node and 1 comment",
        )

        expect_one_step(api, response, before, history)
        assert api.snapshot()["comments"] == []
        assert api.undo().json()["success"] is True
        assert api.snapshot() == before


class TestTargetedRegressions:
    def test_paste_of_configured_node_is_one_step(self):
        api = new_flow(9220)
        seed_two_sources_and_filter(api)
        before, history = api.snapshot(), api.history()
        expect_one_step(api, api.copy(3, 9, "filter", 300, 300), before, history)
        assert api.history()["undo_count"] == 1

    def test_add_explore_data_is_one_step(self):
        api = new_flow(9221)
        seed_two_sources_and_filter(api)
        before, history = api.snapshot(), api.history()
        expect_one_step(api, api.add_node(9, "explore_data", 300, 300), before, history)

    def test_paste_resets_group_to_the_promise_group(self):
        api = new_flow(9222)
        seed_two_sources_and_filter(api)
        assert api.create_group([3], "Filters").status_code == 200
        assert api.copy(3, 9, "filter", 300, 300).status_code == 200
        assert api.flow.get_node(9).setting_input.group_id is None

    def test_settings_save_keeps_server_owned_layout_fields(self):
        api = new_flow(9223)
        seed_two_sources_and_filter(api)
        group_id = api.create_group([3], "Filters").json()["group"]["id"]
        assert api.layout({3: (321, 123)}).status_code == 200
        before, history = api.snapshot(), api.history()

        stale = _filter_payload(api.flow_id, 3, 1, "1")
        stale.update(pos_x=5, pos_y=6, group_id=None)
        expect_no_step(api, api.settings("filter", stale), before, history)

        live = api.flow.get_node(3).setting_input
        assert (live.pos_x, live.pos_y, live.group_id) == (321, 123, group_id)

    def test_mutation_responses_carry_the_flow_id(self, custom_node_type):
        api = new_flow(9224)
        seed_two_sources_and_filter(api)
        for response in (
            api.layout({3: (1, 1)}),
            api.description(3, "d"),
            api.reference(3, "r"),
            api.add_node(6, CUSTOM_NODE_TYPE),
            api.custom_settings(6, "x"),
            api.apply([{"op": "delete_node", "node_id": 6}]),
        ):
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["success"] is True
            assert body["history"]["flow_id"] == api.flow_id

    def test_undo_and_redo_always_return_history(self):
        api = new_flow(9225)
        nothing = api.undo()
        assert nothing.status_code == 200
        assert nothing.json()["success"] is False
        assert nothing.json()["history"]["flow_id"] == api.flow_id
        assert api.add_node(1, "filter").status_code == 200
        undone = api.undo().json()
        assert undone["success"] is True
        assert undone["history"]["can_redo"] is True
        redone = api.redo().json()
        assert redone["history"]["can_undo"] is True
        assert api.redo().json()["history"]["redo_count"] == 0

    def test_reference_the_settings_model_would_reject_is_refused(self):
        api = new_flow(9227)
        seed_two_sources_and_filter(api)
        before, history = api.snapshot(), api.history()
        expect_no_step(api, api.reference(3, "my-ref"), before, history, status=422)
        assert api.undo().json()["success"] is False

    def test_undo_returns_409_when_the_flow_is_busy(self, monkeypatch):
        api = new_flow(9226)
        assert api.add_node(1, "filter").status_code == 200
        monkeypatch.setattr(flow_graph_module, "EDIT_LOCK_TIMEOUT_SECONDS", 0.2)
        holding, release = threading.Event(), threading.Event()

        def hold_lock():
            with api.flow._edit_lock:
                holding.set()
                release.wait(5)

        holder = threading.Thread(target=hold_lock)
        holder.start()
        try:
            assert holding.wait(5)
            response = api.undo()
            assert response.status_code == 409
            assert response.json()["detail"] == "Flow is busy"
        finally:
            release.set()
            holder.join()
        assert api.undo().json()["success"] is True


class TestServerOwnedState:
    def test_flow_settings_post_keeps_identity_and_run_state(self):
        api = new_flow(9240, path="/tmp/owned.yaml")
        flow = api.flow
        flow.flow_settings.is_running = True
        flow.flow_settings.source_registration_id = 77
        stale = api.flow_settings()
        stale.pop("has_unsaved_changes", None)
        stale.pop("display_name", None)
        stale.update(
            is_running=False,
            is_canceled=True,
            name="renamed_behind_the_server",
            path="/elsewhere.yaml",
            source_registration_id=None,
            track_history=False,
            show_edge_labels=True,
            execution_mode="Performance",
        )

        assert client.post("/flow_settings", json=stale).status_code == 200

        live = flow.flow_settings
        assert (live.is_running, live.is_canceled, live.track_history) == (True, False, True)
        assert (live.name, live.path, live.source_registration_id) == ("history_routes_9240", "/tmp/owned.yaml", 77)
        assert (live.show_edge_labels, live.execution_mode) == (True, "Performance")
        live.is_running = False

    def test_delete_connection_on_a_missing_right_input_is_rejected(self):
        api = new_flow(9241)
        seed_two_sources_and_filter(api)
        before, history = api.snapshot(), api.history()
        expect_no_step(api, api.delete_connection(2, 3, input_class="input-1"), before, history, status=422)

    def test_reading_a_join_drawer_changes_nothing(self, tmp_path):
        path = tmp_path / "join_read.yaml"
        api = new_flow(9242, path=str(path))
        for node_id, rows in ((1, [{"a": 1, "x": 2}]), (2, [{"a": 1, "y": 3}])):
            assert api.add_node(node_id, "manual_input").status_code == 200
            assert api.settings("manual_input", _manual_payload(api.flow_id, node_id, rows)).status_code == 200
        assert api.add_node(4, "join").status_code == 200
        assert api.connect(1, 4, "input-0").status_code == 200
        assert api.connect(2, 4, "input-1").status_code == 200
        join = input_schema.NodeJoin(
            flow_id=api.flow_id,
            node_id=4,
            depending_on_ids=[1, 2],
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("a", "a")], left_select=[], right_select=[]
            ),
        )
        assert api.settings("join", join.model_dump(mode="json")).status_code == 200
        api.flow.save_flow(str(path))
        before, history = api.snapshot(), api.history()

        response = client.get("/node", params={"flow_id": api.flow_id, "node_id": 4, "include_output": False})

        assert response.status_code == 200
        shown = response.json()["setting_input"]["join_input"]["left_select"]["renames"]
        assert {column["old_name"] for column in shown} == {"a", "x"}
        assert api.snapshot() == before
        assert api.history() == history
        assert api.flow_settings()["has_unsaved_changes"] is False

    def test_node_data_reports_the_live_setup_state_not_the_proposal(self):
        api = new_flow(9244)
        fid = api.flow_id
        for node_id, rows in ((1, [{"a": 1, "x": 2}]), (2, [{"a": 1, "y": 3}])):
            assert api.add_node(node_id, "manual_input").status_code == 200
            assert api.settings("manual_input", _manual_payload(fid, node_id, rows)).status_code == 200
        proposals = {3: ("join", "join_input"), 4: ("cross_join", "cross_join_input"), 5: ("filter", "filter_input")}
        for node_id, (node_type, _) in proposals.items():
            assert api.add_node(node_id, node_type).status_code == 200
            assert api.connect(1, node_id).status_code == 200
            if node_type != "filter":
                assert api.connect(2, node_id, "input-1").status_code == 200
        assert api.add_node(6, "polars_code").status_code == 200
        assert api.connect(1, 6).status_code == 200
        before = api.snapshot()

        def shown(node_id: int) -> dict:
            response = client.get("/node", params={"flow_id": fid, "node_id": node_id, "include_output": False})
            assert response.status_code == 200, response.text
            return response.json()

        for node_id, (_, settings_field) in proposals.items():
            data = shown(node_id)
            assert data["is_setup"] is False
            assert settings_field in data["setting_input"]
            assert isinstance(api.flow.get_node(node_id).setting_input, input_schema.NodePromise)
        assert shown(6)["is_setup"] is False
        assert shown(1)["is_setup"] is True
        assert api.snapshot() == before

    def test_resaving_filter_settings_after_a_run_records_nothing(self, tmp_path):
        path = tmp_path / "filter_run.yaml"
        api = new_flow(9243, path=str(path))
        seed_two_sources_and_filter(api)
        loaded = api.flow.get_node(3).setting_input.model_dump(mode="json")
        api.flow.save_flow(str(path))

        assert api.flow.run_graph().success is True
        before, history = api.snapshot(), api.history()

        expect_no_step(api, api.settings("filter", loaded), before, history)
        assert api.flow_settings()["has_unsaved_changes"] is False


class TestDirtyState:
    def test_save_undo_redo_dirty_cycle(self, tmp_path):
        path = tmp_path / "dirty_routes.yaml"
        api = new_flow(9230, path=str(path))
        seed_two_sources_and_filter(api)
        assert api.add_node(4, "sample").status_code == 200
        api.flow.save_flow(str(path))
        assert api.flow_settings()["has_unsaved_changes"] is False

        assert api.undo().json()["success"] is True
        assert api.flow_settings()["has_unsaved_changes"] is True
        assert api.redo().json()["success"] is True
        assert api.flow_settings()["has_unsaved_changes"] is False

    def test_flow_settings_change_is_dirty_without_an_undo_step(self, tmp_path):
        path = tmp_path / "dirty_settings.yaml"
        api = new_flow(9231, path=str(path))
        seed_two_sources_and_filter(api)
        api.flow.save_flow(str(path))
        history = api.history()

        settings = api.flow_settings()
        settings["description"] = "changed description"
        settings.pop("has_unsaved_changes", None)
        settings.pop("display_name", None)
        assert client.post("/flow_settings", json=settings).status_code == 200

        assert api.flow_settings()["has_unsaved_changes"] is True
        assert api.history()["undo_count"] == history["undo_count"]


def _snapshot_edges(snapshot: dict) -> list[tuple[int, int, str]]:
    edges = []
    for node in snapshot["nodes"]:
        for src in node["input_ids"] or []:
            edges.append((src, node["id"], "input-0"))
        if node["right_input_id"] is not None:
            edges.append((node["right_input_id"], node["id"], "input-1"))
        if node["left_input_id"] is not None:
            edges.append((node["left_input_id"], node["id"], "input-2"))
    return edges


class GestureSession:
    """Random but mostly-valid editor gestures against one flow."""

    TRANSFORMS = ("filter", "sample", "sort", "select", "union", "join")

    def __init__(self, api: Api, rng: random.Random):
        self.api = api
        self.rng = rng
        self.next_id = 100
        self.next_ref = 0
        self.last_connect: tuple | None = None

    def _nodes(self) -> list[dict]:
        return self.api.snapshot()["nodes"]

    def _new_id(self) -> int:
        self.next_id += 1
        return self.next_id

    def gestures(self):
        return [
            self.add_node,
            self.add_node,
            self.configure,
            self.connect,
            self.connect,
            self.delete_connection,
            self.delete_node,
            self.move,
            self.paste,
            self.group,
            self.comment,
            self.describe,
            self.reference,
            self.batch,
            self.splice,
        ]

    def add_node(self):
        node_type = self.rng.choice(("manual_input",) + self.TRANSFORMS)
        return self.api.add_node(self._new_id(), node_type, self.rng.randint(0, 900), self.rng.randint(0, 900))

    def configure(self):
        nodes = [n for n in self._nodes() if n["type"] in ("manual_input", "filter", "sample")]
        if not nodes:
            return self.add_node()
        node = self.rng.choice(nodes)
        fid, nid = self.api.flow_id, node["id"]
        upstream = (node["input_ids"] or [-1])[0]
        if node["type"] == "manual_input":
            return self.api.settings("manual_input", _manual_payload(fid, nid, [{"a": self.rng.randint(0, 3)}]))
        if node["type"] == "filter":
            return self.api.settings("filter", _filter_payload(fid, nid, upstream, str(self.rng.randint(0, 3))))
        return self.api.settings("sample", _sample_payload(fid, nid, upstream, self.rng.randint(1, 4)))

    def connect(self):
        """Occupied inputs (a replace), repeats of the last connect and join sides included."""
        if self.last_connect is not None and self.rng.random() < 0.2:
            return self.api.connect(*self.last_connect)
        nodes = self._nodes()
        targets = [n for n in nodes if n["type"] != "manual_input"]
        if len(nodes) < 2 or not targets:
            return self.add_node()
        target = self.rng.choice(targets)
        source = self.rng.choice([n for n in nodes if n["id"] != target["id"]])
        input_class = self.rng.choice(("input-1", "input-2")) if target["type"] == "join" else "input-0"
        self.last_connect = (source["id"], target["id"], input_class)
        return self.api.connect(*self.last_connect)

    def delete_connection(self):
        edges = _snapshot_edges(self.api.snapshot())
        if not edges:
            return self.connect()
        src, dst, input_class = self.rng.choice(edges)
        return self.api.delete_connection(src, dst, input_class)

    def delete_node(self):
        nodes = self._nodes()
        if not nodes:
            return self.add_node()
        return self.api.delete_node(self.rng.choice(nodes)["id"])

    def move(self):
        nodes = self._nodes()
        if not nodes:
            return self.comment()
        picked = self.rng.sample(nodes, k=min(len(nodes), self.rng.randint(1, 3)))
        return self.api.layout({n["id"]: (self.rng.randint(0, 900), self.rng.randint(0, 900)) for n in picked})

    def paste(self):
        nodes = self._nodes()
        if not nodes:
            return self.add_node()
        node = self.rng.choice(nodes)
        return self.api.copy(node["id"], self._new_id(), node["type"], self.rng.randint(0, 900), 950)

    def group(self):
        nodes = self._nodes()
        if not nodes:
            return self.add_node()
        groups = self.api.snapshot()["groups"]
        if groups and self.rng.random() < 0.5:
            return self.api.group_add(self.rng.choice(groups)["id"], [self.rng.choice(nodes)["id"]])
        picked = self.rng.sample(nodes, k=min(len(nodes), 2))
        return self.api.create_group([n["id"] for n in picked], f"G{self.rng.randint(0, 99)}")

    def comment(self):
        comments = self.api.snapshot()["comments"]
        if comments and self.rng.random() < 0.5:
            return self.api.update_comment(self.rng.choice(comments)["id"], f"edited {self.rng.randint(0, 9)}")
        return self.api.create_comment(f"note {self.rng.randint(0, 9)}", self.rng.randint(0, 900), 0)

    def describe(self):
        nodes = self._nodes()
        if not nodes:
            return self.add_node()
        return self.api.description(self.rng.choice(nodes)["id"], self.rng.choice(("", "note A", "note B")))

    def reference(self):
        nodes = self._nodes()
        if not nodes:
            return self.add_node()
        self.next_ref += 1
        return self.api.reference(self.rng.choice(nodes)["id"], f"ref_{self.next_ref}")

    def batch(self):
        nodes = self._nodes()
        new_id = self._new_id()
        operations = [{"op": "add_node", "node_id": new_id, "node_type": "filter", "pos_x": 5, "pos_y": 5}]
        if nodes:
            operations.append({"op": "connect", "connection": _connection(self.rng.choice(nodes)["id"], new_id)})
        operations.append(
            {"op": "update_layout", "layout": {"node_positions": [{"node_id": new_id, "pos_x": 50, "pos_y": 60}]}}
        )
        comments = self.api.snapshot()["comments"]
        if comments and self.rng.random() < 0.5:
            operations.append({"op": "delete_comment", "comment_id": self.rng.choice(comments)["id"]})
        return self.api.apply(operations, label="Random batch")

    def splice(self):
        """Drop a new node onto an existing edge (the edge's own output handle included)."""
        snapshot = self.api.snapshot()
        edges = _snapshot_edges(snapshot)
        if not edges:
            return self.connect()
        src, dst, input_class = self.rng.choice(edges)
        source = next(n for n in snapshot["nodes"] if n["id"] == src)
        handle = source["output_handles"][source["outputs"].index(dst)]
        new_id = self._new_id()
        operations = [
            {"op": "add_node", "node_id": new_id, "node_type": self.rng.choice(("filter", "sample")), "pos_x": 5},
            _insert_on_edge(new_id, src, dst, input_class, handle),
        ]
        return self.api.apply(operations, label="Drop on edge")


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5, 6, 7, 8])
def test_random_gesture_sequences_obey_history_algebra(seed):
    api = new_flow(9300 + seed)
    session = GestureSession(api, random.Random(seed))
    initial = api.snapshot()
    changes = 0
    for _ in range(40):
        before, history = api.snapshot(), api.history()
        gesture = session.rng.choice(session.gestures())
        response = gesture()
        assert response.status_code < 500, f"{gesture.__name__}: {response.status_code} {response.text}"
        after = api.snapshot()
        now = api.history()
        assert edge_bookkeeping_errors(api.flow) == [], gesture.__name__
        if response.status_code >= 400 or after == before:
            assert after == before, f"{gesture.__name__} failed ({response.status_code}) but changed the graph"
            assert (now["undo_count"], now["redo_count"]) == (
                history["undo_count"],
                history["redo_count"],
            ), gesture.__name__
        else:
            changes += 1
            assert now["undo_count"] == history["undo_count"] + 1, (gesture.__name__, history, now)
            assert now["redo_count"] == 0
    final = api.snapshot()
    assert api.history()["undo_count"] == changes

    for _ in range(changes):
        assert api.undo().json()["success"] is True
    assert api.snapshot() == initial
    for _ in range(changes):
        assert api.redo().json()["success"] is True
    assert api.snapshot() == final
    assert edge_bookkeeping_errors(api.flow) == []
    assert getattr(api.flow, "_untracked_writes", None) == 0


def test_concurrent_undo_redo_and_mutations_keep_history_consistent():
    api = new_flow(9400)
    seed_two_sources_and_filter(api)
    seeded = api.snapshot()
    failures: list[str] = []

    def worker(thread_index: int):
        http = _client()
        worker_api = Api(api.flow_id, http)
        rng = random.Random(thread_index)
        for i in range(12):
            roll = rng.random()
            if roll < 0.25:
                response = worker_api.undo()
            elif roll < 0.4:
                response = worker_api.redo()
            elif roll < 0.75:
                response = worker_api.layout({rng.choice((1, 2, 3)): (rng.randint(0, 900), rng.randint(0, 900))})
            else:
                response = worker_api.add_node(1000 * (thread_index + 1) + i, "sample", i, i)
            if response.status_code >= 500 or response.status_code == 409:
                failures.append(f"thread {thread_index}: {response.status_code} {response.text}")

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not failures, failures

    final = api.snapshot()
    history = api.history()
    for _ in range(history["undo_count"]):
        assert api.undo().json()["success"] is True
    assert api.snapshot() == seeded
    for _ in range(history["undo_count"]):
        assert api.redo().json()["success"] is True
    assert api.snapshot() == final
    assert api.history()["redo_count"] == history["redo_count"]
