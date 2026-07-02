"""End-to-end tests for the run_flow node (subflow execution)."""

import uuid
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import subflow
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.schemas import FlowParameter

# Helpers


def make_graph(flow_id: int, name: str = "run_flow_test") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name=name,
            path=".",
            execution_mode="Development",
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def register_flow_file(path: Path, name: str) -> int:
    with get_db_context() as db:
        reg = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()),
            name=name,
            flow_path=str(path),
            owner_id=1,
        )
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.id


def run_and_assert_ok(graph: FlowGraph):
    result = graph.run_graph()
    assert result is not None
    if not result.success:
        errors = "\n".join(f"  node {r.node_id}: {r.error}" for r in result.node_step_result if not r.success)
        raise AssertionError(f"Flow failed:\n{errors}")
    return result


def run_and_expect_failure(graph: FlowGraph) -> str:
    result = graph.run_graph()
    assert result is not None and result.success is False, "expected the run to fail"
    return "\n".join(r.error or "" for r in result.node_step_result if not r.success)


def customer_rows(n: int) -> input_schema.RawData:
    return input_schema.RawData.from_pylist([{"name": f"c{i}", "rank": i} for i in range(1, n + 1)])


def connect(graph: FlowGraph, from_id: int, to_id: int, target_handle: str, source_handle: str = "output-0"):
    add_connection(
        graph,
        input_schema.NodeConnection(
            input_connection=input_schema.NodeInputConnection(node_id=to_id, connection_class=target_handle),
            output_connection=input_schema.NodeOutputConnection(node_id=from_id, connection_class=source_handle),
        ),
    )


# Fixtures: subflow files on disk + catalog registrations


@pytest.fixture()
def head_subflow(tmp_path):
    """Subflow: flow_input 'customers' -> polars_code head(${limit}) -> flow_output 'result'
    plus record_count -> flow_output 'row_count'. Sample data: 4 rows."""
    graph = make_graph(301, "head_subflow")
    graph.flow_settings.parameters = [FlowParameter(name="limit", default_value="10", type="integer")]
    add_promise(graph, 1, "flow_input")
    graph.add_flow_input(
        input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=1, input_name="customers", raw_data_format=customer_rows(4))
    )
    add_promise(graph, 2, "polars_code")
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="output_df = input_df.head(${limit})"),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    add_promise(graph, 3, "flow_output")
    graph.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=graph.flow_id, node_id=3, output_name="result", depending_on_id=2)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    add_promise(graph, 4, "record_count")
    graph.add_record_count(input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=4, depending_on_id=2))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 4))
    add_promise(graph, 5, "flow_output")
    graph.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=graph.flow_id, node_id=5, output_name="row_count", depending_on_id=4)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(4, 5))

    path = tmp_path / "head_subflow.yaml"
    graph.save_flow(str(path))
    registration_id = register_flow_file(path, "head_subflow")
    return {"path": path, "registration_id": registration_id}


@pytest.fixture()
def two_port_subflow(tmp_path):
    """Subflow with two independent lanes: a -> out_a, b -> out_b."""
    graph = make_graph(302, "two_port_subflow")
    add_promise(graph, 1, "flow_input")
    graph.add_flow_input(input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=1, input_name="a"))
    add_promise(graph, 2, "flow_input")
    graph.add_flow_input(input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=2, input_name="b"))
    add_promise(graph, 3, "flow_output")
    graph.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=graph.flow_id, node_id=3, output_name="out_a", depending_on_id=1)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3))
    add_promise(graph, 4, "flow_output")
    graph.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=graph.flow_id, node_id=4, output_name="out_b", depending_on_id=2)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 4))

    path = tmp_path / "two_port_subflow.yaml"
    graph.save_flow(str(path))
    registration_id = register_flow_file(path, "two_port_subflow")
    return {"path": path, "registration_id": registration_id}


def head_run_flow_settings(flow_id: int, node_id: int, registration_id: int, **overrides) -> input_schema.NodeRunFlow:
    defaults = dict(
        flow_id=flow_id,
        node_id=node_id,
        user_id=1,
        flow_reference=input_schema.SubflowReference(registration_id=registration_id),
        input_slots=["customers"],
        output_slots=["result", "row_count"],
        parameter_bindings=[],
        iteration_mode="first_value",
        append_run_metadata=True,
    )
    defaults.update(overrides)
    return input_schema.NodeRunFlow(**defaults)


def build_parent_with_data(graph: FlowGraph, registration_id: int, n_rows: int = 10, **overrides):
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=1, raw_data_format=customer_rows(n_rows))
    )
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(head_run_flow_settings(graph.flow_id, 9, registration_id, **overrides))
    connect(graph, 1, 9, "input-1")
    return graph.get_node(9)


# Tests


def test_single_run_with_constant_param(head_subflow):
    graph = make_graph(401)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        n_rows=10,
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="constant", constant_value="3")
        ],
    )
    run_and_assert_ok(graph)
    result = node.get_output("output-0").collect()
    assert result.height == 3
    assert set(result.columns) == {"name", "rank"}
    row_count = node.get_output("output-1").collect()
    assert row_count["number_of_records"][0] == 3


def test_unbound_param_uses_subflow_default(head_subflow):
    graph = make_graph(402)
    node = build_parent_with_data(graph, head_subflow["registration_id"], n_rows=15)
    run_and_assert_ok(graph)
    # subflow default limit=10
    assert node.get_output("output-0").collect().height == 10


def test_run_without_any_connections_uses_sample_data(head_subflow):
    graph = make_graph(403)
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        head_run_flow_settings(
            graph.flow_id,
            9,
            head_subflow["registration_id"],
            parameter_bindings=[
                input_schema.RunFlowParameterBinding(parameter_name="limit", source="constant", constant_value="2")
            ],
        )
    )
    run_and_assert_ok(graph)
    assert graph.get_node(9).get_output("output-0").collect().height == 2


def test_first_value_mode_uses_first_row(head_subflow):
    graph = make_graph(404)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        n_rows=10,
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")
        ],
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{"limit": 7}, {"limit": 9}]),
        )
    )
    connect(graph, 2, 9, "input-0")
    run_and_assert_ok(graph)
    result = node.get_output("output-0").collect()
    assert result.height == 7
    assert "run_index" not in result.columns


def test_iterate_concatenates_runs_with_metadata(head_subflow):
    graph = make_graph(405)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        n_rows=10,
        iteration_mode="iterate",
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")
        ],
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}, {"limit": 2}]),
        )
    )
    connect(graph, 2, 9, "input-0")
    run_and_assert_ok(graph)
    result = node.get_output("output-0").collect()
    assert result.height == 3  # head(1) + head(2)
    assert "param_limit" in result.columns
    assert "run_index" in result.columns
    assert sorted(result["run_index"].to_list()) == [1, 2, 2]
    assert result.filter(pl.col("run_index") == 2)["param_limit"].to_list() == [2, 2]

    row_counts = node.get_output("output-1").collect()
    assert sorted(row_counts["number_of_records"].to_list()) == [1, 2]


def test_iterate_without_metadata_toggle(head_subflow):
    graph = make_graph(406)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        n_rows=10,
        iteration_mode="iterate",
        append_run_metadata=False,
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")
        ],
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}, {"limit": 2}]),
        )
    )
    connect(graph, 2, 9, "input-0")
    run_and_assert_ok(graph)
    result = node.get_output("output-0").collect()
    assert result.height == 3
    assert "run_index" not in result.columns
    assert "param_limit" not in result.columns


def test_iterate_with_zero_rows_produces_empty_outputs(head_subflow):
    graph = make_graph(407)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        n_rows=5,
        iteration_mode="iterate",
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")
        ],
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="limit", data_type="Int64")], data=[[]]
            ),
        )
    )
    connect(graph, 2, 9, "input-0")
    run_and_assert_ok(graph)
    assert node.get_output("output-0").collect().height == 0


def test_missing_mapped_column_fails_with_clear_error(head_subflow):
    graph = make_graph(408)
    build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="nope")
        ],
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=2, raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}])
        )
    )
    connect(graph, 2, 9, "input-0")
    errors = run_and_expect_failure(graph)
    assert "missing mapped column" in errors


def test_zero_output_subflow_returns_run_summary(tmp_path):
    graph = make_graph(409, "no_output_subflow")
    add_promise(graph, 1, "flow_input")
    graph.add_flow_input(
        input_schema.NodeFlowInput(flow_id=graph.flow_id, node_id=1, input_name="data", raw_data_format=customer_rows(2))
    )
    path = tmp_path / "no_output_subflow.yaml"
    graph.save_flow(str(path))
    registration_id = register_flow_file(path, "no_output_subflow")

    parent = make_graph(410)
    add_promise(parent, 9, "run_flow")
    parent.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=parent.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=registration_id),
            input_slots=["data"],
            output_slots=[],
        )
    )
    run_and_assert_ok(parent)
    summary = parent.get_node(9).get_resulting_data().collect()
    assert summary.height == 1
    assert "run_index" in summary.columns
    assert summary["success"].to_list() == [True]


def test_two_port_routing_and_order_independent_connections(two_port_subflow, tmp_path):
    graph = make_graph(411)
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"side": "A"}])
        )
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{"side": "B"}, {"side": "B"}]),
        )
    )
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=two_port_subflow["registration_id"]),
            input_slots=["a", "b"],
            output_slots=["out_a", "out_b"],
        )
    )
    # Connect slot b (input-2) BEFORE slot a (input-1): routing must be keyed, not order-based.
    connect(graph, 2, 9, "input-2")
    connect(graph, 1, 9, "input-1")
    run_and_assert_ok(graph)
    node = graph.get_node(9)
    assert node.get_output("output-0").collect().height == 1  # out_a <- A (1 row)
    assert node.get_output("output-1").collect().height == 2  # out_b <- B (2 rows)

    # Save/reload: keyed edges must reattach to the same handles.
    parent_path = tmp_path / "parent_keyed.yaml"
    graph.save_flow(str(parent_path))
    reloaded = open_flow(parent_path)
    rnode = reloaded.get_node(9)
    keyed = rnode.node_inputs.keyed_inputs
    assert keyed is not None
    assert keyed["input-1"].node_id == 1
    assert keyed["input-2"].node_id == 2
    edges = rnode.get_edge_input()
    assert {(e.source, e.targetHandle) for e in edges} == {("1", "input-1"), ("2", "input-2")}
    run_and_assert_ok(reloaded)
    assert reloaded.get_node(9).get_output("output-1").collect().height == 2


def test_remap_on_slot_change_drops_vanished_and_follows_renames(two_port_subflow):
    graph = make_graph(412)
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"x": 1}])
        )
    )
    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=2, raw_data_format=input_schema.RawData.from_pylist([{"x": 2}])
        )
    )
    base = dict(
        flow_id=graph.flow_id,
        node_id=9,
        user_id=1,
        flow_reference=input_schema.SubflowReference(registration_id=two_port_subflow["registration_id"]),
        output_slots=["out_a", "out_b"],
    )
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(input_schema.NodeRunFlow(input_slots=["a", "b"], **base))
    connect(graph, 1, 9, "input-1")
    connect(graph, 2, 9, "input-2")

    # New interface: 'a' vanished, 'b' moved to slot 0.
    graph.add_run_flow(input_schema.NodeRunFlow(input_slots=["b"], **base))
    node = graph.get_node(9)
    keyed = node.node_inputs.keyed_inputs
    assert list(keyed.keys()) == ["input-1"]
    assert keyed["input-1"].node_id == 2
    # Node 1's edge was dropped entirely.
    assert all(lead.node_id != 9 for lead in graph.get_node(1).leads_to_nodes)


def test_recursion_guard(tmp_path):
    path = tmp_path / "self_ref.yaml"
    path.write_text("placeholder")  # ensure the file exists for registration
    registration_id = register_flow_file(path, "self_ref")

    graph = make_graph(413, "self_ref")
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=registration_id),
            input_slots=[],
            output_slots=[],
        )
    )
    graph.save_flow(str(path))

    parent = open_flow(path)
    errors = run_and_expect_failure(parent)
    assert "recursion" in errors.lower()


def test_depth_cap(head_subflow, monkeypatch):
    monkeypatch.setattr(subflow, "MAX_SUBFLOW_DEPTH", 0)
    graph = make_graph(414)
    build_parent_with_data(graph, head_subflow["registration_id"])
    errors = run_and_expect_failure(graph)
    assert "depth" in errors.lower()


def test_dangling_registration_fails_clearly(tmp_path):
    graph = make_graph(415)
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=99999999),
            input_slots=[],
            output_slots=[],
        )
    )
    errors = run_and_expect_failure(graph)
    assert "no longer exists" in errors


def test_stale_slots_prompt_resync(head_subflow):
    graph = make_graph(416)
    build_parent_with_data(graph, head_subflow["registration_id"], input_slots=["renamed_port"])
    errors = run_and_expect_failure(graph)
    assert "re-sync" in errors


def test_canceled_parent_aborts_before_running(head_subflow):
    graph = make_graph(417)
    node = build_parent_with_data(graph, head_subflow["registration_id"])
    graph.flow_settings.is_canceled = True
    settings = node.setting_input
    with pytest.raises(Exception, match="cancel"):
        subflow.execute_run_flow_node(graph, settings, None, (None,))


def test_schema_prediction_matches_subflow_outputs(head_subflow):
    graph = make_graph(418)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        iteration_mode="iterate",
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")
        ],
    )
    predicted = node.get_predicted_schema(force=True)
    names = [c.column_name for c in predicted]
    assert "name" in names and "rank" in names
    assert "param_limit" in names and "run_index" in names
    assert "output-1" in node._named_schemas
    second = [c.column_name for c in node._named_schemas["output-1"]]
    assert "number_of_records" in second


def test_interface_cache_invalidates_on_mtime_change(head_subflow):
    import os

    path = head_subflow["path"]
    first = subflow.get_subflow_interface(path)
    assert [p.name for p in first.inputs] == ["customers"]
    # Same mtime -> cached object identity
    assert subflow.get_subflow_interface(path) is first
    os.utime(path, (path.stat().st_atime + 5, path.stat().st_mtime + 5))
    assert subflow.get_subflow_interface(path) is not first


def test_connect_validation_rejects_out_of_range_and_occupied(head_subflow):
    from fastapi import HTTPException

    graph = make_graph(419)
    node = build_parent_with_data(graph, head_subflow["registration_id"])
    del node
    add_promise(graph, 3, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=3, raw_data_format=input_schema.RawData.from_pylist([{"x": 1}])
        )
    )
    with pytest.raises(HTTPException) as excinfo:
        connect(graph, 3, 9, "input-5")  # only 1 slot -> input-5 out of range
    assert excinfo.value.status_code == 422
    with pytest.raises(HTTPException) as excinfo:
        connect(graph, 3, 9, "input-1")  # already occupied by node 1
    assert excinfo.value.status_code == 422


def test_node_input_serialization_carries_handle_names(head_subflow):
    """The frontend derives run_flow handles from input_names/output_names."""
    graph = make_graph(420)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        parameter_specs=[FlowParameter(name="limit", default_value="10", type="integer")],
    )
    node_input = node.get_node_input()
    assert node_input.input_names == ["Parameters", "customers"]
    assert node_input.output_names == ["result", "row_count"]
    assert node_input.dynamic_inputs is True

    # A flow_output-less template stays None-labeled for static nodes.
    manual = graph.get_node(1).get_node_input()
    assert manual.input_names is None


def test_node_list_exposes_new_templates():
    from fastapi.testclient import TestClient

    from flowfile_core import main

    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
        response = client.get("/node_list", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    by_item = {n["item"]: n for n in response.json()}
    assert by_item["run_flow"]["dynamic_inputs"] is True
    assert by_item["run_flow"]["input"] == 1 and by_item["run_flow"]["output"] == 1
    assert by_item["flow_input"]["input"] == 0 and by_item["flow_input"]["output"] == 1
    assert by_item["flow_output"]["input"] == 1 and by_item["flow_output"]["output"] == 0
    assert by_item["union"].get("dynamic_inputs") is False


def test_node_data_main_input_is_param_connection_only(head_subflow):
    """Settings panel column dropdown must reflect the input-0 (param) connection, not data slots."""
    graph = make_graph(421)
    node = build_parent_with_data(graph, head_subflow["registration_id"])  # only input-1 connected
    data = node.get_node_data(flow_id=graph.flow_id, include_output=False)
    assert data.main_input is None

    add_promise(graph, 2, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=2, raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}])
        )
    )
    connect(graph, 2, 9, "input-0")
    data = node.get_node_data(flow_id=graph.flow_id, include_output=False)
    assert data.main_input is not None
    assert [c.name for c in data.main_input.table_schema] == ["limit"]


def test_input_names_marker_hides_param_handle_without_parameters(two_port_subflow):
    """A subflow without parameters serializes an empty index-0 label (hidden param handle)."""
    graph = make_graph(422)
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=two_port_subflow["registration_id"]),
            input_slots=["a", "b"],
            output_slots=["out_a", "out_b"],
        )
    )
    node_input = graph.get_node(9).get_node_input()
    assert node_input.input_names == ["", "a", "b"]


def test_iterate_constants_only_appends_metadata_matching_prediction(head_subflow):
    """Execution must append metadata whenever prediction promises it (no param edge needed)."""
    graph = make_graph(423)
    node = build_parent_with_data(
        graph,
        head_subflow["registration_id"],
        n_rows=10,
        iteration_mode="iterate",
        parameter_bindings=[
            input_schema.RunFlowParameterBinding(parameter_name="limit", source="constant", constant_value="4")
        ],
    )
    run_and_assert_ok(graph)
    result = node.get_output("output-0").collect()
    assert result.height == 4
    assert result["run_index"].to_list() == [1, 1, 1, 1]
    assert result["param_limit"].to_list() == [4, 4, 4, 4]
    assert result.schema["param_limit"] == pl.Int64
    assert result.schema["run_index"] == pl.UInt32


def test_load_with_over_range_keyed_edge_keeps_valid_edges(two_port_subflow, tmp_path):
    """A stale over-range keyed edge is dropped on load without breaking valid edges from the same source."""
    import yaml

    graph = make_graph(424)
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"x": 1}])
        )
    )
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=two_port_subflow["registration_id"]),
            input_slots=["a", "b"],
            output_slots=["out_a", "out_b"],
        )
    )
    connect(graph, 1, 9, "input-1")
    connect(graph, 1, 9, "input-2")  # same source on two handles
    parent_path = tmp_path / "over_range_parent.yaml"
    graph.save_flow(str(parent_path))

    data = yaml.safe_load(parent_path.read_text())
    run_flow_node = next(n for n in data["nodes"] if n["type"] == "run_flow")
    run_flow_node["input_connections"][1]["input_handle"] = "input-7"  # over-range vs 2 slots
    parent_path.write_text(yaml.dump(data, sort_keys=False))

    reloaded = open_flow(parent_path)
    rnode = reloaded.get_node(9)
    assert list(rnode.node_inputs.keyed_inputs.keys()) == ["input-1"]
    # The valid edge's lead must survive the dropped over-range entry.
    assert any(lead.node_id == 9 for lead in reloaded.get_node(1).leads_to_nodes)
    run_and_assert_ok(reloaded)
