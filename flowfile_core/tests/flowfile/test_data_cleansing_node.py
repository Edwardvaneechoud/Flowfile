"""Graph-level integration tests for the data_cleansing node.

Run with:
    pytest flowfile_core/tests/flowfile/test_data_cleansing_node.py -v
"""
from typing import Literal

from flowfile_core.configs.node_store import check_if_has_default_setting
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.routes.routes import get_node_model
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import RunInformation
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS


def create_graph(flow_id: int = 1, execution_mode: Literal["Development", "Performance"] = "Development") -> FlowGraph:
    """Create a new FlowGraph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="test_flow", path=".", execution_mode=execution_mode)
    )
    return handler.get_flow(flow_id)


def make_raw_data(schema: dict[str, str], columns: dict[str, list]) -> input_schema.RawData:
    """Build RawData with explicit dtypes.

    `RawData.from_pylist` stringifies any column mixing nulls with numbers, which would
    hide the numeric-only rules under test, so the dtypes are pinned here instead.
    """
    return input_schema.RawData(
        columns=[input_schema.MinimalFieldInfo(name=name, data_type=dtype) for name, dtype in schema.items()],
        data=[columns[name] for name in schema],
    )


def add_manual_input(graph: FlowGraph, raw_data: input_schema.RawData, node_id: int = 1) -> FlowGraph:
    """Add a manual input node with data."""
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    )
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id, raw_data_format=raw_data)
    )
    return graph


def add_node_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    """Add a node promise."""
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def handle_run_info(run_info: RunInformation) -> None:
    """Check run info for errors."""
    if run_info is None:
        raise ValueError("Run info is None")
    if not run_info.success:
        errors = "errors:"
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f"\n node_id:{node_step.node_id}, error: {node_step.error}"
        raise ValueError(f"Graph should run successfully:\n{errors}")


def build_cleansing_graph(raw_data: input_schema.RawData, **cleansing_kwargs) -> FlowGraph:
    """Wire manual_input -> data_cleansing and configure the cleansing node."""
    graph = create_graph()
    add_manual_input(graph, raw_data, node_id=1)
    add_node_promise(graph, "data_cleansing", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_data_cleansing(
        input_schema.NodeDataCleansing(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            cleansing_input=transform_schema.DataCleansingInput(**cleansing_kwargs),
        )
    )
    return graph


def run_and_collect(graph: FlowGraph, node_id: int = 2) -> list[dict]:
    run_info = graph.run_graph()
    handle_run_info(run_info)
    return graph.get_node(node_id).get_resulting_data().collect().to_dicts()


def test_data_cleansing_defaults():
    graph = build_cleansing_graph(
        make_raw_data(
            {"name": "String", "score": "Int64"},
            {"name": ["  Alice  ", None], "score": [1, None]},
        )
    )
    assert run_and_collect(graph) == [
        {"name": "Alice", "score": 1},
        {"name": "", "score": 0},
    ]


def test_data_cleansing_removes_null_rows_and_columns():
    graph = build_cleansing_graph(
        make_raw_data(
            {"name": "String", "empty": "String", "score": "Int64"},
            {"name": ["Alice", None, "Bob"], "empty": [None, None, None], "score": [1, None, 3]},
        ),
        remove_null_rows=True,
        remove_null_columns=True,
    )
    assert run_and_collect(graph) == [
        {"name": "Alice", "score": 1},
        {"name": "Bob", "score": 3},
    ]


def test_data_cleansing_with_column_list():
    graph = build_cleansing_graph(
        make_raw_data(
            {"code": "String", "label": "String"},
            {"code": [" a-1 ", " c-3 "], "label": [" b-2 ", " d-4 "]},
        ),
        selection_mode="list",
        selected_columns=["code"],
        remove_punctuation=True,
        remove_numbers=True,
        case_mode="uppercase",
    )
    assert run_and_collect(graph) == [
        {"code": "A", "label": " b-2 "},
        {"code": "C", "label": " d-4 "},
    ]


def test_data_cleansing_predicted_schema_matches_input_schema():
    """Schema prediction runs `_func` on a 0-row placeholder and must not drop columns."""
    graph = build_cleansing_graph(
        make_raw_data(
            {"name": "String", "empty": "String", "score": "Int64"},
            {"name": ["Alice"], "empty": [None], "score": [1]},
        ),
        remove_null_rows=True,
        remove_null_columns=True,
    )
    predicted = [c.column_name for c in graph.get_node(2).schema]
    assert predicted == ["name", "empty", "score"]


def test_data_cleansing_node_settings_round_trip():
    graph = build_cleansing_graph(
        make_raw_data({"name": "String"}, {"name": [" a "]}),
        trim_whitespace=True,
        case_mode="uppercase",
    )
    settings = graph.get_node(2).setting_input
    assert isinstance(settings, input_schema.NodeDataCleansing)
    assert settings.cleansing_input.case_mode == "uppercase"
    assert settings.get_default_description() == (
        "nulls to blank, nulls to zero, trim whitespace, uppercase on all columns"
    )


def test_data_cleansing_is_configured_when_dropped_on_the_canvas():
    """Mirrors the drop path in `routes.py::add_node`.

    Without `data_cleansing` in `nodes_with_defaults` the dropped node stays
    `is_setup=False` and `run_graph` silently skips it (green run, fewer nodes).
    """
    assert check_if_has_default_setting("data_cleansing")
    graph = create_graph()
    add_manual_input(graph, make_raw_data({"name": "String"}, {"name": ["  Alice  "]}), node_id=1)
    add_node_promise(graph, "data_cleansing", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    node_model = get_node_model("node" + "data_cleansing".replace("_", ""))
    initial_settings = node_model(
        flow_id=graph.flow_id, node_id=2, cache_results=False, pos_x=0, pos_y=0, node_type="data_cleansing"
    )
    getattr(graph, "add_data_cleansing")(initial_settings)

    node = graph.get_node(2)
    assert node.is_setup
    assert node.setting_input.cleansing_input == transform_schema.DataCleansingInput()
    assert run_and_collect(graph) == [{"name": "Alice"}]


def test_generic_settings_dispatch_names_resolve():
    """`/update_settings/?node_type=data_cleansing` resolves by string convention."""
    graph = create_graph()
    assert callable(getattr(graph, "add_data_cleansing"))
    assert get_node_model("node" + "data_cleansing".replace("_", "")) is input_schema.NodeDataCleansing
    assert NODE_TYPE_TO_SETTINGS_CLASS["data_cleansing"] is input_schema.NodeDataCleansing


def test_data_cleansing_yaml_roundtrip(tmp_path):
    graph = build_cleansing_graph(
        make_raw_data({"code": "String"}, {"code": [" a-1 "]}),
        remove_null_rows=True,
        remove_punctuation=True,
        case_mode="lowercase",
        selection_mode="list",
        selected_columns=["code"],
    )
    yaml_path = tmp_path / "data_cleansing.yaml"
    graph.save_flow(str(yaml_path))

    loaded = open_flow(yaml_path)
    loaded_settings = loaded.get_node(2).setting_input
    assert isinstance(loaded_settings, input_schema.NodeDataCleansing)
    assert loaded_settings.cleansing_input == graph.get_node(2).setting_input.cleansing_input
