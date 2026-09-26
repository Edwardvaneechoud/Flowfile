"""Graph-level integration tests for the explode_hierarchy node.

The bike-factory recipes mirror polars-grouper's own end-to-end tests, run through a flow instead.

Run with:
    pytest flowfile_core/tests/flowfile/test_explode_hierarchy_node.py -v
"""

from typing import Literal

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from polars_grouper import hierarchy_paths, hierarchy_totals
from pydantic import ValidationError

from flowfile_core.configs.node_store import node_dict
from flowfile_core.flowfile.flow_data_engine.hierarchy import (
    HIERARCHY_FUNCTIONS,
    explode_hierarchy_frame,
    hierarchy_function_name,
    hierarchy_node_id_cast,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.routes.routes import get_node_model
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import RunInformation
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS

BOM_LINES = [
    ("bike", "frame", 1.0),
    ("bike", "wheel", 2.0),
    ("bike", "screw", 10.0),
    ("bike", "handlebar", 1.0),
    ("ebike", "frame", 1.0),
    ("ebike", "wheel", 2.0),
    ("ebike", "battery", 1.0),
    ("ebike", "motor", 1.0),
    ("ebike", "screw", 12.0),
    ("frame", "steel_tube", 3.5),
    ("frame", "screw", 6.0),
    ("wheel", "rim", 1.0),
    ("wheel", "spoke", 32.0),
    ("wheel", "tyre", 1.0),
    ("wheel", "screw", 2.0),
    ("rim", "aluminium", 0.75),
    ("handlebar", "steel_tube", 0.5),
    ("handlebar", "grip", 2.0),
    ("motor", "copper_wire", 12.0),
    ("motor", "screw", 4.0),
]
BOM_SCHEMA = {"assembly": "String", "component": "String", "qty": "Float64", "plant": "String"}

TOTALS_COLUMNS = [
    ("ancestor", "String"),
    ("descendant", "String"),
    ("level", "UInt32"),
    ("quantity", "Float64"),
    ("is_leaf", "Boolean"),
]


def create_graph(flow_id: int = 1, execution_location: Literal["local", "remote"] = "local") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="test_flow",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def make_raw_data(schema: dict[str, str], columns: dict[str, list]) -> input_schema.RawData:
    """Build RawData with explicit dtypes, so id columns keep the integer or categorical type under test."""
    return input_schema.RawData(
        columns=[input_schema.MinimalFieldInfo(name=name, data_type=dtype) for name, dtype in schema.items()],
        data=[columns[name] for name in schema],
    )


def bom_raw_data() -> input_schema.RawData:
    assembly, component, qty = (list(col) for col in zip(*BOM_LINES))
    return make_raw_data(
        BOM_SCHEMA,
        {"assembly": assembly, "component": component, "qty": qty, "plant": ["NL01"] * len(BOM_LINES)},
    )


def edges_raw_data(
    parents: list, children: list, dtype: str = "String", qty: list | None = None
) -> input_schema.RawData:
    schema = {"parent": dtype, "child": dtype}
    columns = {"parent": parents, "child": children}
    if qty is not None:
        schema["qty"] = "Float64"
        columns["qty"] = qty
    return make_raw_data(schema, columns)


def build_graph(
    raw_data: input_schema.RawData,
    execution_location: Literal["local", "remote"] = "local",
    **settings,
) -> FlowGraph:
    """Wire manual_input -> explode_hierarchy and configure the hierarchy node."""
    graph = create_graph(execution_location=execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=1, raw_data_format=raw_data))
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="explode_hierarchy"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_explode_hierarchy(
        input_schema.NodeExplodeHierarchy(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            explode_hierarchy_input=transform_schema.ExplodeHierarchyInput(**settings),
        )
    )
    return graph


def build_bom_graph(execution_location: Literal["local", "remote"] = "local", **settings) -> FlowGraph:
    return build_graph(
        bom_raw_data(),
        execution_location,
        parent_column="assembly",
        child_column="component",
        **{"quantity_column": "qty", **settings},
    )


def handle_run_info(run_info: RunInformation) -> None:
    if run_info is None:
        raise ValueError("Run info is None")
    if not run_info.success:
        errors = "errors:"
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f"\n node_id:{node_step.node_id}, error: {node_step.error}"
        raise ValueError(f"Graph should run successfully:\n{errors}")


def run_and_collect(graph: FlowGraph, node_id: int = 2) -> pl.DataFrame:
    handle_run_info(graph.run_graph())
    return graph.get_node(node_id).get_resulting_data().collect()


def predicted(graph: FlowGraph, node_id: int = 2) -> list[tuple[str, str]]:
    return [(c.column_name, c.data_type) for c in graph.get_node(node_id).get_predicted_schema()]


@pytest.fixture
def remote_execution() -> None:
    """Skip unless worker offload is genuinely active (same guard as the `execution_location` fixture)."""
    from flowfile_core.schemas.schemas import is_valid_execution_location_in_current_global_settings
    from tests.conftest import is_worker_running

    if not (is_worker_running() and is_valid_execution_location_in_current_global_settings("remote")):
        pytest.skip("Remote execution not active")


def test_node_template_is_registered_in_the_palette():
    template = node_dict["explode_hierarchy"]
    assert template.name == "Explode hierarchy"
    assert template.item == "explode_hierarchy"
    assert (template.input, template.output) == (1, 1)
    assert template.node_group == "combine"
    assert template.node_type == "process"
    assert template.transform_type == "other"
    assert template.image == "explode_hierarchy.svg"
    assert template.laziness == "lazy"
    assert template.drawer_title == "Explode Hierarchy"
    assert "bill of materials" in template.drawer_intro
    assert "chart of accounts" in template.drawer_intro
    assert template.multi is False
    assert {"hierarchy", "bom", "bill of materials"} <= {tag.value for tag in template.tags}


def test_generic_settings_dispatch_names_resolve():
    """`/update_settings/?node_type=explode_hierarchy` resolves by string convention."""
    graph = create_graph()
    node_type = "explode_hierarchy"
    assert callable(getattr(graph, f"add_{node_type}"))
    assert get_node_model("node" + node_type.replace("_", "")) is input_schema.NodeExplodeHierarchy
    assert NODE_TYPE_TO_SETTINGS_CLASS[node_type] is input_schema.NodeExplodeHierarchy


def test_settings_defaults():
    settings = transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c")
    assert settings.quantity_column is None
    assert settings.output_detail == "totals"
    assert (settings.top_level_only, settings.include_self, settings.max_depth) == (False, False, None)


def test_negative_max_depth_is_rejected():
    with pytest.raises(ValidationError):
        transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c", max_depth=-1)
    assert transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c", max_depth=0).max_depth == 0


def test_max_depth_beyond_the_plugin_limit_is_rejected():
    """The plugin decodes max_depth as a u32; a larger value would only fail at run time."""
    with pytest.raises(ValidationError):
        transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c", max_depth=2**32)
    limit = transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c", max_depth=2**32 - 1)
    assert limit.max_depth == 2**32 - 1


def test_unknown_output_detail_is_rejected():
    with pytest.raises(ValidationError):
        transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c", output_detail="tree")


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"quantity_column": "qty"}, "assembly -> component (totals, qty)"),
        ({"output_detail": "paths"}, "assembly -> component (paths)"),
    ],
)
def test_default_description(settings, expected):
    node = input_schema.NodeExplodeHierarchy(
        flow_id=1,
        node_id=2,
        explode_hierarchy_input=transform_schema.ExplodeHierarchyInput(
            parent_column="assembly", child_column="component", **settings
        ),
    )
    assert node.get_default_description() == expected


def test_yaml_round_trip(tmp_path):
    graph = build_bom_graph(output_detail="levels", top_level_only=True, include_self=True, max_depth=3)
    yaml_path = tmp_path / "explode_hierarchy.yaml"
    graph.save_flow(str(yaml_path))

    loaded = open_flow(yaml_path)
    loaded_settings = loaded.get_node(2).setting_input
    assert isinstance(loaded_settings, input_schema.NodeExplodeHierarchy)
    assert loaded_settings.explode_hierarchy_input == graph.get_node(2).setting_input.explode_hierarchy_input
    level_rows, product_self_rows = 23, 2
    assert run_and_collect(loaded).height == level_rows + product_self_rows


PATHS_COLUMNS = [
    ("ancestor", "{id}"),
    ("descendant", "{id}"),
    ("level", "UInt32"),
    ("parent", "{id}"),
    ("quantity_per", "Float64"),
    ("quantity", "Float64"),
    ("is_leaf", "Boolean"),
    ("path", "List({id})"),
]
TOTALS_TEMPLATE = [("ancestor", "{id}"), ("descendant", "{id}"), *TOTALS_COLUMNS[2:]]


@pytest.mark.parametrize("output_detail", ["totals", "levels", "paths"])
@pytest.mark.parametrize(
    "id_dtype,ids,expected_id_dtype",
    [
        ("String", ["a", "a", "b"], "String"),
        ("Int64", [1, 1, 2], "Int64"),
        ("Int16", [1, 1, 2], "Int64"),
        ("Categorical", ["a", "a", "b"], "String"),
    ],
)
def test_predicted_schema_before_running(output_detail, id_dtype, ids, expected_id_dtype):
    children = ["b", "c", "d"] if isinstance(ids[0], str) else [2, 3, 4]
    graph = build_graph(
        edges_raw_data(ids, children, dtype=id_dtype, qty=[1.0, 2.0, 3.0]),
        parent_column="parent",
        child_column="child",
        quantity_column="qty",
        output_detail=output_detail,
    )
    template = PATHS_COLUMNS if output_detail == "paths" else TOTALS_TEMPLATE
    assert predicted(graph) == [(name, dtype.format(id=expected_id_dtype)) for name, dtype in template]


def test_input_columns_are_not_carried_through():
    graph = build_bom_graph()
    assert predicted(graph) == TOTALS_COLUMNS
    assert run_and_collect(graph).columns == [name for name, _ in TOTALS_COLUMNS]


def test_prediction_and_result_never_collect_in_core(monkeypatch):
    graph = build_bom_graph(output_detail="paths")
    original = pl.LazyFrame.collect
    calls = []

    def spy(self, *args, **kwargs):
        calls.append(1)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", spy)
    schema = graph.get_node(2).get_predicted_schema()
    result = graph.get_node(2).get_resulting_data()
    monkeypatch.undo()

    assert calls == []
    assert [c.column_name for c in schema][-1] == "path"
    assert isinstance(result.data_frame, pl.LazyFrame)


def test_totals_explode_every_assembly(execution_location):
    result = run_and_collect(build_bom_graph(execution_location))
    assert result.height == 33
    assert [(name, str(dtype)) for name, dtype in result.schema.items()] == TOTALS_COLUMNS
    where_used = result.filter(pl.col("descendant") == "spoke").select("ancestor", "quantity")
    assert sorted(where_used.rows()) == [("bike", 64.0), ("ebike", 64.0), ("wheel", 32.0)]


def test_purchase_requirements_for_a_production_plan(execution_location):
    exploded = run_and_collect(build_bom_graph(execution_location, top_level_only=True))
    assert exploded.height == 21
    assert exploded["is_leaf"].sum() == 13
    screws = exploded.filter(pl.col("descendant") == "screw").select("ancestor", "quantity")
    assert sorted(screws.rows()) == [("bike", 20.0), ("ebike", 26.0)]

    plan = pl.DataFrame({"product": ["bike", "ebike"], "units": [100, 50]})
    stock = pl.DataFrame({"part": ["screw", "tyre", "battery"], "on_hand": [1000, 150, 10]})
    to_buy = (
        exploded.filter(pl.col("is_leaf"))
        .join(plan, left_on="ancestor", right_on="product")
        .group_by(part="descendant")
        .agg(needed=(pl.col("quantity") * pl.col("units")).sum())
        .join(stock, on="part", how="left")
        .select("part", to_buy=pl.col("needed") - pl.col("on_hand").fill_null(0))
    )
    assert dict(to_buy.iter_rows()) == {
        "screw": 100 * 20 + 50 * 26 - 1000,
        "spoke": 150 * 64,
        "tyre": 150 * 2 - 150,
        "steel_tube": 100 * 4.0 + 50 * 3.5,
        "aluminium": 150 * 1.5,
        "grip": 100 * 2,
        "battery": 50 - 10,
        "copper_wire": 50 * 12,
    }


def test_levels_give_mrp_low_level_codes(execution_location):
    levels = run_and_collect(
        build_bom_graph(execution_location, output_detail="levels", top_level_only=True, include_self=True)
    )
    codes = levels.group_by(item="descendant").agg(low_level_code=pl.col("level").max())
    assert dict(codes.iter_rows()) == {
        "bike": 0,
        "ebike": 0,
        "frame": 1,
        "wheel": 1,
        "handlebar": 1,
        "battery": 1,
        "motor": 1,
        "rim": 2,
        "spoke": 2,
        "tyre": 2,
        "steel_tube": 2,
        "grip": 2,
        "copper_wire": 2,
        "screw": 2,
        "aluminium": 3,
    }


def test_levels_row_count(execution_location):
    graph = build_bom_graph(execution_location, output_detail="levels", top_level_only=True)
    assert run_and_collect(graph).height == 23


def test_paths_give_an_indented_bom(execution_location):
    paths = run_and_collect(build_bom_graph(execution_location, output_detail="paths", top_level_only=True))
    assert paths.height == 27
    lines = paths.filter(pl.col("ancestor") == "bike").select("level", "descendant", "quantity_per")
    report = ["  " * (level - 1) + f"{part} x{qty:g}" for level, part, qty in lines.iter_rows()]
    assert report == [
        "frame x1",
        "  steel_tube x3.5",
        "  screw x6",
        "wheel x2",
        "  rim x1",
        "    aluminium x0.75",
        "  spoke x32",
        "  tyre x1",
        "  screw x2",
        "screw x10",
        "handlebar x1",
        "  steel_tube x0.5",
        "  grip x2",
    ]


def test_paths_trace_where_a_quantity_comes_from(execution_location):
    paths = run_and_collect(build_bom_graph(execution_location, output_detail="paths"))
    routes = paths.filter(pl.col("ancestor") == "bike", pl.col("descendant") == "screw").select(
        route=pl.col("path").list.join(" > "), quantity="quantity"
    )
    assert routes.rows() == [
        ("bike > frame > screw", 6.0),
        ("bike > wheel > screw", 4.0),
        ("bike > screw", 10.0),
    ]


def test_max_depth_stops_at_direct_components(execution_location):
    result = run_and_collect(build_bom_graph(execution_location, top_level_only=True, max_depth=1))
    assert result.height == 9
    assert result["level"].unique().to_list() == [1]
    assert result.filter(pl.col("ancestor") == "bike", pl.col("descendant") == "screw")["quantity"].item() == 10.0
    assert result.filter(pl.col("descendant") == "frame")["is_leaf"].to_list() == [False, False]


def test_without_quantity_every_edge_counts_once(execution_location):
    result = run_and_collect(build_bom_graph(execution_location, quantity_column=None, top_level_only=True))
    screws = result.filter(pl.col("descendant") == "screw").select("ancestor", "quantity")
    assert sorted(screws.rows()) == [("bike", 3.0), ("ebike", 4.0)]


def test_general_ledger_roll_up_keeps_integer_accounts(execution_location):
    chart = make_raw_data(
        {"parent_account": "Int64", "account": "Int64"},
        {"parent_account": [1000, 1000, 1100, 1100, 1200], "account": [1100, 1200, 1110, 1120, 1210]},
    )
    closure = run_and_collect(
        build_graph(
            chart, execution_location, parent_column="parent_account", child_column="account", include_self=True
        )
    )
    assert closure.schema["ancestor"] == pl.Int64
    assert closure.schema["descendant"] == pl.Int64
    journal = pl.DataFrame({"account": [1110, 1110, 1120, 1210], "amount": [100, 50, 30, 500]})
    balances = (
        closure.join(journal, left_on="descendant", right_on="account")
        .group_by(account="ancestor")
        .agg(pl.col("amount").sum())
    )
    assert dict(balances.iter_rows()) == {1000: 680, 1100: 180, 1110: 150, 1120: 30, 1200: 500, 1210: 500}


@pytest.mark.parametrize("qty_dtype", ["Int32", "Int16"])
def test_integer_quantity_is_rolled_up_as_float(execution_location, qty_dtype):
    raw = make_raw_data(
        {"parent": "String", "child": "String", "qty": qty_dtype},
        {
            "parent": ["car", "car", "wheel", "wheel"],
            "child": ["wheel", "screw", "screw", "tyre"],
            "qty": [4, 20, 5, 1],
        },
    )
    result = run_and_collect(
        build_graph(raw, execution_location, parent_column="parent", child_column="child", quantity_column="qty")
    )
    assert result.schema["quantity"] == pl.Float64
    assert sorted(result.select("ancestor", "descendant", "quantity").rows()) == [
        ("car", "screw", 40.0),
        ("car", "tyre", 4.0),
        ("car", "wheel", 4.0),
        ("wheel", "screw", 5.0),
        ("wheel", "tyre", 1.0),
    ]


CYCLIC = {"parents": ["a", "b", "c", "c"], "children": ["b", "c", "a", "d"], "qty": [1.0] * 4}


def test_cycle_still_predicts_a_schema_and_fails_on_collect():
    graph = build_graph(
        edges_raw_data(CYCLIC["parents"], CYCLIC["children"], qty=CYCLIC["qty"]),
        parent_column="parent",
        child_column="child",
        quantity_column="qty",
    )
    assert predicted(graph) == TOTALS_COLUMNS
    lf = graph.get_node(2).get_resulting_data().data_frame
    with pytest.raises(pl.exceptions.ComputeError, match="a -> b -> c -> a"):
        lf.collect()


def test_cycle_fails_the_node_in_remote_development_mode(remote_execution):
    graph = build_graph(
        edges_raw_data(CYCLIC["parents"], CYCLIC["children"], qty=CYCLIC["qty"]),
        "remote",
        parent_column="parent",
        child_column="child",
        quantity_column="qty",
    )
    run_info = graph.run_graph()
    assert run_info.success is False
    failed = [step for step in run_info.node_step_result if not step.success]
    assert [step.node_id for step in failed] == [2]
    assert "cycle" in failed[0].error
    assert "a -> b -> c -> a" in failed[0].error


def test_self_loop_is_a_cycle():
    graph = build_graph(edges_raw_data(["a", "b"], ["b", "b"]), parent_column="parent", child_column="child")
    with pytest.raises(pl.exceptions.ComputeError, match="b -> b"):
        graph.get_node(2).get_resulting_data().data_frame.collect()


def test_empty_input_gives_an_empty_table(execution_location):
    graph = build_graph(
        edges_raw_data([], [], qty=[]),
        execution_location,
        parent_column="parent",
        child_column="child",
        quantity_column="qty",
    )
    assert predicted(graph) == TOTALS_COLUMNS
    result = run_and_collect(graph)
    assert result.height == 0
    assert result.columns == [name for name, _ in TOTALS_COLUMNS]


def test_missing_column_fails_the_node():
    graph = build_graph(edges_raw_data(["a"], ["b"]), parent_column="zzz", child_column="child")
    run_info = graph.run_graph()
    assert run_info.success is False
    failed = [step for step in run_info.node_step_result if not step.success]
    assert [step.node_id for step in failed] == [2]
    assert 'unable to find column "zzz"' in failed[0].error


def test_function_table_covers_every_output_detail():
    assert set(HIERARCHY_FUNCTIONS) == {"totals", "levels", "paths"}
    assert [hierarchy_function_name(d) for d in ("totals", "levels", "paths")] == [
        "hierarchy_totals",
        "hierarchy_levels",
        "hierarchy_paths",
    ]


@pytest.mark.parametrize(
    "dtype,expected",
    [
        (pl.String, None),
        (pl.Int32, None),
        (pl.Int64, None),
        (pl.UInt32, None),
        (pl.UInt64, None),
        (pl.Int8, pl.Int64),
        (pl.Int16, pl.Int64),
        (pl.UInt8, pl.Int64),
        (pl.UInt16, pl.Int64),
        (pl.Int128, pl.String),
        (pl.UInt128, pl.String),
        (pl.Categorical(), pl.String),
        (pl.Enum(["a"]), pl.String),
        (pl.Decimal(10, 2), pl.String),
        (pl.Float64, pl.String),
        (pl.Date, pl.String),
        (pl.Datetime("us", "UTC"), pl.String),
        (pl.Boolean, pl.String),
        (pl.Null, pl.String),
    ],
)
def test_node_id_cast_rule(dtype, expected):
    assert hierarchy_node_id_cast(dtype) == expected


EDGES = {"p": [1, 1, 2, 2], "c": [2, 3, 4, 5], "q": [2.0, 3.0, 4.0, 5.0]}
SETTINGS = transform_schema.ExplodeHierarchyInput(parent_column="p", child_column="c", quantity_column="q")
PATH_SETTINGS = SETTINGS.model_copy(update={"output_detail": "paths"})


def _edges(dtype: pl.DataType) -> pl.LazyFrame:
    ids = pl.col("p", "c")
    if dtype.base_type() in (pl.Categorical, pl.Enum):
        ids = ids.cast(pl.String)
    return pl.LazyFrame(EDGES).with_columns(ids.cast(dtype))


@pytest.mark.parametrize("settings", [SETTINGS, PATH_SETTINGS], ids=["totals", "paths"])
@pytest.mark.parametrize("dtype", [pl.Int8, pl.Int16, pl.UInt8, pl.UInt16])
def test_small_integer_ids_explode_like_int64(dtype, settings):
    """The plugin panics on these; widened they give exactly what Int64 ids give."""
    expected = explode_hierarchy_frame(_edges(pl.Int64), settings).collect()
    assert_frame_equal(explode_hierarchy_frame(_edges(dtype), settings).collect(), expected)


@pytest.mark.parametrize("settings", [SETTINGS, PATH_SETTINGS], ids=["totals", "paths"])
@pytest.mark.parametrize(
    "dtype",
    [pl.Int128, pl.UInt128, pl.Categorical(), pl.Enum(["1", "2", "3", "4", "5"]), pl.Decimal(10, 0)],
    ids=["Int128", "UInt128", "Categorical", "Enum", "Decimal"],
)
def test_unsupported_ids_explode_like_strings(dtype, settings):
    """The plugin panics on these; as strings they give exactly what a String input gives."""
    expected = explode_hierarchy_frame(_edges(dtype).with_columns(pl.col("p", "c").cast(pl.String)), settings)
    assert_frame_equal(explode_hierarchy_frame(_edges(dtype), settings).collect(), expected.collect())


def _mixed(parent: pl.DataType, child: pl.DataType) -> pl.LazyFrame:
    return pl.LazyFrame(EDGES).with_columns(pl.col("p").cast(parent), pl.col("c").cast(child))


@pytest.mark.parametrize("function", [hierarchy_totals, hierarchy_paths])
@pytest.mark.parametrize(
    "lf",
    [
        _edges(pl.Float64),
        pl.LazyFrame({"p": [True, True], "c": [False, False], "q": [2.0, 3.0]}),
        _edges(pl.Date),
        _edges(pl.Datetime("us")),
        _mixed(pl.Int64, pl.String),
        _mixed(pl.Int32, pl.Int64),
    ],
    ids=["Float64", "Boolean", "Date", "Datetime", "Int64-String", "Int32-Int64"],
)
def test_cast_matches_the_plugin_where_it_already_stringifies(lf, function):
    settings = SETTINGS.model_copy(update={"output_detail": "paths" if function is hierarchy_paths else "totals"})
    plugin = lf.select(function("p", "c", "q").alias("h")).unnest("h").collect()
    assert plugin.schema["ancestor"] == pl.String
    assert_frame_equal(explode_hierarchy_frame(lf, settings).collect(), plugin)


def test_timezone_aware_ids_work_as_strings():
    """The plugin cannot format tz-aware datetimes itself; the String cast can."""
    lf = _edges(pl.Datetime("us")).with_columns(pl.col("p", "c").dt.replace_time_zone("UTC"))
    result = explode_hierarchy_frame(lf, SETTINGS).collect()
    assert result.schema["ancestor"] == pl.String
    assert result.height == 6


def test_missing_column_is_polars_column_not_found():
    lf = explode_hierarchy_frame(pl.LazyFrame(EDGES), SETTINGS.model_copy(update={"parent_column": "nope"}))
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        lf.collect_schema()


@pytest.mark.parametrize("qty_dtype", [pl.Int16, pl.Int8, pl.Decimal(10, 2), pl.Float32])
def test_quantity_is_cast_to_float(qty_dtype):
    lf = pl.LazyFrame(EDGES).with_columns(pl.col("q").cast(qty_dtype))
    result = explode_hierarchy_frame(lf, SETTINGS).collect()
    expected = explode_hierarchy_frame(pl.LazyFrame(EDGES), SETTINGS).collect()
    assert_frame_equal(result, expected)


def test_null_quantity_names_the_edge():
    lf = pl.LazyFrame({"p": ["a", "b"], "c": ["b", "c"], "q": [1.0, None]})
    with pytest.raises(pl.exceptions.ComputeError, match="`quantity` is null for the edge b -> c"):
        explode_hierarchy_frame(lf, SETTINGS).collect()


def test_text_quantity_fails_on_the_cast():
    lf = pl.LazyFrame({"p": ["a"], "c": ["b"], "q": ["x"]})
    with pytest.raises(pl.exceptions.InvalidOperationError):
        explode_hierarchy_frame(lf, SETTINGS).collect()


def test_null_edges_are_skipped():
    lf = pl.LazyFrame({"p": ["a", None, "a"], "c": ["b", "c", None], "q": [2.0, 1.0, 1.0]})
    assert explode_hierarchy_frame(lf, SETTINGS).collect().rows() == [("a", "b", 1, 2.0, True)]
