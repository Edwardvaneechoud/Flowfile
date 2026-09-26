"""FlowFrame.explode_hierarchy: a native explode_hierarchy node whose result matches polars-grouper."""

import os

os.environ["TESTING"] = "True"

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from polars_grouper import hierarchy_levels, hierarchy_paths, hierarchy_totals

import flowfile_frame as ff
from flowfile_core.schemas.input_schema import NodeExplodeHierarchy

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
BOM = {name: list(values) for name, values in zip(("assembly", "component", "qty"), zip(*BOM_LINES))}
FUNCTIONS = {"totals": hierarchy_totals, "levels": hierarchy_levels, "paths": hierarchy_paths}


def _bom() -> ff.FlowFrame:
    return ff.from_dict(BOM)


def _reference(output_detail: str = "totals", quantity: str | None = "qty", **kwargs) -> pl.DataFrame:
    qty = pl.col(quantity) if quantity else None
    function = FUNCTIONS[output_detail]
    return (
        pl.LazyFrame(BOM)
        .select(function(pl.col("assembly"), pl.col("component"), qty, **kwargs).alias("h"))
        .unnest("h")
        .collect()
    )


def test_explode_hierarchy_adds_a_native_node():
    out = _bom().explode_hierarchy(
        "assembly", "component", "qty", output_detail="levels", top_level_only=True, description="MRP levels"
    )
    node = out.flow_graph.get_node(out.node_id)
    assert node.node_type == "explode_hierarchy"
    assert isinstance(node.setting_input, NodeExplodeHierarchy)
    settings = node.setting_input.explode_hierarchy_input
    assert (settings.parent_column, settings.child_column, settings.quantity_column) == ("assembly", "component", "qty")
    assert (settings.output_detail, settings.top_level_only, settings.include_self, settings.max_depth) == (
        "levels",
        True,
        False,
        None,
    )
    assert node.setting_input.description == "MRP levels"
    assert "polars_code" not in [n.node_type for n in out.flow_graph.nodes]


@pytest.mark.parametrize(
    "output_detail,quantity,kwargs",
    [
        ("totals", "qty", {}),
        ("totals", None, {}),
        ("totals", "qty", {"top_level_only": True}),
        ("levels", "qty", {"top_level_only": True, "include_self": True}),
        ("paths", "qty", {"top_level_only": True}),
        ("paths", "qty", {"max_depth": 1}),
        ("levels", "qty", {"max_depth": 0, "include_self": True}),
    ],
)
def test_explode_hierarchy_matches_polars_grouper(output_detail, quantity, kwargs):
    out = _bom().explode_hierarchy("assembly", "component", quantity, output_detail=output_detail, **kwargs)
    assert_frame_equal(out.collect(), _reference(output_detail, quantity, **kwargs))


def test_explode_hierarchy_bike_factory_totals():
    totals = _bom().explode_hierarchy("assembly", "component", "qty", top_level_only=True).collect()
    assert totals.height == 21
    assert totals["is_leaf"].sum() == 13
    screws = totals.filter(pl.col("descendant") == "screw").sort("ancestor")
    assert screws.select("ancestor", "quantity").rows() == [("bike", 20.0), ("ebike", 26.0)]


def test_explode_hierarchy_schema_resolves_before_collect(monkeypatch):
    source = _bom()
    calls = []
    real_collect = pl.LazyFrame.collect

    def spy(self, *args, **kwargs):
        calls.append(1)
        return real_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", spy)
    out = source.explode_hierarchy("assembly", "component", "qty", output_detail="paths")
    schema = out.collect_schema()
    assert calls == []
    assert list(schema.items()) == [
        ("ancestor", pl.String),
        ("descendant", pl.String),
        ("level", pl.UInt32),
        ("parent", pl.String),
        ("quantity_per", pl.Float64),
        ("quantity", pl.Float64),
        ("is_leaf", pl.Boolean),
        ("path", pl.List(pl.String)),
    ]


def test_explode_hierarchy_small_integer_ids_are_widened():
    edges = pl.DataFrame({"parent": [1, 1, 2], "child": [2, 3, 3]}, schema={"parent": pl.Int16, "child": pl.Int16})
    out = ff.FlowFrame(edges).explode_hierarchy("parent", "child")
    expected = (
        edges.lazy()
        .select(hierarchy_totals(pl.col("parent").cast(pl.Int64), pl.col("child").cast(pl.Int64)).alias("h"))
        .unnest("h")
        .collect()
    )
    assert_frame_equal(out.collect(), expected)


@pytest.mark.parametrize(
    "kwargs",
    [{"max_depth": -1}, {"output_detail": "tree"}],
    ids=["negative_max_depth", "unknown_output_detail"],
)
def test_explode_hierarchy_rejects_invalid_settings(kwargs):
    source = _bom()
    node_count = len(source.flow_graph.nodes)
    with pytest.raises(ValueError):
        source.explode_hierarchy("assembly", "component", "qty", **kwargs)
    assert len(source.flow_graph.nodes) == node_count


def test_explode_hierarchy_cycle_raises_on_collect():
    cyclic = ff.from_dict({"parent": ["a", "b", "c", "c"], "child": ["b", "c", "a", "d"], "qty": [1.0] * 4})
    out = cyclic.explode_hierarchy("parent", "child", "qty")
    assert out.columns == ["ancestor", "descendant", "level", "quantity", "is_leaf"]
    with pytest.raises(pl.exceptions.ComputeError, match="a -> b -> c -> a"):
        out.collect()
