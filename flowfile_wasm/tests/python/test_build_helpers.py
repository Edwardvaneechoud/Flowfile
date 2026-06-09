"""Unit tests for the pure `build_*` helpers.

Each helper takes (LazyFrame, settings) and returns a LazyFrame with no global
state. Settings shapes mirror `toPythonJson(node.settings)` from flow-store.ts.
"""
import polars as pl
import pytest

import engine


def lf(**cols):
    return pl.LazyFrame(cols)


# --------------------------------------------------------------------------- #
# build_filter
# --------------------------------------------------------------------------- #
def test_filter_equals_casts_to_column_dtype():
    out = engine.build_filter(
        lf(id=[1, 2, 3], name=["a", "b", "c"]),
        {"filter_input": {"basic_filter": {"field": "id", "operator": "equals", "value": "2"}}},
    ).collect()
    assert out["name"].to_list() == ["b"]


def test_filter_greater_than():
    out = engine.build_filter(
        lf(id=[1, 2, 3]),
        {"filter_input": {"basic_filter": {"field": "id", "operator": "greater_than", "value": "1"}}},
    ).collect()
    assert out["id"].to_list() == [2, 3]


def test_filter_in_splits_and_casts_list():
    out = engine.build_filter(
        lf(status=["active", "pending", "closed"]),
        {"filter_input": {"basic_filter": {"field": "status", "operator": "in", "value": "active, closed"}}},
    ).collect()
    assert sorted(out["status"].to_list()) == ["active", "closed"]


def test_filter_between_inclusive():
    out = engine.build_filter(
        lf(n=[1, 5, 10, 20]),
        {"filter_input": {"basic_filter": {"field": "n", "operator": "between", "value": "5", "value2": "10"}}},
    ).collect()
    assert out["n"].to_list() == [5, 10]


def test_filter_contains():
    out = engine.build_filter(
        lf(name=["alice", "bob", "carol"]),
        {"filter_input": {"basic_filter": {"field": "name", "operator": "contains", "value": "a"}}},
    ).collect()
    assert out["name"].to_list() == ["alice", "carol"]


def test_filter_advanced_expr_is_evaluated():
    out = engine.build_filter(
        lf(a=[1, 2, 3], b=[10, 20, 30]),
        {"filter_input": {"mode": "advanced", "advanced_filter": "pl.col('b') > 15"}},
    ).collect()
    assert out["a"].to_list() == [2, 3]


def test_filter_without_field_is_passthrough():
    out = engine.build_filter(lf(a=[1, 2]), {"filter_input": {"basic_filter": {}}}).collect()
    assert out["a"].to_list() == [1, 2]


# --------------------------------------------------------------------------- #
# build_select
# --------------------------------------------------------------------------- #
def test_select_keeps_drops_and_reorders_by_position():
    out = engine.build_select(
        lf(a=[1], b=[2], c=[3]),
        {"select_input": [
            {"old_name": "c", "new_name": "c", "keep": True, "position": 0},
            {"old_name": "a", "new_name": "a", "keep": True, "position": 1},
            {"old_name": "b", "new_name": "b", "keep": False, "position": 2},
        ]},
    ).collect()
    assert out.columns == ["c", "a"]


def test_select_renames():
    out = engine.build_select(
        lf(a=[1], b=[2]),
        {"select_input": [{"old_name": "a", "new_name": "alpha", "keep": True, "position": 0}]},
    ).collect()
    assert out.columns == ["alpha"]


def test_select_ignores_missing_columns():
    out = engine.build_select(
        lf(a=[1]),
        {"select_input": [
            {"old_name": "a", "new_name": "a", "keep": True, "position": 0},
            {"old_name": "ghost", "new_name": "ghost", "keep": True, "position": 1},
        ]},
    ).collect()
    assert out.columns == ["a"]


# --------------------------------------------------------------------------- #
# build_group_by / _build_agg_exprs
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("agg,expected", [
    ("sum", 6),
    ("max", 3),
    ("min", 1),
    ("mean", 2.0),
    ("count", 3),
    ("n_unique", 3),
])
def test_group_by_aggregations(agg, expected):
    out = engine.build_group_by(
        lf(g=["x", "x", "x"], v=[1, 2, 3]),
        {"groupby_input": {"agg_cols": [
            {"old_name": "g", "new_name": "g", "agg": "groupby"},
            {"old_name": "v", "new_name": "v_agg", "agg": agg},
        ]}},
    ).collect()
    assert out["v_agg"].to_list() == [expected]


def test_group_by_concat_joins_with_comma():
    out = engine.build_group_by(
        lf(g=["x", "x"], v=["a", "b"]),
        {"groupby_input": {"agg_cols": [
            {"old_name": "g", "new_name": "g", "agg": "groupby"},
            {"old_name": "v", "new_name": "v_cat", "agg": "concat"},
        ]}},
    ).collect()
    assert out["v_cat"].to_list() == ["a,b"]


def test_group_by_renames_key_and_aggregates():
    out = engine.build_group_by(
        lf(cat=["x", "x", "y"], amount=[10, 20, 30]),
        {"groupby_input": {"agg_cols": [
            {"old_name": "cat", "new_name": "category", "agg": "groupby"},
            {"old_name": "amount", "new_name": "total", "agg": "sum"},
        ]}},
    ).sort("category").collect()
    assert out.columns == ["category", "total"]
    assert out["total"].to_list() == [30, 30]


# --------------------------------------------------------------------------- #
# build_join
# --------------------------------------------------------------------------- #
def test_join_inner():
    out = engine.build_join(
        lf(id=[1, 2, 3], lval=["a", "b", "c"]),
        lf(id=[2, 3, 4], rval=["x", "y", "z"]),
        {"join_input": {"join_type": "inner", "join_mapping": [{"left_col": "id", "right_col": "id"}]}},
    ).sort("id").collect()
    assert out["id"].to_list() == [2, 3]
    assert out["rval"].to_list() == ["x", "y"]


def test_join_left_fills_nulls():
    out = engine.build_join(
        lf(id=[1, 2], lval=["a", "b"]),
        lf(id=[2], rval=["x"]),
        {"join_input": {"join_type": "left", "join_mapping": [{"left_col": "id", "right_col": "id"}]}},
    ).sort("id").collect()
    assert out["rval"].to_list() == [None, "x"]


def test_join_missing_left_column_raises():
    with pytest.raises(ValueError, match="Left columns not found"):
        engine.build_join(
            lf(id=[1]), lf(id=[1]),
            {"join_input": {"join_mapping": [{"left_col": "ghost", "right_col": "id"}]}},
        )


def test_join_without_mapping_raises():
    with pytest.raises(ValueError, match="No join columns"):
        engine.build_join(lf(id=[1]), lf(id=[1]), {"join_input": {"join_mapping": []}})


# --------------------------------------------------------------------------- #
# build_sort / build_unique / build_head / build_unpivot
# --------------------------------------------------------------------------- #
def test_sort_descending():
    out = engine.build_sort(lf(n=[3, 1, 2]), {"sort_input": [{"column": "n", "how": "desc"}]}).collect()
    assert out["n"].to_list() == [3, 2, 1]


def test_sort_multi_column_mixed_direction():
    out = engine.build_sort(
        lf(a=[1, 1, 2], b=[2, 1, 3]),
        {"sort_input": [{"column": "a", "how": "asc"}, {"column": "b", "how": "desc"}]},
    ).collect()
    assert out["b"].to_list() == [2, 1, 3]


def test_unique_on_subset_keeps_first():
    out = engine.build_unique(
        lf(k=[1, 1, 2], v=["a", "b", "c"]),
        {"unique_input": {"subset": ["k"], "keep": "first", "maintain_order": True}},
    ).collect()
    assert out["k"].to_list() == [1, 2]
    assert out["v"].to_list() == ["a", "c"]


def test_unique_full_row():
    out = engine.build_unique(
        lf(a=[1, 1, 2], b=["x", "x", "y"]), {"unique_input": {}},
    ).sort("a").collect()
    assert out.to_dict(as_series=False) == {"a": [1, 2], "b": ["x", "y"]}


def test_head_limits_rows():
    out = engine.build_head(lf(n=list(range(100))), {"head_input": {"n": 5}}).collect()
    assert out["n"].to_list() == [0, 1, 2, 3, 4]


def test_unpivot_wide_to_long():
    out = engine.build_unpivot(
        lf(id=[1, 2], q1=[10, 30], q2=[20, 40]),
        {"unpivot_input": {"index_columns": ["id"], "value_columns": ["q1", "q2"]}},
    ).collect()
    assert set(out.columns) == {"id", "variable", "value"}
    assert out.height == 4


def test_unpivot_missing_value_column_raises():
    with pytest.raises(ValueError, match="Columns not found"):
        engine.build_unpivot(
            lf(id=[1], q1=[10]),
            {"unpivot_input": {"index_columns": ["id"], "value_columns": ["ghost"]}},
        )
