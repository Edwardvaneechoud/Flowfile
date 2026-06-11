"""Tests for the data-free schema-propagation pass (`propagate_schemas`).

Covers two fixes:
  * Self-contained `polars_code`/`formula` START nodes (no inputs) resolve their
    output schema by running the code on empty frames — so downstream columns
    appear WITHOUT a run.
  * A `known_schemas` fallback seeds nodes the pass can't build (build errors,
    pivot) from their last-known schema, so downstream doesn't freeze on a stale
    schema until a re-run.
"""
import engine


def _names(schema):
    return [c["name"] for c in schema]


def _types(schema):
    return {c["name"]: c["data_type"] for c in schema}


# polars_code(start, generator) -> group_by(two aggs on column_0) -> filter(column_0_mean)
GEN_GRAPH = {
    "order": [5, 8, 9],
    "nodes": {
        "5": {
            "type": "polars_code",
            "input_ids": [],
            "left": None,
            "right": None,
            "settings": {"polars_code_input": {"polars_code": "output_df = pl.LazyFrame([i for i in range(5)])"}},
        },
        "8": {
            "type": "group_by",
            "input_ids": [5],
            "left": None,
            "right": None,
            "settings": {
                "groupby_input": {
                    "agg_cols": [
                        {"old_name": "column_0", "new_name": "column_0_mean", "agg": "mean"},
                        {"old_name": "column_0", "new_name": "column_0_sum", "agg": "sum"},
                    ]
                }
            },
        },
        "9": {
            "type": "filter",
            "input_ids": [8],
            "left": None,
            "right": None,
            "settings": {"filter_input": {"basic_filter": {"field": "column_0_mean", "operator": "less_than", "value": "22222"}}},
        },
    },
}


def test_self_contained_start_node_resolves_without_run():
    """A generator polars_code start node resolves on its own, and the two-agg
    group_by + filter see both columns — no run, no known schema needed.
    (Reproduces the reported failure where column_0_sum was missing downstream.)"""
    res = engine.propagate_schemas(GEN_GRAPH, {})

    assert res["5"]["schema_resolved"] is True
    assert _names(res["5"]["schema"]) == ["column_0"]

    assert res["8"]["schema_resolved"] is True
    assert _names(res["8"]["schema"]) == ["column_0_mean", "column_0_sum"]
    types8 = _types(res["8"]["schema"])
    assert types8["column_0_mean"] == "Float64"
    assert types8["column_0_sum"] == "Int64"

    assert _names(res["9"]["schema"]) == ["column_0_mean", "column_0_sum"]


# polars_code start node whose code can't be inferred (references an undefined input).
FAIL_GRAPH = {
    "order": [5, 8],
    "nodes": {
        "5": {
            "type": "polars_code",
            "input_ids": [],
            "left": None,
            "right": None,
            "settings": {"polars_code_input": {"polars_code": "output_df = input_lf.select(pl.col('column_0'))"}},
        },
        "8": {
            "type": "group_by",
            "input_ids": [5],
            "left": None,
            "right": None,
            "settings": {
                "groupby_input": {
                    "agg_cols": [
                        {"old_name": "column_0", "new_name": "column_0_mean", "agg": "mean"},
                        {"old_name": "column_0", "new_name": "column_0_sum", "agg": "sum"},
                    ]
                }
            },
        },
    },
}


def test_unbuildable_node_freezes_downstream_without_known():
    """When a node genuinely can't be built (code error) and has no known schema,
    the chain is unresolved — the freeze the known_schemas fallback fixes."""
    res = engine.propagate_schemas(FAIL_GRAPH, {})
    assert res["5"]["schema_resolved"] is False
    assert res["8"]["schema_resolved"] is False


def test_known_schema_unfreezes_downstream_on_build_failure():
    """Seeding the failing node's last-known schema lets the group_by re-predict
    both aggregations instead of freezing."""
    known = {"5": [{"name": "column_0", "data_type": "Int64"}]}
    res = engine.propagate_schemas(FAIL_GRAPH, {}, known)

    assert res["5"]["schema_resolved"] is True
    assert _names(res["5"]["schema"]) == ["column_0"]

    assert res["8"]["schema_resolved"] is True
    assert _names(res["8"]["schema"]) == ["column_0_mean", "column_0_sum"]


def test_statically_computable_node_ignores_known_schema():
    """Edit-wins: a polars_code node that resolves statically (has inputs) uses
    its computed schema, never the stale known one."""
    graph = {
        "order": [1, 2],
        "nodes": {
            "1": {"type": "read", "input_ids": [], "left": None, "right": None, "settings": {}},
            "2": {
                "type": "polars_code",
                "input_ids": [1],
                "left": None,
                "right": None,
                "settings": {"polars_code_input": {"polars_code": "output_df = input_lf.select(pl.col('a').alias('renamed'))"}},
            },
        },
    }
    source_schemas = {"1": [{"name": "a", "data_type": "Int64"}]}
    known = {"2": [{"name": "STALE", "data_type": "String"}]}
    res = engine.propagate_schemas(graph, source_schemas, known)
    assert res["2"]["schema_resolved"] is True
    assert _names(res["2"]["schema"]) == ["renamed"]


def test_pivot_downstream_seeded_from_known():
    """Pivot is data-dependent, but its last-known schema seeds the chain so a
    node after the pivot still resolves without re-running."""
    graph = {
        "order": [1, 2, 3],
        "nodes": {
            "1": {"type": "read", "input_ids": [], "left": None, "right": None, "settings": {}},
            "2": {"type": "pivot", "input_ids": [1], "left": None, "right": None, "settings": {"pivot_input": {}}},
            "3": {
                "type": "select",
                "input_ids": [2],
                "left": None,
                "right": None,
                "settings": {
                    "select_input": [
                        {"old_name": "idx", "new_name": "idx", "keep": True, "position": 0},
                        {"old_name": "A", "new_name": "A", "keep": True, "position": 1},
                        {"old_name": "B", "new_name": "B", "keep": False, "position": 2},
                    ]
                },
            },
        },
    }
    source_schemas = {"1": [{"name": "idx", "data_type": "Int64"}, {"name": "cat", "data_type": "String"}, {"name": "val", "data_type": "Int64"}]}
    known = {"2": [{"name": "idx", "data_type": "Int64"}, {"name": "A", "data_type": "Int64"}, {"name": "B", "data_type": "Int64"}]}
    res = engine.propagate_schemas(graph, source_schemas, known)

    assert res["2"]["schema_resolved"] is True
    assert _names(res["2"]["schema"]) == ["idx", "A", "B"]
    assert _names(res["3"]["schema"]) == ["idx", "A"]


def test_pivot_without_known_reports_run_the_flow():
    """A never-run pivot still reports the data-dependent message (unchanged)."""
    graph = {
        "order": [1, 2],
        "nodes": {
            "1": {"type": "read", "input_ids": [], "left": None, "right": None, "settings": {}},
            "2": {"type": "pivot", "input_ids": [1], "left": None, "right": None, "settings": {"pivot_input": {}}},
        },
    }
    source_schemas = {"1": [{"name": "idx", "data_type": "Int64"}]}
    res = engine.propagate_schemas(graph, source_schemas)
    assert res["2"]["schema_resolved"] is False
    assert "run the flow" in res["2"]["error"].lower()
