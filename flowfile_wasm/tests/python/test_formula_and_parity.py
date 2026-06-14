"""Tests for the formula node and the cheap parity nodes (cross_join, union,
record_id, rename). The formula node turns an expression string like
``[a] + [b]`` into a Polars expression via polars-expr-transformer (lazily
imported — it is micropip-installed in the browser)."""
import engine


def read_csv(node_id, csv, delimiter=",", has_headers=True):
    return engine.execute_read_csv(
        node_id, csv,
        {"received_file": {"table_settings": {"has_headers": has_headers, "delimiter": delimiter}}},
    )


def _names(schema):
    return [c["name"] for c in schema]


# --- formula -----------------------------------------------------------------

def _formula_settings(name, expr, data_type="Auto"):
    return {"function": {"field": {"name": name, "data_type": data_type}, "function": expr}}


def test_build_formula_arithmetic():
    import polars as pl

    out = engine.build_formula(
        pl.LazyFrame({"a": [1, 2, 3], "b": [10, 20, 30]}),
        _formula_settings("c", "[a] + [b] * 2"),
    ).collect()
    assert out["c"].to_list() == [21, 42, 63]


def test_build_formula_string_concat_and_cast():
    import polars as pl

    out = engine.build_formula(
        pl.LazyFrame({"first": ["ann", "bob"], "last": ["x", "y"]}),
        _formula_settings("full", 'concat([first], " ", [last])'),
    ).collect()
    assert out["full"].to_list() == ["ann x", "bob y"]

    casted = engine.build_formula(
        pl.LazyFrame({"a": [1, 2]}),
        _formula_settings("a_str", "[a] + 1", data_type="String"),
    ).collect()
    assert casted.schema["a_str"] == pl.String


def test_build_formula_empty_expr_is_passthrough():
    import polars as pl

    lf = pl.LazyFrame({"a": [1, 2]})
    assert engine.build_formula(lf, _formula_settings("c", "")).collect_schema().names() == ["a"]


def test_execute_formula_chain_and_error():
    assert read_csv(1, "a,b\n1,10\n2,20\n")["success"] is True
    r = engine.execute_formula(2, 1, _formula_settings("total", "[a] + [b]"))
    assert r["success"] is True
    assert "total" in _names(r["schema"])

    err = engine.execute_formula(99, 404, _formula_settings("x", "[a]"))
    assert err["success"] is False
    assert "No input data" in err["error"]


# --- cross_join --------------------------------------------------------------

def test_cross_join_cartesian_product():
    import polars as pl

    left = pl.LazyFrame({"a": [1, 2]})
    right = pl.LazyFrame({"b": [10, 20, 30]})
    out = engine.build_cross_join(left, right, {}).collect()
    assert out.shape == (6, 2)
    assert set(out.columns) == {"a", "b"}


# --- union -------------------------------------------------------------------

def test_union_vertical_and_diagonal():
    import polars as pl

    a = pl.LazyFrame({"x": [1], "y": [2]})
    b = pl.LazyFrame({"x": [3], "y": [4]})
    vert = engine.build_union([a, b], {"union_input": {"mode": "vertical"}}).collect()
    assert vert.sort("x")["x"].to_list() == [1, 3]

    c = pl.LazyFrame({"x": [5], "z": [6]})
    diag = engine.build_union([a, c], {"union_input": {"mode": "diagonal"}}).collect()
    assert set(diag.columns) == {"x", "y", "z"}
    assert diag.height == 2


def test_union_single_input_passthrough():
    import polars as pl

    a = pl.LazyFrame({"x": [1, 2]})
    assert engine.build_union([a, None], {}).collect_schema().names() == ["x"]


# --- record_id ---------------------------------------------------------------

def test_record_id_offset_and_dtype():
    import polars as pl

    out = engine.build_record_id(
        pl.LazyFrame({"a": [10, 20, 30]}),
        {"record_id_input": {"name": "rid", "offset": 1}},
    ).collect()
    assert out["rid"].to_list() == [1, 2, 3]
    assert out.columns == ["rid", "a"]


# --- rename ------------------------------------------------------------------

def test_rename_mapping_skips_missing():
    import polars as pl

    out = engine.build_rename(
        pl.LazyFrame({"a": [1], "b": [2]}),
        {"rename_input": [{"old_name": "a", "new_name": "A"}, {"old_name": "gone", "new_name": "X"}]},
    ).collect()
    assert out.columns == ["A", "b"]


# --- schema propagation (data-free) ------------------------------------------

def test_schema_propagation_resolves_new_nodes():
    source = {"1": [{"name": "a", "data_type": "Int64"}, {"name": "b", "data_type": "Int64"}]}
    graph = {
        "order": [1, 2, 3, 4],
        "nodes": {
            "1": {"type": "manual_input", "input_ids": [], "left": None, "right": None, "settings": {}},
            "2": {"type": "formula", "input_ids": [1], "left": 1, "right": None,
                  "settings": _formula_settings("c", "[a] + [b]")},
            "3": {"type": "record_id", "input_ids": [2], "left": 2, "right": None,
                  "settings": {"record_id_input": {"name": "rid", "offset": 1}}},
            "4": {"type": "rename", "input_ids": [3], "left": 3, "right": None,
                  "settings": {"rename_input": [{"old_name": "a", "new_name": "A"}]}},
        },
    }
    res = engine.propagate_schemas(graph, source)
    assert res["2"]["schema_resolved"] and "c" in _names(res["2"]["schema"])
    assert "rid" in _names(res["3"]["schema"])
    assert "A" in _names(res["4"]["schema"])
