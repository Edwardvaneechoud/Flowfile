"""Integration tests for the side-effecting `execute_*` node wrappers.

These drive real node chains through the engine's global LazyFrame registry and
assert on the dicts returned to the JS bridge (success / schema / download), plus
the error paths and their helpful messages. The autouse `_reset_engine_state`
fixture (conftest) clears the registry between tests.
"""
import engine


def read_csv(node_id, csv, delimiter=",", has_headers=True):
    return engine.execute_read_csv(
        node_id, csv,
        {"received_file": {"table_settings": {"has_headers": has_headers, "delimiter": delimiter}}},
    )


def output(node_id, input_id, delimiter=",", name="output.csv", file_type="csv"):
    return engine.execute_output(
        node_id, input_id,
        {"output_settings": {"name": name, "file_type": file_type, "table_settings": {"delimiter": delimiter}}},
    )


def test_read_csv_parses_dates_for_format_date():
    # try_parse_dates parity with flowfile_core: a date column must land as Date
    # so a format_date formula (.dt.to_string under the hood) works instead of
    # raising "to_string operation not supported for dtype str".
    r = read_csv(1, "date,qty\n2019-01-05,2\n2019-02-11,1\n")
    assert r["success"] is True
    assert dict((c["name"], c["data_type"]) for c in r["schema"])["date"] == "Date"

    f = engine.execute_formula(
        2, 1, {"function": {"field": {"name": "month", "data_type": "String"}, "function": 'format_date([date], "%Y-%m")'}}
    )
    assert f["success"] is True, f.get("error")
    assert engine.get_lazyframe(2).collect()["month"].to_list() == ["2019-01", "2019-02"]


def test_read_csv_falls_back_when_date_parsing_would_error():
    # Non-date content in a mixed column must not break the read; fallback yields
    # a successful read (as strings) rather than surfacing a parse error.
    r = read_csv(1, "id,name\n1,alice\n2,bob\n")
    assert r["success"] is True
    assert [c["name"] for c in r["schema"]] == ["id", "name"]


def test_read_filter_select_output_chain():
    assert read_csv(1, "id,name,age\n1,alice,30\n2,bob,25\n3,carol,40\n")["success"] is True

    r = engine.execute_filter(
        2, 1, {"filter_input": {"basic_filter": {"field": "age", "operator": "greater_than", "value": "28"}}}
    )
    assert r["success"] is True

    r = engine.execute_select(3, 2, {"select_input": [
        {"old_name": "name", "new_name": "person", "keep": True, "position": 0},
        {"old_name": "age", "new_name": "age", "keep": True, "position": 1},
        {"old_name": "id", "new_name": "id", "keep": False, "position": 2},
    ]})
    assert r["success"] is True
    assert [c["name"] for c in r["schema"]] == ["person", "age"]

    out = output(4, 3, name="people.csv")
    assert out["success"] is True
    dl = out["download"]
    assert dl["file_name"] == "people.csv"
    assert dl["row_count"] == 2
    lines = dl["content"].strip().split("\n")
    assert lines[0] == "person,age"
    assert set(lines[1:]) == {"alice,30", "carol,40"}


def test_join_chain():
    read_csv(1, "id,lval\n1,a\n2,b\n")
    read_csv(2, "id,rval\n2,x\n3,y\n")
    r = engine.execute_join(
        3, 1, 2,
        {"join_input": {"join_type": "inner", "join_mapping": [{"left_col": "id", "right_col": "id"}]}},
    )
    assert r["success"] is True
    assert output(4, 3)["download"]["row_count"] == 1


def test_group_by_chain():
    read_csv(1, "cat,amount\nx,10\nx,20\ny,5\n")
    r = engine.execute_group_by(2, 1, {"groupby_input": {"agg_cols": [
        {"old_name": "cat", "new_name": "cat", "agg": "groupby"},
        {"old_name": "amount", "new_name": "total", "agg": "sum"},
    ]}})
    assert r["success"] is True
    rows = output(3, 2)["download"]["content"].strip().split("\n")
    assert rows[0] == "cat,total"
    assert set(rows[1:]) == {"x,30", "y,5"}


def test_pivot_zero_fills_absent_combinations():
    # Like core's do_pivot: an absent combination reads 0 for sum/count.
    read_csv(1, "k,q,v\na,x,1\nb,x,3\nb,y,5\n")
    r = engine.execute_pivot(2, 1, {"pivot_input": {
        "index_columns": ["k"], "pivot_column": "q", "value_col": "v",
        "aggregations": ["sum", "count", "mean", "min"],
    }})
    assert r["success"] is True, r.get("error")
    rows = {row["k"]: row for row in engine.get_lazyframe(2).collect().to_dicts()}
    assert rows["a"]["y_sum"] == 0
    assert rows["a"]["y_count"] == 0
    assert rows["a"]["y_mean"] is None
    assert rows["a"]["y_min"] is None
    assert rows["b"]["y_sum"] == 5

    # A single aggregation names the column after the value alone; still zero-filled.
    single = engine.execute_pivot(3, 1, {"pivot_input": {
        "index_columns": ["k"], "pivot_column": "q", "value_col": "v", "aggregations": ["sum"],
    }})
    assert single["success"] is True, single.get("error")
    assert {row["k"]: row["y"] for row in engine.get_lazyframe(3).collect().to_dicts()} == {"a": 0, "b": 5}


def test_pivot_refuses_an_aggregation_the_value_dtype_cannot_carry():
    # core refuses `sum` on String; the browser's polars 1.18 would sum to null.
    read_csv(1, "k,q,v\na,x,one\nb,x,two\n")
    r = engine.execute_pivot(2, 1, {"pivot_input": {
        "index_columns": ["k"], "pivot_column": "q", "value_col": "v", "aggregations": ["mean", "sum"],
    }})
    assert r["success"] is False
    assert "`sum` operation not supported for dtype `String`" in r["error"]
    assert "value column 'v'" in r["error"]
    assert engine.get_lazyframe(2) is None

    # Only what core refuses is refused: everything else works on any dtype.
    ok = engine.execute_pivot(3, 1, {"pivot_input": {
        "index_columns": ["k"], "pivot_column": "q", "value_col": "v",
        "aggregations": ["count", "first", "last", "min", "max", "mean", "median"],
    }})
    assert ok["success"] is True, ok.get("error")


def test_pivot_refuses_sum_over_an_empty_string_column():
    # Zero rows: core still resolves the aggregation's schema and rejects it.
    engine.execute_manual_input(1, "", {"raw_data_format": {
        "columns": [{"name": "k", "data_type": "String"}, {"name": "q", "data_type": "String"},
                    {"name": "v", "data_type": "String"}],
        "data": [[], [], []],
    }})
    r = engine.execute_pivot(2, 1, {"pivot_input": {
        "index_columns": [], "pivot_column": "q", "value_col": "v", "aggregations": ["sum"],
    }})
    assert r["success"] is False
    assert "`sum` operation not supported for dtype `String`" in r["error"]
    assert engine.get_lazyframe(2) is None


def test_pivot_refuses_a_null_in_the_pivot_column():
    # A pivot turns each label into a column NAME, and a null is not a name.
    engine.execute_manual_input(1, "", {"raw_data_format": {
        "columns": [{"name": "k", "data_type": "Int64"}, {"name": "q", "data_type": "String"},
                    {"name": "v", "data_type": "Int64"}],
        "data": [[1, 1, 2], ["a", None, None], [1, 2, 3]],
    }})
    r = engine.execute_pivot(2, 1, {"pivot_input": {
        "index_columns": ["k"], "pivot_column": "q", "value_col": "v", "aggregations": ["sum"],
    }})
    assert r["success"] is False
    assert "Pivot column 'q' contains null values (2 row(s))" in r["error"]
    assert "cannot become a column name" in r["error"]
    assert engine.get_lazyframe(2) is None


def test_pivot_allows_nulls_in_the_value_column():
    # The refusal reads the pivot column, not the value column: nulls in the
    # values are ordinary (they aggregate), only null *labels* are refused.
    engine.execute_manual_input(1, "", {"raw_data_format": {
        "columns": [{"name": "k", "data_type": "String"}, {"name": "q", "data_type": "String"},
                    {"name": "v", "data_type": "Int64"}],
        "data": [["a", "a", "b"], ["x", "y", "x"], [None, 2, None]],
    }})
    r = engine.execute_pivot(2, 1, {"pivot_input": {
        "index_columns": ["k"], "pivot_column": "q", "value_col": "v", "aggregations": ["sum", "min"],
    }})
    assert r["success"] is True, r.get("error")
    rows = {row["k"]: row for row in engine.get_lazyframe(2).collect().to_dicts()}
    assert rows["a"]["x_sum"] == 0
    assert rows["a"]["x_min"] is None
    assert rows["a"]["y_sum"] == 2


def test_manual_input_raw_data_format():
    r = engine.execute_manual_input(
        1, "",
        {"raw_data_format": {"columns": [{"name": "x"}, {"name": "y"}], "data": [[1, 2], ["a", "b"]]}},
    )
    assert r["success"] is True
    assert [c["name"] for c in r["schema"]] == ["x", "y"]


def test_output_tab_delimiter_emits_real_tab():
    # Guards the JS template-literal "\\t" -> tab resolution preserved at extraction.
    read_csv(1, "a,b\n1,2\n")
    out = output(2, 1, delimiter="tab")
    assert out["success"] is True
    assert out["download"]["content"].startswith("a\tb")


def test_output_parquet_stages_arrow_ipc():
    # Parquet output stages Arrow IPC bytes for the JS side (parquet-wasm
    # encodes the final .parquet); the wasm polars build can't write parquet.
    read_csv(1, "a\n1\n")
    out = output(2, 1, name="x.parquet", file_type="parquet")
    assert out["success"] is True
    dl = out["download"]
    assert dl["content_kind"] == "binary"
    assert dl["transport"] == "arrow-ipc"
    assert dl["mime_type"] == "application/vnd.apache.parquet"

    import io

    import polars as pl

    ipc = engine.take_output_binary(2)
    df = pl.read_ipc_stream(io.BytesIO(ipc))
    assert df["a"].to_list() == [1]
    assert engine.take_output_binary(2) is None


def test_filter_missing_column_reports_available_columns():
    read_csv(1, "id,name\n1,a\n")
    r = engine.execute_filter(
        2, 1, {"filter_input": {"basic_filter": {"field": "ghost", "operator": "equals", "value": "1"}}}
    )
    assert r["success"] is False
    assert "ghost" in r["error"]
    assert "Available columns" in r["error"]


def test_join_missing_column_error():
    read_csv(1, "id,a\n1,x\n")
    read_csv(2, "id,b\n1,y\n")
    r = engine.execute_join(
        3, 1, 2,
        {"join_input": {"join_type": "inner", "join_mapping": [{"left_col": "ghost", "right_col": "id"}]}},
    )
    assert r["success"] is False
    assert "not found" in r["error"].lower()


def test_execute_with_no_upstream_input_errors():
    r = engine.execute_filter(
        5, 999, {"filter_input": {"basic_filter": {"field": "x", "operator": "equals", "value": "1"}}}
    )
    assert r["success"] is False
    assert "No input data" in r["error"]
