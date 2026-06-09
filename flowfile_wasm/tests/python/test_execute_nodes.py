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


def test_output_parquet_is_rejected():
    read_csv(1, "a\n1\n")
    out = output(2, 1, name="x.parquet", file_type="parquet")
    assert out["success"] is False
    assert "Parquet" in out["error"]


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
