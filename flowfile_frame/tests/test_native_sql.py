"""``ff.sql`` and ``FlowFrame.sql``: a SQL Query node from Python, its tables named in the stored SQL."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff

from .native_helpers import core_node, results_by_id, round_trip

ORDERS = {"id": [1, 2, 3], "customer_id": [10, 20, 10], "amount": [120.0, 40.0, 900.0]}
CUSTOMERS = {"customer_id": [10, 20], "name": ["Ann", "Bob"]}
TEN_X = "output_df = input_df.with_columns((pl.col('amount') * 10).alias('amount10'))"


def _sql_code(frame: ff.FlowFrame) -> str:
    return core_node(frame).setting_input.sql_query_input.sql_code


# FlowFrame.sql


def test_frame_sql_places_a_sql_query_node_named_self():
    orders = ff.from_dict(ORDERS)
    big = orders.sql("select * from self where amount > 50")

    node = core_node(big)
    assert node.node_type == "sql_query"
    assert _sql_code(big) == "WITH self AS (SELECT * FROM input_1)\nselect * from self where amount > 50"
    assert [n.node_id for n in node.node_inputs.main_inputs] == [orders.node_id]
    expected = pl.LazyFrame(ORDERS).sql("select * from self where amount > 50").collect()
    assert_frame_equal(big.collect(), expected)


def test_frame_sql_with_a_table_name():
    orders = ff.from_dict(ORDERS)
    big = orders.sql("select id from orders where amount > 50", table_name="orders")
    assert _sql_code(big) == "WITH orders AS (SELECT * FROM input_1)\nselect id from orders where amount > 50"
    assert big.collect()["id"].to_list() == [1, 3]


def test_frame_sql_named_input_1_needs_no_header():
    orders = ff.from_dict(ORDERS)
    big = orders.sql("select * from input_1 where amount > 50", table_name="input_1")
    assert _sql_code(big) == "select * from input_1 where amount > 50"
    assert big.collect()["id"].to_list() == [1, 3]


def test_frame_sql_is_no_longer_polars_code():
    big = ff.from_dict(ORDERS).sql("select * from self")
    assert core_node(big).node_type == "sql_query"


# ff.sql


def test_named_tables_on_different_graphs_are_merged_and_wired_in_order():
    orders = ff.from_dict(ORDERS)
    customers = ff.from_dict(CUSTOMERS)
    query = (
        "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.customer_id ORDER BY o.id"
    )

    joined = ff.sql(query, orders=orders, customers=customers, description="orders with names")

    node = core_node(joined)
    assert orders.flow_graph is customers.flow_graph is joined.flow_graph
    assert [n.node_id for n in node.node_inputs.main_inputs] == [orders.node_id, customers.node_id]
    assert node.setting_input.depending_on_ids == [orders.node_id, customers.node_id]
    assert node.setting_input.description == "orders with names"
    assert _sql_code(joined) == (
        "WITH orders AS (SELECT * FROM input_1),\n     customers AS (SELECT * FROM input_2)\n" + query
    )
    expected = pl.DataFrame({"id": [1, 2, 3], "name": ["Ann", "Bob", "Ann"], "amount": [120.0, 40.0, 900.0]})
    assert_frame_equal(joined.collect(), expected)


def test_positional_frames_are_input_n_and_named_ones_follow():
    orders = ff.from_dict(ORDERS)
    customers = ff.from_dict(CUSTOMERS)
    joined = ff.sql(
        "SELECT i.id, c.name FROM input_1 i JOIN customers c ON i.customer_id = c.customer_id ORDER BY i.id",
        orders,
        customers=customers,
    )
    assert _sql_code(joined).startswith("WITH customers AS (SELECT * FROM input_2)\nSELECT")
    assert joined.collect()["name"].to_list() == ["Ann", "Bob", "Ann"]


def test_positional_only_stores_the_query_unchanged():
    orders = ff.from_dict(ORDERS)
    query = "select count(*) as n from input_1"
    assert _sql_code(ff.sql(query, orders)) == query


def test_the_users_own_with_is_merged_into_the_header():
    orders = ff.from_dict(ORDERS)
    big = ff.sql(
        """
        -- keep the large ones
        WITH big AS (SELECT * FROM orders WHERE amount > 50)
        SELECT id FROM big ORDER BY id
        """,
        orders=orders,
    )
    assert _sql_code(big) == (
        "WITH orders AS (SELECT * FROM input_1),\n"
        "-- keep the large ones\n"
        "big AS (SELECT * FROM orders WHERE amount > 50)\n"
        "SELECT id FROM big ORDER BY id"
    )
    assert big.collect()["id"].to_list() == [1, 3]


def test_indented_query_is_dedented_and_labels_the_node():
    orders = ff.from_dict(ORDERS)
    big = ff.sql(
        """
            SELECT id
            FROM orders
        """,
        orders=orders,
    )
    assert _sql_code(big) == "WITH orders AS (SELECT * FROM input_1)\nSELECT id\nFROM orders"
    assert core_node(big).setting_input.description == "SELECT id"


def test_long_first_line_is_cut_for_the_label():
    orders = ff.from_dict(ORDERS)
    query = "SELECT " + ", ".join(f"id AS id_{i}" for i in range(20)) + " FROM self"
    description = core_node(orders.sql(query)).setting_input.description
    assert len(description) == 80 and description.endswith("...")


# refused


def test_no_frames_places_a_source_node():
    source = ff.sql("select 1 as x")
    node = core_node(source)
    assert node.node_type == "sql_query" and not node.node_inputs.main_inputs
    assert source.collect().to_dicts() == [{"x": 1}]


def test_a_table_may_be_named_query():
    orders = ff.from_dict(ORDERS)
    picked = ff.sql("select id from query where amount > 50 order by id", query=orders)
    assert picked.collect()["id"].to_list() == [1, 3]


def test_table_names_are_case_sensitive_like_polars():
    orders = ff.from_dict(ORDERS)
    query = "select count(*) as n from Input_1"
    counted = orders.sql(query, table_name="Input_1")
    assert _sql_code(counted).startswith("WITH Input_1 AS (SELECT * FROM input_1)\n")
    assert_frame_equal(counted.collect(), pl.LazyFrame(ORDERS).sql(query, table_name="Input_1").collect())
    other = ff.from_dict(CUSTOMERS)
    assert ff.sql("select * from INPUT_1", orders, INPUT_1=other).collect().height == len(CUSTOMERS["name"])


# the query's own leading WITH and comments


@pytest.mark.parametrize(
    "query",
    [
        "-- big and small orders, together with\nSELECT id FROM self WHERE amount > 500 UNION ALL\n"
        "SELECT id FROM self WHERE amount < 50",
        "-- orders joined with\nSELECT id FROM self WHERE amount > 50",
        "-- orders with a big amount\nSELECT id FROM self WHERE amount > 50 ORDER BY id",
        "/* with */ SELECT id FROM self WHERE amount > 50",
        "SELECT id FROM self WHERE amount > 50 -- with",
    ],
)
def test_with_inside_a_comment_is_not_the_querys_with(query):
    orders = ff.from_dict(ORDERS)
    placed = orders.sql(query)
    assert _sql_code(placed) == f"WITH self AS (SELECT * FROM input_1)\n{query}"
    expected = pl.LazyFrame(ORDERS).sql(query).collect()
    assert_frame_equal(placed.collect(), expected, check_row_order=False)


def test_a_lowercase_with_behind_a_block_comment_is_merged():
    orders = ff.from_dict(ORDERS)
    placed = orders.sql("/* keep */ with big as (select * from self where amount > 50) select id from big order by id")
    assert _sql_code(placed) == (
        "WITH self AS (SELECT * FROM input_1),\n/* keep */ big as (select * from self where amount > 50) "
        "select id from big order by id"
    )
    assert placed.collect()["id"].to_list() == [1, 3]


def test_deeply_indented_leading_comments_build_quickly():
    pad = " " * 40
    query = f"-- Revenue report\n{pad}-- only the large orders\n{pad}-- sorted by id\n{pad}SELECT id FROM self ORDER BY id"
    assert ff.from_dict(ORDERS).sql(query).collect()["id"].to_list() == [1, 2, 3]


# the stored text


@pytest.mark.parametrize(
    "query",
    [
        "SELECT count(*) AS n FROM self WHERE note = 'x\n  \ny'",
        "\n    SELECT count(*) AS n FROM self\n    WHERE note = 'line1\n    line2'\n    ",
        '\n    SELECT count(*) AS n FROM self\n    WHERE "note" = \'line1\n    line2\'\n    ',
    ],
)
def test_a_multi_line_quoted_string_is_never_dedented(query):
    data = {"note": ["x\n  \ny", "line1\n    line2", "line1\nline2"]}
    placed = ff.from_dict(data).sql(query)
    assert _sql_code(placed) == "WITH self AS (SELECT * FROM input_1)\n" + query.strip()
    assert_frame_equal(placed.collect(), pl.LazyFrame(data).sql(query).collect())
    assert placed.collect()["n"].to_list() == [1]


def test_empty_query_raises():
    with pytest.raises(ff.NativeNodeError, match="empty"):
        ff.from_dict(ORDERS).sql("   ")


def test_a_polars_frame_is_refused():
    with pytest.raises(ff.NativeNodeError, match="wrap a Polars frame"):
        ff.sql("select * from input_1", pl.LazyFrame(ORDERS))


def test_a_frame_passed_as_description_raises_and_leaves_no_node():
    orders = ff.from_dict(ORDERS)
    node_count = len(orders.flow_graph.nodes)
    with pytest.raises(ff.NativeNodeError, match="description must be a string, got FlowFrame") as raised:
        ff.sql("select * from orders", orders=orders, description=orders)
    assert len(str(raised.value)) < 200
    with pytest.raises(ff.NativeNodeError, match="description must be a string, got int"):
        orders.sql("select * from self", description=1)
    assert len(orders.flow_graph.nodes) == node_count


@pytest.mark.parametrize("name", ["my table", "1orders", "orders;drop", ""])
def test_a_table_name_that_is_no_identifier_raises(name):
    orders = ff.from_dict(ORDERS)
    node_count = len(orders.flow_graph.nodes)
    with pytest.raises(ff.NativeNodeError, match="plain identifier"):
        orders.sql("select * from self", table_name=name)
    assert len(orders.flow_graph.nodes) == node_count


def test_a_name_that_shadows_another_input_raises():
    orders = ff.from_dict(ORDERS)
    customers = ff.from_dict(CUSTOMERS)
    with pytest.raises(ff.NativeNodeError, match="shadow another input"):
        ff.sql("select * from input_1", orders, input_1=customers)


def test_unsafe_sql_raises_and_leaves_no_node():
    orders = ff.from_dict(ORDERS)
    node_count = len(orders.flow_graph.nodes)
    with pytest.raises(ff.NativeNodeError, match="Only SELECT queries are allowed"):
        ff.sql("DELETE FROM input_1", orders)
    assert len(orders.flow_graph.nodes) == node_count


def test_an_unknown_table_raises_and_leaves_no_node():
    orders = ff.from_dict(ORDERS)
    node_count = len(orders.flow_graph.nodes)
    with pytest.raises(ff.NativeNodeError):
        orders.sql("select * from nowhere")
    assert len(orders.flow_graph.nodes) == node_count


# deferred, gated, parameterised


def test_a_deferred_input_gives_a_deferred_output_that_runs_on_collect():
    orders = ff.from_dict(ORDERS)
    deferred = ff.Node("polars_code", orders, settings={"polars_code_input": {"polars_code": TEN_X}}, deferred=True)

    big = deferred.output.sql("select id, amount10 from self where amount > 50")

    assert big._deferred is True
    assert big.columns == ["id", "amount10"]
    assert big.data.collect().height == 0
    assert_frame_equal(big.collect(), pl.DataFrame({"id": [1, 3], "amount10": [1200.0, 9000.0]}))


def test_below_a_gate_only_the_live_side_is_returned():
    orders = ff.from_dict(ORDERS)
    ff.add_flow_parameter(orders, ff.Parameter("env", default="dev"))
    gate = ff.Gate(orders, parameter="env", value="prod")
    live = gate.otherwise.sql("select id from self where amount > 50")
    dead = gate.then.sql("select id from self")

    assert live.collect()["id"].to_list() == [1, 3]
    assert dead.collect().height == 0
    results = results_by_id(orders.flow_graph.run_graph())
    assert results[dead.node_id].skipped and not results[live.node_id].skipped


def test_a_parameter_reference_resolves_at_build_and_at_run():
    orders = ff.from_dict(ORDERS)
    graph = orders.flow_graph
    min_amount = ff.add_flow_parameter(graph, ff.Parameter("min_amount", default=100, type="integer"))

    kept = orders.sql(f"select id from self where amount >= {min_amount.ref} order by id")

    assert _sql_code(kept).endswith("where amount >= ${min_amount} order by id")
    assert kept.collect()["id"].to_list() == [1, 3]
    ff.set_flow_parameter(graph, min_amount, 500)
    run = graph.run_graph()
    assert run.success
    assert core_node(kept).get_resulting_data().data_frame.collect()["id"].to_list() == [3]


def test_a_string_parameter_is_substituted_as_text_inside_sql_quotes():
    customers = ff.from_dict(CUSTOMERS)
    name = ff.add_flow_parameter(customers, ff.Parameter("name", default="Bob"))
    picked = customers.sql(f"select customer_id from self where name = '{name.ref}'")
    assert picked.collect()["customer_id"].to_list() == [20]


def test_an_undeclared_parameter_raises():
    orders = ff.from_dict(ORDERS)
    with pytest.raises(ff.NativeNodeError, match="undeclared flow parameter"):
        orders.sql("select * from self where amount >= ${nope}")


# round trip


def test_save_and_open_keep_the_sql_node():
    orders = ff.from_dict(ORDERS)
    customers = ff.from_dict(CUSTOMERS)
    joined = ff.sql(
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
        orders=orders,
        customers=customers,
    )
    expected = joined.collect()

    reopened, first_doc = round_trip(joined, "sql_roundtrip.yaml")

    node = reopened.get_node(joined.node_id)
    assert node.node_type == "sql_query"
    assert node.setting_input.sql_query_input.sql_code == _sql_code(joined)
    assert [n.node_id for n in node.node_inputs.main_inputs] == [orders.node_id, customers.node_id]
    reopened.run_graph()
    assert_frame_equal(node.get_resulting_data().data_frame.collect(), expected, check_row_order=False)
