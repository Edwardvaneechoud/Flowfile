"""Integration tests for the standalone sql_query node type in FlowGraph.

Covers:
- Single-input SQL query (SELECT, filter, aggregation)
- Multi-input SQL query (JOIN across input_1, input_2)
- Chaining: manual_input → sql_query → sql_query
- Invalid SQL produces node errors without crashing the graph
- Node description formatting
"""

from typing import Literal

from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_handler() -> FlowfileHandler:
    handler = FlowfileHandler()
    assert handler._flows == {}
    return handler


def _create_graph(
    flow_id: int = 1,
    execution_location: Literal["local", "remote"] = "local",
) -> FlowGraph:
    handler = _create_handler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="sql_query_test",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def _add_manual_input(graph: FlowGraph, data: list[dict], node_id: int) -> None:
    promise = input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"
    )
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )


def _add_sql_query(
    graph: FlowGraph,
    node_id: int,
    sql_code: str,
    depending_on_ids: list[int],
) -> None:
    promise = input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type="sql_query"
    )
    graph.add_node_promise(promise)
    graph.add_sql_query(
        input_schema.NodeSqlQuery(
            flow_id=graph.flow_id,
            node_id=node_id,
            sql_query_input=transform_schema.SqlQueryInput(sql_code=sql_code),
            depending_on_ids=depending_on_ids,
        )
    )


def _connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "main") -> None:
    conn = input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type=input_type)
    add_connection(graph, conn)


def _run_graph(graph: FlowGraph) -> RunInformation:
    run_info = graph.run_graph()
    if not run_info.success:
        errors = []
        for step in run_info.node_step_result:
            if not step.success:
                errors.append(f"node {step.node_id}: {step.error}")
        raise AssertionError("Graph execution failed:\n" + "\n".join(errors))
    return run_info


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

CUSTOMERS = [
    {"id": 1, "name": "Alice", "city": "Amsterdam"},
    {"id": 2, "name": "Bob", "city": "Berlin"},
    {"id": 3, "name": "Charlie", "city": "Amsterdam"},
]

ORDERS = [
    {"customer_id": 1, "amount": 100},
    {"customer_id": 2, "amount": 200},
    {"customer_id": 1, "amount": 150},
    {"customer_id": 3, "amount": 300},
]


# ---------------------------------------------------------------------------
# Single-input tests
# ---------------------------------------------------------------------------


class TestSqlQuerySingleInput:
    """Tests for sql_query nodes with a single upstream input."""

    def test_select_all(self, execution_location):
        """SELECT * passes through all rows and columns."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, CUSTOMERS, node_id=1)
        _add_sql_query(graph, node_id=2, sql_code="SELECT * FROM input_1", depending_on_ids=[1])
        _connect(graph, 1, 2)

        _run_graph(graph)

        result = graph.get_node(2).get_resulting_data().collect()
        assert len(result) == 3
        assert set(result.columns) == {"id", "name", "city"}

    def test_select_with_filter(self, execution_location):
        """WHERE clause filters rows correctly."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, CUSTOMERS, node_id=1)
        _add_sql_query(
            graph,
            node_id=2,
            sql_code="SELECT name, city FROM input_1 WHERE city = 'Amsterdam'",
            depending_on_ids=[1],
        )
        _connect(graph, 1, 2)

        _run_graph(graph)

        result = graph.get_node(2).get_resulting_data().collect()
        assert len(result) == 2
        assert set(result.columns) == {"name", "city"}
        assert sorted(result["name"].to_list()) == ["Alice", "Charlie"]

    def test_aggregation(self, execution_location):
        """GROUP BY with aggregate functions works."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, CUSTOMERS, node_id=1)
        _add_sql_query(
            graph,
            node_id=2,
            sql_code="SELECT city, COUNT(*) AS cnt FROM input_1 GROUP BY city",
            depending_on_ids=[1],
        )
        _connect(graph, 1, 2)

        _run_graph(graph)

        result = graph.get_node(2).get_resulting_data().collect()
        assert len(result) == 2
        assert "city" in result.columns
        assert "cnt" in result.columns

    def test_column_expression(self, execution_location):
        """Computed columns via SQL expressions work."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, ORDERS, node_id=1)
        _add_sql_query(
            graph,
            node_id=2,
            sql_code="SELECT customer_id, amount, amount * 2 AS doubled FROM input_1",
            depending_on_ids=[1],
        )
        _connect(graph, 1, 2)

        _run_graph(graph)

        result = graph.get_node(2).get_resulting_data().collect()
        assert "doubled" in result.columns
        amounts = result["amount"].to_list()
        doubled = result["doubled"].to_list()
        assert all(d == a * 2 for a, d in zip(amounts, doubled))


# ---------------------------------------------------------------------------
# Multi-input tests
# ---------------------------------------------------------------------------


class TestSqlQueryMultiInput:
    """Tests for sql_query nodes with multiple upstream inputs."""

    def test_join_two_inputs(self, execution_location):
        """JOIN between input_1 and input_2 produces correct results."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, CUSTOMERS, node_id=1)
        _add_manual_input(graph, ORDERS, node_id=2)

        _add_sql_query(
            graph,
            node_id=3,
            sql_code=(
                "SELECT c.name, SUM(o.amount) AS total "
                "FROM input_1 c "
                "JOIN input_2 o ON c.id = o.customer_id "
                "GROUP BY c.name"
            ),
            depending_on_ids=[1, 2],
        )
        _connect(graph, 1, 3, input_type="main")
        _connect(graph, 2, 3, input_type="right")

        _run_graph(graph)

        result = graph.get_node(3).get_resulting_data().collect()
        assert len(result) == 3
        assert "name" in result.columns
        assert "total" in result.columns

        result_sorted = result.sort("name")
        assert result_sorted["name"].to_list() == ["Alice", "Bob", "Charlie"]
        assert result_sorted["total"].to_list() == [250, 200, 300]

    def test_left_join_two_inputs(self, execution_location):
        """LEFT JOIN keeps all rows from input_1."""
        extra_customers = CUSTOMERS + [{"id": 99, "name": "Diana", "city": "Dublin"}]
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, extra_customers, node_id=1)
        _add_manual_input(graph, ORDERS, node_id=2)

        _add_sql_query(
            graph,
            node_id=3,
            sql_code=(
                "SELECT c.name, o.amount "
                "FROM input_1 c "
                "LEFT JOIN input_2 o ON c.id = o.customer_id"
            ),
            depending_on_ids=[1, 2],
        )
        _connect(graph, 1, 3, input_type="main")
        _connect(graph, 2, 3, input_type="right")

        _run_graph(graph)

        result = graph.get_node(3).get_resulting_data().collect()
        # Diana has no orders, so she should appear with NULL amount
        diana_rows = result.filter(result["name"] == "Diana")
        assert len(diana_rows) == 1
        assert diana_rows["amount"][0] is None


# ---------------------------------------------------------------------------
# Chaining tests
# ---------------------------------------------------------------------------


class TestSqlQueryChaining:
    """Tests for chaining sql_query nodes together."""

    def test_chain_two_sql_nodes(self, execution_location):
        """Output of one sql_query feeds into another sql_query."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, CUSTOMERS, node_id=1)

        # First SQL: filter to Amsterdam
        _add_sql_query(
            graph,
            node_id=2,
            sql_code="SELECT * FROM input_1 WHERE city = 'Amsterdam'",
            depending_on_ids=[1],
        )
        _connect(graph, 1, 2)

        # Second SQL: count the filtered results
        _add_sql_query(
            graph,
            node_id=3,
            sql_code="SELECT COUNT(*) AS total FROM input_1",
            depending_on_ids=[2],
        )
        _connect(graph, 2, 3)

        _run_graph(graph)

        result = graph.get_node(3).get_resulting_data().collect()
        assert result["total"][0] == 2


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


class TestSqlQueryErrors:
    """Tests for error handling in sql_query nodes."""

    def test_invalid_sql_sets_node_error(self):
        """Malformed SQL sets an error on the node during validation.

        Note: add_connection resets node errors, so we check the error
        before the connection is established.
        """
        graph = _create_graph(execution_location="local")
        _add_manual_input(graph, CUSTOMERS, node_id=1)

        promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=2, node_type="sql_query"
        )
        graph.add_node_promise(promise)
        graph.add_sql_query(
            input_schema.NodeSqlQuery(
                flow_id=graph.flow_id,
                node_id=2,
                sql_query_input=transform_schema.SqlQueryInput(sql_code="SELEC * FORM input_1"),
                depending_on_ids=[1],
            )
        )

        node = graph.get_node(2)
        assert node.results.errors is not None
        assert "SELECT" in node.results.errors

    def test_unsafe_sql_sets_node_error(self):
        """DDL/DML statements are rejected by validation."""
        graph = _create_graph(execution_location="local")
        _add_manual_input(graph, CUSTOMERS, node_id=1)

        promise = input_schema.NodePromise(
            flow_id=graph.flow_id, node_id=2, node_type="sql_query"
        )
        graph.add_node_promise(promise)
        graph.add_sql_query(
            input_schema.NodeSqlQuery(
                flow_id=graph.flow_id,
                node_id=2,
                sql_query_input=transform_schema.SqlQueryInput(sql_code="DROP TABLE input_1"),
                depending_on_ids=[1],
            )
        )

        node = graph.get_node(2)
        assert node.results.errors is not None
        assert "SELECT" in node.results.errors

    def test_invalid_sql_fails_at_runtime(self, execution_location):
        """Malformed SQL causes the graph run to fail."""
        graph = _create_graph(execution_location=execution_location)
        _add_manual_input(graph, CUSTOMERS, node_id=1)
        _add_sql_query(
            graph,
            node_id=2,
            sql_code="SELEC * FORM input_1",
            depending_on_ids=[1],
        )
        _connect(graph, 1, 2)

        run_info = graph.run_graph()
        assert not run_info.success


# ---------------------------------------------------------------------------
# Description tests
# ---------------------------------------------------------------------------


class TestSqlQueryDescription:
    """Tests for NodeSqlQuery description formatting."""

    def test_short_query_description(self):
        """Short queries appear in full in the description."""
        node = input_schema.NodeSqlQuery(
            flow_id=1,
            node_id=1,
            sql_query_input=transform_schema.SqlQueryInput(sql_code="SELECT * FROM input_1"),
            depending_on_ids=[],
        )
        assert node.get_default_description() == "SELECT * FROM input_1"

    def test_long_query_truncated(self):
        """Queries over 80 chars are truncated with ellipsis."""
        long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(50)) + " FROM input_1"
        node = input_schema.NodeSqlQuery(
            flow_id=1,
            node_id=1,
            sql_query_input=transform_schema.SqlQueryInput(sql_code=long_sql),
            depending_on_ids=[],
        )
        desc = node.get_default_description()
        assert len(desc) <= 80
        assert desc.endswith("...")

    def test_multiline_uses_first_line(self):
        """Multi-line SQL only uses the first line for the description."""
        sql = "SELECT *\nFROM input_1\nWHERE id > 10"
        node = input_schema.NodeSqlQuery(
            flow_id=1,
            node_id=1,
            sql_query_input=transform_schema.SqlQueryInput(sql_code=sql),
            depending_on_ids=[],
        )
        assert node.get_default_description() == "SELECT *"
