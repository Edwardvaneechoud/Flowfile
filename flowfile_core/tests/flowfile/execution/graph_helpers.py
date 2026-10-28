"""Flow builders the execution tests share: a flow, the nodes on it and the edges between them."""

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas import input_schema, transform_schema
from tests.flowfile.conftest import add_test_manual_input, create_test_graph


def make_graph(flow_id: int, execution_location: str = "remote", **settings_fields) -> FlowGraph:
    """A Development-mode flow on ``execution_location``; ``settings_fields`` set any other flow setting."""
    return create_test_graph(flow_id, execution_location=execution_location, **settings_fields)


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "main") -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type=input_type))


def add_manual_input(graph: FlowGraph, node_id: int, rows: list[dict], **node_fields) -> None:
    """Manual input ``node_id`` holding ``rows``; ``node_fields`` set more of its settings (``cache_results``)."""
    add_test_manual_input(graph, rows, node_id, **node_fields)


def ranks(n: int) -> list[dict]:
    """``n`` rows of a ``name`` and a ``rank``, ranked from ``n`` down to 1."""
    return [{"name": f"r{i}", "rank": i} for i in range(n, 0, -1)]


def add_select_on(graph: FlowGraph, node_id: int, source_id: int, select: list[tuple[str, str]], keep_missing: bool):
    add_promise(graph, node_id, "select")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=source_id,
            select_input=[transform_schema.SelectInput(old_name=old, new_name=new) for old, new in select],
            keep_missing=keep_missing,
        )
    )
    connect(graph, source_id, node_id)


def add_filter_on(graph: FlowGraph, node_id: int, source_id: int, expression: str, **node_fields) -> None:
    """Filter ``node_id`` of ``source_id`` by the advanced ``expression``; ``node_fields`` set more of its settings."""
    add_promise(graph, node_id, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=source_id,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter=expression),
            **node_fields,
        )
    )
    connect(graph, source_id, node_id)


def add_sort_on(graph: FlowGraph, node_id: int, source_id: int, column: str, **node_fields) -> FlowNode:
    """Sort ``node_id`` of ``source_id`` on ``column`` ascending; ``node_fields`` set more of its settings."""
    add_promise(graph, node_id, "sort")
    connect(graph, source_id, node_id)
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=source_id,
            sort_input=[transform_schema.SortByInput(column=column, how="asc")],
            **node_fields,
        )
    )
    return graph.get_node(node_id)


def input_then_sort(graph: FlowGraph, rows: list[dict], column: str, *, cache_sort: bool = False) -> FlowGraph:
    """Manual input 1 holding ``rows`` → sort 2 on ``column``, which caches its result when ``cache_sort``."""
    add_manual_input(graph, 1, rows)
    add_sort_on(graph, 2, 1, column, cache_results=cache_sort)
    return graph


def code_flow(flow_id: int, node_type: str, code: str, rows: list[dict], **settings_fields) -> FlowGraph:
    """Manual input 1 holding ``rows`` → node 2, a ``polars_code`` or ``sql_query`` node running ``code`` over it
    (as ``input_1`` in SQL); neither has run. ``settings_fields`` go to ``make_graph``."""
    graph = make_graph(flow_id, **settings_fields)
    add_manual_input(graph, 1, rows)
    add_promise(graph, 2, node_type)
    if node_type == "polars_code":
        graph.add_polars_code(
            input_schema.NodePolarsCode(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_ids=[1],
                polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
            )
        )
    else:
        graph.add_sql_query(
            input_schema.NodeSqlQuery(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_ids=[1],
                sql_query_input=transform_schema.SqlQueryInput(sql_code=code),
            )
        )
    connect(graph, 1, 2)
    return graph
