"""The canvas-notebook corpus: every flow the renderer, clean run and reconcile are held to.

Three sources, all built Docker-free under the test session's scratch DB and storage:

* the codegen corpus: flows built by ``tests/flowfile/test_code_generator.py``. Its builders live inline in
  its test functions, so each listed test runs with an ``export_func`` that raises at the first export and
  hands back the flow as built. Only tests without Docker, Kafka or catalog-wipe side effects are listed;
* frame-built native nodes (a parameter gate with then/else, a flow input and output);
* the showcase demo (``flowfile_frame/tests/fixtures/demo_catalog_pipeline.py``): a seeded catalog with the
  ``sales`` and ``regions`` tables, its child flow registered through ``publish_clean_orders``, the
  ``mood_emoji`` custom node installed, and ``build_sales_analytics`` built with ``register_flow`` stubbed.
  It covers ``catalog_reader``/``catalog_writer``, ``run_flow``, ``python_script`` with cells and declared
  output schemas, ``sql_query``, a gate with then/else, a union and a custom node.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl

from flowfile_core.flowfile.flow_graph import FlowGraph
from test_utils.notebook_demo import REGIONS, SALES, installed_mood_emoji, load_demo

PLACEHOLDERS_FILE = Path(__file__).parent / "placeholders.json"

CODEGEN_TESTS = (
    "test_aggregation_functions",
    "test_complex_workflow",
    "test_cross_join_operation",
    "test_custom_polars_code_multiple_inputs",
    "test_custom_polars_no_inputs",
    "test_data_cleansing_every_string_rule",
    "test_data_type_conversions",
    "test_dynamic_rename_formula_round_trip",
    "test_excel_read",
    "test_filter_split_mode_pass_and_fail",
    "test_flow_with_disconnected_nodes",
    "test_formula_node",
    "test_fusion_explore_data_elided_mid_chain",
    "test_fusion_grouped_record_id_self_reference_preserved",
    "test_fusion_keeps_named_boundaries_at_join",
    "test_fuzzy_match_with_multiple_columns",
    "test_graph_solver",
    "test_manual_input_with_select",
    "test_multi_field_formula_replace_all_round_trip",
    "test_multiple_output_formats",
    "test_node_reference_in_join",
    "test_parquet_read",
    "test_pivot_operation",
    "test_random_split_per_handle_downstream",
    "test_sample_random_rows_operation",
    "test_simple_csv_read_and_filter",
    "test_sort_and_unique_operations",
    "test_sql_query_flow_parameter_becomes_a_pipeline_argument",
    "test_sql_query_multiple_inputs_keep_their_order",
    "test_text_to_rows_operation",
    "test_train_apply_evaluate_full_chain_round_trip",
    "test_union_multiple_dataframes",
    "test_unpivot_operation",
    "test_wait_for_round_trip",
    "test_window_functions_partition_aggregate",
)

class _Captured(Exception):
    def __init__(self, flow: FlowGraph):
        self.flow = flow


_EXPORTERS = ("export_flow_to_polars", "export_flow_to_flowframe")


def _capture(flow: FlowGraph, *args, **kwargs):
    raise _Captured(flow)


def capture_codegen_flow(test_name: str, tmp_dir: Path) -> FlowGraph:
    """Run a codegen test up to its first export and return the flow it built.

    Both the ``export_func`` parameter and the module's own exporter names are swapped for the capture, since
    some tests call ``export_flow_to_flowframe`` directly.
    """
    import inspect

    from tests.flowfile import test_code_generator

    test = getattr(test_code_generator, test_name)
    params = inspect.signature(test).parameters
    kwargs = {"export_func": _capture} if "export_func" in params else {}
    if "tmp_path" in params:
        kwargs["tmp_path"] = tmp_dir
    exporters = {name: getattr(test_code_generator, name) for name in _EXPORTERS}
    for name in _EXPORTERS:
        setattr(test_code_generator, name, _capture)
    try:
        test(**kwargs)
    except _Captured as captured:
        return captured.flow
    finally:
        for name, exporter in exporters.items():
            setattr(test_code_generator, name, exporter)
    raise AssertionError(f"{test_name} returned without exporting a flow")


def build_native_gate() -> FlowGraph:
    import flowfile as fl

    mode = fl.Parameter("mode", default="full", type="enum", enum_values=["full", "quick"])
    source = fl.from_dict({"region": ["N", "S", "N"], "amount": [1.0, 2.0, 3.0]})
    fl.add_flow_parameter(source, mode)
    gate = fl.Gate(source, parameter=mode, value="full", description="Full or quick?")
    full = gate.then.with_columns(fl.lit("full").alias("mode"))
    quick = gate.otherwise.group_by("region").agg(fl.col("amount").sum()).with_columns(fl.lit("quick").alias("mode"))
    return fl.concat([full, quick], how="diagonal_relaxed").flow_graph


def build_native_flow_io() -> FlowGraph:
    import flowfile as fl

    graph = fl.create_flow_graph()
    orders = fl.FlowInput("orders", sample=pl.DataFrame({"id": [1, 2, 3], "amount": [5, 15, 25]}), flow_graph=graph)
    orders.filter(fl.col("amount") > 10).to_flow_output(fl.FlowOutput("big_orders"))
    return graph


def build_python_script_cells() -> FlowGraph:
    """A kernel node with cells and a declared output schema, built on the canvas API (no kernel runs)."""
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.schemas import input_schema, schemas

    graph = FlowGraph(flow_settings=schemas.FlowSettings(flow_id=1, name="python_script_cells", path="."))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="amount", data_type="Float64")], data=[[1.0, 2.0]]
            ),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script"))
    cells = [
        input_schema.NotebookCell(id="load", code="df = flowfile.read_input().collect()"),
        input_schema.NotebookCell(
            id="publish", code='flowfile.publish_output(df.with_columns(double=df["amount"] * 2))'
        ),
    ]
    graph.add_python_script(
        input_schema.NodePythonScript(
            flow_id=1,
            node_id=2,
            depending_on_ids=[1],
            python_script_input=input_schema.PythonScriptInput(
                code="\n\n".join(cell.code for cell in cells), kernel_id="corpus_kernel", cells=cells
            ),
            output_schemas={
                "main": [
                    input_schema.MinimalFieldInfo(name="amount", data_type="Float64"),
                    input_schema.MinimalFieldInfo(name="double", data_type="Float64"),
                ]
            },
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


def _drop_catalog(name: str) -> None:
    """Delete the catalog ``name`` with its schemas, tables and flow registrations (the corpus's own seed)."""
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.service import CatalogService
    from flowfile_core.database import models as db_models
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        root = (
            db.query(db_models.CatalogNamespace)
            .filter(db_models.CatalogNamespace.name == name, db_models.CatalogNamespace.parent_id.is_(None))
            .first()
        )
        if root is None:
            return
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        children = db.query(db_models.CatalogNamespace).filter(db_models.CatalogNamespace.parent_id == root.id).all()
        for namespace_id in [child.id for child in children] + [root.id]:
            for table in db.query(db_models.CatalogTable).filter_by(namespace_id=namespace_id).all():
                service.delete_table(table.id, delete_file=True)
            for flow in db.query(db_models.FlowRegistration).filter_by(namespace_id=namespace_id).all():
                service.delete_flow(flow.id, delete_file=True)
            service.delete_namespace(namespace_id)


def _no_register(self, flow_or_frame, *, name, overwrite=False):
    return None


@contextmanager
def demo_graph() -> Iterator[FlowGraph]:
    """Seed the demo's catalog, register its child flow, install ``mood_emoji`` and build the analytics graph."""
    import flowfile as fl
    from flowfile_frame.catalog_reference import SchemaReference

    demo = load_demo()
    _drop_catalog(demo.CATALOG)
    with installed_mood_emoji():
        try:
            schema = fl.CatalogReference(demo.CATALOG, auto_create=True).schema(demo.SCHEMA, auto_create=True)
            schema.write_table(fl.from_dict(SALES), "sales")
            schema.write_table(fl.from_dict(REGIONS), "regions")
            clean_ref = demo.publish_clean_orders(schema)
            original = SchemaReference.register_flow
            SchemaReference.register_flow = _no_register
            try:
                graph = demo.build_sales_analytics(schema, clean_ref)
            finally:
                SchemaReference.register_flow = original
            yield graph
        finally:
            _drop_catalog(demo.CATALOG)


def build_corpus(tmp_dir_factory: Callable[[str], Path]) -> list[tuple[str, FlowGraph]]:
    """The Docker-free part of the corpus: the codegen flows and the frame-built native nodes."""
    corpus = [(name.removeprefix("test_"), capture_codegen_flow(name, tmp_dir_factory(name))) for name in CODEGEN_TESTS]
    corpus.append(("native_gate", build_native_gate()))
    corpus.append(("native_flow_io", build_native_flow_io()))
    corpus.append(("python_script_cells", build_python_script_cells()))
    return corpus


def load_expected_placeholders() -> dict[str, list[int]]:
    return json.loads(PLACEHOLDERS_FILE.read_text())
