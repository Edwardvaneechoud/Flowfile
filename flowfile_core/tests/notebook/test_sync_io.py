"""A sync reads, connects, walks, decrypts and executes nothing beyond what the canvas does to show a schema.

A sync is the clean run a push makes: every cell interpreted on a fresh session graph, entered on the canvas
snapshot as data (``enter_snapshot_session``). Nodes that would read, connect or run code are held and seeded
from their unchanged canvas twin, else from what the cell declares, else from the canvas's own header probe
of a local file or a catalog table's registered schema, else with no columns; the cells below a column-less
node still build. In multi-user mode a cell placing a cloud, database or Kafka node that the canvas would
refuse fails on its own line before the node exists.
"""

from __future__ import annotations

import glob
import logging
import os
import socket
import traceback
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
from cryptography.fernet import Fernet

from flowfile_core.auth import jwt as jwt_module
from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import flow_graph as flow_graph_module
from flowfile_core.flowfile import subflow
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_database_connection,
    store_database_connection,
)
from flowfile_core.flowfile.flow_data_engine.create import funcs as create_funcs
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import PolarsCodeParser
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import streaming, subprocess_operations
from flowfile_core.flowfile.flow_graph import FlowGraph, list_files_schema
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.schema_callback import SingleExecutionFuture
from flowfile_core.flowfile.manage import io_flowfile
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
from flowfile_core.notebook.bridge import CleanRunRequest
from flowfile_core.notebook.interpret import CellInterpreter
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from flowfile_core.notebook.runner import NotebookRunner
from flowfile_core.schemas import input_schema
from flowfile_frame import native, notebook
from flowfile_frame.notebook_cells import clean_run, enter_snapshot_session, execute_cell, new_namespace
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, no_kernel_manager

EXCEL = Path(__file__).resolve().parents[1] / "support_files" / "data" / "fake_data.xlsx"
IMPORTS = "import flowfile as fl"
DATABASE_CONNECTION = "notebook_sync_database"
LITERAL_COLLECTS = {
    "add_datasource": "a manual input's own rows",
    "_raw_data_from_sample": "a flow input's sample rows",
    "_compute_renamed_names": "column names through a rename formula",
}
READERS = (
    "scan_csv", "scan_parquet", "scan_ipc", "scan_ndjson", "scan_delta", "read_csv", "read_parquet", "read_ipc",
    "read_ipc_stream", "read_json", "read_ndjson", "read_excel", "read_avro", "read_database", "read_database_uri",
)  # fmt: skip
PACKAGES = ("/flowfile_core/flowfile_core/", "/flowfile_frame/flowfile_frame/", "/shared/")


def _rendered(graph: FlowGraph) -> tuple[list[tuple[str, str]], dict, int, dict]:
    """The cells, provenance, ceiling and canvas snapshot a push sends for ``graph``."""
    rendering = render(graph)
    cells = [(cell.cell_id, cell.code) for cell in rendering.cells]
    provenance = {
        cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
        for cell in rendering.cells
        if cell.node_ids
    }
    return cells, provenance, max((node.node_id for node in graph.nodes), default=0), seed_snapshot(graph)


def _sync(cells, snapshot=None, provenance=None, ceiling=0) -> dict:
    """The sync a push makes: the snapshot session, then every cell through the interpreter."""
    with no_kernel_manager(), notebook.RUN_LOCK:
        try:
            if snapshot is not None:
                enter_snapshot_session(snapshot, user_id=NOTEBOOK_OWNER_ID)
            return clean_run(cells, ceiling, provenance or {}, user_id=NOTEBOOK_OWNER_ID, executor=CellInterpreter())
        finally:
            notebook.exit()


@contextmanager
def _snapshot_session(snapshot: dict, provenance: dict):
    """A sync mode on ``snapshot`` whose cells run one at a time, so each binding's schema can be read."""
    with no_kernel_manager(), notebook.RUN_LOCK:
        try:
            mode = enter_snapshot_session(snapshot, user_id=NOTEBOOK_OWNER_ID)
            mode.expected = {cell: [tuple(entry) for entry in entries] for cell, entries in provenance.items()}
            namespace = new_namespace()
            yield mode, namespace
        finally:
            notebook.exit()


def _run(namespace: dict, cell_id: str, code: str) -> None:
    result = execute_cell(cell_id, code, namespace, executor=CellInterpreter())
    assert result.ok, (cell_id, result.line, result.message)


def _names(frame) -> list[str]:
    return frame.collect_schema().names()


class _Calls:
    """Records calls to patched functions, each with the function names on its stack."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def record(self, monkeypatch, owner, name: str, *, label: str | None = None, ours_only: bool = False) -> None:
        original = getattr(owner, name)
        calls = self.calls
        label = label or name

        def recorded(*args, **kwargs):
            stack = traceback.extract_stack()[:-1]
            if not ours_only or any(any(p in frame.filename.replace("\\", "/") for p in PACKAGES) for frame in stack):
                calls.append((label, tuple(frame.name for frame in stack)))
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, recorded)

    def named(self, label: str) -> list[tuple[str, ...]]:
        return [stack for called, stack in self.calls if called == label]

    def labels(self) -> set[str]:
        return {label for label, _ in self.calls}


def _record_io(monkeypatch, calls: _Calls) -> list[str]:
    """Record every read, connection, worker offload, walk, decrypt, flow-file load, polars-code run and callback start.

    Returns the held nodes that ran.
    """
    for reader in READERS:
        calls.record(monkeypatch, pl, reader, label=f"pl.{reader}")
    calls.record(monkeypatch, pl.LazyFrame, "collect", label="collect")
    calls.record(monkeypatch, FlowDataEngine, "create_from_path")
    for eager in ("create_from_path_excel", "create_from_path_avro", "create_from_path_ipc_stream"):
        calls.record(monkeypatch, create_funcs, eager)
    calls.record(monkeypatch, PolarsCodeParser, "get_executable")
    calls.record(monkeypatch, subflow, "predict_run_flow_named_schemas")
    calls.record(monkeypatch, SqlSource, "get_schema")
    calls.record(monkeypatch, flow_graph_module, "infer_topic_schema")
    calls.record(monkeypatch, flow_graph_module, "read_kafka_source")
    calls.record(monkeypatch, FlowDataEngine, "from_cloud_storage_obj")
    calls.record(monkeypatch, Fernet, "decrypt")
    calls.record(monkeypatch, socket.socket, "connect")
    calls.record(monkeypatch, subprocess_operations.BaseFetcher, "__init__", label="worker offload")
    for module in (streaming, subprocess_operations):
        calls.record(monkeypatch, module, "streaming_start")
    for module in (jwt_module, streaming, subprocess_operations):
        calls.record(monkeypatch, module, "get_internal_token")
    calls.record(monkeypatch, FlowDataEngine, "_fetch_null_profile")
    calls.record(monkeypatch, io_flowfile, "_load_flow_storage")
    calls.record(monkeypatch, SingleExecutionFuture, "start")
    for module, name in ((os, "scandir"), (os, "walk"), (os, "listdir"), (glob, "glob"), (glob, "iglob")):
        calls.record(monkeypatch, module, name, ours_only=True)
    calls.record(monkeypatch, Path, "glob", ours_only=True)
    calls.record(monkeypatch, Path, "iterdir", ours_only=True)
    held_ran: list[str] = []
    original = FlowNode.get_resulting_data

    def get_resulting_data(node):
        runs = node.results.resulting_data is None and node.results.errors is None and not node.deferred_until_run
        if runs and notebook.current() is not None and native._held(node):
            held_ran.append(node.node_type)
        return original(node)

    monkeypatch.setattr(FlowNode, "get_resulting_data", get_resulting_data)
    return held_ran


def _collects_beyond(calls: _Calls, allowed: set[str]) -> list[tuple[str, ...]]:
    return [stack for stack in calls.named("collect") if not allowed & set(stack)]


def test_a_corpus_sync_reads_connects_walks_decrypts_and_executes_nothing(notebook_corpus, monkeypatch):
    prepared = [(name, _rendered(graph)) for name, graph in notebook_corpus]
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    results = {}
    for name, (cells, provenance, ceiling, snapshot) in prepared:
        results[name] = _sync(cells, snapshot, provenance, ceiling)
    failed = {name: result.get("message") for name, result in results.items() if not result["ok"]}
    assert not failed, failed
    assert held_ran == []
    assert _collects_beyond(calls, set(LITERAL_COLLECTS)) == []
    assert calls.labels() <= {"collect"}, sorted(calls.labels())


def test_every_corpus_flow_still_syncs_when_no_held_node_has_a_canvas_twin(notebook_corpus, monkeypatch):
    prepared = [(name, _rendered(graph)) for name, graph in notebook_corpus]
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    failed = {}
    for name, (cells, _, ceiling, snapshot) in prepared:
        result = _sync(cells, snapshot, {}, ceiling)
        if not result["ok"]:
            failed[name] = (result["cell_id"], result["line"], result["message"])
    assert not failed, failed
    assert held_ran == []
    assert _collects_beyond(calls, set(LITERAL_COLLECTS) | {"create_from_path"}) == []
    probes = {label for label in calls.labels() if label not in {"collect", "create_from_path"}}
    assert all(all("create_from_path" in stack for stack in calls.named(label)) for label in probes), sorted(probes)


def test_a_sync_starts_no_schema_callback_and_leaves_no_logger_or_log_file(notebook_corpus, monkeypatch):
    cells, provenance, ceiling, snapshot = _rendered(dict(notebook_corpus)["simple_csv_read_and_filter"])
    graphs: list[FlowGraph] = []
    scratch = notebook._scratch_graph
    monkeypatch.setattr(notebook, "_scratch_graph", lambda: graphs.append(scratch()) or graphs[-1])
    calls = _Calls()
    calls.record(monkeypatch, SingleExecutionFuture, "start")
    new_cells = cells + [("cell-new", "more = fl.read_csv(" + repr(_csv_path_of(snapshot)) + ")")]
    assert _sync(new_cells, snapshot, provenance, ceiling)["ok"]
    assert calls.named("start") == []
    assert len(graphs) == 2
    for graph in graphs:
        log_file = get_flow_log_file(graph.flow_id)
        assert FlowLogger._instances.get(graph.flow_id) is None
        assert not log_file.exists()
        handlers = [h for logger in _loggers() for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert all(Path(handler.baseFilename) != log_file for handler in handlers)


def _loggers() -> list[logging.Logger]:
    return [logger for logger in logging.Logger.manager.loggerDict.values() if isinstance(logger, logging.Logger)]


def _csv_path_of(snapshot: dict) -> str:
    return next(
        node["setting_input"]["received_file"]["path"]
        for node in snapshot["flowfile_data"]["nodes"]
        if node["type"] == "read"
    )


@pytest.fixture
def canvas(tmp_path):
    """A canvas with an Excel read and a CSV read, built as a script would, plus a second CSV file."""
    import flowfile_frame as ff

    pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_csv(tmp_path / "rows.csv")
    pl.DataFrame({"n": [1], "m": [2.0]}).write_csv(tmp_path / "other.csv")
    graph = ff.create_flow_graph()
    excel = ff.read_excel(str(EXCEL), sheet_name="Sheet1", flow_graph=graph)
    rows = ff.read_csv(str(tmp_path / "rows.csv"), flow_graph=graph)
    return graph, excel.node_id, rows.node_id, tmp_path


def _cell_of(graph: FlowGraph, node_id: int) -> tuple[str, str]:
    cell = next(cell for cell in render(graph).cells if node_id in cell.node_ids)
    return cell.cell_id, cell.code


def test_an_unchanged_held_source_is_seeded_from_its_canvas_twin_without_reading(canvas, monkeypatch):
    graph, excel_id, rows_id, _ = canvas
    _, provenance, _, snapshot = _rendered(graph)
    snapshot["schemas"][excel_id] = {"output-0": [{"name": "from_canvas", "data_type": "String"}]}
    snapshot["schemas"][rows_id] = {"output-0": [{"name": "also_from_canvas", "data_type": "Int64"}]}
    calls = _Calls()
    _record_io(monkeypatch, calls)
    with _snapshot_session(snapshot, provenance) as (_, namespace):
        for node_id in (excel_id, rows_id):
            _run(namespace, *_cell_of(graph, node_id))
        bound = [value for value in namespace.values() if hasattr(value, "node_id") and hasattr(value, "_deferred")]
    assert sorted(_names(frame)[0] for frame in bound) == ["also_from_canvas", "from_canvas"]
    assert calls.labels() <= {"collect"} and calls.named("collect") == []


def test_an_edited_held_source_seeds_from_the_declaration_then_the_probe_then_no_columns(canvas, monkeypatch):
    graph, excel_id, _, tmp_path = canvas
    _, provenance, _, snapshot = _rendered(graph)
    excel_cell, excel_code = _cell_of(graph, excel_id)
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    with _snapshot_session(snapshot, provenance) as (mode, namespace):
        _run(namespace, "imports", IMPORTS)
        _run(namespace, excel_cell, excel_code.replace('"Sheet1"', '"Sheet2"').replace("'Sheet1'", "'Sheet2'"))
        edited = next(value for name, value in namespace.items() if name.startswith(("source_", "read_")))
        _run(namespace, "declared", f"listed = fl.list_files({str(tmp_path)!r})")
        _run(namespace, "probed", f"probed = fl.read_csv({str(tmp_path / 'other.csv')!r})")
        assert _names(edited) == [] and edited.node_id in mode.column_less
        assert _names(namespace["listed"]) == [column.column_name for column in list_files_schema()]
        assert _names(namespace["probed"]) == ["n", "m"]
        assert namespace["probed"].node_id not in mode.column_less
    assert held_ran == []
    assert [stack[-1] for stack in calls.named("create_from_path")] == ["_canvas_probe"]
    assert _collects_beyond(calls, {"create_from_path"}) == []
    assert calls.labels() <= {"collect", "create_from_path", "pl.scan_csv"}, sorted(calls.labels())
    assert all("create_from_path" in stack for stack in calls.named("pl.scan_csv"))


def test_a_canvas_twin_without_columns_gives_way_to_the_probe_and_then_to_no_columns(canvas, monkeypatch):
    graph, excel_id, rows_id, _ = canvas
    cells, provenance, _, snapshot = _rendered(graph)
    snapshot["schemas"][excel_id] = snapshot["schemas"][rows_id] = {"output-0": []}
    names = render(graph).var_by_node
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    with _snapshot_session(snapshot, provenance) as (mode, namespace):
        for cell_id, code in cells:
            _run(namespace, cell_id, code)
        _run(namespace, "below", f"kept = {names[excel_id]}.filter(fl.col('ID') > 1)")
        excel, rows = namespace[names[excel_id]], namespace[names[rows_id]]
        assert _names(rows) == ["a", "b"] and rows.node_id not in mode.column_less
        assert _names(excel) == [] and excel.node_id in mode.column_less
    assert held_ran == []
    assert [stack[-1] for stack in calls.named("create_from_path")] == ["_canvas_probe"]
    assert _collects_beyond(calls, {"create_from_path"}) == []
    assert calls.labels() <= {"collect", "create_from_path", "pl.scan_csv"}, sorted(calls.labels())


def test_an_unbound_same_type_line_above_a_held_source_leaves_it_its_canvas_twin(canvas, monkeypatch):
    graph, _, rows_id, tmp_path = canvas
    cells, provenance, ceiling, snapshot = _rendered(graph)
    snapshot["schemas"][rows_id] = {"output-0": [{"name": "from_canvas", "data_type": "Int64"}]}
    rows_cell, _ = _cell_of(graph, rows_id)
    scratch = f"fl.read_csv({str(tmp_path / 'other.csv')!r})\n"
    edited = [(cell_id, scratch + code if cell_id == rows_cell else code) for cell_id, code in cells]
    with _snapshot_session(snapshot, provenance) as (_, namespace):
        for cell_id, code in edited:
            _run(namespace, cell_id, code)
        assert _names(namespace[render(graph).var_by_node[rows_id]]) == ["from_canvas"]
    result = _sync(edited, snapshot, provenance, ceiling)
    assert result["ok"], result.get("message")
    assert result["cells"][rows_cell] == [rows_id]


def test_a_python_script_seeds_from_its_declared_returns(notebook_corpus):
    cells = [
        (IMPORTS + "\nsrc = fl.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}], 'data': [[1]]})"),
        "@fl.python_script(kernel='nb', returns={'total': fl.Int64})\ndef summed(frame):\n    return frame\n\n\n"
        "out = summed(src)",
    ]
    with _snapshot_session({"flowfile_data": _empty_flow(), "schemas": {}}, {}) as (_, namespace):
        for index, code in enumerate(cells):
            _run(namespace, f"cell-{index}", code)
        assert _names(namespace["out"]) == ["total"]


def _empty_flow() -> dict:
    return FlowGraph().get_flowfile_data().model_dump(mode="json")


def test_a_new_catalog_read_takes_the_registered_schema_without_opening_the_table(notebook_corpus, monkeypatch):
    demo = dict(notebook_corpus)["demo"]
    reader = next(node for node in demo.nodes if node.node_type == "catalog_reader")
    settings = reader.setting_input
    code = (
        f"{IMPORTS}\nsales = fl.read_catalog_table({settings.catalog_table_name!r}, "
        f"namespace_id={settings.catalog_namespace_id})"
    )
    calls = _Calls()
    _record_io(monkeypatch, calls)
    with _snapshot_session({"flowfile_data": _empty_flow(), "schemas": {}}, {}) as (_, namespace):
        _run(namespace, "cell-new", code)
        names = _names(namespace["sales"])
    assert names == [column.column_name for column in reader.schema]
    assert calls.labels() <= {"collect"} and calls.named("collect") == []


def test_polars_code_keeps_its_twin_schema_only_while_its_code_and_input_columns_hold(notebook_corpus, monkeypatch):
    graph = dict(notebook_corpus)["custom_polars_code_multiple_inputs"]
    cells, provenance, _, snapshot = _rendered(graph)
    code_node = next(node for node in graph.nodes if node.node_type == "polars_code")
    code_cell = next(
        cell_id for cell_id, entries in provenance.items() if ("polars_code", code_node.node_id) in entries
    )
    source_cell = next(cell_id for cell_id, code in cells if "'name': 'col1'" in code)
    calls = _Calls()
    _record_io(monkeypatch, calls)

    def run(edit=None) -> list[str]:
        with _snapshot_session(snapshot, provenance) as (_, namespace):
            for cell_id, code in cells:
                _run(namespace, cell_id, edit(cell_id, code) if edit else code)
            frame = next(v for v in namespace.values() if getattr(v, "node_id", None) and _is_code(v))
            return _names(frame)

    def edit(cell_id, old, new):
        return lambda cell, code: code.replace(old, new) if cell == cell_id else code

    assert run() == ["col1", "col2"] == [column.column_name for column in code_node.schema]
    assert run(edit(code_cell, "how='cross')", "how='cross').select('col1')")) == []
    assert run(edit(source_cell, "'name': 'col1'", "'name': 'renamed'")) == []
    assert calls.named("get_executable") == []


def _is_code(frame) -> bool:
    node = frame.flow_graph.get_node(frame.node_id)
    return node is not None and node.node_type == "polars_code"


@pytest.fixture
def database_connection():
    connection = input_schema.FullDatabaseConnection(
        connection_name=DATABASE_CONNECTION, database_type="postgresql", host="127.0.0.1", port=1,
        database="nowhere", username="nobody", password="unused", ssl_enabled=False,
    )  # fmt: skip
    with get_db_context() as db:
        if get_database_connection(db, DATABASE_CONNECTION, NOTEBOOK_OWNER_ID) is None:
            store_database_connection(db, connection, user_id=NOTEBOOK_OWNER_ID)
    return DATABASE_CONNECTION


def test_cells_below_a_column_less_source_still_sync_without_connecting(database_connection, monkeypatch):
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    cells = [
        ("imports", IMPORTS),
        ("rows", f"rows = fl.read_database({database_connection!r}, table_name='orders')"),
        ("kept", "kept = rows.filter(fl.col('x') > 1).select('x')"),
    ]
    result = _sync(cells)
    assert result["ok"], result.get("message")
    select = next(node for node in result["flowfile_data"]["nodes"] if node["type"] == "select")
    assert [entry["old_name"] for entry in select["setting_input"]["select_input"]] == ["x"]
    assert held_ran == []
    assert calls.labels() <= {"collect"} and calls.named("collect") == []


def test_a_cell_failing_below_a_column_less_source_syncs_with_a_warning_naming_its_node(database_connection):
    cells = [
        ("imports", IMPORTS),
        ("rows", f"rows = fl.read_database({database_connection!r}, table_name='orders')"),
        ("broken", "broken = rows.filter(fl.col('amonut') > 1)"),
    ]
    with no_kernel_manager():
        result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, CleanRunRequest(cells=cells))
    assert result.error is None, result.error
    [filter_id] = result.node_ids_by_cell["broken"]
    [warning] = result.warnings
    assert warning.startswith(f"Cell broken: node {filter_id} (filter)") and "amonut" in warning


KAFKA_CONNECTION = "notebook_sync_kafka"
SOURCE = "src = fl.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Double'}], 'data': [[1.0, 2.0]]})"
NEW_HELD_NODES = {
    "kafka": (f"rows = fl.read_kafka({KAFKA_CONNECTION!r}, topic_name='orders')", "kafka_source"),
    "rest api": ("rows = fl.read_api('http://127.0.0.1:1/rows')", "rest_api_reader"),
    "directory read": ("rows = fl.read_csv(<tmp>)", "read"),
    "url read": ("rows = fl.read_csv('https://127.0.0.1:1/rows.csv')", "read"),
    "excel read": ("rows = fl.read_excel(<excel>, sheet_name='Sheet1')", "read"),
    "custom node": (
        f"{SOURCE}\nrows = fl.custom_nodes.mood_emoji(src, source_column='a', threshold_value=1, "
        "emoji_column_name='mood', add_random_sparkle=False)",
        "mood_emoji",
    ),
    "polars code source": ("rows = fl.polars_code('output_df = pl.LazyFrame({\"a\": [1]})')", "polars_code"),
    "null column cleansing": (f"{SOURCE}\nrows = src.data_cleansing(remove_null_columns=True)", "data_cleansing"),
}


@pytest.fixture
def kafka_connection():
    from flowfile_core.kafka.connection_manager import get_kafka_connection_by_name, store_kafka_connection
    from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate

    with get_db_context() as db:
        if get_kafka_connection_by_name(db, KAFKA_CONNECTION, NOTEBOOK_OWNER_ID) is None:
            connection = KafkaConnectionCreate(connection_name=KAFKA_CONNECTION, bootstrap_servers="127.0.0.1:1")
            store_kafka_connection(db, connection, NOTEBOOK_OWNER_ID)
    return KAFKA_CONNECTION


@pytest.mark.parametrize("case", sorted(NEW_HELD_NODES))
def test_a_new_held_node_is_placed_and_seeded_without_reading_connecting_or_running(
    case, kafka_connection, notebook_corpus, tmp_path, monkeypatch
):
    pl.DataFrame({"a": [1]}).write_csv(tmp_path / "a.csv")
    code, node_type = NEW_HELD_NODES[case]
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    cell = code.replace("<tmp>", repr(str(tmp_path / "*.csv"))).replace("<excel>", repr(str(EXCEL)))
    result = _sync(
        [("imports", IMPORTS + "\nimport polars as pl"), ("new", cell + "\nkept = rows.select(fl.col('a'))")]
    )
    assert result["ok"], (result.get("line"), result.get("message"))
    assert node_type in {node["type"] for node in result["flowfile_data"]["nodes"]}
    assert held_ran == []
    assert calls.labels() <= {"collect"} and _collects_beyond(calls, set(LITERAL_COLLECTS)) == []


CLEANSING_ROWS = {
    "columns": [
        {"name": "a", "data_type": "Integer"},
        {"name": "empty", "data_type": "String"},
        {"name": "text", "data_type": "String"},
    ],
    "data": [[1, 2], [None, None], [" x", "y "]],
}


def test_a_null_column_cleansing_keeps_its_canvas_schema_and_a_whitespace_one_still_builds(monkeypatch):
    """Dropping all-null columns counts every column's nulls, which the canvas does only when the node runs."""
    import flowfile_frame as ff

    graph = ff.create_flow_graph()
    rows = ff.from_raw_data(CLEANSING_ROWS, flow_graph=graph)
    nulls = rows.data_cleansing(remove_null_columns=True).node_id
    trimmed = rows.data_cleansing(["text"]).node_id
    cells, provenance, _, snapshot = _rendered(graph)
    names = render(graph).var_by_node
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    with _snapshot_session(snapshot, provenance) as (mode, namespace):
        for cell_id, code in cells:
            _run(namespace, cell_id, code)
        assert namespace[names[nulls]]._deferred and not namespace[names[trimmed]]._deferred
        assert _names(namespace[names[nulls]]) == ["a", "text"]
        assert _names(namespace[names[trimmed]]) == ["a", "empty", "text"]
        assert mode.column_less == set()
    assert held_ran == []
    assert calls.labels() <= {"collect"} and _collects_beyond(calls, set(LITERAL_COLLECTS)) == []


DATA_DEPENDENT_HOLDS = {"data_cleansing", "dynamic_rename", "fuzzy_match", "pivot", "random_split"}
HELD_ROWS = {
    "columns": [
        {"name": "k", "data_type": "Integer"},
        {"name": "name", "data_type": "String"},
        {"name": "v", "data_type": "Integer"},
        {"name": "empty", "data_type": "String"},
    ],
    "data": [[1, 1, 2], ["ann", "bob", "ann"], [10, 20, 30], [None, None, None]],
}


@pytest.fixture
def every_data_dependent_hold() -> FlowGraph:
    """A canvas placing, over literal rows, every transform a sync holds because building it reads the rows."""
    import flowfile_frame as ff

    graph = ff.create_flow_graph()
    rows = ff.from_raw_data(HELD_ROWS, flow_graph=graph)
    people = ff.from_raw_data(
        {"columns": [{"name": "name", "data_type": "String"}], "data": [["ann"]]}, flow_graph=graph
    )
    rows.data_cleansing(remove_null_columns=True)
    rows.dynamic_rename("first_row", columns=["name"])
    rows.fuzzy_join(people, [ff.FuzzyMapping("name", threshold_score=40)])
    rows.random_split({"train": 50, "test": 50}, seed=1)
    rows.pivot("name", index="k", values="v", aggregate_function="sum")
    return graph


@pytest.mark.parametrize("twins", [True, False], ids=["canvas twins", "no canvas twins"])
def test_a_sync_of_every_data_dependent_hold_offloads_decrypts_profiles_and_reads_nothing(
    twins, every_data_dependent_hold, monkeypatch
):
    graph = every_data_dependent_hold
    cells, provenance, ceiling, snapshot = _rendered(graph)
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    result = _sync(cells, snapshot, provenance if twins else {}, ceiling)
    assert result["ok"], (result.get("cell_id"), result.get("line"), result.get("message"))
    assert held_ran == []
    assert calls.labels() <= {"collect"} and _collects_beyond(calls, set(LITERAL_COLLECTS)) == []
    held = {node.node_type for node in graph.nodes if native.held_in_sync(node.node_type, node.setting_input)}
    assert held == DATA_DEPENDENT_HOLDS and native.SYNC_HELD_NODE_TYPES <= held
    assert held <= {node["type"] for node in result["flowfile_data"]["nodes"]}


@pytest.mark.parametrize("function", ["read_csv", "`read_csv`"])
def test_a_sql_table_function_fails_the_canvas_check_on_its_line_and_reads_nothing(function, tmp_path, monkeypatch):
    pl.DataFrame({"a": [1]}).write_csv(tmp_path / "a.csv")
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    query = f"SELECT * FROM {function}('{(tmp_path / 'a.csv').as_posix()}')"
    cell = f"kept = 1\nrows = fl.sql({query!r})"
    result = _sync([("imports", IMPORTS), ("reads", cell)])
    assert (result["ok"], result["cell_id"], result["line"]) == (False, "reads", 2)
    assert "SQL table functions are not allowed" in result["message"]
    assert held_ran == []
    assert calls.labels() <= {"collect"} and calls.named("collect") == []


MULTI_USER_REFUSALS = {
    "a local path for a cloud read": (
        "rows = fl.read_from_cloud_storage('/etc/hosts', file_format='csv')",
        "is a local path, which this server does not allow",
    ),
    "a cloud read without a connection": (
        "rows = fl.read_from_cloud_storage('s3://bucket/rows.parquet')",
        "Select a cloud storage connection; server credentials are not available in multi-user mode.",
    ),
    "an unknown cloud connection": (
        "rows = fl.read_from_cloud_storage('s3://bucket/rows.parquet', connection_name='nobody_has_this')",
        "Cloud connection settings not found",
    ),
    "an unknown database connection": (
        "rows = fl.read_database('nobody_has_this', table_name='orders')",
        "Database connection 'nobody_has_this' not found or not accessible for this user",
    ),
    "an unknown Kafka connection": (
        "rows = fl.read_kafka('nobody_has_this', topic_name='orders')",
        "Kafka connection not found",
    ),
}


@pytest.mark.parametrize("case", sorted(MULTI_USER_REFUSALS))
def test_a_placement_the_canvas_refuses_fails_on_its_line_before_the_node_exists(case, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    code, message = MULTI_USER_REFUSALS[case]
    placed: list[str] = []
    add_node_step = FlowGraph.add_node_step

    def recording(graph, *args, **kwargs):
        placed.append(kwargs.get("node_type"))
        return add_node_step(graph, *args, **kwargs)

    monkeypatch.setattr(FlowGraph, "add_node_step", recording)
    calls = _Calls()
    _record_io(monkeypatch, calls)
    result = _sync([("imports", IMPORTS), ("placing", "kept = 1\n" + code)])
    assert not result["ok"]
    assert (result["cell_id"], result["line"], result["kind"]) == ("placing", 2, "refused")
    assert message in result["message"]
    assert [refusal for refusal in result["refusals"] if message in refusal] == [result["refusals"][-1]]
    assert not {"cloud_storage_reader", "database_reader", "kafka_source"} & set(placed)
    assert calls.labels() <= {"collect"} and calls.named("collect") == []


def test_a_single_user_install_places_a_local_cloud_path_and_reads_nothing(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    result = _sync(
        [("imports", IMPORTS), ("rows", "rows = fl.read_from_cloud_storage('/etc/hosts', file_format='csv')")]
    )
    assert result["ok"], result.get("message")
    assert [node["type"] for node in result["flowfile_data"]["nodes"]] == ["cloud_storage_reader"]
    assert held_ran == []
    assert calls.labels() <= {"collect"} and calls.named("collect") == []


DESCRIBED = "description='Orders feed'"
DESCRIBED_READERS = {
    "catalog table": (f"fl.read_catalog_table('orders', namespace_id=1, {DESCRIBED})", "catalog_reader"),
    "catalog sql": (f"fl.read_catalog_sql('SELECT * FROM orders', {DESCRIBED})", "catalog_reader"),
    "database": (f"fl.read_database({DATABASE_CONNECTION!r}, table_name='orders', {DESCRIBED})", "database_reader"),
    "rest api": (f"fl.read_api('http://127.0.0.1:1/rows', {DESCRIBED})", "rest_api_reader"),
    "kafka": (f"fl.read_kafka({KAFKA_CONNECTION!r}, topic_name='orders', {DESCRIBED})", "kafka_source"),
    **{
        f"cloud {file_format}": (
            f"fl.read_from_cloud_storage('/etc/hosts', file_format={file_format!r}, {DESCRIBED})",
            "cloud_storage_reader",
        )
        for file_format in ("csv", "parquet", "json", "delta")
    },
}


@pytest.mark.parametrize("case", sorted(DESCRIBED_READERS))
def test_a_reader_call_the_export_describes_syncs_and_keeps_its_description(
    case, database_connection, kafka_connection, monkeypatch
):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    call, node_type = DESCRIBED_READERS[case]
    calls = _Calls()
    held_ran = _record_io(monkeypatch, calls)
    result = _sync([("imports", IMPORTS), ("rows", f"rows = {call}")])
    assert result["ok"], (result.get("line"), result.get("message"))
    [node] = [node for node in result["flowfile_data"]["nodes"] if node["type"] == node_type]
    assert node["description"] == "Orders feed"
    assert held_ran == []
    assert calls.labels() <= {"collect"} and calls.named("collect") == []
