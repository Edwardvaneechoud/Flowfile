"""Notebook build mode is local to the context that entered it, and a run leaves nothing behind.

Another thread never sees the mode, its user or its kernel refusal; the cell namespace's ``ff``
is built without importing ``flowfile``; and a seed plus a clean run leave no flow logger, log
file, ``linecache`` entry, snapshot or node-store entry behind.
"""

import contextvars
import linecache
import threading

import pytest

import flowfile
import flowfile_core.kernel as kernel_package
import flowfile_frame as ff
from flowfile_core.configs import node_store
from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.schema_callback import SingleExecutionFuture
from flowfile_core.schemas import input_schema
from flowfile_frame import notebook
from flowfile_frame._identity import current_user_id
from flowfile_frame.native import NativeNodeError
from flowfile_frame.notebook_cells import clean_run, exec_cell, new_namespace, seed_session
from shared.node_designer import CustomNodeBase
from shared.storage_config import storage
from test_utils.imports import unimportable

DATA = {"a": [1, 2, 3], "g": ["x", "x", "y"]}
CELLS = [("a", "df = ff.from_dict({'a': [1, 2, 3]})"), ("b", "big = df.filter(ff.col('a') > 1)")]
WEB_UI_NAMES = {"open_graph_in_editor", "start_web_ui"}
CELL_CLASS = (
    "class CellNode(ff.node_designer.CustomNodeBase):\n"
    "    node_name: str = 'Notebook Cell Node'\n"
    "    def process(self, *inputs):\n"
    "        return inputs[0]\n"
    "placed = ff.CustomNode(CellNode, ff.from_dict({'a': [1]}))\n"
)


class NotebookOnlyNode(CustomNodeBase):
    node_name: str = "Notebook Only Node"
    node_category: str = "Testing"

    def process(self, *inputs):
        return inputs[0]


def _node_store() -> tuple:
    return (
        dict(node_store.node_dict),
        [template.item for template in node_store.nodes_list],
        dict(node_store.CUSTOM_NODE_STORE._overrides),
    )


def _cell_files() -> set[str]:
    return {name for name in linecache.cache if name.startswith("<cell-")}


def _flow_logs() -> set:
    return set(storage.logs_directory.glob("flow_*.log"))


def _canvas_payload() -> dict:
    canvas = ff.create_flow_graph()
    ff.from_dict(DATA, flow_graph=canvas)
    return canvas.get_flowfile_data().model_dump(mode="json")


def test_a_mode_in_one_thread_is_invisible_to_another(monkeypatch):
    cached = object()
    monkeypatch.setattr(kernel_package, "_manager", cached)
    entered, released = threading.Event(), threading.Event()
    seen = {}

    def hold_a_mode():
        with notebook.notebook_mode(user_id=7) as mode:
            seen["graph"] = mode.graph
            entered.set()
            released.wait()
            seen["still_active"] = notebook.current() is mode
            seen["user"] = current_user_id()
            try:
                kernel_package.get_kernel_manager()
            except NativeNodeError as exc:
                seen["refusal"] = str(exc)

    holder = threading.Thread(target=hold_a_mode)
    holder.start()
    try:
        assert entered.wait(timeout=60)
        assert notebook.current() is None
        assert current_user_id() == 1
        frame = ff.from_dict(DATA)
        assert frame.flow_graph is not seen["graph"]
        assert "run_graph" not in frame.flow_graph.__dict__
        assert frame.flow_graph.run_graph.__func__ is FlowGraph.run_graph
        assert kernel_package.get_kernel_manager() is cached
        with notebook.notebook_mode(user_id=9) as own:
            assert current_user_id() == 9
            assert ff.from_dict(DATA).flow_graph is own.graph
    finally:
        released.set()
        holder.join(timeout=60)
    assert not holder.is_alive()
    assert seen["still_active"] and seen["user"] == 7
    assert seen["refusal"] == notebook.KERNEL_REFUSAL


def test_a_schema_callback_runs_in_the_context_that_started_it():
    def probe():
        mode = notebook.current()
        return (mode.graph if mode is not None else None), current_user_id()

    with notebook.notebook_mode(user_id=7) as mode:
        assert SingleExecutionFuture(probe)() == (mode.graph, 7)
        with pytest.raises(NativeNodeError, match="use Run on canvas"):
            SingleExecutionFuture(kernel_package.get_kernel_manager)()
    assert SingleExecutionFuture(probe)() == (None, 1)


def _source_with_a_gated_callback(graph: FlowGraph, release: threading.Event):
    def gated():
        release.wait(timeout=60)
        return []

    settings = input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=1, catalog_table_name="t")
    return graph.add_node_step(
        node_id=1, function=gated, input_columns=[], node_type="catalog_reader", setting_input=settings,
        schema_callback=gated,
    )  # fmt: skip


def test_no_prefetch_starts_in_the_mode_and_other_threads_still_prefetch():
    release = threading.Event()
    elsewhere = {}

    def place_outside_the_mode():
        elsewhere["node"] = _source_with_a_gated_callback(ff.create_flow_graph(), release)

    try:
        with notebook.notebook_mode(user_id=1) as mode:
            held = _source_with_a_gated_callback(mode.graph, release)
            thread = threading.Thread(target=place_outside_the_mode)
            thread.start()
            thread.join(timeout=60)
            assert held.is_start and not held.schema_callback._has_started
            assert elsewhere["node"].schema_callback._has_started
    finally:
        release.set()
    assert elsewhere["node"].schema_callback() == []


def test_the_cell_namespace_is_the_fl_surface_without_importing_flowfile():
    expected = set(flowfile.__all__) - WEB_UI_NAMES
    assert len(expected) == len(flowfile.__all__) - len(WEB_UI_NAMES)
    with unimportable("flowfile") as attempts:
        ff = new_namespace()["ff"]
        result = clean_run(CELLS, ceiling=0, user_id=1, executor=exec_cell)
    assert attempts == []
    assert result["ok"], result.get("error")
    assert {name for name in vars(ff) if not name.startswith("__")} == expected
    assert all(getattr(ff, name) is getattr(flowfile, name) for name in expected)


def test_clean_run_needs_a_user():
    with pytest.raises(ValueError, match="needs the user it runs as"):
        clean_run(CELLS, ceiling=0, executor=exec_cell)
    with notebook.notebook_mode():
        with pytest.raises(ValueError, match="needs the user it runs as"):
            clean_run(CELLS, ceiling=0, executor=exec_cell)
    with notebook.notebook_mode(user_id=4):
        assert clean_run(CELLS, ceiling=0, executor=exec_cell)["ok"]


def test_seed_session_needs_a_user():
    payload = _canvas_payload()
    with pytest.raises(ValueError, match="needs the user it runs as"):
        seed_session(payload, [], {}, {})
    assert notebook.current() is None
    with notebook.notebook_mode() as userless:
        with pytest.raises(ValueError, match="needs the user it runs as"):
            seed_session(payload, [], {}, {})
        assert notebook.current() is userless
    with notebook.notebook_mode(user_id=4) as previous:
        seed_session(payload, [], {}, {})
        assert notebook.current() is not previous and current_user_id() == 4
    assert notebook.current() is None


def test_a_clean_run_in_a_copy_of_the_seeded_context_leaves_the_seeded_mode_active():
    unset = object()

    def seeded_run():
        with notebook.notebook_mode(user_id=3) as seeded:
            tokens = notebook._TOKENS.get()
            assert contextvars.copy_context().run(clean_run, CELLS, 0, executor=exec_cell)["ok"]
            assert notebook._TOKENS.get() is tokens
            assert notebook.current() is seeded and "run_graph" in seeded.graph.__dict__
            with pytest.raises(NativeNodeError, match="use Run on canvas"):
                kernel_package.get_kernel_manager()
        assert notebook.current() is None
        assert kernel_package.kernel_manager_refusal.get() is None
        assert notebook._ACTIVE.get(unset) is unset
        assert kernel_package.kernel_manager_refusal.get(unset) is unset

    contextvars.Context().run(seeded_run)


def test_a_seed_and_clean_run_leave_no_logger_log_file_linecache_entry_or_snapshot():
    canvas = ff.create_flow_graph()
    source = ff.from_dict(DATA, flow_graph=canvas)
    big = source.filter(ff.col("a") > 1)
    payload = canvas.get_flowfile_data().model_dump(mode="json")
    loggers, logs, cell_files = set(FlowLogger._instances), _flow_logs(), _cell_files()
    cells = [("s", f"src = ff.from_dict({DATA})"), ("c", f"kept = ff.canvas_node({big.node_id}, src)")]
    with notebook.RUN_LOCK:
        try:
            seed_session(payload, [], {}, {}, user_id=1)
            seeded = notebook.current()
            result = clean_run(cells, ceiling=100, provenance={"c": [("filter", big.node_id)]}, executor=exec_cell)
            assert notebook.current() is seeded and big.node_id in seeded.snapshot
        finally:
            notebook.exit()
    assert result["ok"], result.get("error")
    assert notebook.current() is None
    assert seeded.snapshot == {} and seeded.cell_files == []
    assert not get_flow_log_file(seeded.graph.flow_id).exists()
    assert set(FlowLogger._instances) == loggers
    assert _flow_logs() == logs
    assert _cell_files() == cell_files


def test_placing_an_uninstalled_class_in_the_mode_is_refused_and_registers_nothing():
    before = _node_store()
    with notebook.notebook_mode(user_id=1) as mode:
        with pytest.raises(NativeNodeError, match="is not installed"):
            ff.CustomNode(NotebookOnlyNode, ff.from_dict(DATA))
        assert len(mode.refusals) == 1
    result = clean_run([("a", CELL_CLASS)], ceiling=0, user_id=1, executor=exec_cell)
    assert result["ok"] is False and "is not installed" in result["error"]
    assert len(result["refusals"]) == 1
    assert _node_store() == before


def test_a_class_registered_before_the_mode_is_placed_without_touching_the_store(store_snapshot):
    ff.CustomNode(NotebookOnlyNode, ff.from_dict(DATA))
    before = _node_store()
    with notebook.notebook_mode(user_id=1):
        placed = ff.CustomNode(NotebookOnlyNode, ff.from_dict(DATA))
        assert placed.node.node_type == "notebook_only_node"
    assert _node_store() == before
