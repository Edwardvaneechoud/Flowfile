"""The production notebook runner: installed by ``main.py`` through one function, run as the requesting user
under ``notebook.RUN_LOCK`` on the request thread, bounded before any cell is read, and identical to its
test-only ``exec`` twin except for the cell executor."""

import ast
import copy
import threading
from pathlib import Path

import pytest

import flowfile as fl
from flowfile_core import main
from flowfile_core.notebook import allowlist, bridge
from flowfile_core.notebook import runner as runner_module
from flowfile_core.notebook.interpret import CellInterpreter
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from flowfile_core.notebook.runner import NotebookRunner, install_notebook_runner, request_refusal
from flowfile_frame import notebook, notebook_cells
from flowfile_frame.notebook_cells import exec_cell
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, ExecRunner
from tests.notebook.test_push import _body, _cell_of, _node_of_type, _raise_threshold


def _request(graph, cells=None) -> bridge.CleanRunRequest:
    rendering = render(graph)
    return bridge.CleanRunRequest(
        cells=cells if cells is not None else [(cell.cell_id, cell.code) for cell in rendering.cells],
        provenance={
            cell.cell_id: [(graph.get_node(n).node_type, n) for n in cell.node_ids]
            for cell in rendering.cells
            if cell.node_ids
        },
        ceiling=max(node.node_id for node in graph.nodes),
        snapshot=seed_snapshot(graph),
    )


def _small_graph():
    return fl.from_dict({"a": [1, 2, 3]}).filter(fl.col("a") > 1).flow_graph


def test_main_installs_the_notebook_runner_once_right_after_the_notebook_router():
    tree = ast.parse(Path(main.__file__).read_text(encoding="utf-8"))
    calls = [
        index
        for index, statement in enumerate(tree.body)
        if isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Call)
        and getattr(statement.value.func, "id", None) == "install_notebook_runner"
    ]
    assert len(calls) == 1
    assert ast.unparse(tree.body[calls[0] - 1]).startswith("app.include_router(notebook_router,")
    assert sum(isinstance(node, ast.Name) and node.id == "install_notebook_runner" for node in ast.walk(tree)) == 1
    assert type(bridge.get_clean_runner()) is NotebookRunner


def test_a_fresh_client_plans_and_pushes_through_the_installed_runner(orders_flow, client_as):
    cell_id = _cell_of(orders_flow, _node_of_type(orders_flow, "filter").node_id)
    body = _body(orders_flow, _raise_threshold, [cell_id])
    client = client_as(NOTEBOOK_OWNER_ID)

    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200, plan.text
    push = client.post("/editor/notebook/push/", json=body)
    assert push.status_code == 200, push.text
    assert "20" in _node_of_type(orders_flow, "filter").setting_input.filter_input.advanced_filter


@pytest.mark.parametrize("mode", ["electron", "docker", "package"])
def test_installing_again_gives_a_notebook_runner_in_every_mode(monkeypatch, mode):
    before = bridge._runner
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    try:
        install_notebook_runner()
        assert type(bridge.get_clean_runner()) is NotebookRunner
        assert bridge.get_clean_runner() is not before
    finally:
        bridge.set_clean_runner(before)


def test_the_runner_syncs_as_the_requesting_user_under_the_lock_on_one_thread(open_as, client_as, monkeypatch):
    graph = open_as(_small_graph(), user_id=7)
    enter, run, end = notebook_cells.enter_snapshot_session, notebook_cells.clean_run, notebook.exit
    seen: dict[str, list] = {"enter": [], "run": [], "exit": []}

    def spy_enter(snapshot, *, user_id):
        mode = enter(snapshot, user_id=user_id)
        seen["enter"].append((user_id, threading.get_ident(), notebook.RUN_LOCK.locked(), mode))
        return mode

    def spy_run(*args, user_id, executor, **kwargs):
        seen["run"].append((user_id, threading.get_ident(), notebook.RUN_LOCK.locked(), notebook.current(), executor))
        return run(*args, user_id=user_id, executor=executor, **kwargs)

    def spy_exit():
        end()
        seen["exit"].append((threading.get_ident(), notebook.RUN_LOCK.locked(), notebook.current()))

    monkeypatch.setattr(notebook_cells, "enter_snapshot_session", spy_enter)
    monkeypatch.setattr(notebook_cells, "clean_run", spy_run)
    monkeypatch.setattr(notebook, "exit", spy_exit)

    response = client_as(7).post("/notebook/plan", json=_body(graph))
    assert response.status_code == 200, response.text
    [(enter_user, enter_thread, enter_locked, mode)] = seen["enter"]
    [(run_user, run_thread, run_locked, run_mode, executor)] = seen["run"]
    exit_thread, exit_locked, after_exit = seen["exit"][-1]
    assert enter_user == run_user == 7
    assert enter_thread == run_thread == exit_thread != threading.get_ident()
    assert enter_locked and run_locked and exit_locked
    assert run_mode is mode and mode.sync and mode.user_id == 7
    assert isinstance(executor, CellInterpreter)
    assert after_exit is None and mode.snapshot == {}
    assert not notebook.RUN_LOCK.locked()


@pytest.mark.parametrize("user_id", [0, -3, None, True, "1", 1.0])
def test_the_runner_needs_the_requesting_users_id(user_id):
    with pytest.raises(TypeError, match="positive int"):
        NotebookRunner().clean_run(user_id, 1, bridge.CleanRunRequest(cells=[("imports", "import flowfile as fl")]))


def test_a_run_leaves_the_callers_snapshot_alone():
    request = _request(_small_graph())
    snapshot = copy.deepcopy(request.snapshot)

    result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, request)
    assert result.error is None, result.error
    assert request.snapshot == snapshot
    assert notebook.current() is None and not notebook.RUN_LOCK.locked()


def _refuse_any_run(monkeypatch):
    def never(*args, **kwargs):
        raise AssertionError("a refused request must not reach a clean run")

    monkeypatch.setattr(notebook_cells, "clean_run", never)
    monkeypatch.setattr(notebook_cells, "enter_snapshot_session", never)


@pytest.mark.parametrize(
    "bound, value, cells, cell_id",
    [
        ("cells_per_request", 2, [("a", "x = 1"), ("b", "y = 2"), ("c", "z = 3")], None),
        ("bytes_per_cell", 8, [("a", "x = 1"), ("b", "y = 12345")], "b"),
        ("bytes_per_request", 8, [("a", "x = 1"), ("b", "y = 2")], None),
    ],
)
def test_a_request_over_a_bound_is_refused_before_any_cell_is_read(monkeypatch, bound, value, cells, cell_id):
    _refuse_any_run(monkeypatch)
    monkeypatch.setitem(allowlist.BOUNDS, bound, value)

    result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, bridge.CleanRunRequest(cells=cells))
    assert (result.kind, result.cell_id, result.line) == ("refused", cell_id, None)
    assert str(value) in result.error


@pytest.mark.parametrize(
    "provenance, message",
    [
        ({"a": [("read", 1), ("read", 2)], "b": [("filter", 3)]}, "more than 2 canvas nodes"),
        ({"a": [("read", 1)], "b": [("read", 1)]}, "a canvas node twice"),
        ({"a b": [("read", 1)]}, "a malformed cell id"),
    ],
)
def test_a_provenance_over_its_bound_or_repeating_a_node_is_refused_before_any_cell_is_read(
    monkeypatch, provenance, message
):
    _refuse_any_run(monkeypatch)
    monkeypatch.setitem(allowlist.BOUNDS, "provenance_entries_per_request", 2)

    request = bridge.CleanRunRequest(cells=[("a", "x = 1"), ("b", "y = 2")], provenance=provenance)
    result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, request)
    assert (result.kind, result.cell_id, result.line) == ("refused", None, None)
    assert message in result.error


def test_a_cell_that_is_not_valid_unicode_is_refused_with_its_id(monkeypatch):
    _refuse_any_run(monkeypatch)

    cells = [("a", "x = 1"), ("b", 'x = "\ud800"')]
    result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, bridge.CleanRunRequest(cells=cells))
    assert (result.kind, result.cell_id, result.line) == ("refused", "b", None)


def test_a_sync_waiting_too_long_for_another_is_an_error_not_a_hang(monkeypatch):
    _refuse_any_run(monkeypatch)
    monkeypatch.setattr(runner_module, "EDIT_LOCK_TIMEOUT_SECONDS", 0.01)
    request = _request(_small_graph())
    held, release = threading.Event(), threading.Event()

    def other_sync():
        with notebook.RUN_LOCK:
            held.set()
            release.wait()

    thread = threading.Thread(target=other_sync)
    thread.start()
    try:
        assert held.wait(timeout=30)
        result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, request)
    finally:
        release.set()
        thread.join()
    assert (result.error, result.kind, result.cell_id) == (
        "Another notebook sync is in progress; try again",
        "error",
        None,
    )
    assert notebook.current() is None and not notebook.RUN_LOCK.locked()


@pytest.mark.parametrize("cell_id", ["", "a b", "cell/../x", "x" * 129, "cell\n1", "<cell>"])
def test_a_malformed_cell_id_is_refused_without_echoing_it(monkeypatch, cell_id):
    _refuse_any_run(monkeypatch)

    result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, bridge.CleanRunRequest(cells=[(cell_id, "x = 1")]))
    assert (result.kind, result.cell_id) == ("refused", None)
    assert result.error == "A cell id is 1 to 128 letters, digits or the characters _ . : -"


@pytest.mark.parametrize(
    "cell_id", ["imports", "parameters", "cell-12", "node-99", "3f2a9c1e-7b1d-4e7a-9c55-0b5c6d7e8f90"]
)
def test_the_cell_ids_the_editor_sends_pass(cell_id):
    assert request_refusal(bridge.CleanRunRequest(cells=[(cell_id, "x = 1")])) is None


def test_a_failure_outside_the_cells_ends_the_mode_and_comes_back_as_an_error(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("the relabel broke")

    monkeypatch.setattr(notebook_cells, "clean_run", broken)

    result = NotebookRunner().clean_run(NOTEBOOK_OWNER_ID, 1, _request(_small_graph()))
    assert (result.error, result.kind, result.cell_id, result.line) == (
        "RuntimeError: the relabel broke",
        "error",
        None,
        None,
    )
    assert "Traceback" in result.traceback and "traceback" not in result.model_dump()
    assert notebook.current() is None and not notebook.RUN_LOCK.locked()


def test_the_exec_runner_differs_from_the_production_runner_only_in_its_executor():
    assert ExecRunner.clean_run is NotebookRunner.clean_run
    assert ExecRunner().executor() is exec_cell
    first, second = NotebookRunner().executor(), NotebookRunner().executor()
    assert isinstance(first, CellInterpreter) and isinstance(second, CellInterpreter) and first is not second
