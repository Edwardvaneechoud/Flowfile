"""The notebook session subprocess, for real: spawn, seed, execute, stream, display, reset, clean run,
kill-and-restart of a busy session, stdin EOF, idle TTL and LRU eviction. No Docker and no signals."""

from __future__ import annotations

import threading
import time

import pytest

from flowfile_core.notebook.bridge import CleanRunRequest
from flowfile_core.notebook.registry import RESTARTED_MESSAGE, NotebookSession, NotebookSessionRegistry
from shared.notebook_display import TABLE_MIME
from tests.notebook.session_helpers import small_snapshot


@pytest.fixture(scope="module")
def snapshot():
    return small_snapshot()


@pytest.fixture(scope="module")
def session(snapshot):
    session = NotebookSession(1, 91001, snapshot)
    session.start()
    assert session.wait_ready(60)
    yield session
    session.close()


def test_the_child_starts_seeded_and_reports_its_cold_start(session, snapshot):
    assert session.startup_seconds is not None and session.startup_seconds < 60
    print(f"notebook session cold start: {session.startup_seconds}s")
    result = session.run_cell("c", "print(sorted(n.node_id for n in flow.nodes))")
    assert result["ok"], result["traceback"]
    canvas_ids = sorted(node["id"] for node in snapshot["flowfile_data"]["nodes"])
    assert result["stdout"] == f"{canvas_ids}\n"


def test_a_cell_streams_stdout_and_displays_a_table(session):
    result = session.run_cell("build", "print('hello')\nframe = fl.from_dict({'v': [1, 2, 3]})\nframe")
    assert result["ok"], result["traceback"]
    assert result["stdout"] == "hello\n" and result["stderr"] == ""
    assert len(result["displays"]) == 1 and TABLE_MIME in result["displays"][0]
    assert [c["name"] for c in result["displays"][0]["schema"]] == ["v"]


def test_explicit_display_and_errors_are_payloads(session):
    result = session.run_cell("c", "display(fl.from_dict({'w': [1]}))\n1 / 0")
    assert not result["ok"]
    assert result["error"] == "ZeroDivisionError: division by zero"
    assert "Traceback" in result["traceback"]
    assert len(result["displays"]) == 1


def test_no_kernel_manager_is_constructed_in_the_child(session):
    code = (
        "import flowfile_core.kernel as kernel\n"
        "assert kernel.get_kernel_manager_if_initialized() is None\n"
        "try:\n"
        "    kernel.KernelManager()\n"
        "except Exception as exc:\n"
        "    print(type(exc).__name__)\n"
        "assert kernel.get_kernel_manager_if_initialized() is None\n"
    )
    result = session.run_cell("c", code)
    assert result["ok"], result["traceback"]
    assert result["stdout"] == "NativeNodeError\n"


def test_schemas_lists_bound_frames(session):
    session.run_cell("c", "typed = fl.from_dict({'k': [1], 's': ['a']})")
    frames = session.schemas()
    assert [c["name"] for c in frames["typed"]] == ["k", "s"]


def test_reset_drops_variables_and_reseeds(session):
    session.run_cell("c", "gone = 41")
    generation = session.namespace_generation
    session.reset()
    assert session.namespace_generation != generation
    result = session.run_cell("c", "print(gone)")
    assert not result["ok"] and result["error"].startswith("NameError")
    result = session.run_cell("c", "print(len(flow.nodes))")
    assert result["ok"] and result["stdout"] == "2\n"


def test_clean_run_round_trip_through_the_registry(snapshot):
    registry = NotebookSessionRegistry(idle_ttl=600, max_sessions=3)
    try:
        request = CleanRunRequest(
            cells=[("a", "src = fl.from_dict({'v': [1, 2]})"), ("b", "kept = src.filter(fl.col('v') > 1)")],
            provenance={},
            ceiling=50,
            snapshot=snapshot,
        )
        result = registry.clean_run(1, 91002, request)
        assert result.error is None, result.error
        assert set(result.node_ids_by_cell) == {"a", "b"}
        new_ids = result.node_ids_by_cell["a"] + result.node_ids_by_cell["b"]
        assert len(new_ids) == 2 and all(node_id > 50 for node_id in new_ids)
        assert {node["id"] for node in result.flowfile_data["nodes"]} == set(new_ids)
        assert result.names == {result.node_ids_by_cell["a"][0]: "src", result.node_ids_by_cell["b"][0]: "kept"}
        failing = registry.clean_run(1, 91002, CleanRunRequest(cells=[("x", "raise ValueError('nope')")]))
        assert failing.error and "nope" in failing.error and failing.flowfile_data == {}
        session = registry.sessions()[0]
        result = session.run_cell(
            "c", "import flowfile_core.kernel as kernel\nassert kernel.get_kernel_manager_if_initialized() is None"
        )
        assert result["ok"], result["traceback"]
    finally:
        registry.shutdown()


def test_resetting_a_blocked_session_kills_it_and_restarts_seeded(snapshot):
    session = NotebookSession(1, 91004, snapshot)
    session.start()
    try:
        assert session.wait_ready(60)
        results = []
        cell = threading.Thread(
            target=lambda: results.append(session.run_cell("sleep", "import time\ntime.sleep(3600)"))
        )
        cell.start()
        deadline = time.monotonic() + 10
        while not session.busy and time.monotonic() < deadline:
            time.sleep(0.05)
        old_pid = session.pid
        began = time.monotonic()
        session.reset()
        cell.join(5)
        assert time.monotonic() - began < 5
        assert results and results[0]["ok"] is False and results[0]["error"] == RESTARTED_MESSAGE
        assert session.pid != old_pid
        result = session.run_cell("c", "print(len(flow.nodes))", timeout=60)
        assert result["ok"], result["traceback"]
        assert result["stdout"] == "2\n"
    finally:
        session.close()


def test_closing_stdin_ends_the_child_within_two_seconds():
    session = NotebookSession(1, 91005, None)
    session.start()
    assert session.wait_ready(60)
    process = session._process
    began = time.monotonic()
    session.close_input()
    assert process.wait(timeout=2) == 0
    assert time.monotonic() - began < 2
    session.wait_or_kill()


def _wait_closed(session: NotebookSession, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while session._process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)
    assert session.closed and session._process.poll() is not None


def test_lru_eviction_closes_the_least_recently_used():
    registry = NotebookSessionRegistry(idle_ttl=600, max_sessions=2)
    try:
        first = registry.open(1, 92001)
        time.sleep(0.05)
        second = registry.open(1, 92002)
        time.sleep(0.05)
        assert registry.open(1, 92001) is first
        registry.open(1, 92003)
        assert {s.flow_id for s in registry.sessions()} == {92001, 92003}
        _wait_closed(second)
    finally:
        registry.shutdown()


def test_idle_ttl_closes_unused_sessions():
    registry = NotebookSessionRegistry(idle_ttl=0.5, max_sessions=3)
    try:
        session = registry.open(1, 92010)
        deadline = time.monotonic() + 5
        while registry.sessions() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert registry.sessions() == []
        _wait_closed(session)
    finally:
        registry.shutdown()


def test_shutdown_reaps_every_session_in_parallel():
    registry = NotebookSessionRegistry(idle_ttl=600, max_sessions=3)
    sessions = [registry.open(1, 93000 + i) for i in range(3)]
    for session in sessions:
        assert session.wait_ready(60)
    began = time.monotonic()
    registry.shutdown(timeout=2.0)
    assert time.monotonic() - began < 4
    assert all(s._process.poll() is not None for s in sessions)
    assert registry.sessions() == []
