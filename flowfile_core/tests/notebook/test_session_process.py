"""The notebook session subprocess, for real: spawn, seed, execute, stream, display, reset, clean run,
interrupt, kill-and-restart, stdin EOF, idle TTL and LRU eviction. No Docker and no signals."""

from __future__ import annotations

import time

import pytest

from flowfile_core.notebook.bridge import CleanRunRequest
from flowfile_core.notebook.registry import RESTARTED_MESSAGE, NotebookSession, NotebookSessionRegistry
from shared.notebook_display import TABLE_MIME
from tests.notebook.session_helpers import run_cell, small_snapshot, wait_for


@pytest.fixture(scope="module")
def snapshot():
    return small_snapshot()


@pytest.fixture(scope="module")
def session(snapshot):
    session = NotebookSession(1, 91001, snapshot)
    session.start()
    wait_for(session, lambda e: e["type"] == "ready", timeout=60)
    yield session
    session.close()


def test_the_child_starts_seeded_and_reports_its_cold_start(session, snapshot):
    ready = next(e for e in session.events_after(0) if e["type"] == "ready")
    assert ready["pid"] == session.pid
    assert session.startup_seconds is not None and session.startup_seconds < 60
    print(f"notebook session cold start: {session.startup_seconds}s (child imports {ready['imports_seconds']}s)")
    done, events = run_cell(session, "print(sorted(n.node_id for n in flow.nodes))")
    assert done["ok"], done["traceback"]
    canvas_ids = sorted(node["id"] for node in snapshot["flowfile_data"]["nodes"])
    assert [e["text"] for e in events if e["type"] == "stream"] == [f"{canvas_ids}\n"]


def test_a_cell_streams_stdout_and_displays_a_table(session):
    done, events = run_cell(session, "print('hello')\nframe = fl.from_dict({'v': [1, 2, 3]})\nframe", "build")
    assert done["ok"], done["traceback"]
    assert done["names_bound"] == ["frame"]
    assert [node_type for node_type, _ in done["nodes_created"]] == ["manual_input"]
    streams = [e for e in events if e["type"] == "stream"]
    assert streams and streams[0]["name"] == "stdout" and streams[0]["text"] == "hello\n"
    assert streams[0]["cell_id"] == "build"
    displays = [e for e in events if e["type"] == "display"]
    assert len(displays) == 1 and TABLE_MIME in displays[0]["payload"]
    assert [c["name"] for c in displays[0]["payload"]["schema"]] == ["v"]


def test_explicit_display_and_errors_are_payloads(session):
    done, events = run_cell(session, "display(fl.from_dict({'w': [1]}))\n1 / 0")
    assert not done["ok"]
    assert done["error"] == "ZeroDivisionError: division by zero"
    assert "Traceback" in done["traceback"]
    assert [e["type"] for e in events if e["type"] == "display"] == ["display"]


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
    done, events = run_cell(session, code)
    assert done["ok"], done["traceback"]
    assert [e["text"] for e in events if e["type"] == "stream"] == ["NativeNodeError\n"]


def test_schemas_lists_bound_frames(session):
    run_cell(session, "typed = fl.from_dict({'k': [1], 's': ['a']})")
    frames = session.schemas()
    assert [c["name"] for c in frames["typed"]] == ["k", "s"]


def test_reset_drops_variables_and_reseeds(session):
    run_cell(session, "gone = 41")
    start = session.events_after(0)[-1]["seq"]
    ticket = session.reset()
    wait_for(session, lambda e: e.get("ticket") == ticket and e["type"] == "done", after=start)
    done, _ = run_cell(session, "print(gone)")
    assert not done["ok"] and done["error"].startswith("NameError")
    done, events = run_cell(session, "print(len(flow.nodes))")
    assert done["ok"] and events[0]["text"] == "2\n"


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
        done, _ = run_cell(
            session, "import flowfile_core.kernel as kernel\nassert kernel.get_kernel_manager_if_initialized() is None"
        )
        assert done["ok"], done["traceback"]
    finally:
        registry.shutdown()


def test_interrupt_stops_a_pure_python_loop():
    session = NotebookSession(1, 91003, None)
    session.start()
    try:
        wait_for(session, lambda e: e["type"] == "ready", timeout=60)
        start = session.events_after(0)[-1]["seq"]
        ticket = session.execute("loop", "print('spinning')\nwhile True:\n    pass\n")
        wait_for(session, lambda e: e.get("text") == "spinning\n", after=start)
        began = time.monotonic()
        session.interrupt()
        done = wait_for(session, lambda e: e["type"] == "done" and e.get("ticket") == ticket, timeout=3, after=start)
        assert time.monotonic() - began < 3
        assert done["ok"] is False and done["error"] == "KeyboardInterrupt"
        pid = session.pid
        time.sleep(session.interrupt_grace + 0.5)
        assert session.pid == pid
        assert not any(e["type"] == "restarted" for e in session.events_after(start))
    finally:
        session.close()


def test_a_blocked_sleep_is_killed_and_the_session_restarts_seeded(snapshot):
    session = NotebookSession(1, 91004, snapshot)
    session.start()
    try:
        wait_for(session, lambda e: e["type"] == "ready", timeout=60)
        start = session.events_after(0)[-1]["seq"]
        ticket = session.execute("sleep", "import time\nprint('asleep')\ntime.sleep(3600)\n")
        wait_for(session, lambda e: e.get("text") == "asleep\n", after=start)
        old_pid = session.pid
        began = time.monotonic()
        session.interrupt()
        restarted = wait_for(session, lambda e: e["type"] == "restarted", timeout=6, after=start)
        assert time.monotonic() - began < 5.5
        assert restarted["message"] == RESTARTED_MESSAGE
        dropped = [e for e in session.events_after(start) if e["type"] == "done" and e.get("ticket") == ticket]
        assert dropped and dropped[0]["ok"] is False
        assert session.pid != old_pid
        done, events = run_cell(session, "print(len(flow.nodes))", timeout=60)
        assert done["ok"], done["traceback"]
        assert [e["text"] for e in events if e["type"] == "stream"] == ["2\n"]
    finally:
        session.close()


def test_closing_stdin_ends_the_child_within_two_seconds():
    session = NotebookSession(1, 91005, None)
    session.start()
    wait_for(session, lambda e: e["type"] == "ready", timeout=60)
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
        wait_for(session, lambda e: e["type"] == "ready", timeout=60)
    began = time.monotonic()
    registry.shutdown(timeout=2.0)
    assert time.monotonic() - began < 4
    assert all(s._process.poll() is not None for s in sessions)
    assert registry.sessions() == []
