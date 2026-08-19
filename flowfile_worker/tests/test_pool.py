"""Integration tests for the warm worker pool (flowfile_worker/pool.py).

Everything runs real spawned processes through the public entry points
(spawner.start_process and the /ws/submit WebSocket); the pool is swapped in by
monkeypatching pool.task_pool, which both transports resolve at call time.
"""

import ast
import io
import json
import threading
import time
from base64 import b64decode
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main, models, pool, status_dict, status_dict_lock
from flowfile_worker.pool import POOLABLE_OPERATIONS, TaskPool
from flowfile_worker.spawner import process_manager, start_process

client = TestClient(main.app)

pytestmark = [pytest.mark.worker, pytest.mark.timeout(120)]


@pytest.fixture(autouse=True)
def isolated_pool_settings(monkeypatch, tmp_path):
    """Keep persistence away from the developer's real ~/.flowfile and CI env."""
    monkeypatch.setattr(pool, "_settings_file", lambda: tmp_path / "worker_pool.yaml")
    monkeypatch.delenv("FLOWFILE_WORKER_POOL_SIZE", raising=False)


@pytest.fixture
def warm_pool(monkeypatch):
    """Install a fresh 1-member pool with a fast reaper; tear it down afterwards."""
    test_pool = TaskPool(
        size=1, max_tasks_per_member=100, idle_ttl_seconds=300, rss_limit_mb=100_000, reap_interval_seconds=0.05
    )
    monkeypatch.setattr(pool, "task_pool", test_pool)
    yield test_pool
    test_pool.shutdown()


def _register_status(task_id: str, file_path: str) -> None:
    with status_dict_lock:
        status_dict[task_id] = models.Status(
            background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars"
        )


def _get_status(task_id: str) -> models.Status:
    with status_dict_lock:
        return status_dict[task_id]


def _cleanup_status(task_id: str) -> None:
    with status_dict_lock:
        status_dict.pop(task_id, None)
    process_manager.remove_process(task_id)


def _run_task(task_id: str, operation: str, payload: bytes, file_path: str, **kwargs) -> models.Status:
    """Drive a task through the real REST entry point and return its terminal status."""
    _register_status(task_id, file_path)
    try:
        start_process(payload, task_id, operation, file_path, flowfile_flow_id=1, flowfile_node_id=1, kwargs=kwargs)
        return _get_status(task_id)
    finally:
        _cleanup_status(task_id)


def _idle_pid(test_pool: TaskPool) -> int:
    assert test_pool.stats()["idle"] == 1, f"expected one idle member, stats={test_pool.stats()}"
    return test_pool._idle[0].process.pid


def _sleeping_lazyframe(seconds: float) -> pl.LazyFrame:
    """A plan that provably runs for *seconds* in the child (polars serializes UDFs by value)."""
    return pl.LazyFrame({"a": [1]}).select(pl.col("a").map_batches(lambda s: (time.sleep(seconds), s)[1]))


class _FakeWebSocket:
    """Minimal socket to drive ws_submit directly; raises WebSocketDisconnect on sends fail_on selects."""

    def __init__(self, metadata: dict, payload: bytes, fail_on):
        self.metadata = metadata
        self.payload = payload
        self.fail_on = fail_on

    async def accept(self):
        pass

    async def receive_json(self):
        return self.metadata

    async def receive_bytes(self):
        return self.payload

    async def send_json(self, message):
        if self.fail_on(message):
            from fastapi import WebSocketDisconnect

            raise WebSocketDisconnect(code=1006)

    async def send_bytes(self, data):
        pass


class TestMemberReuse:
    def test_two_store_tasks_reuse_one_member(self, warm_pool, tmp_path):
        lf = pl.LazyFrame({"a": [1, 2, 3]})
        first = _run_task("pool-store-1", "store", lf.serialize(), str(tmp_path / "s1.arrow"))
        assert first.status == "Completed", first.error_message
        assert first.number_of_records == 3
        pid_after_first = _idle_pid(warm_pool)

        second = _run_task("pool-store-2", "store", lf.serialize(), str(tmp_path / "s2.arrow"))
        assert second.status == "Completed", second.error_message
        restored = pl.LazyFrame.deserialize(io.BytesIO(b64decode(second.results)))
        assert restored.collect().equals(lf.collect())
        assert _idle_pid(warm_pool) == pid_after_first, "second task must reuse the warm member"

    def test_store_sample_gets_completion_envelope(self, warm_pool, tmp_path):
        """store_sample puts no result of its own; the member envelope must still complete it fast."""
        lf = pl.LazyFrame({"a": list(range(50))})
        started = time.monotonic()
        status = _run_task(
            "pool-sample-1", "store_sample", lf.serialize(), str(tmp_path / "sample.arrow"), sample_size=10
        )
        elapsed = time.monotonic() - started
        assert status.status == "Completed", status.error_message
        assert elapsed < 15, f"envelope missing: drain stalled for {elapsed:.1f}s"
        assert pl.read_ipc(tmp_path / "sample.arrow").height == 10
        assert warm_pool.stats()["idle"] == 1, "member must return to the pool"


class TestErrorHandling:
    def test_failed_task_keeps_member_and_resets_error_state(self, warm_pool, tmp_path):
        lf = pl.LazyFrame({"a": [1]})

        failed = _run_task("pool-err-1", "store", b"not-a-serialized-lazyframe", str(tmp_path / "e1.arrow"))
        assert failed.status == "Error"
        assert failed.error_message
        pid_after_error = _idle_pid(warm_pool)

        ok = _run_task("pool-err-2", "store", lf.serialize(), str(tmp_path / "e2.arrow"))
        assert ok.status == "Completed", ok.error_message
        assert not ok.error_message, "stale error text must not leak into the next task"
        assert _idle_pid(warm_pool) == pid_after_error, "a cleanly-errored member is reusable"

    def test_shorter_error_is_not_polluted_by_previous_longer_error(self, warm_pool, tmp_path):
        # A missing-column error embeds the column name, giving a long message.
        long_error_plan = pl.LazyFrame({"a": [1]}).select(pl.col("missing_" + "x" * 400)).serialize()
        first = _run_task("pool-err-long", "store", long_error_plan, str(tmp_path / "l.arrow"))
        assert first.status == "Error" and len(first.error_message) > 50

        second = _run_task("pool-err-short", "store", b"y", str(tmp_path / "s.arrow"))
        assert second.status == "Error"
        assert first.error_message[50:] not in second.error_message, (
            "error buffer must be zeroed between tasks: "
            f"second error contains the first one's tail: {second.error_message!r}"
        )


class TestCancellation:
    def test_cancel_route_kills_member_and_pool_recovers(self, warm_pool, tmp_path):
        """Drive the real POST /cancel_task route: terminate-while-leased must retire the member."""
        task_id = "pool-cancel-1"
        _register_status(task_id, str(tmp_path / "c.arrow"))

        def cancel_once_processing():
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with status_dict_lock:
                    processing = status_dict[task_id].status == "Processing"
                if processing:
                    client.post(f"/cancel_task/{task_id}")
                    return
                time.sleep(0.01)

        canceller = threading.Thread(target=cancel_once_processing)
        canceller.start()
        try:
            start_process(
                _sleeping_lazyframe(30).serialize(),
                task_id,
                "store",
                str(tmp_path / "c.arrow"),
                flowfile_flow_id=1,
                flowfile_node_id=1,
            )
            canceller.join()
            assert _get_status(task_id).status == "Cancelled"
        finally:
            _cleanup_status(task_id)

        assert warm_pool.stats()["total"] == 0, "a terminated member must be retired, not reused"

        lf = pl.LazyFrame({"a": [1, 2]})
        after = _run_task("pool-cancel-2", "store", lf.serialize(), str(tmp_path / "after.arrow"))
        assert after.status == "Completed", after.error_message
        assert warm_pool.stats()["total"] == 1, "the pool must refill on the next acquire"

    def test_stale_cancel_of_finished_task_cannot_kill_member(self, warm_pool, tmp_path):
        """The member is unmapped before checkin, so cancelling a done task 404s instead of killing it."""
        lf = pl.LazyFrame({"a": [1, 2, 3]})
        done = _run_task("pool-stale-cancel", "store", lf.serialize(), str(tmp_path / "sc.arrow"))
        assert done.status == "Completed", done.error_message
        member_pid = _idle_pid(warm_pool)

        response = client.post("/cancel_task/pool-stale-cancel")
        assert response.status_code == 404, "a finished pooled task must no longer be cancellable"
        assert warm_pool._idle[0].process.is_alive(), "the idle member must survive a stale cancel"

        again = _run_task("pool-stale-cancel-2", "store", lf.serialize(), str(tmp_path / "sc2.arrow"))
        assert again.status == "Completed", again.error_message
        assert _idle_pid(warm_pool) == member_pid

    def test_task_timeout_retires_member(self, warm_pool, tmp_path, monkeypatch):
        from flowfile_worker import spawner

        monkeypatch.setattr(spawner, "_TASK_TIMEOUT", 1.0)
        status = _run_task("pool-timeout-1", "store", _sleeping_lazyframe(30).serialize(), str(tmp_path / "to.arrow"))
        assert status.status == "Error"
        assert "time limit" in status.error_message
        assert warm_pool.stats()["total"] == 0, "a timed-out (terminated) member must be retired"


class TestBudgetsAndReaping:
    def test_max_tasks_budget_recycles_member(self, monkeypatch, tmp_path):
        test_pool = TaskPool(
            size=1, max_tasks_per_member=2, idle_ttl_seconds=300, rss_limit_mb=100_000, reap_interval_seconds=30
        )
        monkeypatch.setattr(pool, "task_pool", test_pool)
        try:
            lf = pl.LazyFrame({"a": [1]})
            _run_task("pool-budget-1", "store", lf.serialize(), str(tmp_path / "b1.arrow"))
            first_pid = _idle_pid(test_pool)
            _run_task("pool-budget-2", "store", lf.serialize(), str(tmp_path / "b2.arrow"))
            assert test_pool.stats()["total"] == 0, "member at its task budget must be retired on checkin"
            _run_task("pool-budget-3", "store", lf.serialize(), str(tmp_path / "b3.arrow"))
            assert _idle_pid(test_pool) != first_pid, "the replacement must be a fresh process"
        finally:
            test_pool.shutdown()

    def test_idle_ttl_reaps_pool_to_zero(self, monkeypatch, tmp_path):
        test_pool = TaskPool(
            size=1, max_tasks_per_member=100, idle_ttl_seconds=0.5, rss_limit_mb=100_000, reap_interval_seconds=0.05
        )
        monkeypatch.setattr(pool, "task_pool", test_pool)
        try:
            lf = pl.LazyFrame({"a": [1]})
            _run_task("pool-ttl-1", "store", lf.serialize(), str(tmp_path / "t.arrow"))
            member_process = test_pool._idle[0].process

            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and test_pool.stats()["total"] > 0:
                time.sleep(0.05)
            assert test_pool.stats()["total"] == 0, "idle member must be reaped after the TTL"
            # total hits 0 before the reaper's dispose finishes; poll liveness instead of one join.
            while time.monotonic() < deadline and member_process.is_alive():
                time.sleep(0.05)
            assert not member_process.is_alive(), "reaped member process must end"
        finally:
            test_pool.shutdown()


class TestFallbacks:
    def test_disabled_pool_takes_spawn_path(self, monkeypatch, tmp_path):
        test_pool = TaskPool(size=0, max_tasks_per_member=100, idle_ttl_seconds=300, rss_limit_mb=100_000)
        monkeypatch.setattr(pool, "task_pool", test_pool)
        lf = pl.LazyFrame({"a": [7]})
        status = _run_task("pool-off-1", "store", lf.serialize(), str(tmp_path / "off.arrow"))
        assert status.status == "Completed", status.error_message
        assert test_pool.stats()["total"] == 0

    def test_unpoolable_operations_are_refused(self, warm_pool):
        assert warm_pool.acquire("fuzzy") is None
        assert warm_pool.acquire("execute_custom_node") is None
        assert warm_pool.acquire("generic_task") is None

    def test_saturated_pool_returns_none(self, warm_pool):
        member = warm_pool.acquire("store")
        assert member is not None
        try:
            assert warm_pool.acquire("store") is None, "a busy 1-slot pool must signal spawn-per-task"
        finally:
            warm_pool.checkin(member, reusable=True)
        assert warm_pool.stats()["idle"] == 1


class TestWebSocketPath:
    def test_ws_store_reuses_member_and_returns_binary_result(self, warm_pool):
        lf = pl.LazyFrame({"a": [1, 2, 3, 4]})

        def submit(task_id: str):
            with client.websocket_connect("/ws/submit") as ws:
                ws.send_json({"task_id": task_id, "operation": "store", "flow_id": 1, "node_id": -1})
                ws.send_bytes(lf.serialize())
                while True:
                    message = json.loads(ws.receive_text())
                    if message["type"] == "complete":
                        assert message["has_result"] is True
                        assert message["number_of_records"] == 4
                        return ws.receive_bytes()
                    assert message["type"] == "progress", f"unexpected frame: {message}"

        first_result = submit("pool-ws-1")
        pid_after_first = _idle_pid(warm_pool)
        second_result = submit("pool-ws-2")

        assert _idle_pid(warm_pool) == pid_after_first, "WS tasks must reuse the warm member"
        for raw in (first_result, second_result):
            assert pl.LazyFrame.deserialize(io.BytesIO(raw)).collect().equals(lf.collect())

    def test_ws_error_keeps_member(self, warm_pool):
        with client.websocket_connect("/ws/submit") as ws:
            ws.send_json({"task_id": "pool-ws-err", "operation": "store", "flow_id": 1, "node_id": -1})
            ws.send_bytes(b"garbage")
            while True:
                message = json.loads(ws.receive_text())
                if message["type"] == "error":
                    break
        assert warm_pool.stats() == {"size": 1, "idle": 1, "total": 1}, "clean error must not burn the member"

    def _await_terminal(self, task_id: str, timeout: float = 60.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with status_dict_lock:
                current = status_dict[task_id].status
            if current in ("Completed", "Error", "Cancelled", "Unknown Error"):
                return current
            time.sleep(0.05)
        pytest.fail(f"task {task_id} never reached a terminal status")

    def _await_idle(self, test_pool: TaskPool, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if test_pool.stats()["idle"] == 1:
                return
            time.sleep(0.05)
        pytest.fail(f"member never returned to the pool, stats={test_pool.stats()}")

    def test_ws_disconnect_mid_task_hands_off_and_member_survives(self, warm_pool):
        """Socket dies while the task runs: the task finishes off-socket and the member is reused.

        Drives the real ws_submit with a fake socket - TestClient cancels the endpoint
        coroutine on context exit, which cannot model a live client disconnect.
        """
        import asyncio

        from flowfile_worker import streaming

        task_id = "pool-ws-handoff"
        metadata = {"task_id": task_id, "operation": "store", "flow_id": 1, "node_id": -1}
        socket = _FakeWebSocket(metadata, _sleeping_lazyframe(2).serialize(), fail_on=lambda message: True)

        try:
            asyncio.run(streaming.ws_submit(socket))
            assert self._await_terminal(task_id) == "Completed"
            self._await_idle(warm_pool)
        finally:
            with status_dict_lock:
                status_dict.pop(task_id, None)

    def test_ws_disconnect_during_completion_send_keeps_member_and_status(self, warm_pool):
        """Envelope already drained when the send fails: no handoff, no status regression, member reused."""
        import asyncio

        from flowfile_worker import streaming

        task_id = "pool-ws-drop-complete"
        metadata = {"task_id": task_id, "operation": "store", "flow_id": 1, "node_id": -1}
        socket = _FakeWebSocket(
            metadata,
            pl.LazyFrame({"a": [1, 2, 3]}).serialize(),
            fail_on=lambda message: message.get("type") == "complete",
        )

        try:
            asyncio.run(streaming.ws_submit(socket))
            assert self._await_terminal(task_id) == "Completed"
            self._await_idle(warm_pool)
            assert warm_pool._idle[0].process.is_alive()
        finally:
            with status_dict_lock:
                status_dict.pop(task_id, None)


class TestLifecycle:
    def test_dead_idle_member_is_replaced_on_acquire(self, warm_pool, tmp_path):
        lf = pl.LazyFrame({"a": [1]})
        _run_task("pool-dead-1", "store", lf.serialize(), str(tmp_path / "d1.arrow"))
        member_process = warm_pool._idle[0].process
        first_pid = member_process.pid
        member_process.terminate()
        member_process.join()

        status = _run_task("pool-dead-2", "store", lf.serialize(), str(tmp_path / "d2.arrow"))
        assert status.status == "Completed", status.error_message
        assert warm_pool.stats()["total"] == 1
        assert _idle_pid(warm_pool) != first_pid, "a dead idle member must be replaced, not handed out"

    def test_prewarm_fills_and_shutdown_drains(self):
        test_pool = TaskPool(size=2, max_tasks_per_member=100, idle_ttl_seconds=300, rss_limit_mb=100_000)
        try:
            test_pool.prewarm()
            assert test_pool.stats() == {"size": 2, "idle": 2, "total": 2}
            processes = [m.process for m in test_pool._idle]
            assert all(p.is_alive() for p in processes)
        finally:
            test_pool.shutdown()
        assert test_pool.stats()["total"] == 0
        for p in processes:
            p.join(timeout=10)
            assert not p.is_alive(), "shutdown must end every idle member"

    def test_rss_watermark_retires_member(self, monkeypatch, tmp_path):
        pytest.importorskip("psutil")
        test_pool = TaskPool(size=1, max_tasks_per_member=100, idle_ttl_seconds=300, rss_limit_mb=1)
        monkeypatch.setattr(pool, "task_pool", test_pool)
        try:
            lf = pl.LazyFrame({"a": [1]})
            status = _run_task("pool-rss-1", "store", lf.serialize(), str(tmp_path / "r.arrow"))
            assert status.status == "Completed", status.error_message
            assert test_pool.stats()["total"] == 0, "a member over the RSS watermark must be retired at checkin"
        finally:
            test_pool.shutdown()


class TestPoolAdminEndpoints:
    def test_get_reports_live_state(self, warm_pool):
        state = client.get("/pool").json()
        assert state["enabled"] is True
        assert state["size"] == 1
        assert state["busy"] == 0
        assert set(state) == {
            "enabled",
            "size",
            "idle",
            "busy",
            "active_tasks",
            "tasks_completed",
            "members",
            "max_tasks_per_member",
            "idle_ttl_seconds",
            "rss_limit_mb",
            "platform_default_size",
            "env_override",
            "persisted",
        }

    def test_describe_tracks_member_states_and_served_count(self, warm_pool):
        member = warm_pool.acquire("store")
        busy = client.get("/pool").json()
        assert busy["busy"] == 1
        assert [m["state"] for m in busy["members"]] == ["busy"]
        assert busy["members"][0]["pid"] == member.process.pid

        warm_pool.checkin(member, reusable=True)
        idle = client.get("/pool").json()
        assert idle["idle"] == 1 and idle["busy"] == 0
        assert [m["state"] for m in idle["members"]] == ["idle"]
        assert idle["tasks_completed"] == busy["tasks_completed"] + 1

    def test_active_tasks_counts_in_flight_statuses(self, warm_pool, tmp_path):
        _register_status("pool-active-probe", str(tmp_path / "p.arrow"))
        try:
            baseline = client.get("/pool").json()["active_tasks"]
            assert baseline >= 1, "a Starting task must count as active"
        finally:
            _cleanup_status("pool-active-probe")

    def test_resize_grows_and_shrink_to_zero_retires_idle(self, warm_pool, tmp_path):
        lf = pl.LazyFrame({"a": [1]})
        _run_task("pool-admin-1", "store", lf.serialize(), str(tmp_path / "a.arrow"))
        assert warm_pool.stats()["idle"] == 1

        grown = client.post("/pool", json={"size": 3}).json()
        assert grown["size"] == 3 and grown["enabled"] is True

        disabled = client.post("/pool", json={"size": 0}).json()
        assert disabled["enabled"] is False and disabled["size"] == 0
        assert warm_pool.stats()["total"] == 0, "shrinking to zero must retire idle members"
        assert warm_pool.acquire("store") is None, "a disabled pool must refuse leases"

    def test_resize_rejects_out_of_range(self, warm_pool):
        assert client.post("/pool", json={"size": 99}).status_code == 422
        assert client.post("/pool", json={"size": -1}).status_code == 422


class TestPlatformDefault:
    def test_windows_defaults_on(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "win32")
        assert pool.default_pool_size() == 4

    def test_other_platforms_default_off(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "linux")
        assert pool.default_pool_size() == 0
        monkeypatch.setattr("sys.platform", "darwin")
        assert pool.default_pool_size() == 0


class TestPersistence:
    def test_ui_resize_is_saved_and_governs_next_boot(self, warm_pool):
        response = client.post("/pool", json={"size": 3}).json()
        assert response["persisted"] is True
        assert response["env_override"] is False
        assert pool.load_persisted_size() == 3
        assert pool.initial_pool_size() == 3, "a fresh boot must pick up the saved UI setting"

    def test_env_var_beats_saved_setting_at_boot(self, warm_pool, monkeypatch):
        client.post("/pool", json={"size": 3})
        monkeypatch.setenv("FLOWFILE_WORKER_POOL_SIZE", "1")
        assert pool.initial_pool_size() == 1, "the env var is an explicit operator override"
        state = client.get("/pool").json()
        assert state["env_override"] is True
        assert state["persisted"] is False, "the saved value will not govern the next boot"

    def test_never_saved_and_unreadable_files_fall_back(self, monkeypatch, tmp_path):
        assert pool.load_persisted_size() is None
        pool._settings_file().write_text("[broken", encoding="utf-8")  # YAML parse error
        assert pool.load_persisted_size() is None
        pool._settings_file().write_text("just a string", encoding="utf-8")  # valid YAML, wrong shape
        assert pool.load_persisted_size() is None
        monkeypatch.setattr("sys.platform", "win32")
        assert pool.initial_pool_size() == 4, "corrupt settings must fall back to the platform default"

    def test_saved_file_is_commented_yaml(self, warm_pool):
        """The on-disk format is the future git-projection contract: commented, stable YAML."""
        client.post("/pool", json={"size": 2})
        content = pool._settings_file().read_text(encoding="utf-8")
        assert content.startswith("#"), "the file must carry its explanatory header"
        assert "size: 2" in content
        assert pool._settings_file().suffix == ".yaml"


class TestModuleHygiene:
    def test_pool_module_has_no_heavy_top_level_imports(self):
        """Every pool child imports pool.py, so its module-level imports must stay light."""
        source = Path(pool.__file__).read_text(encoding="utf-8")
        banned = {"polars", "pydantic", "psutil", "requests", "numpy"}
        banned_local = {"funcs", "models", "spawner", "streaming"}
        offenders = []
        for node in ast.parse(source).body:
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module] + [f"{node.module}.{alias.name}" for alias in node.names]
            for name in names:
                parts = set(name.split("."))
                if parts & banned or parts & banned_local:
                    offenders.append(name)
        assert offenders == [], f"pool.py drags heavy modules into every pool child: {offenders}"

    def test_poolable_set_matches_funcs_surface(self):
        """Every poolable op must be a real funcs callable with the uniform kwargs contract."""
        from flowfile_worker import funcs

        for operation in POOLABLE_OPERATIONS:
            assert callable(getattr(funcs, operation, None)), f"{operation} is not a funcs target"
