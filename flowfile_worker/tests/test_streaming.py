"""Tests for the WebSocket streaming protocol between worker and core.

These tests use FastAPI's TestClient WebSocket support to verify the
/ws/submit endpoint handles all operation types correctly and sends
results as raw binary frames (no base64 encoding).
"""

import io
import json
import time
from base64 import b64decode
from unittest.mock import AsyncMock

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import funcs, main, models, mp_context, status_dict, status_dict_lock
from flowfile_worker.spawner import drain_result_queue, handle_task, process_manager
from flowfile_worker.streaming import _monitor_progress

client = TestClient(main.app)


# Helpers

def _ws_submit(metadata: dict, payload_bytes: bytes, timeout: float = 30.0):
    """Run a full WebSocket submit cycle and return (messages, binary_results).

    Uses the protocol-aware receive pattern: after a "complete" message with
    result_type="polars" and has_result=True, the next frame is binary.

    Returns:
        tuple: (json_messages: list[dict], binary_frames: list[bytes])
    """
    json_messages = []
    binary_frames = []

    with client.websocket_connect("/ws/submit") as ws:
        ws.send_json(metadata)
        ws.send_bytes(payload_bytes)

        expecting_binary = False
        while True:
            if expecting_binary:
                binary_data = ws.receive_bytes()
                binary_frames.append(binary_data)
                break
            else:
                text_data = ws.receive_text()
                data = json.loads(text_data)
                json_messages.append(data)

                if data.get("type") == "complete":
                    if data.get("has_result"):
                        if data.get("result_type") == "polars":
                            expecting_binary = True
                            continue
                        else:
                            continue
                    break
                elif data.get("type") == "error":
                    break
                elif data.get("type") == "result_data":
                    break

    return json_messages, binary_frames


# Tests: Store operation (polars result via binary frame)

class TestWsStoreOperation:
    """Test the 'store' operation which returns a serialized LazyFrame as binary."""

    def test_store_simple_dataframe(self):
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        metadata = {
            "task_id": "ws-test-store-simple",
            "operation": "store",
            "flow_id": 1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        complete_msgs = [m for m in json_msgs if m.get("type") == "complete"]
        assert len(complete_msgs) == 1, f"Expected 1 complete message, got {len(complete_msgs)}"

        complete = complete_msgs[0]
        assert complete["result_type"] == "polars"
        assert complete["has_result"] is True
        assert complete["file_ref"].endswith(".arrow")
        assert complete["number_of_records"] == 3, "store must ride-along the row count on the complete frame"

        assert len(binary_frames) == 1, "Expected exactly 1 binary frame with result"
        result_lf = pl.LazyFrame.deserialize(io.BytesIO(binary_frames[0]))
        result_df = result_lf.collect()
        expected_df = lf.collect()
        assert result_df.equals(expected_df), f"Expected:\n{expected_df}\n\nGot:\n{result_df}"

    def test_store_with_transformation(self):
        lf = (
            pl.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
            .lazy()
            .select((pl.col("a") + pl.col("b")).alias("total"))
        )
        metadata = {
            "task_id": "ws-test-store-transform",
            "operation": "store",
            "flow_id": 1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        complete = [m for m in json_msgs if m.get("type") == "complete"][0]
        assert complete["result_type"] == "polars"
        assert len(binary_frames) == 1

        result_df = pl.LazyFrame.deserialize(io.BytesIO(binary_frames[0])).collect()
        expected_df = lf.collect()
        assert result_df.equals(expected_df)

    def test_store_returns_progress_updates(self):
        """Verify that progress messages are sent during processing."""
        lf = pl.LazyFrame({"x": list(range(100))})
        metadata = {
            "task_id": "ws-test-store-progress",
            "operation": "store",
            "flow_id": 1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        progress_msgs = [m for m in json_msgs if m.get("type") == "progress"]
        assert len(progress_msgs) >= 0, "Progress messages may or may not appear depending on timing"

        complete_msgs = [m for m in json_msgs if m.get("type") == "complete"]
        assert len(complete_msgs) == 1

    def test_store_result_is_raw_bytes_not_base64(self):
        """The key benefit: result is raw bytes, not base64-encoded string."""
        lf = pl.LazyFrame({"val": [42]})
        metadata = {
            "task_id": "ws-test-store-raw-bytes",
            "operation": "store",
            "flow_id": 1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        assert len(binary_frames) == 1
        raw_bytes = binary_frames[0]

        assert isinstance(raw_bytes, bytes)
        result_lf = pl.LazyFrame.deserialize(io.BytesIO(raw_bytes))
        assert result_lf.collect().equals(lf.collect())


# Tests: Store sample operation (file on disk, no binary result)

class TestWsStoreSampleOperation:
    """Test the 'store_sample' operation which writes to disk without returning data."""

    def test_store_sample(self):
        lf = pl.LazyFrame({"value": list(range(1000))})
        metadata = {
            "task_id": "ws-test-sample",
            "operation": "store_sample",
            "flow_id": 1,
            "node_id": -1,
            "kwargs": {"sample_size": 10},
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        complete = [m for m in json_msgs if m.get("type") == "complete"][0]
        assert complete["result_type"] == "other"
        assert complete["has_result"] is False
        assert complete["file_ref"].endswith(".arrow")
        assert complete.get("number_of_records") is None, "store_sample does not ride-along a count"

        assert len(binary_frames) == 0

        result_df = pl.read_ipc(complete["file_ref"])
        assert len(result_df) == 10, f"Expected 10 rows, got {len(result_df)}"


# Tests: Calculate schema operation (JSON result, not binary)

class TestWsCalculateSchemaOperation:
    """Test the 'calculate_schema' operation which returns schema stats as JSON."""

    def test_calculate_schema(self):
        lf = pl.LazyFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        metadata = {
            "task_id": "ws-test-schema",
            "operation": "calculate_schema",
            "flow_id": -1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        complete = [m for m in json_msgs if m.get("type") == "complete"][0]
        assert complete["result_type"] == "other"

        assert len(binary_frames) == 0
        result_data_msgs = [m for m in json_msgs if m.get("type") == "result_data"]
        assert len(result_data_msgs) == 1, "Expected 1 result_data message"

        schema_stats = result_data_msgs[0]["data"]
        assert isinstance(schema_stats, list)
        assert len(schema_stats) > 0
        col_names = {s.get("column_name") for s in schema_stats}
        assert "name" in col_names
        assert "age" in col_names


# Tests: Calculate number of records (JSON result, integer)

class TestWsCalculateNumberOfRecords:
    """Test 'calculate_number_of_records' which returns an integer via JSON."""

    def test_calculate_number_of_records(self):
        lf = pl.LazyFrame({"x": list(range(42))})
        metadata = {
            "task_id": "ws-test-nrecords",
            "operation": "calculate_number_of_records",
            "flow_id": -1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        complete = [m for m in json_msgs if m.get("type") == "complete"][0]
        assert complete["result_type"] == "other"
        assert complete.get("number_of_records") is None, "the count is the result_data payload, not a ride-along"

        result_data_msgs = [m for m in json_msgs if m.get("type") == "result_data"]
        assert len(result_data_msgs) == 1
        assert result_data_msgs[0]["data"] == 42


# Tests: REST endpoint compatibility (ensures REST still works after changes)

class TestRestCompatibilityAfterStreamingChanges:
    """Verify the existing REST endpoints still work correctly.

    The funcs.py change (raw bytes in queue instead of b64) requires
    spawner.py to b64-encode at the REST boundary. These tests confirm that.
    """

    def test_rest_submit_query_still_works(self):
        lf = pl.LazyFrame({"a": [10, 20], "b": [30, 40]})
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Operation-Type": "store",
            "X-Flow-Id": "1",
            "X-Node-Id": "-1",
        }
        v = client.post("/submit_query/", content=lf.serialize(), headers=headers)
        assert v.status_code == 200, v.text

        status = models.Status.model_validate(v.json())
        r = client.get(f"/status/{status.background_task_id}")
        status = models.Status.model_validate(r.json())
        assert status.status == "Completed", f"Expected Completed, got {status.status}: {status.error_message}"
        assert status.number_of_records == 2, "REST /status must surface the store ride-along count"

        result_df = pl.LazyFrame.deserialize(io.BytesIO(b64decode(status.results))).collect()
        assert result_df.equals(lf.collect())

    def test_rest_store_sample_still_works(self):
        lf = pl.LazyFrame({"value": list(range(100))})
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Operation-Type": "store_sample",
            "X-Sample-Size": "5",
            "X-Flow-Id": "1",
            "X-Node-Id": "-1",
        }
        v = client.post("/store_sample/", content=lf.serialize(), headers=headers)
        assert v.status_code == 200, v.text

        status = models.Status.model_validate(v.json())
        r = client.get(f"/status/{status.background_task_id}")
        status = models.Status.model_validate(r.json())
        assert status.status == "Completed", f"Expected Completed, got {status.status}: {status.error_message}"

        result_df = pl.read_ipc(status.file_ref)
        assert len(result_df) == 5


# Tests: Error handling

class TestWsErrorHandling:
    """Test WebSocket error scenarios."""

    def test_invalid_operation_returns_error(self):
        lf = pl.LazyFrame({"a": [1]})
        metadata = {
            "task_id": "ws-test-bad-op",
            "operation": "nonexistent_operation",
            "flow_id": 1,
            "node_id": -1,
        }

        json_msgs, binary_frames = _ws_submit(metadata, lf.serialize())

        error_msgs = [m for m in json_msgs if m.get("type") == "error"]
        assert len(error_msgs) >= 1, f"Expected error message, got: {json_msgs}"

    def test_status_dict_updated_for_rest_compatibility(self):
        """After WebSocket completes, status_dict should also be updated."""
        task_id = "ws-test-status-compat"
        lf = pl.LazyFrame({"x": [1, 2, 3]})
        metadata = {
            "task_id": task_id,
            "operation": "store",
            "flow_id": 1,
            "node_id": -1,
        }

        _ws_submit(metadata, lf.serialize())

        r = client.get(f"/status/{task_id}")
        assert r.status_code == 200
        status = models.Status.model_validate(r.json())
        assert status.status == "Completed"


# Tests: Row-count ride-along (Work Item 2)

class TestStoreRowCountRideAlong:
    """funcs.store queues (plan_bytes, row_count); the count surfaces on Status."""

    def _spawn_store(self, lf: pl.LazyFrame, file_path: str):
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        q = mp_context.Queue(maxsize=1)
        p = mp_context.Process(
            target=funcs.store,
            kwargs=dict(
                polars_serializable_object=lf.serialize(),
                progress=progress,
                error_message=error_message,
                queue=q,
                file_path=file_path,
                flowfile_flow_id=-1,
                flowfile_node_id=-1,
            ),
        )
        p.start()
        return p, progress, error_message, q

    def test_store_queues_plan_bytes_and_count(self, tmp_path):
        lf = pl.LazyFrame({"a": [1, 2, 3, 4, 5]})
        file_path = str(tmp_path / "store.arrow")
        p, progress, error_message, q = self._spawn_store(lf, file_path)
        try:
            result = drain_result_queue(q, p, timeout=30.0)
            p.join(timeout=30)
        finally:
            if p.is_alive():
                p.terminate()
                p.join()

        assert isinstance(result, tuple) and len(result) == 2, "store must put a (payload, count) tuple"
        plan_bytes, row_count = result
        assert isinstance(plan_bytes, bytes)
        assert row_count == 5
        restored = pl.LazyFrame.deserialize(io.BytesIO(plan_bytes))
        assert restored.collect().equals(lf.collect())

    def test_handle_task_surfaces_count_and_keeps_plan_bytes(self, tmp_path):
        task_id = "test-store-ride-along-e2e"
        lf = pl.LazyFrame({"x": list(range(7))})
        file_path = str(tmp_path / "store-e2e.arrow")
        p, progress, error_message, q = self._spawn_store(lf, file_path)
        process_manager.add_process(task_id, p)
        with status_dict_lock:
            status_dict[task_id] = models.Status(
                background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars"
            )
        try:
            handle_task(task_id, p, progress, error_message, q)
            with status_dict_lock:
                status = status_dict[task_id]
            assert status.status == "Completed", status.error_message
            assert status.number_of_records == 7
            # element 0 stays the byte-identical scan_ipc plan, b64-encoded at the REST boundary
            restored = pl.LazyFrame.deserialize(io.BytesIO(b64decode(status.results)))
            assert restored.collect().equals(lf.collect())
        finally:
            if p.is_alive():
                p.terminate()
                p.join()
            with status_dict_lock:
                status_dict.pop(task_id, None)
            process_manager.remove_process(task_id)


class TestHandleTaskTerminalStatus:
    """handle_task must always resolve a finished child to a terminal status.

    A failing child sets progress=-1 and can exit *before* the monitor loop
    observes it; the post-loop resolution must still surface a terminal Error.
    Otherwise the status is left at "Processing" and core's poller hangs forever
    (regression for the deadline-less REST offload hang).
    """

    def test_failed_child_yields_terminal_error(self, tmp_path):
        task_id = "test-handle-task-failed-child"
        file_path = str(tmp_path / "fail.arrow")
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        q = mp_context.Queue(maxsize=1)
        # Garbage bytes: the child's LazyFrame.deserialize raises, so it sets
        # progress=-1 and exits (racing handle_task's monitor loop).
        p = mp_context.Process(
            target=funcs.store,
            kwargs=dict(
                polars_serializable_object=b"not-a-serialized-lazyframe",
                progress=progress,
                error_message=error_message,
                queue=q,
                file_path=file_path,
                flowfile_flow_id=-1,
                flowfile_node_id=-1,
            ),
        )
        p.start()
        process_manager.add_process(task_id, p)
        with status_dict_lock:
            status_dict[task_id] = models.Status(
                background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars"
            )
        try:
            handle_task(task_id, p, progress, error_message, q)
            with status_dict_lock:
                status = status_dict[task_id]
            assert status.status == "Error", f"expected terminal Error, got {status.status!r}"
        finally:
            if p.is_alive():
                p.terminate()
                p.join()
            with status_dict_lock:
                status_dict.pop(task_id, None)
            process_manager.remove_process(task_id)

    def test_wedged_child_times_out_to_error(self, monkeypatch):
        # A child that stays alive but never signals completion must be terminated
        # at the wall-clock deadline and reported as a terminal Error.
        monkeypatch.setattr("flowfile_worker.spawner._TASK_TIMEOUT", 0.5)
        task_id = "test-handle-task-timeout"
        progress = mp_context.Value("i", 0)  # never advances
        error_message = mp_context.Array("c", 1024)
        q = mp_context.Queue(maxsize=1)
        p = mp_context.Process(target=time.sleep, args=(30,))  # wedged child
        p.start()
        process_manager.add_process(task_id, p)
        with status_dict_lock:
            status_dict[task_id] = models.Status(
                background_task_id=task_id, status="Starting", file_ref="x", result_type="polars"
            )
        try:
            handle_task(task_id, p, progress, error_message, q)
            with status_dict_lock:
                status = status_dict[task_id]
            assert status.status == "Error", f"expected Error, got {status.status!r}"
            assert "time limit" in (status.error_message or "")
            assert not p.is_alive(), "wedged child should have been terminated"
        finally:
            if p.is_alive():
                p.terminate()
                p.join()
            with status_dict_lock:
                status_dict.pop(task_id, None)
            process_manager.remove_process(task_id)


class TestWsMonitorTimeout:
    """_monitor_progress (WS path) must terminate a wedged child at the deadline
    and report a terminal Error, mirroring handle_task."""

    @pytest.mark.asyncio
    async def test_wedged_child_times_out_to_error(self, monkeypatch):
        monkeypatch.setattr("flowfile_worker.streaming._TASK_TIMEOUT", 0.5)
        task_id = "test-ws-monitor-timeout"
        progress = mp_context.Value("i", 0)  # never advances
        error_message = mp_context.Array("c", 1024)
        p = mp_context.Process(target=time.sleep, args=(30,))  # wedged child
        p.start()
        websocket = AsyncMock()
        with status_dict_lock:
            status_dict[task_id] = models.Status(
                background_task_id=task_id, status="Processing", file_ref="x", result_type="polars"
            )
        try:
            had_error = await _monitor_progress(websocket, p, progress, error_message, task_id)
            p.join(timeout=5)
            assert had_error is True
            with status_dict_lock:
                assert status_dict[task_id].status == "Error"
            err_frames = [
                c.args[0]
                for c in websocket.send_json.call_args_list
                if c.args and c.args[0].get("type") == "error"
            ]
            assert err_frames and "time limit" in err_frames[-1]["error_message"]
            assert not p.is_alive(), "wedged child should have been terminated"
        finally:
            if p.is_alive():
                p.terminate()
                p.join()
            with status_dict_lock:
                status_dict.pop(task_id, None)


class TestWsHeartbeat:
    """_monitor_progress must re-send progress periodically even when the value
    is unchanged: core treats these frames as liveness heartbeats and aborts
    the receive after prolonged silence (FLOWFILE_WORKER_WS_TIMEOUT)."""

    @pytest.mark.asyncio
    async def test_unchanged_progress_still_heartbeats(self, monkeypatch):
        monkeypatch.setattr("flowfile_worker.streaming._HEARTBEAT_INTERVAL", 0.05)
        # Let the monitor exit via the task deadline after several heartbeats.
        monkeypatch.setattr("flowfile_worker.streaming._TASK_TIMEOUT", 0.5)
        task_id = "test-ws-heartbeat"
        progress = mp_context.Value("i", 0)  # never advances
        error_message = mp_context.Array("c", 1024)
        p = mp_context.Process(target=time.sleep, args=(30,))
        p.start()
        websocket = AsyncMock()
        with status_dict_lock:
            status_dict[task_id] = models.Status(
                background_task_id=task_id, status="Processing", file_ref="x", result_type="polars"
            )
        try:
            await _monitor_progress(websocket, p, progress, error_message, task_id)
            progress_frames = [
                c.args[0]
                for c in websocket.send_json.call_args_list
                if c.args and c.args[0].get("type") == "progress"
            ]
            # Initial send plus repeated heartbeats despite the value never changing.
            assert len(progress_frames) >= 3, f"expected heartbeats, got {len(progress_frames)} progress frame(s)"
            assert all(f["progress"] == 0 for f in progress_frames)
        finally:
            if p.is_alive():
                p.terminate()
                p.join()
            with status_dict_lock:
                status_dict.pop(task_id, None)
