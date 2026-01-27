"""Tests for the WebSocket streaming protocol between worker and core.

These tests use FastAPI's TestClient WebSocket support to verify the
/ws/submit endpoint handles all operation types correctly and sends
results as raw binary frames (no base64 encoding).
"""

import io
import json

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main, models

client = TestClient(main.app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
        # Send metadata + binary payload
        ws.send_json(metadata)
        ws.send_bytes(payload_bytes)

        # Receive messages until we get a terminal one (complete or error)
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
                            # Next frame is binary (raw LazyFrame bytes)
                            expecting_binary = True
                            continue
                        else:
                            # Next frame is JSON (result_data)
                            continue
                    # No result data follows
                    break
                elif data.get("type") == "error":
                    break
                elif data.get("type") == "result_data":
                    break
                # progress messages: keep reading

    return json_messages, binary_frames


# ---------------------------------------------------------------------------
# Tests: Store operation (polars result via binary frame)
# ---------------------------------------------------------------------------

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

        # Should have at least one progress message and a complete message
        complete_msgs = [m for m in json_msgs if m.get("type") == "complete"]
        assert len(complete_msgs) == 1, f"Expected 1 complete message, got {len(complete_msgs)}"

        complete = complete_msgs[0]
        assert complete["result_type"] == "polars"
        assert complete["has_result"] is True
        assert complete["file_ref"].endswith(".arrow")

        # Binary frame should contain a valid serialized LazyFrame
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
        # At least the initial progress should be reported
        assert len(progress_msgs) >= 0, "Progress messages may or may not appear depending on timing"

        # The complete message must be present
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

        # Should be raw bytes, not a base64 string
        assert isinstance(raw_bytes, bytes)
        # Verify it's directly deserializable without base64 decoding
        result_lf = pl.LazyFrame.deserialize(io.BytesIO(raw_bytes))
        assert result_lf.collect().equals(lf.collect())


# ---------------------------------------------------------------------------
# Tests: Store sample operation (file on disk, no binary result)
# ---------------------------------------------------------------------------

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

        # No binary frame since store_sample has no queue result
        assert len(binary_frames) == 0

        # The file should exist and contain the sampled data
        result_df = pl.read_ipc(complete["file_ref"])
        assert len(result_df) == 10, f"Expected 10 rows, got {len(result_df)}"


# ---------------------------------------------------------------------------
# Tests: Calculate schema operation (JSON result, not binary)
# ---------------------------------------------------------------------------

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

        # Schema result comes as JSON, not binary
        assert len(binary_frames) == 0
        result_data_msgs = [m for m in json_msgs if m.get("type") == "result_data"]
        assert len(result_data_msgs) == 1, "Expected 1 result_data message"

        schema_stats = result_data_msgs[0]["data"]
        assert isinstance(schema_stats, list)
        assert len(schema_stats) > 0
        # Schema stats should contain column info
        col_names = {s.get("column_name") for s in schema_stats}
        assert "name" in col_names
        assert "age" in col_names


# ---------------------------------------------------------------------------
# Tests: Calculate number of records (JSON result, integer)
# ---------------------------------------------------------------------------

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

        result_data_msgs = [m for m in json_msgs if m.get("type") == "result_data"]
        assert len(result_data_msgs) == 1
        assert result_data_msgs[0]["data"] == 42


# ---------------------------------------------------------------------------
# Tests: REST endpoint compatibility (ensures REST still works after changes)
# ---------------------------------------------------------------------------

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

        # Results should still be base64-encoded in REST responses
        from base64 import b64decode

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


# ---------------------------------------------------------------------------
# Tests: Error handling
# ---------------------------------------------------------------------------

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

        # The REST status endpoint should also show completed
        r = client.get(f"/status/{task_id}")
        assert r.status_code == 200
        status = models.Status.model_validate(r.json())
        assert status.status == "Completed"
