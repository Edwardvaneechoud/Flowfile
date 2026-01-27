"""
WebSocket streaming client for core-to-worker communication.

Replaces the HTTP poll-based pattern with a single WebSocket connection:
- Sends task metadata + serialized LazyFrame as binary
- Receives progress updates as JSON pushes
- Receives result as raw binary frame (no base64 encoding)

Falls back to REST automatically if the worker doesn't support WebSocket.
"""

import io
import json
from typing import Any

import polars as pl
from websockets.sync.client import connect

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status


def _get_ws_url() -> str:
    """Convert HTTP worker URL to WebSocket URL."""
    return WORKER_URL.replace("http://", "ws://").replace("https://", "wss://")


# ---------------------------------------------------------------------------
# Message building
# ---------------------------------------------------------------------------

def _build_metadata(
    task_id: str,
    operation_type: str,
    flow_id: int,
    node_id: int | str,
    kwargs: dict | None,
) -> dict:
    """Build the JSON metadata message for the WebSocket protocol."""
    metadata = {
        "task_id": task_id,
        "operation": operation_type,
        "flow_id": flow_id,
        "node_id": node_id,
    }
    if kwargs:
        metadata["kwargs"] = kwargs
    return metadata


# ---------------------------------------------------------------------------
# Message receiving
# ---------------------------------------------------------------------------

def _handle_complete_message(data: dict, task_id: str) -> Status:
    """Build a Status from a 'complete' protocol message."""
    return Status(
        background_task_id=task_id,
        status="Completed",
        file_ref=data.get("file_ref", ""),
        result_type=data.get("result_type", "polars"),
        progress=100,
        results=data.get("results", None),
    )


def _receive_result(ws, task_id: str) -> tuple[Any, Status | None]:
    """Receive messages from the worker until a result or error arrives.

    Handles the three message types in the protocol:
    - progress: ignored in sync mode
    - complete: builds Status, then reads result (binary or JSON) if present
    - error: raises Exception

    Returns:
        (result, status) where result may be a LazyFrame, other data, or None.
    """
    result = None
    status = None
    while True:
        msg = ws.recv()

        if isinstance(msg, bytes):
            # Binary frame = raw serialized LazyFrame bytes
            result = pl.LazyFrame.deserialize(io.BytesIO(msg))
            break

        data = json.loads(msg)
        msg_type = data.get("type")

        if msg_type == "progress":
            continue

        if msg_type == "complete":
            status = _handle_complete_message(data, task_id)
            if not data.get("has_result", False):
                break
            # Next message will be binary (polars) or JSON (result_data)
            continue

        if msg_type == "result_data":
            result = data.get("data")
            break

        if msg_type == "error":
            raise Exception(data.get("error_message", "Unknown worker error"))

    return result, status


def streaming_submit(
    task_id: str,
    operation_type: str,
    flow_id: int,
    node_id: int | str,
    lf_bytes: bytes,
    kwargs: dict | None = None,
) -> tuple[Any, Status]:
    """Submit a task via WebSocket and receive streamed results.

    Opens a single WebSocket connection that:
    1. Sends task metadata (JSON) + payload (binary)
    2. Receives progress updates (JSON)
    3. Receives result as binary frame (polars) or JSON (other)

    Args:
        task_id: Unique identifier for the task
        operation_type: Operation to perform (store, calculate_schema, etc.)
        flow_id: Flow ID for logging and cache organization
        node_id: Node ID for logging
        lf_bytes: Serialized LazyFrame bytes
        kwargs: Extra keyword arguments for the operation

    Returns:
        Tuple of (result, Status)

    Raises:
        Exception: On connection failure, task error, or protocol error
    """

    ws_url = _get_ws_url() + "/ws/submit"
    metadata = _build_metadata(task_id, operation_type, flow_id, node_id, kwargs)
    with connect(ws_url) as ws:
        ws.send(json.dumps(metadata))
        ws.send(lf_bytes)
        result, status = _receive_result(ws, task_id)

    if status is None:
        status = Status(
            background_task_id=task_id,
            status="Completed",
            file_ref="",
            result_type="polars",
            progress=100,
        )

    return result, status


def is_streaming_available() -> bool:
    """Check if the worker supports WebSocket streaming.

    Attempts a WebSocket connection to the worker. Result is not cached
    because worker availability can change.
    """
    try:
        from websockets.sync.client import connect

        ws_url = _get_ws_url() + "/ws/submit"
        # Quick connection test with short timeout
        with connect(ws_url, open_timeout=2, close_timeout=1) as ws:
            ws.close()
        return True
    except Exception:
        return False
