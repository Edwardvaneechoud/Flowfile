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
from base64 import b64encode
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
    """Build a partial Status from a 'complete' protocol message.

    The ``results`` field is set to None here and populated by the caller
    once the actual result payload has been received.
    """
    return Status(
        background_task_id=task_id,
        status="Completed",
        file_ref=data.get("file_ref", ""),
        result_type=data.get("result_type", "polars"),
        progress=100,
        results=data.get("results", None),
    )


def _receive_raw_result(ws, task_id: str) -> tuple[Any, Status | None]:
    """Receive messages from the worker until a raw result or error arrives.

    Returns the result **without** deserializing it so the caller can both
    populate ``Status.results`` (b64-encoded bytes for polars) and
    deserialize into the in-memory object.

    Returns:
        (raw_result, status) where raw_result is ``bytes`` for polars
        results, arbitrary data for "other" results, or ``None``.
    """
    raw_result = None
    status = None

    while True:
        msg = ws.recv()

        if isinstance(msg, bytes):
            raw_result = msg
            break

        data = json.loads(msg)
        msg_type = data.get("type")

        if msg_type == "progress":
            continue

        if msg_type == "complete":
            status = _handle_complete_message(data, task_id)
            if not data.get("has_result", False):
                break
            continue

        if msg_type == "result_data":
            raw_result = data.get("data")
            break

        if msg_type == "error":
            raise Exception(data.get("error_message", "Unknown worker error"))

    return raw_result, status


def _deserialize_and_populate_status(
    raw_result: Any, status: Status
) -> tuple[Any, Status]:
    """Deserialize the raw result and fill ``status.results``.

    For polars results (bytes): deserializes into a LazyFrame and stores
    the b64-encoded bytes in ``status.results`` (matching REST behaviour).
    For other results: stores the value directly in ``status.results``.
    """
    if raw_result is None:
        return None, status

    if isinstance(raw_result, bytes):
        status.results = b64encode(raw_result).decode("ascii")
        return pl.LazyFrame.deserialize(io.BytesIO(raw_result)), status

    status.results = raw_result
    return raw_result, status


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

    The returned ``Status`` object is fully populated (including
    ``results``) so it is equivalent to what the REST polling path
    would produce.

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
        raw_result, status = _receive_raw_result(ws, task_id)

    if status is None:
        status = Status(
            background_task_id=task_id,
            status="Completed",
            file_ref="",
            result_type="polars",
            progress=100,
            results=None,
        )

    return _deserialize_and_populate_status(raw_result, status)


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
