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

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status


def _get_ws_url() -> str:
    """Convert HTTP worker URL to WebSocket URL."""
    return WORKER_URL.replace("http://", "ws://").replace("https://", "wss://")


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
    from websockets.sync.client import connect

    ws_url = _get_ws_url() + "/ws/submit"
    result = None
    status = None

    with connect(ws_url) as ws:
        # Send metadata
        metadata = {
            "task_id": task_id,
            "operation": operation_type,
            "flow_id": flow_id,
            "node_id": node_id,
        }
        if kwargs:
            metadata["kwargs"] = kwargs
        ws.send(json.dumps(metadata))

        # Send binary payload
        ws.send(lf_bytes)

        # Receive messages until result or error
        while True:
            msg = ws.recv()

            if isinstance(msg, bytes):
                # Binary frame = raw serialized LazyFrame bytes
                result = pl.LazyFrame.deserialize(io.BytesIO(msg))
                break
            else:
                data = json.loads(msg)
                msg_type = data.get("type")

                if msg_type == "progress":
                    # Progress updates are received but not acted on in sync mode
                    # Subclasses could override to expose progress
                    continue

                elif msg_type == "complete":
                    status = Status(
                        background_task_id=task_id,
                        status="Completed",
                        file_ref=data.get("file_ref", ""),
                        result_type=data.get("result_type", "polars"),
                        progress=100,
                    )
                    if not data.get("has_result", False):
                        # No result data follows (e.g., store_sample)
                        break
                    # For polars results, next message will be binary
                    # For other results, next message will be result_data JSON
                    continue

                elif msg_type == "result_data":
                    # Non-binary result (schema stats, record count, etc.)
                    result = data.get("data")
                    break

                elif msg_type == "error":
                    raise Exception(data.get("error_message", "Unknown worker error"))

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
