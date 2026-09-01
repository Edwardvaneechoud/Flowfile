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
import os
from base64 import b64encode
from collections.abc import Callable
from time import monotonic
from typing import Any

import polars as pl
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status

# How often the receive loop wakes up to check for cancellation/inactivity.
_WS_RECV_POLL_SECONDS = 2.0

# Abort the receive when the worker has sent NOTHING for this long. The worker
# heartbeats a progress frame at least every ~10s while a task is alive, so
# prolonged silence means the worker parent is wedged or gone (protocol-level
# pings don't catch an app-level wedge). 0 disables. Mirrors the REST path's
# FLOWFILE_WORKER_POLL_TIMEOUT.
_WS_INACTIVITY_TIMEOUT = float(os.getenv("FLOWFILE_WORKER_WS_TIMEOUT", "300"))


class WorkerStreamInterrupted(Exception):
    """Receive phase interrupted after the task was already submitted.

    Callers must NOT fall back to REST on this: the task reached the worker,
    and re-submitting would duplicate work against a worker that is wedged
    or a run that was cancelled.
    """


class WorkerStreamStalled(WorkerStreamInterrupted):
    """The worker went silent past the inactivity timeout (presumed wedged)."""


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
        number_of_records=data.get("number_of_records"),
    )


def _receive_raw_result(
    ws,
    task_id: str,
    should_abort: Callable[[], bool] | None = None,
) -> tuple[Any, Status | None]:
    """Receive messages from the worker until a raw result or error arrives.

    Returns the result **without** deserializing it so the caller can both
    populate ``Status.results`` (b64-encoded bytes for polars) and
    deserialize into the in-memory object.

    The recv is bounded: each poll tick checks ``should_abort`` (cancellation)
    and total silence against ``_WS_INACTIVITY_TIMEOUT``, so a wedged worker
    surfaces as :class:`WorkerStreamStalled` instead of hanging core forever.
    Any received message (including progress heartbeats) resets the clock.

    Returns:
        (raw_result, status) where raw_result is ``bytes`` for polars
        results, arbitrary data for "other" results, or ``None``.
    """
    raw_result = None
    status = None
    last_message_at = monotonic()

    while True:
        try:
            msg = ws.recv(timeout=_WS_RECV_POLL_SECONDS)
        except TimeoutError:
            if should_abort is not None and should_abort():
                raise WorkerStreamInterrupted(f"Canceled while waiting for worker result (task {task_id})") from None
            silence = monotonic() - last_message_at
            if _WS_INACTIVITY_TIMEOUT and silence > _WS_INACTIVITY_TIMEOUT:
                raise WorkerStreamStalled(
                    f"No data from worker for {silence:.0f}s (task {task_id}); worker presumed "
                    f"unresponsive. Tune or disable via FLOWFILE_WORKER_WS_TIMEOUT."
                ) from None
            continue
        except ConnectionClosed:
            # cancel() sets the stop event and then closes the socket, which
            # aborts a blocked recv with ConnectionClosed (never TimeoutError).
            # Only map to an interrupt when a cancel is actually in flight — a
            # worker-initiated close with no cancel must keep its original
            # exception so callers preserve the degrade-gracefully semantics.
            if should_abort is not None and should_abort():
                raise WorkerStreamInterrupted(f"Canceled while waiting for worker result (task {task_id})") from None
            raise
        last_message_at = monotonic()

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


def _deserialize_and_populate_status(raw_result: Any, status: Status) -> tuple[Any, Status]:
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


def streaming_start(
    task_id: str,
    operation_type: str,
    flow_id: int,
    node_id: int | str,
    lf_bytes: bytes,
    kwargs: dict | None = None,
):
    """Open a WebSocket connection and send the task.

    Returns the **open** connection.  The caller must eventually call
    :func:`streaming_receive` (which closes the connection) or close it
    manually.

    Raises immediately on connection failure or send error.
    """
    ws_url = _get_ws_url() + "/ws/submit"
    metadata = _build_metadata(task_id, operation_type, flow_id, node_id, kwargs)

    ws = connect(ws_url, additional_headers={"X-Flowfile-Internal": get_internal_token()})
    try:
        ws.send(json.dumps(metadata))
        ws.send(lf_bytes)
    except Exception:
        ws.close()
        raise
    return ws


def streaming_receive(
    ws,
    task_id: str,
    should_abort: Callable[[], bool] | None = None,
) -> tuple[Any, Status]:
    """Block until the worker sends back a result, then close the connection.

    The returned ``Status`` object is fully populated (including
    ``results``) so it is equivalent to what the REST polling path
    would produce.

    Returns:
        Tuple of (result, Status)
    """
    try:
        raw_result, status = _receive_raw_result(ws, task_id, should_abort=should_abort)
    finally:
        ws.close()

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


def streaming_submit(
    task_id: str,
    operation_type: str,
    flow_id: int,
    node_id: int | str,
    lf_bytes: bytes,
    kwargs: dict | None = None,
) -> tuple[Any, Status]:
    """Submit a task via WebSocket and block until the result arrives.

    Convenience wrapper around :func:`streaming_start` +
    :func:`streaming_receive`.
    """
    ws = streaming_start(task_id, operation_type, flow_id, node_id, lf_bytes, kwargs)
    return streaming_receive(ws, task_id)
