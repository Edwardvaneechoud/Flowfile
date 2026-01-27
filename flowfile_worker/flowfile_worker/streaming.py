"""
WebSocket streaming endpoint for worker-core communication.

Replaces the HTTP poll-based pattern with a single WebSocket connection per task:
1. Core sends JSON metadata + binary payload
2. Worker streams progress updates as JSON
3. Worker sends result as binary frame (no base64 encoding)

This eliminates:
- HTTP polling latency (0.5s+ per poll cycle)
- Base64 encode/decode overhead on result bytes
- Multiple HTTP round-trips per task
"""

import asyncio
import gc
import os
import threading
import uuid
from dataclasses import dataclass
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import Any

from base64 import b64encode
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from flowfile_worker import CACHE_DIR, funcs, models, mp_context, status_dict, status_dict_lock
from flowfile_worker.configs import logger
from flowfile_worker.spawner import process_manager

streaming_router = APIRouter()

# Maps operation type to result type for status tracking
_POLARS_RESULT_OPERATIONS = frozenset({"store"})


def _get_result_type(operation: str) -> str:
    return "polars" if operation in _POLARS_RESULT_OPERATIONS else "other"


# ---------------------------------------------------------------------------
# Task context
# ---------------------------------------------------------------------------

@dataclass
class _TaskContext:
    """Parsed and resolved metadata for a WebSocket task."""
    task_id: str
    operation: str
    flow_id: int
    node_id: int | str
    extra_kwargs: dict
    file_path: str
    result_type: str


def _parse_metadata(metadata: dict) -> _TaskContext:
    """Parse raw WebSocket metadata into a resolved TaskContext."""
    task_id = metadata.get("task_id") or str(uuid.uuid4())
    operation = metadata.get("operation", "store")
    flow_id = int(metadata.get("flow_id", 1))
    node_id = metadata.get("node_id", -1)
    extra_kwargs = metadata.get("kwargs", {})

    try:
        node_id = int(node_id)
    except (ValueError, TypeError):
        pass

    # Set up cache directory and file path
    cache_dir = CACHE_DIR / str(flow_id)
    cache_dir.mkdir(parents=True, exist_ok=True)
    file_path = os.path.join(str(cache_dir), f"{task_id}.arrow")
    result_type = _get_result_type(operation)

    return _TaskContext(
        task_id=task_id,
        operation=operation,
        flow_id=flow_id,
        node_id=node_id,
        extra_kwargs=extra_kwargs,
        file_path=file_path,
        result_type=result_type,
    )


def _register_status(ctx: _TaskContext) -> None:
    """Register the task in status_dict for REST compatibility."""
    status_dict[ctx.task_id] = models.Status(
        background_task_id=ctx.task_id,
        status="Starting",
        file_ref=ctx.file_path,
        result_type=ctx.result_type,
    )


# ---------------------------------------------------------------------------
# Subprocess management
# ---------------------------------------------------------------------------

def _spawn_subprocess(ctx: _TaskContext, polars_bytes: bytes) -> tuple[Process, Any, Any, Queue]:
    """Spawn a worker subprocess and return (process, progress, error_message, queue)."""
    process_task = getattr(funcs, ctx.operation)

    kwargs = dict(ctx.extra_kwargs)
    kwargs["polars_serializable_object"] = polars_bytes

    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    queue = mp_context.Queue(maxsize=1)

    kwargs["progress"] = progress
    kwargs["error_message"] = error_message
    kwargs["queue"] = queue
    kwargs["file_path"] = ctx.file_path
    kwargs["flowfile_flow_id"] = ctx.flow_id
    kwargs["flowfile_node_id"] = ctx.node_id

    p = mp_context.Process(target=process_task, kwargs=kwargs)
    p.start()
    process_manager.add_process(ctx.task_id, p)

    with status_dict_lock:
        status_dict[ctx.task_id].status = "Processing"

    logger.info(f"[WS] Started task {ctx.task_id} with operation: {ctx.operation}")
    return p, progress, error_message, queue


def _read_error_message(error_message) -> str:
    """Extract error string from shared ctypes array."""
    with error_message.get_lock():
        return error_message.value.decode().rstrip("\x00")


def _set_error_status(task_id: str, msg: str) -> None:
    """Update status_dict to reflect an error."""
    with status_dict_lock:
        status_dict[task_id].status = "Error"
        status_dict[task_id].error_message = msg


# ---------------------------------------------------------------------------
# Progress monitoring
# ---------------------------------------------------------------------------

async def _monitor_progress(websocket: WebSocket, p: Process, progress, error_message, task_id: str) -> bool:
    """Stream progress updates while subprocess is alive.

    Returns True if an error was detected and sent to the client.
    """
    last_progress = -1

    while p.is_alive():
        await asyncio.sleep(0.3)

        with progress.get_lock():
            current = progress.value

        if current != last_progress:
            try:
                await websocket.send_json({"type": "progress", "progress": current})
            except Exception:
                return False
            last_progress = current

        if current == -1:
            msg = _read_error_message(error_message)
            _set_error_status(task_id, msg)
            await websocket.send_json({"type": "error", "error_message": msg})
            return True

    return False


# ---------------------------------------------------------------------------
# Result handling
# ---------------------------------------------------------------------------

def _update_completed_status(task_id: str, result_data: Any) -> None:
    """Update status_dict for a successfully completed task."""
    with status_dict_lock:
        status_dict[task_id].status = "Completed"
        status_dict[task_id].progress = 100
        if result_data is not None:
            if isinstance(result_data, bytes):
                status_dict[task_id].results = b64encode(result_data).decode("ascii")
            else:
                status_dict[task_id].results = result_data


async def _send_completion(websocket: WebSocket, task_id: str, result_type: str,
                           file_path: str, queue: Queue) -> None:
    """Send completion message and result data over WebSocket."""
    result_data = queue.get() if not queue.empty() else None
    _update_completed_status(task_id, result_data)

    has_result = result_data is not None
    await websocket.send_json({
        "type": "complete",
        "result_type": result_type,
        "file_ref": file_path,
        "has_result": has_result,
    })

    if has_result:
        if isinstance(result_data, bytes):
            await websocket.send_bytes(result_data)
        else:
            await websocket.send_json({"type": "result_data", "data": result_data})

    logger.info(f"[WS] Task {task_id} completed successfully")


async def _send_final_error(websocket: WebSocket, task_id: str, progress, error_message) -> None:
    """Handle error or unexpected termination after subprocess exits."""
    with progress.get_lock():
        final = progress.value

    if final == -1:
        msg = _read_error_message(error_message)
        _set_error_status(task_id, msg)
        await websocket.send_json({"type": "error", "error_message": msg})
    else:
        with status_dict_lock:
            status_dict[task_id].status = "Unknown Error"
        await websocket.send_json({
            "type": "error",
            "error_message": "Process ended unexpectedly",
        })


# ---------------------------------------------------------------------------
# Disconnect handling
# ---------------------------------------------------------------------------

def _handoff_to_background(task_id: str, p: Process, progress, error_message, queue: Queue) -> None:
    """Hand off a running subprocess to a background thread for REST status updates."""
    from flowfile_worker.spawner import handle_task as _handle_task

    threading.Thread(
        target=_handle_task,
        args=(task_id, p, progress, error_message, queue),
        daemon=True,
    ).start()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def _cleanup_process(task_id: str | None, p: Process | None) -> None:
    """Clean up subprocess resources."""
    if p is not None:
        if p.is_alive():
            p.join(timeout=1)
            if p.is_alive():
                p.terminate()
                p.join()
        process_manager.remove_process(task_id)


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@streaming_router.websocket("/ws/submit")
async def ws_submit(websocket: WebSocket):
    """WebSocket endpoint for streaming task submission and result retrieval.

    Protocol (Core -> Worker):
        1. JSON message: task metadata (task_id, operation, flow_id, node_id, kwargs)
        2. Binary message: serialized Polars LazyFrame bytes

    Protocol (Worker -> Core):
        - JSON: {"type": "progress", "progress": N}  (0-100, sent periodically)
        - JSON: {"type": "complete", "result_type": "polars"|"other", "file_ref": "...", "has_result": bool}
        - Binary: raw result bytes (only if has_result=True and result_type="polars")
        - JSON: {"type": "result_data", "data": ...} (only if has_result=True and result_type="other")
        - JSON: {"type": "error", "error_message": "..."}
    """
    await websocket.accept()
    p = None
    task_id = None
    progress = None
    error_message = None

    try:
        # 1. Receive metadata + binary payload
        metadata = await websocket.receive_json()
        ctx = _parse_metadata(metadata)
        task_id = ctx.task_id
        _register_status(ctx)

        polars_bytes = await websocket.receive_bytes()

        # 2. Spawn subprocess
        p, progress, error_message, queue = _spawn_subprocess(ctx, polars_bytes)

        # 3. Monitor progress (returns True if error was sent)
        had_error = await _monitor_progress(websocket, p, progress, error_message, task_id)
        if had_error:
            return

        p.join()

        # 4. Send result or error
        with progress.get_lock():
            final = progress.value

        if final == 100:
            await _send_completion(websocket, task_id, ctx.result_type, ctx.file_path, queue)
        else:
            await _send_final_error(websocket, task_id, progress, error_message)

    except WebSocketDisconnect:
        logger.warning(f"[WS] Client disconnected for task {task_id}")
        if p is not None and p.is_alive():
            _handoff_to_background(task_id, p, progress, error_message, queue)
            # Prevent finally block from cleaning up - handle_task owns these now
            p = None
            progress = None
            error_message = None
    except Exception as e:
        logger.error(f"[WS] Error for task {task_id}: {e}", exc_info=True)
        try:
            await websocket.send_json({"type": "error", "error_message": str(e)})
        except Exception:
            pass
    finally:
        _cleanup_process(task_id, p)
        del p, progress, error_message
        gc.collect()
