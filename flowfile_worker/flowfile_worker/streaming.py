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
import uuid

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


@streaming_router.websocket("/ws/submit")
async def ws_submit(websocket: WebSocket):
    """WebSocket endpoint for streaming task submission and result retrieval.

    Protocol (Core → Worker):
        1. JSON message: task metadata (task_id, operation, flow_id, node_id, kwargs)
        2. Binary message: serialized Polars LazyFrame bytes

    Protocol (Worker → Core):
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
        # 1. Receive metadata
        metadata = await websocket.receive_json()
        task_id = metadata.get("task_id") or str(uuid.uuid4())
        operation = metadata.get("operation", "store")
        flow_id = int(metadata.get("flow_id", 1))
        node_id = metadata.get("node_id", -1)
        extra_kwargs = metadata.get("kwargs", {})

        try:
            node_id = int(node_id)
        except (ValueError, TypeError):
            pass

        # 2. Receive binary payload (serialized LazyFrame)
        polars_bytes = await websocket.receive_bytes()

        # Set up cache directory and file path
        default_cache_dir = CACHE_DIR / str(flow_id)
        default_cache_dir.mkdir(parents=True, exist_ok=True)
        file_path = os.path.join(str(default_cache_dir), f"{task_id}.arrow")
        result_type = _get_result_type(operation)

        # Register in status_dict for REST compatibility
        status = models.Status(
            background_task_id=task_id,
            status="Starting",
            file_ref=file_path,
            result_type=result_type,
        )
        status_dict[task_id] = status

        # 3. Spawn subprocess
        process_task = getattr(funcs, operation)
        kwargs = dict(extra_kwargs)
        kwargs["polars_serializable_object"] = polars_bytes
        progress = mp_context.Value("i", 0)
        error_message = mp_context.Array("c", 1024)
        queue = mp_context.Queue(maxsize=1)
        kwargs["progress"] = progress
        kwargs["error_message"] = error_message
        kwargs["queue"] = queue
        kwargs["file_path"] = file_path
        kwargs["flowfile_flow_id"] = flow_id
        kwargs["flowfile_node_id"] = node_id

        p = mp_context.Process(target=process_task, kwargs=kwargs)
        p.start()
        process_manager.add_process(task_id, p)

        with status_dict_lock:
            status_dict[task_id].status = "Processing"

        logger.info(f"[WS] Started task {task_id} with operation: {operation}")

        # 4. Monitor subprocess and stream progress
        last_progress = -1
        while p.is_alive():
            await asyncio.sleep(0.3)

            with progress.get_lock():
                current = progress.value

            if current != last_progress:
                try:
                    await websocket.send_json({"type": "progress", "progress": current})
                except Exception:
                    break
                last_progress = current

            if current == -1:
                with error_message.get_lock():
                    msg = error_message.value.decode().rstrip("\x00")
                with status_dict_lock:
                    status_dict[task_id].status = "Error"
                    status_dict[task_id].error_message = msg
                await websocket.send_json({"type": "error", "error_message": msg})
                return

        p.join()

        # 5. Check final state
        with progress.get_lock():
            final = progress.value

        if final == 100:
            result_data = queue.get() if not queue.empty() else None

            # Update status_dict for REST compatibility
            with status_dict_lock:
                status_dict[task_id].status = "Completed"
                status_dict[task_id].progress = 100
                if result_data is not None:
                    if isinstance(result_data, bytes):
                        status_dict[task_id].results = b64encode(result_data).decode("ascii")
                    else:
                        status_dict[task_id].results = result_data

            has_result = result_data is not None

            # Send completion message
            await websocket.send_json({
                "type": "complete",
                "result_type": result_type,
                "file_ref": file_path,
                "has_result": has_result,
            })

            # Send result data
            if has_result:
                if isinstance(result_data, bytes):
                    # Raw serialized LazyFrame bytes - send directly as binary
                    await websocket.send_bytes(result_data)
                else:
                    # Non-binary result (schema stats, record count, etc.)
                    await websocket.send_json({"type": "result_data", "data": result_data})

            logger.info(f"[WS] Task {task_id} completed successfully")

        elif final == -1:
            with error_message.get_lock():
                msg = error_message.value.decode().rstrip("\x00")
            with status_dict_lock:
                status_dict[task_id].status = "Error"
                status_dict[task_id].error_message = msg
            await websocket.send_json({"type": "error", "error_message": msg})
        else:
            with status_dict_lock:
                status_dict[task_id].status = "Unknown Error"
            await websocket.send_json({
                "type": "error",
                "error_message": "Process ended unexpectedly",
            })

    except WebSocketDisconnect:
        logger.warning(f"[WS] Client disconnected for task {task_id}")
        if p is not None and p.is_alive():
            # Hand off monitoring to background thread so REST status updates work
            import threading
            from flowfile_worker.spawner import handle_task as _handle_task

            threading.Thread(
                target=_handle_task,
                args=(task_id, p, progress, error_message, queue),
                daemon=True,
            ).start()
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
        if p is not None:
            if p.is_alive():
                p.join(timeout=1)
                if p.is_alive():
                    p.terminate()
                    p.join()
            process_manager.remove_process(task_id)
            del p
        if progress is not None:
            del progress
        if error_message is not None:
            del error_message
        gc.collect()
