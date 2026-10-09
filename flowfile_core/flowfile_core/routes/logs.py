import asyncio
import functools
import json
import time
from collections.abc import AsyncGenerator
from pathlib import Path

import aiofiles
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse

from flowfile_core import ServerRun, flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user, require_internal_token

# Core modules
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import clear_all_flow_logs

# Schema and models
from flowfile_core.schemas import schemas

router = APIRouter()


@router.post("/clear-logs", tags=["flow_logging"])
async def clear_logs(current_user=Depends(get_current_active_user)):
    clear_all_flow_logs()
    return {"message": "All flow logs have been cleared."}


async def format_sse_message(data: str) -> str:
    """Format the data as a proper SSE message"""
    return f"data: {json.dumps(data)}\n\n"


@functools.cache
def _warn_unsigned_raw_log() -> None:
    """Say once per process why kernel output is missing from a flow log; the 401 alone is silent."""
    logger.warning(
        "Rejected a /raw_logs post without a valid X-Internal-Token: check that FLOWFILE_INTERNAL_TOKEN is "
        "set for core; a kernel image built before log posts were signed must be rebuilt."
    )


def _require_signed_raw_log(x_internal_token: str | None = Header(None, alias="X-Internal-Token")) -> None:
    try:
        require_internal_token(x_internal_token)
    except HTTPException:
        _warn_unsigned_raw_log()
        raise


@router.post("/raw_logs", tags=["flow_logging"], dependencies=[Depends(_require_signed_raw_log)])
async def add_raw_log(raw_log_input: schemas.RawLogInput):
    """Adds a log message to the log file for a given flow_id.

    Only the worker and kernels write here, signed with the internal token (``X-Internal-Token``).
    """
    flow = flow_file_handler.get_flow(raw_log_input.flowfile_flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    flow_logger = flow.flow_logger
    node_id = raw_log_input.node_id if raw_log_input.node_id is not None else -1
    if raw_log_input.log_type == "INFO":
        flow_logger.info(raw_log_input.log_message, extra=raw_log_input.extra, node_id=node_id)
    elif raw_log_input.log_type == "WARNING":
        flow_logger.warning(raw_log_input.log_message, extra=raw_log_input.extra, node_id=node_id)
    elif raw_log_input.log_type == "ERROR":
        flow_logger.error(raw_log_input.log_message, extra=raw_log_input.extra, node_id=node_id)
    return {"message": "Log added successfully"}


async def stream_log_file(
    log_file_path: Path,
    is_running_callable: callable,
    idle_timeout: int = 60,  # timeout in seconds
) -> AsyncGenerator[str, None]:
    logger.info(f"Streaming log file: {log_file_path}")
    last_active = time.monotonic()
    try:
        async with aiofiles.open(log_file_path) as file:
            await file.seek(0)
            while is_running_callable():
                if ServerRun.exit:
                    yield await format_sse_message("Server is shutting down. Closing connection.")
                    break

                line = await file.readline()
                if line:
                    formatted_message = await format_sse_message(line.strip())
                    yield formatted_message
                    last_active = time.monotonic()
                else:
                    if time.monotonic() - last_active > idle_timeout:
                        yield await format_sse_message("Connection timed out due to inactivity.")
                        break
                    # Allow the event loop to process other tasks (like signals)
                    await asyncio.sleep(0.1)

            while True:
                if ServerRun.exit:
                    break
                line = await file.readline()
                if not line:
                    break
                yield await format_sse_message(line.strip())

            logger.info("Streaming completed")

    except FileNotFoundError:
        error_msg = await format_sse_message(f"Log file not found: {log_file_path}")
        yield error_msg
        raise HTTPException(status_code=404, detail=f"Log file not found: {log_file_path}") from None
    except Exception as e:
        error_msg = await format_sse_message(f"Error reading log file: {str(e)}")
        yield error_msg
        raise HTTPException(status_code=500, detail=f"Error reading log file: {e}") from e


async def wait_for_run_start(flow, seconds: float) -> None:
    """Give the run the client just asked for time to be claimed.

    The designer opens a run's stream as it posts ``/flow/run/``, and that route queues the run as a
    background task, so the claim lands after the response: without this wait the stream would find
    an idle flow, send the previous run's file and close.
    """
    deadline = time.monotonic() + seconds
    while not flow.flow_settings.is_running and time.monotonic() < deadline:
        await asyncio.sleep(0.05)


@router.get("/logs/{flow_id}", tags=["flow_logging"])
async def stream_logs(
    flow_id: int,
    idle_timeout: int = 300,
    wait_for_run: float = Query(0, ge=0, le=30),
    current_user=Depends(get_current_active_user),
):
    """
    Streams logs for a given flow_id using Server-Sent Events.
    Requires a Bearer token header (the renderer reads the stream with fetch, not EventSource,
    so the token never goes in the URL). Only flows open in the caller's session are served.

    Core owns the stream's end: opened while the flow runs, the stream follows the run and ends with
    it (``idle_timeout`` only bounds a run that stays silent); opened while the flow is idle, it sends
    the file once and closes. ``wait_for_run`` first waits that many seconds for a run to be claimed.
    The connection will close gracefully if the server shuts down.
    """
    logger.info(f"Starting log stream for flow_id: {flow_id} by user: {current_user.username}")
    flow = flow_file_handler.get_flow(flow_id, current_user.id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if wait_for_run > 0:
        await wait_for_run_start(flow, wait_for_run)
    # A run claims its slot before it truncates the log: let the file be rewritten before it is read.
    await asyncio.sleep(0.3)

    log_file_path = flow.flow_logger.get_log_filepath()
    if not Path(log_file_path).exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    follows_run = flow.flow_settings.is_running
    logger.info("Streaming logs" if follows_run else "Sending the log once: the flow is idle")

    def is_running() -> bool:
        return follows_run and flow.flow_settings.is_running

    return StreamingResponse(
        stream_log_file(log_file_path, is_running, idle_timeout),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
        },
    )
