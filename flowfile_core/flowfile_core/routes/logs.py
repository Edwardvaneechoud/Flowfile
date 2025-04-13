import asyncio
import json
import time
from pathlib import Path
from typing import AsyncGenerator
import aiofiles
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse

from flowfile_core import ServerRun
from flowfile_core import flow_file_handler
# Core modules
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import clear_all_flow_logs
from flowfile_core.auth.jwt import get_current_active_user, get_current_user_from_query

# Schema and models
from flowfile_core.schemas import schemas

router = APIRouter()


@router.post("/clear-logs", tags=['flow_logging'])
async def clear_logs(current_user=Depends(get_current_active_user)):
    clear_all_flow_logs()
    return {"message": "All flow logs have been cleared."}


async def format_sse_message(data: str) -> str:
    """Format the data as a proper SSE message"""
    return f"data: {json.dumps(data)}\n\n"


@router.post("/logs/{flow_id}", tags=['flow_logging'])
async def add_log(flow_id: int, log_message: str):
    """
    Adds a log message to the log file for a given flow_id.
    """
    flow = flow_file_handler.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    flow.flow_logger.info(log_message)
    return {"message": "Log added successfully"}


@router.post("/raw_logs", tags=['flow_logging'])
async def add_raw_log(raw_log_input: schemas.RawLogInput):
    """
    Adds a log message to the log file for a given flow_id.
    """
    logger.info('Adding raw logs')
    flow = flow_file_handler.get_flow(raw_log_input.flowfile_flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    flow.flow_logger.get_log_filepath()
    flow_logger = flow.flow_logger
    flow_logger.get_log_filepath()
    if raw_log_input.log_type == 'INFO':
        flow_logger.info(raw_log_input.log_message,
                         extra=raw_log_input.extra)
    elif raw_log_input.log_type == 'ERROR':
        flow_logger.error(raw_log_input.log_message,
                          extra=raw_log_input.extra)
    return {"message": "Log added successfully"}


async def stream_log_file(
    log_file_path: Path,
    is_running_callable: callable,
    idle_timeout: int = 60  # timeout in seconds
) -> AsyncGenerator[str, None]:
    logger.info(f"Streaming log file: {log_file_path}")
    last_active = time.monotonic()
    try:
        async with aiofiles.open(log_file_path, "r") as file:

            # Ensure we start at the beginning
            await file.seek(0)
            while is_running_callable():
                # Immediately check if shutdown has been triggered
                if ServerRun.exit:
                    yield await format_sse_message("Server is shutting down. Closing connection.")
                    break


                line = await file.readline()
                if line:
                    formatted_message = await format_sse_message(line.strip())
                    logger.info(f'Yielding line: {line.strip()}')
                    yield formatted_message
                    last_active = time.monotonic()  # Reset idle timer on activity
                else:
                    # Check for idle timeout
                    if time.monotonic() - last_active > idle_timeout:
                        yield await format_sse_message("Connection timed out due to inactivity.")
                        break
                    # Allow the event loop to process other tasks (like signals)
                    await asyncio.sleep(0.1)

            # Optionally, read any final lines
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
        raise HTTPException(status_code=404, detail=f"Log file not found: {log_file_path}")
    except Exception as e:
        error_msg = await format_sse_message(f"Error reading log file: {str(e)}")
        yield error_msg
        raise HTTPException(status_code=500, detail=f"Error reading log file: {e}")


@router.get("/logs/{flow_id}", tags=['flow_logging'])
async def stream_logs(
    flow_id: int,
    idle_timeout: int = 300,
    current_user=Depends(get_current_user_from_query)
):
    """
    Streams logs for a given flow_id using Server-Sent Events.
    Requires authentication via token in query parameter.
    The connection will close gracefully if the server shuts down.
    """
    logger.info(f"Starting log stream for flow_id: {flow_id} by user: {current_user.username}")
    await asyncio.sleep(.3)
    flow = flow_file_handler.get_flow(flow_id)
    logger.info('Streaming logs')
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    log_file_path = flow.flow_logger.get_log_filepath()
    if not Path(log_file_path).exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    class RunningState:
        def __init__(self):
            self.has_started = False

        def is_running(self):
            if flow.flow_settings.is_running:
                self.has_started = True
            return flow.flow_settings.is_running or not self.has_started

    running_state = RunningState()

    return StreamingResponse(
        stream_log_file(log_file_path, running_state.is_running, idle_timeout),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
        }
    )

