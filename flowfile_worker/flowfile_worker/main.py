import asyncio
import ipaddress
import os
import signal
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from flowfile_worker import mp_context
from flowfile_worker.configs import FLOWFILE_CORE_URI, SERVICE_HOST, SERVICE_PORT, logger
from flowfile_worker.pool import task_pool
from flowfile_worker.protocol_routes import router as protocol_router
from flowfile_worker.routes import router
from flowfile_worker.streaming import streaming_router, v1_streaming_router
from shared.parent_watcher import start_parent_death_watcher
from shared.storage_config import storage

should_exit = False
server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handle application startup and shutdown"""
    logger.info("Starting application...")
    task_pool.prewarm()
    try:
        yield
    finally:
        logger.info("Shutting down application...")
        try:
            task_pool.shutdown()
        except Exception as e:
            logger.error(f"worker pool shutdown failed: {e}")
        try:
            from flowfile_worker.viz_sessions import viz_session_registry

            viz_session_registry.shutdown()
        except Exception as e:
            logger.error(f"viz registry shutdown failed: {e}")
        logger.info("Cleaning up worker resources...")
        for p in mp_context.active_children():
            try:
                p.terminate()
                p.join()
            except Exception as e:
                logger.error(f"Error cleaning up process: {e}")

        try:
            storage.cleanup_directories()
        except Exception as e:
            print(f"Error cleaning up cache directory: {e}")

        await asyncio.sleep(0.1)


app = FastAPI(lifespan=shutdown_handler)
app.include_router(router)
app.include_router(streaming_router)
app.include_router(protocol_router)
app.include_router(v1_streaming_router)


@app.exception_handler(RequestValidationError)
async def request_validation_without_input(request: Request, exc: RequestValidationError) -> JSONResponse:
    """FastAPI's 422 without each error's ``input``, which repeats the body (an export or import's presigned URI)."""
    errors = [{key: value for key, value in error.items() if key != "input"} for error in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(errors)})


@app.post("/shutdown")
async def shutdown(request: Request):
    """Endpoint to handle graceful shutdown, answered only for a loopback transport peer.

    The peer is the socket address, never a forwarded header; a missing client or a host that
    is not an IP address is refused with 403.
    """
    host = request.client.host if request.client else ""
    try:
        is_loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        is_loopback = False
    if not is_loopback:
        raise HTTPException(status_code=403, detail="Shutdown is only accepted from a loopback address")
    if server_instance:
        await asyncio.create_task(trigger_shutdown())
    return {"message": "Shutting down"}


async def trigger_shutdown():
    """Trigger the actual shutdown after responding to the client"""
    await asyncio.sleep(1)  # Give time for the response to be sent
    if server_instance:
        server_instance.should_exit = True


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}")
    if server_instance:
        server_instance.should_exit = True


def run(host: str = None, port: int = None):
    """Run the FastAPI app with graceful shutdown, over TLS when `FLOWFILE_WORKER_TLS_CERT` and
    `FLOWFILE_WORKER_TLS_KEY` name a certificate and its key."""
    global server_instance

    if host is None:
        host = SERVICE_HOST
    if port is None:
        port = SERVICE_PORT

    logger.info(f"Starting worker service on {host}:{port}")
    logger.info(f"Core service configured at {FLOWFILE_CORE_URI}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    tls_cert = os.environ.get("FLOWFILE_WORKER_TLS_CERT")
    tls_key = os.environ.get("FLOWFILE_WORKER_TLS_KEY")
    if tls_cert:
        logger.info(f"Serving TLS with the certificate {tls_cert}")

    config = uvicorn.Config(app, host=host, port=port, loop="asyncio", ssl_certfile=tls_cert, ssl_keyfile=tls_key)
    server = uvicorn.Server(config)
    server_instance = server

    # In desktop-sidecar mode, exit if the Tauri shell dies without reaping us.
    start_parent_death_watcher(lambda: setattr(server, "should_exit", True))

    logger.info("Starting server...")
    logger.info("Server started")

    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        server_instance = None
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    run()
