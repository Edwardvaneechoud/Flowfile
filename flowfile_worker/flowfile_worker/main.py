import asyncio
import signal
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from flowfile_worker import mp_context
from flowfile_worker.configs import FLOWFILE_CORE_URI, SERVICE_HOST, SERVICE_PORT, logger
from flowfile_worker.routes import router
from flowfile_worker.streaming import streaming_router
from shared.parent_watcher import start_parent_death_watcher
from shared.storage_config import storage

should_exit = False
server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handle application startup and shutdown"""
    logger.info("Starting application...")
    try:
        yield
    finally:
        logger.info("Shutting down application...")
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


@app.post("/shutdown")
async def shutdown():
    """Endpoint to handle graceful shutdown"""
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
    """Run the FastAPI app with graceful shutdown"""
    global server_instance

    if host is None:
        host = SERVICE_HOST
    if port is None:
        port = SERVICE_PORT

    logger.info(f"Starting worker service on {host}:{port}")
    logger.info(f"Core service configured at {FLOWFILE_CORE_URI}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    config = uvicorn.Config(app, host=host, port=port, loop="asyncio")
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
