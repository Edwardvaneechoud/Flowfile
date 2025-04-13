import asyncio
import uvicorn
import signal

from contextlib import asynccontextmanager
from fastapi import FastAPI
from flowfile_worker.routes import router
from flowfile_worker import mp_context, CACHE_DIR
from flowfile_worker.configs import logger, FLOWFILE_CORE_URI, SERVICE_HOST, SERVICE_PORT


should_exit = False
server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handle application startup and shutdown"""
    logger.info('Starting application...')
    try:
        yield
    finally:
        logger.info('Shutting down application...')
        logger.info("Cleaning up worker resources...")
        for p in mp_context.active_children():
            try:
                p.terminate()
                p.join()
            except Exception as e:
                logger.error(f"Error cleaning up process: {e}")

        try:
            CACHE_DIR.cleanup()
        except Exception as e:
            print(f"Error cleaning up cache directory: {e}")

        await asyncio.sleep(0.1)


app = FastAPI(lifespan=shutdown_handler)
app.include_router(router)


@app.post("/shutdown")
async def shutdown():
    """Endpoint to handle graceful shutdown"""
    if server_instance:
        # Schedule the shutdown
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

    # Use values from settings if not explicitly provided
    if host is None:
        host = SERVICE_HOST
    if port is None:
        port = SERVICE_PORT

    # Log service configuration
    logger.info(f"Starting worker service on {host}:{port}")
    logger.info(f"Core service configured at {FLOWFILE_CORE_URI}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        loop="asyncio"
    )
    server = uvicorn.Server(config)
    server_instance = server  # Store server instance globally

    logger.info('Starting server...')
    logger.info('Server started')

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
