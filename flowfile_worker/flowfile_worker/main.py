import asyncio
import signal
import threading
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from flowfile_worker import mp_context
from flowfile_worker.configs import FLOWFILE_CORE_URI, GRPC_HOST, GRPC_PORT, SERVICE_HOST, SERVICE_PORT, logger
from flowfile_worker.routes import router
from shared.storage_config import storage

should_exit = False
server_instance = None
grpc_server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handle application startup and shutdown"""
    logger.info("Starting application...")
    try:
        yield
    finally:
        logger.info("Shutting down application...")

        # Shutdown gRPC server if running
        global grpc_server_instance
        if grpc_server_instance:
            logger.info("Stopping gRPC server...")
            grpc_server_instance.stop(grace=5)
            grpc_server_instance = None

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


def start_grpc_server(host: str, port: int):
    """Start the gRPC server in a separate thread."""
    global grpc_server_instance
    try:
        from flowfile_worker.grpc_server import serve

        grpc_server_instance = serve(host=host, port=port)
        logger.info(f"gRPC server started on {host}:{port}")
        grpc_server_instance.wait_for_termination()
    except Exception as e:
        logger.error(f"Error starting gRPC server: {e}")
        raise


def run(host: str = None, port: int = None, grpc_host: str = None, grpc_port: int = None, enable_grpc: bool = True):
    """Run the FastAPI app with graceful shutdown and optional gRPC server"""
    global server_instance

    # Use values from settings if not explicitly provided
    if host is None:
        host = SERVICE_HOST
    if port is None:
        port = SERVICE_PORT
    if grpc_host is None:
        grpc_host = GRPC_HOST
    if grpc_port is None:
        grpc_port = GRPC_PORT

    # Log service configuration
    logger.info(f"Starting worker service on {host}:{port}")
    logger.info(f"Core service configured at {FLOWFILE_CORE_URI}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start gRPC server in a separate thread if enabled
    grpc_thread = None
    if enable_grpc:
        logger.info(f"Starting gRPC server on {grpc_host}:{grpc_port}")
        grpc_thread = threading.Thread(
            target=start_grpc_server,
            args=(grpc_host, grpc_port),
            daemon=True,
        )
        grpc_thread.start()

    config = uvicorn.Config(app, host=host, port=port, loop="asyncio")
    server = uvicorn.Server(config)
    server_instance = server  # Store server instance globally

    logger.info("Starting REST server...")
    logger.info("Server started")

    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        # Cleanup gRPC server
        global grpc_server_instance
        if grpc_server_instance:
            logger.info("Stopping gRPC server...")
            grpc_server_instance.stop(grace=5)
            grpc_server_instance = None

        server_instance = None
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    run()
