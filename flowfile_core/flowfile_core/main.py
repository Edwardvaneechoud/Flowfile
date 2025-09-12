import asyncio
import os
import signal
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.storage_config import storage

from flowfile_core import ServerRun
from flowfile_core.configs.settings import (SERVER_HOST, SERVER_PORT, WORKER_HOST, WORKER_PORT, WORKER_URL,)

from flowfile_core.routes.auth import router as auth_router
from flowfile_core.routes.secrets import router as secrets_router
from flowfile_core.routes.routes import router
from flowfile_core.routes.public import router as public_router
from flowfile_core.routes.logs import router as logs_router
from flowfile_core.routes.cloud_connections import router as cloud_connections_router
from flowfile_core.routes.user_defined_components import router as user_defined_components_router

from flowfile_core.configs.flow_logger import clear_all_flow_logs
storage.cleanup_directories()

os.environ["FLOWFILE_MODE"] = "electron"

should_exit = False
server_instance = None

@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handles the graceful startup and shutdown of the FastAPI application.

    This context manager ensures that resources, such as log files, are cleaned
    up properly when the application is terminated.
    """
    print('Starting core application...')
    try:
        yield
    finally:
        print('Shutting down core application...')
        print("Cleaning up core service resources...")
        clear_all_flow_logs()
        await asyncio.sleep(0.1)  # Give a moment for cleanup


# Initialize FastAPI with metadata
app = FastAPI(
    title='Flowfile Backend',
    version='0.1',
    description='Backend for the Flowfile application',
    lifespan=shutdown_handler
)

# Configure CORS
origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://localhost:8081",
    "http://localhost:4173",
    "http://localhost:4174",
    "http://localhost:63578",
    "http://127.0.0.1:63578"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(public_router)
app.include_router(router)
app.include_router(logs_router, tags=["logs"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(secrets_router, prefix="/secrets", tags=["secrets"])
app.include_router(cloud_connections_router, prefix="/cloud_connections", tags=["cloud_connections"])
app.include_router(user_defined_components_router, prefix="/user_defined_components", tags=["user_defined_components"])


@app.post("/shutdown")
async def shutdown():
    """An API endpoint to gracefully shut down the server.

    This endpoint sets a flag that the Uvicorn server checks, allowing it
    to terminate cleanly. A background task is used to trigger the shutdown
    after the HTTP response has been sent.
    """
    # Use a background task to trigger the shutdown after the response is sent
    background_tasks = ServerRun()
    background_tasks.add_task(trigger_shutdown)
    return {"message": "Server is shutting down"}


async def trigger_shutdown():
    """(Internal) Triggers the actual server shutdown.

    Waits for a moment to allow the `/shutdown` response to be sent before
    telling the Uvicorn server instance to exit.
    """
    await asyncio.sleep(1)
    if server_instance:
        server_instance.should_exit = True


def signal_handler(signum, frame):
    """Handles OS signals like SIGINT (Ctrl+C) and SIGTERM for graceful shutdown."""
    print(f"Received signal {signum}")
    if server_instance:
        server_instance.should_exit = True


def run(host: str = None, port: int = None):
    """Runs the FastAPI application using Uvicorn.

    This function configures and starts the Uvicorn server, setting up
    signal handlers to ensure a graceful shutdown.

    Args:
        host: The host to bind the server to. Defaults to `SERVER_HOST` from settings.
        port: The port to bind the server to. Defaults to `SERVER_PORT` from settings.
    """
    global server_instance

    # Use values from settings if not explicitly provided
    if host is None:
        host = SERVER_HOST
    if port is None:
        port = SERVER_PORT
    print(f"Starting server on {host}:{port}")
    print(f"Worker configured at {WORKER_URL} (host: {WORKER_HOST}, port: {WORKER_PORT})")

    # Setup signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    server_instance = server  # Store server instance globally

    print('Starting core server...')
    print('Core server started')

    try:
        server.run()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
    finally:
        server_instance = None
        print("Server has shut down.")


if __name__ == "__main__":
    run()
