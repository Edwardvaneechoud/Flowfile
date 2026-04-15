from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from flowfile_core.artifacts import router as artifacts_router
from flowfile_core.configs.flow_logger import clear_all_flow_logs
from flowfile_core.configs.settings import (
    SERVER_HOST,
    SERVER_PORT,
    WORKER_HOST,
    WORKER_PORT,
    WORKER_URL,
)
from flowfile_core.kernel import router as kernel_router
from flowfile_core.routes.auth import router as auth_router
from flowfile_core.routes.catalog import router as catalog_router
from flowfile_core.routes.cloud_connections import router as cloud_connections_router
from flowfile_core.routes.file_manager import router as file_manager_router
from flowfile_core.routes.ga_connections import router as ga_connections_router
from flowfile_core.routes.kafka import router as kafka_router
from flowfile_core.routes.logs import router as logs_router
from flowfile_core.routes.public import router as public_router
from flowfile_core.routes.routes import router
from flowfile_core.routes.secrets import router as secrets_router
from flowfile_core.routes.user_defined_components import router as user_defined_components_router
from flowfile_core.scheduler import FlowScheduler, get_scheduler, set_scheduler
from shared.storage_config import storage

storage.cleanup_directories()

# Set default mode to electron if not already set (allows Docker mode override)
if "FLOWFILE_MODE" not in os.environ:
    os.environ["FLOWFILE_MODE"] = "electron"

should_exit = False
server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handles the graceful startup and shutdown of the FastAPI application.

    This context manager ensures that resources, such as log files and kernel
    containers, are cleaned up properly when the application is terminated.
    """
    # Ensure scheduler and subprocess loggers are visible on stdout (Electron pipes this)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("Starting core application...")

    # Only auto-start scheduler if explicitly opted in via env var
    if os.environ.get("FLOWFILE_SCHEDULER_ENABLED", "").lower() in ("true", "1", "yes"):
        scheduler = FlowScheduler()
        await scheduler.start()
        set_scheduler(scheduler)
        print("Flow scheduler started")

    try:
        yield
    finally:
        print("Shutting down core application...")

        # Stop scheduler
        scheduler = get_scheduler()
        if scheduler is not None:
            await scheduler.stop()
            set_scheduler(None)
            print("Flow scheduler stopped")

        print("Cleaning up core service resources...")
        _shutdown_kernels()
        clear_all_flow_logs()
        await asyncio.sleep(0.1)  # Give a moment for cleanup


def _shutdown_kernels():
    """Stop all running kernel containers during shutdown."""
    try:
        from flowfile_core.kernel import get_kernel_manager

        manager = get_kernel_manager()
        manager.shutdown_all()
    except Exception as exc:
        print(f"Error shutting down kernels: {exc}")


# Initialize FastAPI with metadata
app = FastAPI(
    title="Flowfile Backend",
    version="0.1",
    description="Backend for the Flowfile application",
    lifespan=shutdown_handler,
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
    "http://127.0.0.1:63578",
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
app.include_router(catalog_router)
app.include_router(artifacts_router)
app.include_router(logs_router, tags=["logs"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(secrets_router, prefix="/secrets", tags=["secrets"])
app.include_router(cloud_connections_router, prefix="/cloud_connections", tags=["cloud_connections"])
app.include_router(ga_connections_router, prefix="/ga_connections", tags=["ga_connections"])
app.include_router(kafka_router)
app.include_router(user_defined_components_router, prefix="/user_defined_components", tags=["user_defined_components"])
app.include_router(kernel_router, tags=["kernels"])
app.include_router(file_manager_router, prefix="/file_manager", tags=["file_manager"])


@app.post("/shutdown")
async def shutdown(background_tasks: BackgroundTasks):
    """An API endpoint to gracefully shut down the server.

    This endpoint sets a flag that the Uvicorn server checks, allowing it
    to terminate cleanly. A background task is used to trigger the shutdown
    after the HTTP response has been sent.
    """
    # Use a background task to trigger the shutdown after the response is sent
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

    print("Starting core server...")
    print("Core server started")

    try:
        server.run()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
    finally:
        server_instance = None
        print("Server has shut down.")


_cli_logger = logging.getLogger("flowfile.run_flow_cli")


def _run_flow_cli(flow_path: str, run_id: int) -> int:
    """Execute a flow in-process (used by PyInstaller builds via ``--run-flow``).

    Replicates the logic from ``flowfile/__main__.py:run_flow()`` without
    importing from the top-level ``flowfile`` package (which is not bundled
    in the PyInstaller binary).
    """
    # Configure logging early so all messages are captured in the subprocess log file
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    _cli_logger.debug("_run_flow_cli started: flow_path=%s, run_id=%s", flow_path, run_id)
    _cli_logger.debug("sys.executable=%s, frozen=%s", sys.executable, getattr(sys, "frozen", False))

    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    OFFLOAD_TO_WORKER.set(False)

    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    path = Path(flow_path)
    if not path.exists():
        _cli_logger.error("File not found: %s", flow_path)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    if path.suffix.lower() not in (".yaml", ".yml", ".json"):
        _cli_logger.error("Unsupported file format: %s", path.suffix)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    _cli_logger.debug("Loading flow from: %s", flow_path)
    try:
        from flowfile_core.auth.utils import get_local_user_id

        flow = open_flow(path, user_id=get_local_user_id())
    except Exception as e:
        _cli_logger.exception("Error loading flow: %s", e)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    flow.execution_location = "local"

    # Remove explore_data nodes — they're UI-only and require a worker service
    explore_data_nodes = [n.node_id for n in flow.nodes if n.node_type == "explore_data"]
    for node_id in explore_data_nodes:
        flow.delete_node(node_id)
    if explore_data_nodes:
        _cli_logger.debug("Skipped %d explore_data node(s) (UI-only)", len(explore_data_nodes))

    flow_name = flow.flow_settings.name or f"Flow {flow.flow_id}"
    _cli_logger.debug("Running flow: %s (id=%s), nodes: %d", flow_name, flow.flow_id, len(flow.nodes))

    try:
        result = flow.run_graph()
    except Exception as e:
        _cli_logger.exception("Error running flow: %s", e)
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    if result is None:
        _cli_logger.error("Flow execution returned no result")
        _complete_run(run_id, success=False, nodes_completed=0)
        return 1

    _cli_logger.debug(
        "Flow execution finished: success=%s, nodes_completed=%s/%s",
        result.success,
        result.nodes_completed,
        result.number_of_nodes,
    )

    _complete_run(
        run_id,
        success=result.success,
        nodes_completed=result.nodes_completed,
        number_of_nodes=result.number_of_nodes,
    )

    if result.success:
        duration = ""
        if result.start_time and result.end_time:
            duration = f" in {(result.end_time - result.start_time).total_seconds():.2f}s"
        _cli_logger.debug("Flow completed successfully%s", duration)
        return 0
    else:
        _cli_logger.error("Flow execution failed")
        for node_result in result.node_step_result:
            if not node_result.success and node_result.error:
                node_name = node_result.node_name or f"Node {node_result.node_id}"
                _cli_logger.error("  - %s: %s", node_name, node_result.error)
        return 1


def _complete_run(run_id: int, success: bool, nodes_completed: int, number_of_nodes: int = 0) -> None:
    """Report results back to a pre-created run record."""
    _cli_logger.debug("Completing run %d: success=%s, nodes_completed=%d, number_of_nodes=%d",
                      run_id, success, nodes_completed, number_of_nodes)
    try:
        from shared.run_completion import complete_run

        complete_run(
            run_id=run_id,
            success=success,
            nodes_completed=nodes_completed,
            number_of_nodes=number_of_nodes,
        )
    except Exception as e:
        _cli_logger.exception("Failed to update run record %d: %s", run_id, e)


if __name__ == "__main__":
    if "--run-flow" in sys.argv:
        idx = sys.argv.index("--run-flow")
        _flow_path = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        _run_id = None
        if "--run-id" in sys.argv:
            rid_idx = sys.argv.index("--run-id")
            _run_id = int(sys.argv[rid_idx + 1]) if rid_idx + 1 < len(sys.argv) else None
        if not _flow_path:
            print("Usage: flowfile_core --run-flow <path> --run-id <id>", file=sys.stderr)
            sys.exit(1)
        if _run_id is None:
            print("Error: --run-id is required", file=sys.stderr)
            sys.exit(1)
        sys.exit(_run_flow_cli(_flow_path, _run_id))
    else:
        run()
