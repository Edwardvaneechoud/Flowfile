# ruff: noqa: E402
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import threading
from contextlib import asynccontextmanager

# Headless run child: dispatch before the router imports and the storage sweep below.
if __name__ == "__main__" and "--run-flow" in sys.argv:
    from flowfile_core.run_flow_cli import main as _run_flow_main

    sys.exit(_run_flow_main(sys.argv))

import uvicorn
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from flowfile_core import change_feed
from flowfile_core import telemetry as core_telemetry
from flowfile_core.ai import router as ai_router
from flowfile_core.ai.admin_routes import router as ai_admin_router
from flowfile_core.artifacts import router as artifacts_router
from flowfile_core.configs.access_log import install_access_log_redaction
from flowfile_core.configs.settings import (
    SERVER_HOST,
    SERVER_PORT,
    WORKER_HOST,
    WORKER_PORT,
    WORKER_URL,
)
from flowfile_core.events import publish
from flowfile_core.kernel import router as kernel_router
from flowfile_core.lsp.admin_routes import router as lsp_admin_router
from flowfile_core.lsp.routes import router as lsp_router
from flowfile_core.ml import router as ml_router
from flowfile_core.notebook.runner import install_notebook_runner
from flowfile_core.routes.api_consumers import router as api_consumers_router
from flowfile_core.routes.auth import router as auth_router
from flowfile_core.routes.catalog import router as catalog_router
from flowfile_core.routes.cloud_connections import router as cloud_connections_router
from flowfile_core.routes.cloud_delta import router as cloud_delta_router
from flowfile_core.routes.community_github import router as community_github_router
from flowfile_core.routes.community_nodes import router as community_nodes_router
from flowfile_core.routes.converters import router as converters_router
from flowfile_core.routes.custom_node_mounts import router as custom_node_mounts_router
from flowfile_core.routes.file_manager import router as file_manager_router
from flowfile_core.routes.flow_api import data_router as flow_api_data_router
from flowfile_core.routes.flow_api import management_router as flow_api_management_router
from flowfile_core.routes.ga_connections import router as ga_connections_router
from flowfile_core.routes.kafka import router as kafka_router
from flowfile_core.routes.logs import router as logs_router
from flowfile_core.routes.node_requests import router as node_requests_router
from flowfile_core.routes.notebook import router as notebook_router
from flowfile_core.routes.notifications import router as notifications_router
from flowfile_core.routes.project import router as project_router
from flowfile_core.routes.public import router as public_router
from flowfile_core.routes.routes import router
from flowfile_core.routes.secrets import router as secrets_router
from flowfile_core.routes.shares import router as shares_router
from flowfile_core.routes.storage_browser import router as storage_browser_router
from flowfile_core.routes.system_backups import router as system_backups_router
from flowfile_core.routes.system_worker import router as system_worker_router
from flowfile_core.routes.telemetry import router as telemetry_router
from flowfile_core.routes.user_defined_components import router as user_defined_components_router
from flowfile_core.routes.user_groups import router as user_groups_router
from flowfile_core.scheduler import FlowScheduler, get_scheduler, set_scheduler
from shared.parent_watcher import start_parent_death_watcher
from shared.run_completion import reap_orphaned_runs
from shared.run_logs import cleanup_old_logs
from shared.storage_config import storage

storage.cleanup_directories()

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

    # Mint the kernel<->core token before anything can spawn a CLI child: an
    # unstamped env makes the child mint its own and 401 on kernel callbacks.
    try:
        from flowfile_core.auth.jwt import get_internal_token

        get_internal_token()
    except ValueError as exc:
        logging.getLogger(__name__).warning("Internal service token unavailable: %s", exc)

    # Runs whose process died with the last app instance would otherwise stay "active"
    # forever and block every relaunch. Best-effort; must never fail startup.
    try:
        reaped = reap_orphaned_runs()
        if reaped:
            print(f"Reaped {reaped} orphaned flow run(s)")
    except Exception:
        logging.getLogger(__name__).exception("Startup orphaned-run reap failed")

    try:
        removed = cleanup_old_logs()
        if removed:
            print(f"Removed {removed} expired log file(s)")
    except Exception:
        logging.getLogger(__name__).exception("Startup log retention sweep failed")

    publish("app_started")

    # Only auto-start scheduler if explicitly opted in via env var
    if os.environ.get("FLOWFILE_SCHEDULER_ENABLED", "").lower() in ("true", "1", "yes"):
        scheduler = FlowScheduler()
        await scheduler.start()
        set_scheduler(scheduler)
        print("Flow scheduler started")

    # Warm the kernel manager off the request path: its first construction makes
    # slow Docker daemon calls that would otherwise land on the first /kernels/.
    if os.environ.get("FLOWFILE_KERNEL_WARMUP", "1").lower() in ("true", "1", "yes"):
        threading.Thread(target=_warm_kernel_manager, name="kernel-manager-warmup", daemon=True).start()

    try:
        yield
    finally:
        print("Shutting down core application...")
        change_feed.close_all()

        # Stop scheduler
        scheduler = get_scheduler()
        if scheduler is not None:
            await scheduler.stop()
            set_scheduler(None)
            print("Flow scheduler stopped")

        print("Cleaning up core service resources...")
        _shutdown_kernels()
        _shutdown_local_model()
        await asyncio.sleep(0.1)  # Give a moment for cleanup


def _warm_kernel_manager():
    """Best-effort kernel-manager construction; Docker-absent machines skip quietly."""
    try:
        from flowfile_core.kernel import get_kernel_manager

        get_kernel_manager()
        print("Kernel manager warmed up")
    except Exception as exc:
        print(f"Kernel manager warm-up skipped: {exc}")


def _shutdown_kernels():
    """Stop all running kernel containers during shutdown."""
    try:
        from flowfile_core.kernel import get_kernel_manager_if_initialized

        manager = get_kernel_manager_if_initialized()
        if manager is not None:
            manager.shutdown_all()
    except Exception as exc:
        print(f"Error shutting down kernels: {exc}")


def _shutdown_local_model():
    """Stop the optional local LLM server (if running) during shutdown."""
    try:
        from flowfile_core.ai.local_model import manager as local_model_manager

        local_model_manager.stop()
    except Exception as exc:
        print(f"Error stopping local model: {exc}")


app = FastAPI(
    title="Flowfile Backend",
    version="0.1",
    description="Backend for the Flowfile application",
    lifespan=shutdown_handler,
)

# The Tauri 2 desktop shell loads the renderer from a custom protocol — the
# exact origin differs per OS (`tauri://localhost` on macOS/iOS, `http://tauri.localhost`
# on Linux, `https://tauri.localhost` on Windows/Android). A regex covers all
# of them without us having to enumerate. The explicit list below stays for
# the web/Docker/dev flows that hit the backend over plain HTTP.
origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:4173",
    "http://localhost:4174",
    "http://localhost:63578",
    "http://127.0.0.1:63578",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"^(tauri|http|https)://(tauri\.localhost|localhost(:\d+)?)$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(change_feed.ClientOriginMiddleware)

app.include_router(public_router)
app.include_router(router)
app.include_router(catalog_router)
# Flow-as-API: public key-authenticated data endpoint + JWT management endpoints.
app.include_router(flow_api_data_router)
app.include_router(flow_api_management_router)
app.include_router(api_consumers_router)
app.include_router(artifacts_router)
app.include_router(ml_router)
app.include_router(logs_router, tags=["logs"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
# Group-based sharing (multi-user mode only; both routers 404 in electron mode).
app.include_router(user_groups_router, prefix="/user-groups", tags=["user-groups"])
app.include_router(shares_router, prefix="/shares", tags=["shares"])
app.include_router(secrets_router, prefix="/secrets", tags=["secrets"])
app.include_router(notifications_router, prefix="/notifications", tags=["notifications"])
app.include_router(project_router, prefix="/project", tags=["project"])
app.include_router(cloud_connections_router, prefix="/cloud_connections", tags=["cloud_connections"])
app.include_router(storage_browser_router, prefix="/storage_browser", tags=["storage_browser"])
app.include_router(cloud_delta_router, prefix="/cloud_storage/delta", tags=["cloud_delta"])
app.include_router(ga_connections_router, prefix="/ga_connections", tags=["ga_connections"])
app.include_router(kafka_router)
app.include_router(user_defined_components_router, prefix="/user_defined_components", tags=["user_defined_components"])
app.include_router(custom_node_mounts_router, prefix="/custom-node-mounts", tags=["custom_node_mounts"])
app.include_router(community_nodes_router, prefix="/community_nodes", tags=["community_nodes"])
app.include_router(community_github_router, prefix="/community_nodes/github", tags=["community_nodes"])
app.include_router(kernel_router, tags=["kernels"])
app.include_router(lsp_router, tags=["lsp"])
app.include_router(file_manager_router, prefix="/file_manager", tags=["file_manager"])
app.include_router(converters_router, prefix="/converters", tags=["converters"])
app.include_router(node_requests_router, prefix="/node_requests", tags=["node_requests"])
app.include_router(notebook_router, prefix="/notebook", tags=["notebook"])
install_notebook_runner()
app.include_router(telemetry_router)
app.include_router(ai_router, prefix="/ai", tags=["ai"])
# Feature-flag admin endpoints. Mounted on /system (NOT /ai or /lsp) so admins can flip
# a gate from the UI without first satisfying the gate they're trying to flip.
app.include_router(ai_admin_router, prefix="/system", tags=["system"])
app.include_router(lsp_admin_router, prefix="/system", tags=["system"])
app.include_router(system_worker_router, prefix="/system", tags=["system"])
app.include_router(system_backups_router, prefix="/system", tags=["system"])

core_telemetry.install(app)


@app.post("/shutdown")
async def shutdown(background_tasks: BackgroundTasks):
    """An API endpoint to gracefully shut down the server.

    This endpoint sets a flag that the Uvicorn server checks, allowing it
    to terminate cleanly. A background task is used to trigger the shutdown
    after the HTTP response has been sent.
    """
    background_tasks.add_task(trigger_shutdown)
    return {"message": "Server is shutting down"}


async def trigger_shutdown():
    """(Internal) Triggers the actual server shutdown.

    Waits for a moment to allow the `/shutdown` response to be sent before
    telling the Uvicorn server instance to exit.
    """
    await asyncio.sleep(1)
    if server_instance:
        _stop_server(server_instance)


def _stop_server(server: uvicorn.Server) -> None:
    """Ask uvicorn to exit. Open event streams are closed first: uvicorn waits for every
    connection before it runs the lifespan shutdown, and a stream would otherwise hold it."""
    change_feed.close_all()
    server.should_exit = True


class _Server(uvicorn.Server):
    """uvicorn handles SIGINT/SIGTERM itself while serving (``capture_signals``), so the streams
    are closed from its handler; ``signal_handler`` below only sees a signal outside ``serve``."""

    def handle_exit(self, sig, frame) -> None:
        change_feed.close_all()
        super().handle_exit(sig, frame)


def signal_handler(signum, frame):
    """Handles OS signals like SIGINT (Ctrl+C) and SIGTERM for graceful shutdown."""
    print(f"Received signal {signum}")
    if server_instance:
        _stop_server(server_instance)


def run(host: str = None, port: int = None):
    """Runs the FastAPI application using Uvicorn.

    This function configures and starts the Uvicorn server, setting up
    signal handlers to ensure a graceful shutdown.

    Args:
        host: The host to bind the server to. Defaults to `SERVER_HOST` from settings.
        port: The port to bind the server to. Defaults to `SERVER_PORT` from settings.
    """
    global server_instance

    if host is None:
        host = SERVER_HOST
    if port is None:
        port = SERVER_PORT
    print(f"Starting server on {host}:{port}")
    print(f"Worker configured at {WORKER_URL} (host: {WORKER_HOST}, port: {WORKER_PORT})")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        loop="asyncio",
    )
    install_access_log_redaction()
    server = _Server(config)
    server_instance = server

    # In desktop-sidecar mode, exit if the Tauri shell dies without reaping us.
    start_parent_death_watcher(lambda: _stop_server(server))

    print("Starting core server...")
    print("Core server started")

    try:
        server.run()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
    finally:
        server_instance = None
        print("Server has shut down.")


if __name__ == "__main__":
    run()
