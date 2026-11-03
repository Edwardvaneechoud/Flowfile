import asyncio
import ipaddress
import os
import signal
import socket
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from flowfile_worker import NODE_STORAGE_DIR, mp_context
from flowfile_worker.auth import carries_internal_token
from flowfile_worker.configs import (
    FLOWFILE_CORE_URI,
    HUB_URL,
    JOIN_TOKEN,
    NODE_NAME,
    NODE_URL,
    PORT_GIVEN,
    SERVICE_HOST,
    SERVICE_PORT,
    logger,
)
from flowfile_worker.pool import task_pool
from flowfile_worker.protocol_routes import router as protocol_router
from flowfile_worker.routes import router
from flowfile_worker.streaming import streaming_router, v1_streaming_router
from shared import hub_node
from shared._version import get_version
from shared.parent_watcher import start_parent_death_watcher
from shared.storage_config import storage

should_exit = False
server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handle application startup and shutdown.

    A worker that joined a hub alone, with keys of its own, tells the hub the node is up while it serves; beside a core
    that joined for both, the core does (``hub_node.start_heartbeat``).
    """
    logger.info("Starting application...")
    beating = hub_node.start_heartbeat("worker")
    task_pool.prewarm()
    try:
        yield
    finally:
        logger.info("Shutting down application...")
        if beating is not None:
            beating.set()
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


@app.get("/health")
async def health() -> dict[str, str]:
    """Say the worker is up and which Flowfile release it runs, to any caller, with or without a token.

    Nothing more: the host, Python, Polars and the operations are in ``/v1/capabilities``, behind a token.
    Core never calls this route, so it is not part of the worker contract.
    """
    return {"status": "ok", "version": get_version()}


def _supervising_shell(request: Request) -> bool:
    """Whether the caller can be the desktop shell that runs this worker: a loopback peer, when a shell supervises it.

    The shell sets ``FLOWFILE_SUPERVISOR_PID`` for its sidecars only. Uvicorn takes the peer from ``X-Forwarded-For``
    behind a trusted (loopback) proxy, so behind a same-host proxy that sends none every client reads as loopback.
    That is why no worker a shell does not supervise trusts the peer.
    """
    if not os.environ.get("FLOWFILE_SUPERVISOR_PID"):
        return False
    host = request.client.host if request.client else ""
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@app.post("/shutdown")
async def shutdown(request: Request):
    """Shut the worker down gracefully, for a caller with the internal token or for the desktop shell that runs it.

    The internal token is accepted from any peer; without it only the supervising shell is (``_supervising_shell``),
    which posts from the same host and holds no token. Anything else is 403.
    """
    if not (carries_internal_token(request) or _supervising_shell(request)):
        raise HTTPException(status_code=403, detail="Shutdown needs the internal token")
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


def _found_by_name() -> bool:
    """Whether this worker took the folder of the node it is named after, with no join token: the one beside a core."""
    return NODE_STORAGE_DIR is not None and not JOIN_TOKEN


def listen(host: str, port: int, *, given: bool = True) -> socket.socket:
    """The socket the worker serves on, bound before it joins a hub: a worker that cannot serve never spends a token.

    A port the operator ``given`` (``--port``, or ``run``'s), and any port of a worker without a join token: that port,
    else exit code 1. A worker that already joined with this token: the port it joined with, which the hub has, else
    exit code 1 saying so (``port`` when its file is from before the port was kept). A worker about to join: ``port``,
    else the port it last joined with, else a free one (``hub_node.listen``), which the join then reports. The worker
    beside a core, found by its name: the port that core's join gave for it, else exit code 1 saying so
    (``hub_node.serving_socket``).
    """
    return hub_node.serving_socket(
        "worker", host, port, given=given, hub_url=HUB_URL, token=JOIN_TOKEN, found=_found_by_name()
    )


def _worker_url(host: str, port: int, *, tls: bool) -> str:
    url = hub_node.node_url(HUB_URL, host, port, tls=tls)
    if hub_node.is_loopback_host(host):
        logger.warning(
            "This worker serves on %s, which only this machine reaches, so the address it gives the hub is %s; to be "
            "reached from other machines, start it with --host 0.0.0.0",
            host,
            url,
        )
    return url


def join_hub(port: int | None = None, host: str | None = None) -> None:
    """Join the hub ``--join`` names, before the worker serves on ``host`` and ``port``; without a join token, nothing.

    The worker asks for ``--name``, else the machine's name, and gives ``--url``, else the address the hub reaches it
    at (``hub_node.node_url``: on every address, this machine's address towards the hub; on one address, that one,
    loopback with a warning), ``https`` when it serves TLS, and the port it serves on. A worker that joined already
    makes no request (``shared.hub_node``).
    """
    if not JOIN_TOKEN:
        return
    port = port or SERVICE_PORT
    tls = bool(os.environ.get("FLOWFILE_WORKER_TLS_CERT"))
    worker_url = NODE_URL or _worker_url(host or SERVICE_HOST, port, tls=tls)
    if hub_node.already_joined(HUB_URL, JOIN_TOKEN) is None:
        logger.info("Telling the hub this worker is at %s", worker_url)
    hub_node.join_at_start(
        HUB_URL,
        JOIN_TOKEN,
        name=NODE_NAME or hub_node.default_name(),
        worker_url=worker_url,
        worker_port=port,
    )


def _say_found(port: int) -> None:
    joined = hub_node.read()
    folder = hub_node.home_relative(NODE_STORAGE_DIR)
    if joined is not None and joined.worker_port == port:
        logger.info("Found node %s in %s; serving on %s, the port its join gave the hub", joined.name, folder, port)
    else:
        logger.info("Found node %s in %s; serving on %s", NODE_STORAGE_DIR.name, folder, port)


def run(host: str = None, port: int = None):
    """Run the FastAPI app with graceful shutdown.

    The worker binds its socket first (``listen``), then a worker started with a join token joins its hub
    (``join_hub``) with the port it serves on, and serves on that same socket: one that cannot bind exits with code 1
    before any request to the hub, one the hub refuses with code 2 before it serves. The worker beside a core, started
    with the node's name and no join token, serves from the core's folder on the port the core's join gave for it.
    Serves TLS when ``FLOWFILE_WORKER_TLS_CERT`` and ``FLOWFILE_WORKER_TLS_KEY`` name a certificate and its key.
    """
    global server_instance

    given = port is not None or PORT_GIVEN
    if host is None:
        host = SERVICE_HOST
    if port is None:
        port = SERVICE_PORT

    if not _found_by_name():
        hub_node.log_storage(NODE_STORAGE_DIR)
    sock = listen(host, port, given=given)
    port = sock.getsockname()[1]
    if _found_by_name():
        _say_found(port)
    try:
        join_hub(port, host)
    except BaseException:
        sock.close()
        raise

    from connectorx import __version__ as connectorx_version

    logger.info(f"ConnectorX version: {connectorx_version}")
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
        server.run(sockets=[sock])
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        server_instance = None
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    run()
