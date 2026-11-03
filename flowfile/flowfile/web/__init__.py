"""
flowfile/web/__init__.py
Web interface for Flowfile.
Extends the flowfile_core FastAPI app to serve the Vue.js frontend
and includes worker functionality.
"""

import asyncio
import os
import time
import webbrowser
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

static_dir = Path(__file__).parent / "static"


class StripApiPrefixMiddleware(BaseHTTPMiddleware):
    """Middleware that strips the /api prefix from incoming requests.

    In unified mode (flowfile run ui), the frontend uses /api/ as its base URL
    (intended for nginx reverse proxy in Docker). This middleware makes those
    requests work by stripping the /api prefix so they match the backend routes.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.scope["path"]
        if path.startswith("/api/") or path == "/api":
            request.scope["path"] = path[4:] or "/"
        return await call_next(request)


def extend_app(app: FastAPI):
    """
    Extend the flowfile_core FastAPI app with routes to serve the Vue.js frontend
    and worker functionality.
    """
    app.add_middleware(StripApiPrefixMiddleware)

    if (static_dir / "index.html").is_file():
        app.state.editor_path = "/ui"

    if static_dir.exists():
        if (static_dir / "assets").exists():
            app.mount("/assets", StaticFiles(directory=str(static_dir / "assets")), name="assets")

        for dir_name in ["css", "js", "img", "fonts", "icons", "images"]:
            dir_path = static_dir / dir_name
            if dir_path.exists() and dir_path.is_dir():
                app.mount(f"/{dir_name}", StaticFiles(directory=str(dir_path)), name=dir_name)

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        """Serve the favicon.ico file"""
        favicon_path = static_dir / "favicon.ico"
        if favicon_path.exists():
            return FileResponse(favicon_path)
        return Response(status_code=404)

    @app.get("/flowfile.svg", include_in_schema=False)
    async def svg_logo():
        """Serve the SVG logo file"""
        svg_path = static_dir / "flowfile.svg"
        if svg_path.exists():
            return FileResponse(svg_path, media_type="image/svg+xml")
        return Response(status_code=404)

    @app.get("/single_mode")
    async def in_single_mode() -> bool:
        from flowfile_core.configs.settings import SINGLE_FILE_MODE

        return bool(SINGLE_FILE_MODE)

    @app.get("/ui", include_in_schema=False)
    async def web_ui_root():
        """Serve the main index.html file for the web UI"""
        index_path = static_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
        return {"error": "Web UI not installed. Build the frontend and install it in the package."}

    @app.get("/ui/{path:path}", include_in_schema=False)
    async def serve_vue_app(path: str):
        """Serve a file of the built editor, else its index.html for client-side routing, ``/ui/`` included.

        Only files inside the editor's folder are served: a path that leaves it (``/ui//etc/hosts``, ``..``) gets
        the index.html like any other route.
        """
        root = static_dir.resolve()
        file_path = (root / path).resolve()
        if file_path.is_file() and file_path.is_relative_to(root):
            return FileResponse(file_path)

        index_path = static_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)

        return {"error": f"File not found: {path}"}

    include_worker_routes(app)

    return app


def include_worker_routes(app: FastAPI):
    """
    Include worker routes from flowfile_worker for simplified deployments.
    This creates a unified API that serves both the web UI and processes the worker operations.

    Under ``/worker`` it mounts the legacy REST routes, the ``/v1`` routes and ``/v1/ws/submit``, each
    behind its own token and version checks. The legacy ``/ws/submit`` is not mounted.

    The mounted worker runs in core's process and sees core's files, so this sets
    ``FLOWFILE_WORKER_LOCAL_FILES`` and the worker declares the ``file`` scheme.

    Core's own compute does not go through the mount: in single-file mode the registry's ``worker`` is
    that same worker, called directly. The mount serves callers that still post to a worker URL
    (``registry.worker_url()``).
    """
    try:
        from flowfile_worker import CACHE_DIR, mp_context
        from flowfile_worker.protocol_routes import router as protocol_router
        from flowfile_worker.routes import router as worker_router
        from flowfile_worker.streaming import v1_streaming_router

        @app.on_event("shutdown")
        async def shutdown_worker():
            """Clean up worker resources on shutdown"""
            print("Cleaning up worker resources...")
            for p in mp_context.active_children():
                try:
                    p.terminate()
                    p.join()
                except Exception as e:
                    print(f"Error cleaning up process: {e}")

            try:
                CACHE_DIR.cleanup()
            except Exception as e:
                print(f"Error cleaning up cache directory: {e}")

            await asyncio.sleep(0.1)

        app.include_router(worker_router, prefix="/worker")
        app.include_router(protocol_router, prefix="/worker")
        app.include_router(v1_streaming_router, prefix="/worker")
        app.state.worker_path = "/worker"
        os.environ["FLOWFILE_WORKER_LOCAL_FILES"] = "1"

        print("Worker functionality included in unified API")

    except ImportError as e:
        print(f"Worker module could not be imported, running without worker functionality: {e}")
        print("This is normal for lightweight deployments that don't need data processing.")


def start_server(host="127.0.0.1", port=63578, open_browser=True):
    """
    Start the flowfile_core FastAPI app with the web UI routes and worker functionality.
    This function is a wrapper around flowfile_core.main.run().

    It sets single-file mode: flows compute on the worker embedded in this process. ``run`` marks the
    process as serving, so the worker's jobs run in its pool or a child, never in the serving process.
    ``FLOWFILE_WORKER_POOL_SIZE`` sizes the pool. ``host`` and ``port`` are where it serves; a server bound to
    every address (``0.0.0.0``) is reached from this machine on the loopback address.
    """
    if "FLOWFILE_MODE" not in os.environ:
        os.environ["FLOWFILE_MODE"] = "electron"

    from flowfile_core.configs import settings
    from flowfile_core.main import app as core_app
    from flowfile_core.main import run

    local = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    settings.OFFLOAD_TO_WORKER.value = True
    settings.SINGLE_FILE_MODE.value = True
    settings.WORKER_URL = f"http://{local}:{port}/worker"

    extend_app(core_app)

    if open_browser:
        time.sleep(5)
        webbrowser.open_new_tab(f"http://{local}:{port}/ui")

    print("\n" + "=" * 60)
    print("    FlowFile - Visual ETL Tool (Unified Mode)")
    print(f"    Web UI: http://{local}:{port}/ui")
    print(f"    API Docs: http://{local}:{port}/docs")
    print("=" * 60 + "\n")

    run(host=host, port=port)
