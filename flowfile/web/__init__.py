"""
flowfile/web/__init__.py
Web interface for Flowfile.
Extends the flowfile_core FastAPI app to serve the Vue.js frontend
and includes worker functionality.
"""
import os
import time
from pathlib import Path
import webbrowser
import asyncio
from fastapi import FastAPI, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

static_dir = Path(__file__).parent / "static"


def extend_app(app: FastAPI):
    """
    Extend the flowfile_core FastAPI app with routes to serve the Vue.js frontend
    and worker functionality.
    """
    # Serve static files if the directory exists
    if static_dir.exists():
        # Mount the assets directory
        if (static_dir / "assets").exists():
            app.mount("/assets", StaticFiles(directory=str(static_dir / "assets")), name="assets")

        # Mount other common directories
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
    async def in_single_mode():
        return os.environ.get('SINGLE_FILE_MODE', "0")

    @app.get("/ui", include_in_schema=False)
    async def web_ui_root():
        """Serve the main index.html file for the web UI"""
        index_path = static_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
        return {"error": "Web UI not installed. Build the frontend and install it in the package."}

    @app.get("/ui/{path:path}", include_in_schema=False)
    async def serve_vue_app(path: str):
        """Serve static files or the index.html for client-side routing"""
        # Try to serve the requested file
        file_path = static_dir / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)

        # If it's a directory, redirect to add trailing slash
        if (static_dir / path).exists() and (static_dir / path).is_dir():
            return RedirectResponse(f"/ui/{path}/")

        # For client-side routing, serve the index.html
        index_path = static_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)

        return {"error": f"File not found: {path}"}

    # Include worker routes if simplified mode is enabled
    include_worker_routes(app)

    return app


def include_worker_routes(app: FastAPI):
    """
    Include worker routes from flowfile_worker for simplified deployments.
    This creates a unified API that serves both the web UI and processes the worker operations.
    """
    try:
        # Import worker modules
        from flowfile_worker.routes import router as worker_router
        from flowfile_worker import mp_context, CACHE_DIR

        # Add lifecycle event handler for worker cleanup
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

        # Include the worker router with a prefix
        app.include_router(worker_router, prefix="/worker")

        print("Worker functionality included in unified API")

    except ImportError as e:
        print(f"Worker module could not be imported, running without worker functionality: {e}")
        print("This is normal for lightweight deployments that don't need data processing.")


def start_server(host="127.0.0.1", port=63578, open_browser=True):
    """
    Start the flowfile_core FastAPI app with the web UI routes and worker functionality.
    This function is a wrapper around flowfile_core.main.run().
    """
    # Set electron mode
    os.environ["FLOWFILE_MODE"] = "electron"

    # Import core app
    from flowfile_core.main import run, app as core_app

    # Extend the core app with web UI routes and worker functionality
    extend_app(core_app)

    # Open browser if requested
    if open_browser:
        time.sleep(5)
        webbrowser.open_new_tab(f"http://{host}:{port}/ui")

    print("\n" + "=" * 60)
    print("    FlowFile - Visual ETL Tool (Unified Mode)")
    print(f"    Web UI: http://{host}:{port}/ui")
    print(f"    API Docs: http://{host}:{port}/docs")
    print("=" * 60 + "\n")

    # Run the core app
    run(host=host, port=port)
