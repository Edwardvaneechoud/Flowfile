
import asyncio
import os
import signal
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from flowfile_core import ServerRun
from flowfile_core.app_routes.auth import router as auth_router
from flowfile_core.app_routes.secrets import router as secrets_router
from flowfile_core.configs.flow_logger import clear_all_flow_logs
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import engine, SessionLocal
from flowfile_core.database.init_db import create_default_local_user
from flowfile_core.routes import router

os.environ["FLOWFILE_MODE"] = "electron"

db_models.Base.metadata.create_all(bind=engine)

db = SessionLocal()
try:
    create_default_local_user(db)
finally:
    db.close()

should_exit = False
server_instance = None


@asynccontextmanager
async def shutdown_handler(app: FastAPI):
    """Handle graceful shutdown of the application."""
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
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the router with all endpoints
app.include_router(router)
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(secrets_router, prefix="/secrets", tags=["secrets"])


@app.post("/shutdown")
async def shutdown():
    """Endpoint to handle graceful shutdown"""

    ServerRun.exit = True
    print(ServerRun.exit)
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
    print(f"Received signal {signum}")
    if server_instance:
        server_instance.should_exit = True


def run(host: str = '0.0.0.0', port: int = 63578):
    """Run the FastAPI app with graceful shutdown"""
    global server_instance

    # Setup signal handlers
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

    print('Starting core server...')
    print('Core server started')

    try:
        server.run()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
    finally:
        server_instance = None
        print("Core server shutdown complete")


if __name__ == "__main__":
    run()
