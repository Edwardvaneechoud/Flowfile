from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import signal
import asyncio
from contextlib import asynccontextmanager
from flowfile_core.routes import router, register_flow
from flowfile_core.schemas import schemas

# Global shutdown flag and server reference
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
        await asyncio.sleep(0.1)  # Give a moment for cleanup


# Initialize FastAPI with metadata
app = FastAPI(
    title='FlowFile Backend',
    version='0.1',
    description='Backend for the FlowFile application',
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

# Register the default flow
# register_flow(schemas.FlowSettings(flow_id=1))


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