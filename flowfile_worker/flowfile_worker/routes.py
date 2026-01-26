# flowfile_worker/flowfile_worker/routes.py
"""
Minimal REST routes for worker health checks and diagnostics.
All worker-core communication is now handled via gRPC.
"""

from fastapi import APIRouter

from flowfile_worker import status_dict
from flowfile_worker.configs import logger

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "flowfile-worker"}


@router.get("/stats")
async def get_stats():
    """Get worker statistics for monitoring."""
    task_count = len(status_dict)
    task_statuses = {}
    for status in status_dict.values():
        task_statuses[status.status] = task_statuses.get(status.status, 0) + 1

    return {
        "total_tasks": task_count,
        "task_statuses": task_statuses,
    }
