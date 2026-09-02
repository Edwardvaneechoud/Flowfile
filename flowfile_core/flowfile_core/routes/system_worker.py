"""Admin proxy for the worker's warm process pool.

Mounted under ``/system`` in :mod:`flowfile_core.main`, next to the AI
feature-flag admin route: admin-only, live runtime state. Unlike the AI
flag, a resize IS persisted — the worker saves it to ``worker_pool.yaml``
under its internal storage dir, so UI changes survive restarts. The
``FLOWFILE_WORKER_POOL_SIZE`` env var remains an explicit operator
override that beats the saved value at boot; ``env_override`` in the
response tells the UI when that is the case.

The frontend never talks to the worker directly (it only knows core), so
core forwards to the worker's ``GET/POST /pool``. A worker that is down or
unreachable surfaces as 503 ``WORKER_UNAVAILABLE`` — never 401, which the
frontend axios interceptor would treat as JWT expiry.
"""

from __future__ import annotations

import requests
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import _worker_session

router = APIRouter()

_TIMEOUT = (2.0, 10.0)


class WorkerPoolInput(BaseModel):
    """Request body for ``POST /system/worker_pool``."""

    size: int = Field(..., ge=0, le=32, description="Target warm-member capacity; 0 disables the pool.")


class WorkerPoolMember(BaseModel):
    """One live pool member, for the dashboard's member table."""

    pid: int
    state: str
    tasks_served: int
    idle_seconds: float
    rss_mb: float | None = None


class WorkerPoolState(BaseModel):
    """Response shape for the worker-pool admin endpoints (mirrors the worker's /pool)."""

    enabled: bool
    size: int
    idle: int
    busy: int
    active_tasks: int
    tasks_completed: int
    members: list[WorkerPoolMember]
    max_tasks_per_member: int
    idle_ttl_seconds: float
    rss_limit_mb: int
    platform_default_size: int
    env_override: bool = Field(
        description="FLOWFILE_WORKER_POOL_SIZE is set and will beat the saved setting at the next boot."
    )
    persisted: bool = Field(description="The current size is saved on the worker and will govern the next boot.")


def _forward(method: str, json_body: dict | None = None) -> WorkerPoolState:
    try:
        response = _worker_session.request(method, f"{WORKER_URL}/pool", json=json_body, timeout=_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "WORKER_UNAVAILABLE", "message": f"Worker not reachable: {exc}"},
        ) from exc
    return WorkerPoolState(**response.json())


@router.get("/worker_pool", response_model=WorkerPoolState, tags=["system"])
def get_worker_pool(_current_user: User = Depends(get_current_admin_user)) -> WorkerPoolState:
    """Live warm-pool state of the connected worker. Admin-only, like the AI flag."""
    return _forward("GET")


@router.post("/worker_pool", response_model=WorkerPoolState, tags=["system"])
def set_worker_pool(
    payload: WorkerPoolInput,
    _current_user: User = Depends(get_current_admin_user),
) -> WorkerPoolState:
    """Resize the worker's warm pool at runtime (0 disables) and persist it.

    Applies immediately to the running worker process (shrinking retires excess
    idle members now, busy ones at their next checkin) and is saved on the worker
    so it survives restarts. A set FLOWFILE_WORKER_POOL_SIZE still wins at boot.
    """
    return _forward("POST", {"size": payload.size})
