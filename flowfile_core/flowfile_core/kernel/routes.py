import logging

from fastapi import APIRouter, Depends, HTTPException

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.kernel.models import (
    CleanupRequest,
    CleanupResult,
    DockerStatus,
    ExecuteRequest,
    ExecuteResult,
    KernelConfig,
    KernelInfo,
    PersistenceInfo,
    RecoveryStatus,
)

logger = logging.getLogger(__name__)


def _get_manager():
    from flowfile_core.kernel import get_kernel_manager

    try:
        return get_kernel_manager()
    except Exception as exc:
        logger.error("Kernel manager unavailable: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Docker is not available. Please ensure Docker is installed and running.",
        )


router = APIRouter(prefix="/kernels", dependencies=[Depends(get_current_active_user)])


@router.get("/", response_model=list[KernelInfo])
async def list_kernels(current_user=Depends(get_current_active_user)):
    return await _get_manager().list_kernels(user_id=current_user.id)


@router.post("/", response_model=KernelInfo)
async def create_kernel(config: KernelConfig, current_user=Depends(get_current_active_user)):
    try:
        return await _get_manager().create_kernel(config, user_id=current_user.id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/docker-status", response_model=DockerStatus)
async def docker_status():
    """Check if Docker is reachable and the kernel image is available."""
    import docker as _docker

    try:
        client = _docker.from_env()
        client.ping()
    except Exception as exc:
        return DockerStatus(available=False, image_available=False, error=str(exc))

    from flowfile_core.kernel.manager import _KERNEL_IMAGE

    try:
        client.images.get(_KERNEL_IMAGE)
        image_available = True
    except _docker.errors.ImageNotFound:
        image_available = False
    except Exception:
        image_available = False

    return DockerStatus(available=True, image_available=image_available)


@router.get("/{kernel_id}", response_model=KernelInfo)
async def get_kernel(kernel_id: str, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    return kernel


@router.delete("/{kernel_id}")
async def delete_kernel(kernel_id: str, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        await manager.delete_kernel(kernel_id)
        return {"status": "deleted", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{kernel_id}/start", response_model=KernelInfo)
async def start_kernel(kernel_id: str, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        return await manager.start_kernel(kernel_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{kernel_id}/stop")
async def stop_kernel(kernel_id: str, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        await manager.stop_kernel(kernel_id)
        return {"status": "stopped", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{kernel_id}/execute", response_model=ExecuteResult)
async def execute_code(kernel_id: str, request: ExecuteRequest, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        return await manager.execute(kernel_id, request)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{kernel_id}/artifacts")
async def get_artifacts(kernel_id: str, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    if kernel.state.value not in ("idle", "executing"):
        raise HTTPException(status_code=400, detail=f"Kernel '{kernel_id}' is not running")

    try:
        import httpx

        url = f"http://localhost:{kernel.port}/artifacts"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{kernel_id}/clear")
async def clear_artifacts(kernel_id: str, current_user=Depends(get_current_active_user)):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        await manager.clear_artifacts(kernel_id)
        return {"status": "cleared", "kernel_id": kernel_id}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ------------------------------------------------------------------
# Recovery & persistence endpoints
# ------------------------------------------------------------------


@router.post("/{kernel_id}/recover", response_model=RecoveryStatus)
async def recover_artifacts(kernel_id: str, current_user=Depends(get_current_active_user)):
    """Trigger artifact recovery from persisted storage."""
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        result = await manager.recover_artifacts(kernel_id)
        return RecoveryStatus(
            mode=result.get("mode", "unknown"),
            status=result.get("status", "completed"),
            recovered_artifacts=result.get("recovered_artifacts", []),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{kernel_id}/recovery-status", response_model=RecoveryStatus)
async def get_recovery_status(kernel_id: str, current_user=Depends(get_current_active_user)):
    """Get the current recovery status for a kernel."""
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        result = await manager.get_recovery_status(kernel_id)
        return RecoveryStatus(
            mode=result.get("mode", "unknown"),
            status=result.get("status", "unknown"),
            recovered_artifacts=result.get("recovered_artifacts", []),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{kernel_id}/cleanup", response_model=CleanupResult)
async def cleanup_artifacts(kernel_id: str, request: CleanupRequest | None = None, current_user=Depends(get_current_active_user)):
    """Clean up old persisted artifacts."""
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    max_age_hours = request.max_age_hours if request else 24
    try:
        result = await manager.cleanup_artifacts(kernel_id, max_age_hours)
        return CleanupResult(
            removed_artifacts=result.get("removed_artifacts", []),
            remaining_count=result.get("remaining_count", 0),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{kernel_id}/persistence", response_model=PersistenceInfo)
async def get_persistence_info(kernel_id: str, current_user=Depends(get_current_active_user)):
    """Get persistence statistics for a kernel."""
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        result = await manager.get_persistence_info(kernel_id)
        return PersistenceInfo(**result)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
