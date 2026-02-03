import logging

from fastapi import APIRouter, Depends, HTTPException

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.kernel.models import (
    ClearNodeArtifactsRequest,
    ClearNodeArtifactsResult,
    DockerStatus,
    ExecuteRequest,
    ExecuteResult,
    KernelConfig,
    KernelInfo,
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


@router.post("/{kernel_id}/execute_cell", response_model=ExecuteResult)
async def execute_cell(kernel_id: str, request: ExecuteRequest, current_user=Depends(get_current_active_user)):
    """Execute a single notebook cell interactively.

    Same as /execute but sets interactive=True to enable auto-display of the last expression.
    """
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        # Force interactive mode for cell execution
        request.interactive = True
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


@router.post("/{kernel_id}/clear_node_artifacts", response_model=ClearNodeArtifactsResult)
async def clear_node_artifacts(
    kernel_id: str,
    request: ClearNodeArtifactsRequest,
    current_user=Depends(get_current_active_user),
):
    """Clear only artifacts published by specific node IDs."""
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    try:
        return await manager.clear_node_artifacts(kernel_id, request.node_ids, flow_id=request.flow_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{kernel_id}/artifacts/node/{node_id}")
async def get_node_artifacts(
    kernel_id: str,
    node_id: int,
    current_user=Depends(get_current_active_user),
):
    """Get artifacts published by a specific node."""
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to access this kernel")
    if kernel.state.value not in ("idle", "executing"):
        raise HTTPException(status_code=400, detail=f"Kernel '{kernel_id}' is not running")
    try:
        return await manager.get_node_artifacts(kernel_id, node_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
