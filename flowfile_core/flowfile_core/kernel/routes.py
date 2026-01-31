from fastapi import APIRouter, Depends, HTTPException

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult, KernelConfig, KernelInfo


def _get_manager():
    from flowfile_core.kernel import get_kernel_manager

    return get_kernel_manager()


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
