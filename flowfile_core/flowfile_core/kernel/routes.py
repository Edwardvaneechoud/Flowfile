from fastapi import APIRouter, HTTPException

from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult, KernelConfig, KernelInfo

router = APIRouter(prefix="/kernels")
manager = KernelManager()


@router.get("/", response_model=list[KernelInfo])
async def list_kernels():
    return await manager.list_kernels()


@router.post("/", response_model=KernelInfo)
async def create_kernel(config: KernelConfig):
    try:
        return await manager.create_kernel(config)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{kernel_id}", response_model=KernelInfo)
async def get_kernel(kernel_id: str):
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    return kernel


@router.delete("/{kernel_id}")
async def delete_kernel(kernel_id: str):
    try:
        await manager.delete_kernel(kernel_id)
        return {"status": "deleted", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{kernel_id}/start", response_model=KernelInfo)
async def start_kernel(kernel_id: str):
    try:
        return await manager.start_kernel(kernel_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{kernel_id}/stop")
async def stop_kernel(kernel_id: str):
    try:
        await manager.stop_kernel(kernel_id)
        return {"status": "stopped", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{kernel_id}/execute", response_model=ExecuteResult)
async def execute_code(kernel_id: str, request: ExecuteRequest):
    try:
        return await manager.execute(kernel_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{kernel_id}/artifacts")
async def get_artifacts(kernel_id: str):
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    if kernel.state.value != "running":
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
async def clear_artifacts(kernel_id: str):
    try:
        await manager.clear_artifacts(kernel_id)
        return {"status": "cleared", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
