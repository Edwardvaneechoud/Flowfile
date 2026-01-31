import logging

from fastapi import APIRouter, HTTPException

from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult, KernelConfig, KernelInfo

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


router = APIRouter(prefix="/kernels")


@router.get("/", response_model=list[KernelInfo])
async def list_kernels():
    return await _get_manager().list_kernels()


@router.post("/", response_model=KernelInfo)
async def create_kernel(config: KernelConfig):
    try:
        return await _get_manager().create_kernel(config)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{kernel_id}", response_model=KernelInfo)
async def get_kernel(kernel_id: str):
    kernel = await _get_manager().get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
    return kernel


@router.delete("/{kernel_id}")
async def delete_kernel(kernel_id: str):
    try:
        await _get_manager().delete_kernel(kernel_id)
        return {"status": "deleted", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{kernel_id}/start", response_model=KernelInfo)
async def start_kernel(kernel_id: str):
    try:
        return await _get_manager().start_kernel(kernel_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{kernel_id}/stop")
async def stop_kernel(kernel_id: str):
    try:
        await _get_manager().stop_kernel(kernel_id)
        return {"status": "stopped", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{kernel_id}/execute", response_model=ExecuteResult)
async def execute_code(kernel_id: str, request: ExecuteRequest):
    try:
        return await _get_manager().execute(kernel_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{kernel_id}/artifacts")
async def get_artifacts(kernel_id: str):
    manager = _get_manager()
    kernel = await manager.get_kernel(kernel_id)
    if kernel is None:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found")
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
async def clear_artifacts(kernel_id: str):
    try:
        await _get_manager().clear_artifacts(kernel_id)
        return {"status": "cleared", "kernel_id": kernel_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
