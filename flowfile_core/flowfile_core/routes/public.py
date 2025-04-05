from fastapi import APIRouter
from fastapi.responses import RedirectResponse

# Router setup
router = APIRouter()


@router.get("/", tags=['admin'])
async def docs_redirect():
    return RedirectResponse(url='/docs')
