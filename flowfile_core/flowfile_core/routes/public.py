from fastapi import APIRouter
from fastapi.responses import RedirectResponse

# Router setup
router = APIRouter()


@router.get("/", tags=['admin'])
async def docs_redirect():
    """ Redirects to the documentation page."""
    return RedirectResponse(url='/docs')
