"""FastAPI router for ``/ai/*`` endpoints.

W10 (this skeleton) registers a single ``GET /ai/health`` placeholder so the
router exists and can be mounted on ``main.py``. Real endpoints land per
workstream — chat stream (W13), suggest_next_node (W31/W32), agent start /
resume / abort (W40/W42), diff accept / reject (W41), provider list / test
(W11/W12).

Per W17, the entire router will be gated behind ``FEATURE_FLAG_AI`` via a
``Depends(require_ai_enabled)`` dependency once that workstream lands.
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def ai_health() -> dict[str, str]:
    """Liveness probe for the AI subsystem.

    Mounted at ``/ai/health`` via the ``prefix="/ai"`` on
    ``app.include_router``. Returns ``{"status": "skeleton"}`` while only W10
    has shipped; subsequent workstreams should not change this contract.
    """
    return {"status": "skeleton"}
