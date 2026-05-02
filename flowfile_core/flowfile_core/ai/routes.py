"""FastAPI router for ``/ai/*`` endpoints.

W10 (this skeleton) registers a single ``GET /ai/health`` placeholder so the
router exists and can be mounted on ``main.py``. Real endpoints land per
workstream — chat stream (W13), suggest_next_node (W31/W32), agent start /
resume / abort (W40/W42), diff accept / reject (W41), provider list / test
(W12 — shipped via :mod:`flowfile_core.ai.byok_routes`).

W17 wires :func:`flowfile_core.ai.feature_flag.require_ai_enabled` as a
router-level dependency, so every endpoint on ``ai_router`` (including the
nested ``byok_router``) returns ``503 Service Unavailable`` whenever
``FEATURE_FLAG_AI`` is off. FastAPI's ``include_router`` re-registers child
routes through the parent's ``add_api_route``, which prepends the parent's
constructor ``dependencies`` — so the gate covers W12's BYOK routes today
and any future sub-router automatically.
"""

from fastapi import APIRouter, Depends

from flowfile_core.ai.autocomplete_routes import router as autocomplete_router
from flowfile_core.ai.byok_routes import router as byok_router
from flowfile_core.ai.chat_routes import router as chat_router
from flowfile_core.ai.feature_flag import require_ai_enabled
from flowfile_core.ai.run_failure_routes import router as run_failure_router

router = APIRouter(dependencies=[Depends(require_ai_enabled)])
router.include_router(byok_router)
router.include_router(chat_router)
router.include_router(run_failure_router)
router.include_router(autocomplete_router)


@router.get("/health")
async def ai_health() -> dict[str, str]:
    """Liveness probe for the AI subsystem.

    Mounted at ``/ai/health`` via the ``prefix="/ai"`` on
    ``app.include_router``. Returns ``{"status": "skeleton"}`` while only W10
    has shipped; subsequent workstreams should not change this contract.

    Returns ``503`` (with ``detail`` from
    :data:`flowfile_core.ai.feature_flag.DISABLED_DETAIL`) when
    ``FEATURE_FLAG_AI`` is off — gating is enforced by the router-level
    dependency, not per-route logic.
    """
    return {"status": "skeleton"}
