"""FastAPI router for ``/ai/*`` endpoints.

Aggregates every per-surface sub-router (chat, autocomplete, agent,
diff, BYOK credentials, etc.) under a single mount point.

:func:`flowfile_core.ai.feature_flag.require_ai_enabled` is wired as
a router-level dependency, so every endpoint on ``ai_router``
(including the nested ``byok_router``) returns
``503 Service Unavailable`` whenever ``FEATURE_FLAG_AI`` is off.
FastAPI's ``include_router`` re-registers child routes through the
parent's ``add_api_route``, which prepends the parent's constructor
``dependencies`` — so the gate covers BYOK routes and any future
sub-router automatically.
"""

from fastapi import APIRouter, Depends

from flowfile_core.ai.agent_routes import router as agent_router
from flowfile_core.ai.autocomplete_routes import router as autocomplete_router
from flowfile_core.ai.byok_routes import router as byok_router
from flowfile_core.ai.chat_routes import router as chat_router
from flowfile_core.ai.command_palette_routes import router as command_palette_router
from flowfile_core.ai.cron_routes import router as cron_router
from flowfile_core.ai.diff_routes import router as diff_router
from flowfile_core.ai.docgen_routes import router as docgen_router
from flowfile_core.ai.feature_flag import require_ai_enabled
from flowfile_core.ai.generate_routes import router as generate_router
from flowfile_core.ai.inline_action_routes import router as inline_action_router
from flowfile_core.ai.intent_router_routes import router as intent_router
from flowfile_core.ai.lineage_routes import router as lineage_router
from flowfile_core.ai.local_model_routes import router as local_model_router
from flowfile_core.ai.run_failure_routes import router as run_failure_router
from flowfile_core.ai.suggest_next_node_routes import router as suggest_next_node_router

router = APIRouter(dependencies=[Depends(require_ai_enabled)])
router.include_router(byok_router)
router.include_router(chat_router)
router.include_router(run_failure_router)
router.include_router(autocomplete_router)
router.include_router(cron_router)
router.include_router(docgen_router)
router.include_router(diff_router)
router.include_router(suggest_next_node_router)
router.include_router(inline_action_router)
router.include_router(lineage_router)
router.include_router(command_palette_router)
router.include_router(agent_router)
router.include_router(intent_router)
router.include_router(local_model_router)
router.include_router(generate_router)


@router.get("/health")
async def ai_health() -> dict[str, str]:
    """Liveness probe for the AI subsystem.

    Mounted at ``/ai/health`` via the ``prefix="/ai"`` on
    ``app.include_router``. Returns ``{"status": "skeleton"}``.

    Returns ``503`` (with ``detail`` from
    :data:`flowfile_core.ai.feature_flag.DISABLED_DETAIL`) when
    ``FEATURE_FLAG_AI`` is off — gating is enforced by the router-level
    dependency, not per-route logic.
    """
    return {"status": "skeleton"}
