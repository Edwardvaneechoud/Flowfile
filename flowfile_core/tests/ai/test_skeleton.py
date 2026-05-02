"""W10 — skeleton smoke test.

Asserts that:
* every public submodule of ``flowfile_core.ai`` imports without error
  (canary for circular imports introduced by later workstreams);
* the ``/ai/health`` placeholder route is registered on the FastAPI app
  via the ``ai_router`` mounted in ``main.py``.
"""

import importlib

import pytest

AI_SUBMODULES = [
    "flowfile_core.ai",
    "flowfile_core.ai.routes",
    "flowfile_core.ai.streaming",
    "flowfile_core.ai.scheduler",
    "flowfile_core.ai.sessions",
    "flowfile_core.ai.diff",
    "flowfile_core.ai.safety",
    "flowfile_core.ai.metrics",
    "flowfile_core.ai.providers",
    "flowfile_core.ai.providers.base",
    "flowfile_core.ai.providers.anthropic",
    "flowfile_core.ai.providers.openai",
    "flowfile_core.ai.providers.google",
    "flowfile_core.ai.providers.groq",
    "flowfile_core.ai.providers.openrouter",
    "flowfile_core.ai.providers.ollama",
    "flowfile_core.ai.providers.registry",
    "flowfile_core.ai.tools",
    "flowfile_core.ai.tools.registry",
    "flowfile_core.ai.tools.graph_ops",
    "flowfile_core.ai.tools.schema_ops",
    "flowfile_core.ai.tools.codegen_ops",
    "flowfile_core.ai.tools.executor",
    "flowfile_core.ai.context",
    "flowfile_core.ai.context.builder",
    "flowfile_core.ai.context.mentions",
    "flowfile_core.ai.context.budget",
    "flowfile_core.ai.agents",
    "flowfile_core.ai.agents.assist",
    "flowfile_core.ai.agents.copilot",
    "flowfile_core.ai.agents.planner",
]


@pytest.mark.parametrize("module_name", AI_SUBMODULES)
def test_ai_submodule_imports(module_name: str) -> None:
    importlib.import_module(module_name)


def test_ai_router_exposes_health_route() -> None:
    from flowfile_core.ai import router

    paths = {route.path for route in router.routes}
    assert "/health" in paths, f"Expected /health on ai_router; saw {paths}"


def test_ai_router_mounted_on_app() -> None:
    from flowfile_core.main import app

    paths = {route.path for route in app.routes}
    assert "/ai/health" in paths, (
        "Expected /ai/health on the FastAPI app; the ai_router may not be mounted "
        f"(saw {sorted(p for p in paths if p.startswith('/ai'))})."
    )


def test_ai_prompt_stubs_exist() -> None:
    """D008 layered prompts: each surface needs a file (even if empty)."""
    from pathlib import Path

    import flowfile_core.ai as ai_pkg

    prompts_dir = Path(ai_pkg.__file__).parent / "prompts"
    for surface in ("base", "assist", "copilot", "planner"):
        assert (prompts_dir / f"{surface}.md").is_file(), (
            f"Missing prompt stub: prompts/{surface}.md"
        )
