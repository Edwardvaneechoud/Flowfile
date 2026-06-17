"""Natural-language → node code generation, backing ``POST /ai/generate_node_code``.

The user describes what a code-bearing node (``polars_code`` / ``python_script``
/ ``sql_query``) should do; the LLM returns a code snippet grounded in the
node's real upstream schema. JSON-mode, hard timeout, never raises — soft
failures return ``degraded=True`` + a stable ``reason``.

Mirrors the cron-generation surface (:mod:`flowfile_core.ai.cron_nl`) but adds
the schema-grounded snapshot from
:func:`flowfile_core.ai.context.render_prompt_context` (surface ``"explain"``)
so generated code uses the actual upstream columns. Must not import ``litellm``
at module load.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from flowfile_core.ai.context import render_prompt_context
from flowfile_core.ai.metrics import record_autocomplete_call
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler

logger = logging.getLogger(__name__)

SURFACE: str = "node_codegen"
DEFAULT_TIMEOUT_SECONDS: float = 30.0  # a deliberate button press, not per-keystroke
MAX_PROMPT_LEN: int = 2000
_MAX_TOKENS: int = 1500

# Node types whose primary configuration is a code snippet. Matches the
# inline-action ``_CODE_BEARING_NODE_TYPES`` set.
CODE_BEARING_NODE_TYPES: frozenset[str] = frozenset(
    {"polars_code", "python_script", "sql_query"}
)

# Per-node-type contract the generated code must satisfy. Mirrors the default
# templates the editors ship (polarsCode/utils.ts, pythonScript/utils.ts,
# sqlQuery/utils.ts) so generated code drops straight into the node.
_NODE_CONTRACTS: dict[str, str] = {
    "polars_code": (
        "Target: a Flowfile **Polars code** node. Polars is imported as `pl`.\n"
        "- The single input is a Polars LazyFrame named `input_df`. With multiple "
        "inputs they are `input_df_0`, `input_df_1`, ... in connection order.\n"
        "- Either end with a single expression that evaluates to the result frame, "
        "or assign the final frame to a variable named `output_df`.\n"
        "- With no inputs the node is a starter: build `output_df` from scratch "
        "(e.g. `output_df = pl.DataFrame({...})`).\n"
        "- Use the Polars lazy API; do not call `.collect()`."
    ),
    "python_script": (
        "Target: a Flowfile **Python script** node running on a kernel.\n"
        "- `import polars as pl` for dataframe work.\n"
        "- Read inputs via the `flowfile_ctx` API: `df = flowfile_ctx.read_input()` "
        "for the first input, or `flowfile_ctx.read_inputs()` for all of them.\n"
        "- Publish the result with `flowfile_ctx.publish_output(df)`.\n"
        "- Keep it self-contained; the standard library and polars are available."
    ),
    "sql_query": (
        "Target: a Flowfile **SQL query** node (DuckDB SQL dialect).\n"
        "- Connected inputs are tables named `input_1`, `input_2`, ... in "
        "connection order.\n"
        "- Return a single `SELECT` statement producing the desired output.\n"
        "- Use the real column names from the schema above; do not invent columns."
    ),
}


class NodeCodeGenerationResponse(BaseModel):
    """``code`` is a non-empty snippet on success, else ``None`` with
    ``degraded`` + a ``reason`` code (and possibly an ``explanation``)."""

    model_config = ConfigDict(extra="forbid")

    code: str | None = None
    explanation: str | None = None
    degraded: bool = False
    reason: str | None = None


class _CodeLLMOutput(BaseModel):
    """The JSON shape the LLM must emit."""

    model_config = ConfigDict(extra="ignore")

    code: str
    explanation: str | None = None


_SYSTEM_PROMPT = """\
You generate code for a single node in Flowfile, a visual ETL tool. The user
describes what the node should do; you return code that implements it, grounded
in the node's real upstream schema (the column names and types are provided).

Output a single JSON object — no prose, no markdown fences. Shape:

  {"code": "<the code>", "explanation": "<one short sentence>"}

Rules:
* Put ONLY the code in the `code` field. Do not wrap it in markdown fences. The
  code itself may contain comments.
* Use the EXACT upstream column names and types from the snapshot. Never invent
  columns that aren't in the schema.
* Follow the target node's code contract exactly (given below).
* If the request is impossible or unrelated to a data transformation, return
  {"code": "", "explanation": "<why>"} instead of guessing.
"""


def _build_messages(*, rendered_context: str, node_type: str, prompt: str) -> list[Message]:
    contract = _NODE_CONTRACTS.get(node_type, "")
    user = (
        f"{rendered_context}\n\n"
        f"## Code contract\n\n{contract}\n\n"
        f"## Task\n\n{prompt}\n\n"
        "Generate the code now as the JSON object described above."
    )
    return [
        Message(role="system", content=_SYSTEM_PROMPT),
        Message(role="user", content=user),
    ]


async def _call_provider_for_json(
    *,
    provider: Provider,
    messages: list[Message],
    timeout: float,
    scheduler: RateLimitScheduler | None,
) -> tuple[str | None, str | None]:
    """Call ``provider.chat`` in JSON-mode with a hard timeout; no retries.
    Returns ``(content, error_reason)``."""
    sched = scheduler or default_scheduler()

    async def _do_call() -> str | None:
        async with sched.acquire(provider.name, surface=SURFACE):
            response = await provider.chat(
                messages=messages,
                tools=None,
                max_tokens=_MAX_TOKENS,
                response_format={"type": "json_object"},
            )
        return response.content

    try:
        content = await asyncio.wait_for(_do_call(), timeout=timeout)
    except asyncio.TimeoutError:
        return None, "timeout"
    except Exception as exc:  # noqa: BLE001
        logger.warning("node code generation provider call failed: %s", exc, exc_info=False)
        return None, "provider_error"
    return content, None


def _parse_json_payload(content: str | None) -> tuple[Any, str | None]:
    """Parse JSON, tolerating a ```json code fence around the whole object."""
    if content is None or not content.strip():
        return None, "empty_response"
    try:
        return json.loads(content), None
    except json.JSONDecodeError:
        pass
    stripped = content.strip()
    if stripped.startswith("```"):
        without_open = stripped.split("\n", 1)[-1]
        if without_open.rstrip().endswith("```"):
            without_open = without_open.rstrip()[: -len("```")].rstrip()
        try:
            return json.loads(without_open), None
        except json.JSONDecodeError:
            pass
    return None, "parse_error"


def _strip_code_fences(code: str) -> str:
    """Defensively remove a leading ```lang line and trailing ``` if the model
    wrapped the snippet in a markdown fence inside the JSON string."""
    text = code.strip()
    if not text.startswith("```"):
        return code.strip("\n")
    body = text.split("\n", 1)[-1] if "\n" in text else ""
    if body.rstrip().endswith("```"):
        body = body.rstrip()[: -len("```")]
    return body.strip("\n")


def _record(provider: Provider, started: float, reason: str | None, *, success: bool = False) -> None:
    record_autocomplete_call(
        surface=SURFACE,
        provider=provider.name,
        latency_ms=int((time.monotonic() - started) * 1000),
        suggestion_count=1 if success else 0,
        degraded_reason=reason,
    )


async def generate_node_code(
    *,
    flow: Any,
    node_id: int,
    node_type: str,
    prompt: str,
    provider: Provider,
    samples_mode: str = "off",
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> NodeCodeGenerationResponse:
    """Turn a plain-English request into a code snippet for a code-bearing node.
    Never raises; soft failures return ``degraded=True`` with a stable ``reason``."""
    started = time.monotonic()
    text = (prompt or "").strip()
    if not text:
        _record(provider, started, "empty_prompt")
        return NodeCodeGenerationResponse(degraded=True, reason="empty_prompt")

    ctx = render_prompt_context(flow, [node_id], surface="explain", samples_mode=samples_mode)

    content, err = await _call_provider_for_json(
        provider=provider,
        messages=_build_messages(rendered_context=ctx.user, node_type=node_type, prompt=text),
        timeout=timeout,
        scheduler=scheduler,
    )
    if err is not None:
        _record(provider, started, err)
        return NodeCodeGenerationResponse(degraded=True, reason=err)

    payload, parse_err = _parse_json_payload(content)
    if parse_err is not None:
        _record(provider, started, parse_err)
        return NodeCodeGenerationResponse(degraded=True, reason=parse_err)

    try:
        parsed = _CodeLLMOutput.model_validate(payload)
    except ValidationError:
        _record(provider, started, "validation_error")
        return NodeCodeGenerationResponse(degraded=True, reason="validation_error")

    code = _strip_code_fences(parsed.code)
    if not code.strip():
        # model declined (impossible / unrelated request) — forward its reason
        _record(provider, started, "no_code")
        return NodeCodeGenerationResponse(degraded=True, reason="no_code", explanation=parsed.explanation)

    _record(provider, started, None, success=True)
    return NodeCodeGenerationResponse(code=code, explanation=parsed.explanation, degraded=False)


__all__ = [
    "SURFACE",
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_PROMPT_LEN",
    "CODE_BEARING_NODE_TYPES",
    "NodeCodeGenerationResponse",
    "generate_node_code",
]
