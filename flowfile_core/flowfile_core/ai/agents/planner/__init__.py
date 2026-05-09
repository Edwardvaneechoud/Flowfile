"""Multi-turn agent planner — opens a session, plans, and dispatches tool calls.

The planner:

* opens a session via ``POST /ai/agent/start``;
* surfaces a narrowed tool catalog (default ``surface="agent"`` uses a
  two-stage pick_category pattern; ``surface="agent_complex"`` exposes the
  full catalog in one shot);
* dispatches each LLM tool call through :func:`execute_tool_call` with
  ``mode="stage"`` — the live graph is never mutated mid-run;
* snapshots the graph before every dispatch — if the user mutated the
  canvas mid-run, yields ``drift_detected`` + ``paused`` and exits cleanly
  so the route can close the SSE while the session waits for
  ``POST /ai/agent/{session_id}/resume``;
* retries a rejected step up to ``max_retries_per_step`` times by feeding
  the executor's ``refusal_detail`` back as a ``role="tool"`` message and
  asking the LLM to correct;
* on completion, bundles the per-step :class:`StagedToolEntry` list into a
  single :class:`flowfile_core.ai.diff.GraphDiff` via
  :func:`flowfile_core.ai.diff.bundle_staged_results` and registers it for
  the user to review and accept atomically through ``AiDiffPreview``.

The function is a **pure async generator** that never raises — every
failure mode becomes a :class:`PlannerEvent` of type ``"error"`` /
``"tool_call_rejected"`` / ``"drift_detected"`` / ``"abort"`` so the SSE
wrapper can stream the failure to the client without a structured
exception escaping the generator boundary.

The ``PlannerEvent`` ``id:`` headers carry ``f"{session_id}.{step_count}"``
so EventSource clients can re-attach via ``Last-Event-ID`` against the
replay buffer.

System prompt: ``prompts/base.md`` + ``prompts/planner.md``.

The lazy-litellm contract is preserved — this package must not import
``litellm`` at load time. The provider call goes through the provider
seam, which lazy-loads litellm in its own subclass.

The module was split into a package for navigability. The public API
is preserved verbatim — every symbol the old ``planner.py`` exposed
(including the underscored helpers tests reach for directly) is
re-exported here so ``from flowfile_core.ai.agents.planner import ...``
continues to work without churn.
"""

from __future__ import annotations

# Public types + constants
from ._internal import (
    _ADD_PREFIX,
    _AGENT_STAGED_OP_TO_SURFACE,
    _MANDATORY_TOOL_CALL_STAGES,
    _SETTINGS_DEPENDENCY_FIELDS,
    _STAGED_SINGLE_OP_TOOL_NAMES,
    _STAGED_STATE_MACHINE_SURFACES,
    DEFAULT_MAX_RETRIES_PER_STEP,
    DEFAULT_MAX_STEPS,
    DEFAULT_MAX_TOKENS,
    RATIONALE_MAX_LEN,
    OpKind,
    PlannerEvent,
    PlannerEventName,
    _check_self_loop,
    _classify_op_kind,
    _collect_live_node_ids,
    _op_count,
)
from .catalog import (
    _build_staged_tool_catalog,
    _log_stage_transition,
    _resolve_current_surface,
)
from .coercions import (
    _coerce_formula_bare_string_args,
    _derive_formula_output_column_name,
    _looks_like_outer_envelope_value,
)
from .insertion import (
    _allocate_node_id,
    _collect_staged_upstream_positions,
    _count_prior_staged_with_same_upstream,
    _format_ambiguous_insertion_detail,
    _read_settings_dependency_field,
    _resolve_insertion_context,
)
from .llm_replies import (
    _extract_staged_settings_for_reply,
    _payload_node_id,
    _summarise_result_for_llm,
)
from .loop import (
    _REJECTED_DIFF_DEFAULT_NOTE,
    FollowupAction,
    inject_followup_message,
    run_planner_session,
)
from .messages import (
    _build_fill_settings_user_message,
    _build_initial_messages,
    _build_pick_upstream_staged_addendum,
    _refresh_system_prompt_for_stage,
)
from .rationale import (
    _arg_summary,
    _arg_summary_for_add,
    _capture_rationale,
    _format_columns,
    _looks_like_question,
)
from .recovery import (
    _recover_textual_tool_call,
    _try_parse_function_call_shape,
    _try_parse_json_object_shape,
    _walk_balanced_braces,
)
from .staged_schemas import (
    _collect_staged_upstream_schemas,
    _staged_dict_to_flowfile_column,
)

__all__ = [
    # Public API — same surface the original ``planner.py`` exported.
    "DEFAULT_MAX_RETRIES_PER_STEP",
    "DEFAULT_MAX_STEPS",
    "DEFAULT_MAX_TOKENS",
    "FollowupAction",
    "OpKind",
    "PlannerEvent",
    "PlannerEventName",
    "RATIONALE_MAX_LEN",
    "inject_followup_message",
    "run_planner_session",
    # Internals re-exported because tests / agent_routes reach for
    # them directly via the package facade. Listing them in __all__
    # keeps ruff happy and documents the intentional public-from-tests
    # surface.
    "_ADD_PREFIX",
    "_AGENT_STAGED_OP_TO_SURFACE",
    "_MANDATORY_TOOL_CALL_STAGES",
    "_REJECTED_DIFF_DEFAULT_NOTE",
    "_SETTINGS_DEPENDENCY_FIELDS",
    "_STAGED_SINGLE_OP_TOOL_NAMES",
    "_STAGED_STATE_MACHINE_SURFACES",
    "_allocate_node_id",
    "_arg_summary",
    "_arg_summary_for_add",
    "_build_fill_settings_user_message",
    "_build_initial_messages",
    "_build_pick_upstream_staged_addendum",
    "_build_staged_tool_catalog",
    "_capture_rationale",
    "_check_self_loop",
    "_classify_op_kind",
    "_coerce_formula_bare_string_args",
    "_collect_live_node_ids",
    "_collect_staged_upstream_positions",
    "_collect_staged_upstream_schemas",
    "_count_prior_staged_with_same_upstream",
    "_derive_formula_output_column_name",
    "_extract_staged_settings_for_reply",
    "_format_ambiguous_insertion_detail",
    "_format_columns",
    "_log_stage_transition",
    "_looks_like_outer_envelope_value",
    "_looks_like_question",
    "_op_count",
    "_payload_node_id",
    "_read_settings_dependency_field",
    "_recover_textual_tool_call",
    "_refresh_system_prompt_for_stage",
    "_resolve_current_surface",
    "_resolve_insertion_context",
    "_staged_dict_to_flowfile_column",
    "_summarise_result_for_llm",
    "_try_parse_function_call_shape",
    "_try_parse_json_object_shape",
    "_walk_balanced_braces",
    "inject_followup_message",
    "run_planner_session",
]
