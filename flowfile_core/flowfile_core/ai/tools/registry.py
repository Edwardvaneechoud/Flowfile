"""Tool catalog generation.

Every entry in ``NODE_TYPE_TO_SETTINGS_CLASS`` is exposed as an
MCP-shaped tool whose ``parameters`` is the Pydantic settings class's
``model_json_schema()`` (JSON Schema 2020-12). The catalog is
augmented with the four ops surfaces declared in this package
(``graph_ops`` / ``schema_ops`` / ``codegen_ops`` / ``meta_ops``).

Surfacing strategy:

* **Per-surface presets** for narrow surfaces (``cmd_k``, ``ghost_node``,
  ``explain``, ``docgen``) — hand-picked subsets that keep token cost
  low and meet the sub-1s TTFB target on Cmd+K.
* **Multi-stage agent** — ``agent_staged`` exposes one tool per round
  via the ``staged_*`` presets; ``agent_complex`` exposes the full
  catalog in a single call.

User-defined nodes registered via the ``CUSTOM_NODE_STORE`` are picked
up by ``_iter_node_types()`` and surfaced under their own
``flowfile.graph.add_<type>`` name automatically.

The executor consumes ``ToolSpec.name`` → node-type / op resolution
and dispatches accordingly.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Final, Literal, get_args, get_origin

from pydantic import BaseModel

from flowfile_core.ai.providers.base import ToolSpec
from flowfile_core.ai.tools.codegen_ops import CODEGEN_OPS_TOOLS
from flowfile_core.ai.tools.graph_ops import GRAPH_OPS_TOOLS
from flowfile_core.ai.tools.meta_ops import (
    CLASSIFY_INTENT_TOOL_NAME,
    EMIT_PLAN_TOOL_NAME,
    META_OPS_TOOLS,
    PICK_NODE_TYPE_TOOL_NAME,
    PICK_UPSTREAM_TOOL_NAME,
    VERIFY_COMPLETION_TOOL_NAME,
)
from flowfile_core.ai.tools.node_docs import (
    NODE_AGENT_PAYLOAD_EXAMPLES,
    NODE_LONG_DESCRIPTIONS,
    NODE_USER_INSTRUCTIONS,
    palette_label_for,
    sidebar_section_for,
)
from flowfile_core.ai.tools.schema_ops import SCHEMA_OPS_TOOLS
from flowfile_core.schemas.schemas import (
    NODE_TYPE_TO_SETTINGS_CLASS,
    get_settings_class_for_node_type,
)

MCP_TOOL_NAMESPACE: Final[str] = "flowfile"
JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"

SurfaceLiteral = Literal[
    "cmd_k",
    "ghost_node",
    "explain",
    "agent_complex",
    "agent_staged",
    "staged_plan",
    "staged_classify",
    "staged_pick_type",
    "staged_pick_upstream",
    "staged_modify",
    "staged_delete",
    "staged_connect",
    "staged_disconnect",
    "staged_verify_completion",
    "docgen",
    "settings_autocomplete",
    "lineage",
    "intent_classifier",
    "cron",
]

# Regex that every emitted tool name must match.
_MCP_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^flowfile\.(graph|schema|codegen|meta)\.[a-z_][a-z0-9_]*$")


def mcp_tool_name(domain: str, op: str) -> str:
    """Return the canonical MCP-shaped tool name for ``(domain, op)``.

    Example: ``mcp_tool_name("graph", "add_filter")`` →
    ``"flowfile.graph.add_filter"``. Use this everywhere the catalog
    is constructed so the naming convention stays in one place.
    """
    name = f"{MCP_TOOL_NAMESPACE}.{domain}.{op}"
    if not _MCP_NAME_RE.match(name):
        raise ValueError(
            f"Tool name {name!r} does not match MCP naming convention "
            f"{_MCP_NAME_RE.pattern!r}. Domain must be one of "
            "graph/schema/codegen/meta and op must be a snake_case identifier."
        )
    return name


_MAX_REF_RESOLUTIONS: Final[int] = 8


def _inline_ref_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Resolve every ``{"$ref": "#/$defs/X"}`` against the top-level
    ``$defs`` block, dropping the block once nothing in the resulting tree
    references it. The output is a self-contained JSON Schema with the inner
    shape spelled out at every property site.

    Pydantic v2's ``model_json_schema()`` defaults to emitting nested
    ``BaseModel`` fields as cross-references. Agents observed in
    practice fail to follow ``$ref`` and JSON-string-encode structured
    payloads instead. Inlining removes the cognitive hop.

    Each branch of the walk tracks how many ``$ref`` resolutions have happened
    on the path from the root; the bound at :data:`_MAX_REF_RESOLUTIONS`
    guards against self-referential models (none today, defensive against
    future ones). Tree-traversal depth is *not* counted — only ref
    resolutions — so deeply-nested non-cyclic schemas inline fully.
    """
    defs = schema.get("$defs", {}) or {}
    if not defs:
        return schema

    def walk(node: Any, refs_resolved: int) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                if refs_resolved >= _MAX_REF_RESOLUTIONS:
                    return node
                key = ref[len("#/$defs/") :]
                target = defs.get(key)
                if target is not None:
                    resolved = copy.deepcopy(target)
                    # Sibling keys on a $ref node override the target's ($ref-with-siblings semantics).
                    for k, v in node.items():
                        if k == "$ref":
                            continue
                        resolved[k] = v
                    return walk(resolved, refs_resolved + 1)
            return {k: walk(v, refs_resolved) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(item, refs_resolved) for item in node]
        return node

    inlined = {k: walk(v, 0) for k, v in schema.items() if k != "$defs"}
    return inlined


def _node_settings_to_tool_spec(node_type: str, settings_cls: type) -> ToolSpec:
    """Project a node settings Pydantic class into a ``ToolSpec``.

    Pydantic v2's ``model_json_schema()`` already emits a
    JSON-Schema-2020-12 compatible document with
    ``$defs``/``properties``/``required``. We inject the explicit
    ``$schema`` dialect URI so MCP consumers see the declared dialect,
    and we inline ``$defs`` references at the field site so
    nested-Pydantic shapes are visible without follow-up resolution.
    """
    schema = _inline_ref_schema(dict(settings_cls.model_json_schema()))
    # Pydantic omits $schema; declare the 2020-12 dialect so MCP clients don't have to guess it.
    schema.setdefault("$schema", JSON_SCHEMA_DIALECT)
    description = (settings_cls.__doc__ or f"Create a {node_type} node.").strip()
    # First paragraph only, to keep the prompt cheap.
    description = description.split("\n\n", 1)[0].strip()
    return ToolSpec(
        name=mcp_tool_name("graph", f"add_{node_type}"),
        description=description,
        long_description=NODE_LONG_DESCRIPTIONS.get(node_type, ""),
        user_instructions=_compose_user_instructions(node_type),
        agent_payload_example=NODE_AGENT_PAYLOAD_EXAMPLES.get(node_type, ""),
        parameters=schema,
    )


_PLANNER_INJECTED_SETTINGS_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "flow_id",
        "node_id",
        "depending_on_id",
        "depending_on_ids",
        "cache_results",
        "execution_location",
        "pos_x",
        "pos_y",
        "group_id",
        "is_setup",
        "description",
        "node_reference",
        "user_id",
        "is_flow_output",
        "is_user_defined",
        "output_field_config",
    }
)
"""``NodeBase`` fields the planner fills from session state, or leaves to their defaults, before validation.

The ``fill_settings`` tool spec strips them so the LLM sees only the settings shape (models were seen filling them with
garbage), and ``_resolve_inner_input_field`` ignores them to find a single-input type's one field.
"""


def _unwrap_optional_annotation(annotation: Any) -> Any:
    """Strip ``Optional[X]`` / ``Union[X, None]`` / ``X | None`` to ``X``.

    Pydantic v2 field annotations frequently appear as ``X | None = None``
    (e.g. ``groupby_input: GroupByInput = None`` is annotated as
    ``Optional[GroupByInput]``). Inner-input detection needs the underlying
    type to inspect whether it's a ``BaseModel`` subclass.
    """
    import types as _types
    import typing as _typing

    origin = _typing.get_origin(annotation)
    if origin is _typing.Union or origin is _types.UnionType:
        args = [a for a in _typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _resolve_inner_input_field(settings_cls: type) -> tuple[str, type] | None:
    """Return ``(field_name, inner_class)`` for single-input settings
    types, else ``None`` for multi-field / empty types.

    Auto-detect: filter out :data:`_PLANNER_INJECTED_SETTINGS_FIELDS` from
    the model's fields. If exactly one type-specific field remains AND its
    annotation is a ``BaseModel`` subclass, it's a single-input type and
    we can expose just the inner schema.

    Examples (single-input → returns inner):

    * ``NodeGroupBy`` → ``("groupby_input", GroupByInput)``
    * ``NodeSort`` → ``("sort_input", SortInput)``
    * ``NodeManualInput`` → ``("raw_data_format", RawData)``

    Multi-field types (``NodeFilter`` has ``filter_input + split_mode``,
    ``NodeGate`` has ``gate_input + else_output``,
    ``NodeFormula`` has ``function + functions`` — the legacy single
    entry and the ordered multi-entry list, both deliberately visible so
    the planner can emit a chained formula node,
    ``NodeJoin`` has ``join_input + auto_keep_*`` etc.), empty types
    (``NodeRecordCount``, ``NodeWaitFor``), and types whose only
    type-specific field is a primitive (``NodeSample.sample_size: int``)
    return ``None`` so the caller falls back to the flat-stripped variant.
    """
    type_specific: dict[str, Any] = {
        name: info.annotation
        for name, info in settings_cls.model_fields.items()
        if name not in _PLANNER_INJECTED_SETTINGS_FIELDS
    }
    if len(type_specific) != 1:
        return None
    field_name, annotation = next(iter(type_specific.items()))
    inner = _unwrap_optional_annotation(annotation)
    # On Python 3.10 list[X] passes isinstance(_, type) and issubclass on it raises TypeError.
    if get_origin(inner) is not None:
        return None
    if isinstance(inner, type) and issubclass(inner, BaseModel):
        return field_name, inner
    return None


def _node_settings_to_tool_spec_stripped(node_type: str, settings_cls: type) -> ToolSpec:
    """Stage-3 variant of :func:`_node_settings_to_tool_spec`.

    Identical to the standard projection except that planner-injected
    fields are removed from both ``properties`` and ``required`` (see
    :data:`_PLANNER_INJECTED_SETTINGS_FIELDS`). The LLM at
    ``fill_settings`` only sees the type-specific settings shape; the
    planner threads the stripped fields in from the session's
    ``picked_*`` state before validation.

    Re-uses the standard projection (descriptions, long_description, the
    payload example) so the fill-settings prompt sees the same per-type
    grounding the legacy surfaces show.

    Used as the **fallback** when a node type has multiple type-specific
    fields and ``_resolve_inner_input_field`` declined to return an inner
    class. For single-input types,
    :func:`_node_settings_to_inner_tool_spec` is preferred instead — it
    drops the envelope entirely.
    """
    base = _node_settings_to_tool_spec(node_type, settings_cls)
    schema = copy.deepcopy(base.parameters)

    props = schema.get("properties")
    if isinstance(props, dict):
        for field_name in _PLANNER_INJECTED_SETTINGS_FIELDS:
            props.pop(field_name, None)

    required = schema.get("required")
    if isinstance(required, list):
        schema["required"] = [r for r in required if r not in _PLANNER_INJECTED_SETTINGS_FIELDS]

    return base.model_copy(update={"parameters": schema})


def _node_settings_to_inner_tool_spec(
    node_type: str, settings_cls: type, inner_field_name: str, inner_cls: type
) -> ToolSpec:
    """Stage-3 inner-shape variant.

    Returns a tool spec whose parameters schema is the *inner* class
    (e.g. ``GroupByInput``) rather than the wrapper (``NodeGroupBy``).
    The LLM never sees the envelope — it just fills ``agg_cols`` (or
    whatever the inner shape is). The planner re-wraps before validation
    via :func:`get_staged_fill_inner_field_name`.

    Reuses the per-type ``long_description`` and ``user_instructions``
    from the wrapper class so the prompt grounding remains identical.
    The tool name (``flowfile.graph.add_<type>``) also stays the same so
    the executor's dispatch table still routes correctly.
    """
    schema = _inline_ref_schema(dict(inner_cls.model_json_schema()))
    schema.setdefault("$schema", JSON_SCHEMA_DIALECT)
    description = (settings_cls.__doc__ or f"Create a {node_type} node.").strip()
    description = description.split("\n\n", 1)[0].strip()
    return ToolSpec(
        name=mcp_tool_name("graph", f"add_{node_type}"),
        description=description,
        long_description=NODE_LONG_DESCRIPTIONS.get(node_type, ""),
        user_instructions=_compose_user_instructions(node_type),
        agent_payload_example=NODE_AGENT_PAYLOAD_EXAMPLES.get(node_type, ""),
        parameters=schema,
    )


def get_staged_fill_inner_field_name(node_type: str) -> str | None:
    """Return the wrapper field name when stage 3 uses the inner-input
    tool spec, else ``None``.

    The planner consults this at fill-settings dispatch: when non-``None``,
    it wraps the LLM-emitted ``tc.arguments`` as
    ``{<field_name>: tc.arguments, flow_id, node_id, ...}`` so the
    executor's settings validation receives the full Pydantic shape.
    Returns ``None`` for multi-field / empty types — the planner passes
    ``tc.arguments`` through unchanged in that case.
    """
    settings_cls = get_settings_class_for_node_type(node_type)
    if settings_cls is None:
        return None
    inner = _resolve_inner_input_field(settings_cls)
    return inner[0] if inner is not None else None


def build_staged_fill_tool_spec(node_type: str) -> ToolSpec | None:
    """Return the single stage-3 tool spec for the picked node type.

    Looked up per-turn by the planner (``agent_staged`` surface, stage
    ``fill_settings``) using ``session.picked_node_type``. Returns
    ``None`` when the node type is unknown so the planner can refuse
    cleanly rather than dispatching against a missing settings class.

    Prefers the inner-shape variant when the settings class has a
    single type-specific field whose type is a ``BaseModel`` subclass
    — e.g. ``group_by`` exposes ``GroupByInput`` directly so the LLM
    sees ``agg_cols`` at the top level instead of the ``NodeGroupBy``
    envelope. Multi-field types fall back to the flat-stripped variant.
    """
    settings_cls = get_settings_class_for_node_type(node_type)
    if settings_cls is None:
        return None
    inner = _resolve_inner_input_field(settings_cls)
    if inner is not None:
        field_name, inner_cls = inner
        return _node_settings_to_inner_tool_spec(node_type, settings_cls, field_name, inner_cls)
    return _node_settings_to_tool_spec_stripped(node_type, settings_cls)


def _compose_user_instructions(node_type: str) -> str:
    """Compose the user-instructions block for ``node_type``.

    Combines the runtime palette label + sidebar section (read from
    ``nodes.py``, the canonical source) with the per-node prose from
    ``NODE_USER_INSTRUCTIONS``. Returns ``""`` when no user-instructions
    entry exists; the test suite asserts non-empty for every node type
    in ``NODE_TYPE_TO_SETTINGS_CLASS``, so this fallback only matters
    for new node types that haven't been documented yet.
    """

    body = NODE_USER_INSTRUCTIONS.get(node_type, "").strip()
    if not body:
        return ""
    palette = palette_label_for(node_type)
    section = sidebar_section_for(node_type)
    if section:
        header = f"(palette: {palette!r}, section: {section!r})"
    else:
        header = f"(palette: {palette!r}, internal — not in palette)"
    return f"{header}\n{body}"


def _iter_node_types() -> list[str]:
    """Return every node-type registered, including UDFs from the custom store.

    The custom-node store is imported lazily so this module stays
    importable in isolation (mirrors the lazy-litellm contract — no
    eager side-effect imports).
    """
    seen: set[str] = set()
    types: list[str] = []
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        if node_type not in seen:
            seen.add(node_type)
            types.append(node_type)
    # Lazy, so catalog-only callers of flowfile_core.ai.tools don't pull in the configs package.
    from flowfile_core.configs.node_store import CUSTOM_NODE_STORE

    for node_type in CUSTOM_NODE_STORE:
        if node_type not in seen:
            seen.add(node_type)
            types.append(node_type)
    return types


def _build_node_type_tools() -> list[ToolSpec]:
    """Generate the ``flowfile.graph.add_<type>`` tool list for every node type."""
    tools: list[ToolSpec] = []
    for node_type in _iter_node_types():
        settings_cls = get_settings_class_for_node_type(node_type)
        if settings_cls is None:
            # A custom node can leave the store between enumeration and lookup; skip it.
            continue
        tools.append(_node_settings_to_tool_spec(node_type, settings_cls))
    return tools


# Needed in every category; meta ops stay out so the agent can't loop on pick_category mid-stream.
_UNIVERSAL_OP_NAMES: Final[frozenset[str]] = frozenset({tool.name for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS)})


def _node_tool_name(node_type: str) -> str:
    return mcp_tool_name("graph", f"add_{node_type}")


# Per-surface presets. Frozensets so callers can't mutate them.
SURFACE_PRESETS: Final[dict[str, frozenset[str]]] = {
    "cmd_k": frozenset(
        {
            _node_tool_name(nt)
            for nt in (
                "filter",
                "select",
                "sort",
                "unique",
                "record_id",
            )
        }
        | {"flowfile.schema.read_node_schema"}
    ),
    "ghost_node": frozenset(
        {
            _node_tool_name(nt)
            for nt in (
                "filter",
                "select",
                "sort",
                "formula",
                "group_by",
                "join",
                "union",
            )
        }
        | {"flowfile.schema.read_node_schema"}
    ),
    "explain": frozenset(
        {
            "flowfile.schema.read_node_schema",
            "flowfile.schema.read_node_preview",
        }
    ),
    # The full catalog in one call, for when context budget and provider quality allow it.
    "agent_complex": frozenset(),  # populated below after _build_node_type_tools
    # Placeholder: the planner dispatches on session.stage to the per-stage presets below.
    "agent_staged": frozenset(),
    # One tool, so the up-front plan goes through function calling like every other staged stage.
    "staged_plan": frozenset({EMIT_PLAN_TOOL_NAME}),
    "staged_classify": frozenset({CLASSIFY_INTENT_TOOL_NAME}),
    "staged_pick_type": frozenset({PICK_NODE_TYPE_TOOL_NAME}),
    # Static placeholder; the planner builds a per-turn spec so the upstream-id enum stays fresh.
    "staged_pick_upstream": frozenset({PICK_UPSTREAM_TOOL_NAME}),
    "staged_modify": frozenset({"flowfile.graph.update_node_settings"}),
    "staged_delete": frozenset({"flowfile.graph.delete_node"}),
    "staged_connect": frozenset({"flowfile.graph.connect"}),
    "staged_disconnect": frozenset({"flowfile.graph.delete_connection"}),
    # Opt-in: reached only when classify picks op_kind="other" and verify_plan_completion is set.
    "staged_verify_completion": frozenset({VERIFY_COMPLETION_TOOL_NAME}),
    "docgen": frozenset(
        {
            "flowfile.schema.read_node_schema",
            "flowfile.schema.read_node_preview",
        }
    ),
    # Text-only: the LLM answers in JSON and never calls a tool, so the catalog is deliberately empty.
    "settings_autocomplete": frozenset(),
    # Read-only Assist surface; the route passes tools=None.
    "lineage": frozenset(),
    # Single-shot judgement read as strict JSON; the route passes tools=None.
    "intent_classifier": frozenset(),
    # Single-shot NL-to-cron JSON; the route passes tools=None.
    "cron": frozenset(),
}


def _full_catalog() -> list[ToolSpec]:
    """Internal: the complete deduplicated tool list."""
    tools = list(_build_node_type_tools())
    seen = {tool.name for tool in tools}
    for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS, *CODEGEN_OPS_TOOLS, *META_OPS_TOOLS):
        if tool.name not in seen:
            seen.add(tool.name)
            tools.append(tool)
    return tools


_AGENT_SURFACES_THAT_BLOCK_WRITERS: frozenset[str] = frozenset(
    {"agent_complex", "agent_staged", "agent_live"}
)


def build_tool_catalog(*, surface: str | None = None) -> list[ToolSpec]:
    """Return the tool catalog, optionally filtered by surface.

    With ``surface=None`` returns the full deduplicated catalog (every
    node-type tool + every ops-surface tool). Otherwise filters by
    :data:`SURFACE_PRESETS` lookup. ``"agent_complex"`` is the
    single-shot full-catalog convenience alias.

    For agent-shaped surfaces (``agent_complex`` / ``agent_staged`` /
    ``agent_live``) the :data:`AGENT_BLOCKED_NODE_TYPES` writer set is
    filtered out of the returned catalog so the LLM never sees an
    ``add_output`` / ``add_database_writer`` / etc. tool entry.
    Defense-in-depth pairs with the executor refusal in
    ``_handle_add_node``: even if the LLM hallucinates the call, the
    executor refuses with a ``writer_blocked`` reason.

    Raises ``KeyError`` for unknown surface names so callers get a hard
    failure rather than a silently empty catalog.
    """
    from flowfile_core.ai.safety import AGENT_BLOCKED_NODE_TYPES

    catalog = _full_catalog()

    def _strip_writers(tools: list[ToolSpec]) -> list[ToolSpec]:
        blocked_names = {
            mcp_tool_name("graph", f"add_{nt}") for nt in AGENT_BLOCKED_NODE_TYPES
        }
        return [tool for tool in tools if tool.name not in blocked_names]

    if surface is None:
        return catalog
    if surface == "agent_complex":
        # Its preset is empty, so resolve it to the full catalog here (minus writers).
        return _strip_writers(catalog)
    if surface in SURFACE_PRESETS:
        names = SURFACE_PRESETS[surface]
    else:
        valid = sorted(set(SURFACE_PRESETS) | {"agent_complex"})
        raise KeyError(
            f"Unknown surface {surface!r}. Valid: {valid}. "
            "(Use surface=None to return the full catalog.)"
        )
    filtered = [tool for tool in catalog if tool.name in names]
    if surface in _AGENT_SURFACES_THAT_BLOCK_WRITERS:
        filtered = _strip_writers(filtered)
    return filtered


# Fails at import when a surface literal has no preset entry, or the reverse.
def _check_preset_coverage() -> None:
    surface_keys = set(get_args(SurfaceLiteral))
    preset_keys = set(SURFACE_PRESETS) | {"agent_complex"}
    if surface_keys != preset_keys:
        missing = surface_keys - preset_keys
        extra = preset_keys - surface_keys
        raise RuntimeError(f"SurfaceLiteral / SURFACE_PRESETS coverage mismatch: missing={missing}, extra={extra}")


_check_preset_coverage()


__all__ = [
    "MCP_TOOL_NAMESPACE",
    "JSON_SCHEMA_DIALECT",
    "SurfaceLiteral",
    "SURFACE_PRESETS",
    "build_tool_catalog",
    "mcp_tool_name",
]
