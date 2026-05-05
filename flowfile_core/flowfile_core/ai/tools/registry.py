"""Tool catalog generation — owned by W30.

Per plan §4.1, every entry in ``NODE_TYPE_TO_SETTINGS_CLASS`` is exposed as
an MCP-shaped (D004) tool whose ``parameters`` is the Pydantic settings
class's ``model_json_schema()`` (JSON Schema 2020-12). The catalog is
augmented with the four ops surfaces declared in this package
(``graph_ops`` / ``schema_ops`` / ``codegen_ops`` / ``meta_ops``).

Surfacing strategy (D002):

* **Per-surface presets** for narrow surfaces (``cmd_k``, ``ghost_node``,
  ``explain``, ``docgen``) — hand-picked subsets that keep token cost low
  and meet the sub-1s TTFB target on Cmd+K.
* **Two-stage pick_category** for the full Level 3 agent — first call
  surfaces only ``flowfile.meta.pick_category``; second call surfaces the
  chosen category's tools (``CATEGORY_PRESETS``).

User-defined nodes registered via the ``CUSTOM_NODE_STORE`` are picked up
by ``_iter_node_types()`` and surfaced under their own ``flowfile.graph.add_<type>``
name automatically.

W30 generates the catalog. W31's executor consumes ``ToolSpec.name`` →
node-type / op resolution and dispatches accordingly. W40's planner runs
the LLM-driven first stage; this module's ``pick_category`` heuristic is
the fallback.
"""

from __future__ import annotations

import re
from typing import Final, Literal, get_args

from flowfile_core.ai.providers.base import ToolSpec
from flowfile_core.ai.tools.codegen_ops import CODEGEN_OPS_TOOLS
from flowfile_core.ai.tools.graph_ops import GRAPH_OPS_TOOLS
from flowfile_core.ai.tools.meta_ops import CATEGORY_NAMES, META_OPS_TOOLS
from flowfile_core.ai.tools.node_docs import (
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

ToolCategory = Literal[
    "transformations",
    "joins",
    "aggregations",
    "io",
    "code",
    "ml",
    "meta",
    "graph",
]

SurfaceLiteral = Literal[
    "cmd_k",
    "ghost_node",
    "explain",
    "agent",
    "agent_complex",
    "docgen",
    "settings_autocomplete",
    "lineage",
    "intent_classifier",
]

# Regex that every emitted tool name must match (D004).
_MCP_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^flowfile\.(graph|schema|codegen|meta)\.[a-z_][a-z0-9_]*$")


def mcp_tool_name(domain: str, op: str) -> str:
    """Return the canonical MCP-shaped tool name for ``(domain, op)``.

    Example: ``mcp_tool_name("graph", "add_filter")`` →
    ``"flowfile.graph.add_filter"``. Use this everywhere the catalog is
    constructed so the naming convention from D004 stays in one place.
    """
    name = f"{MCP_TOOL_NAMESPACE}.{domain}.{op}"
    if not _MCP_NAME_RE.match(name):
        raise ValueError(
            f"Tool name {name!r} does not match MCP naming convention "
            f"{_MCP_NAME_RE.pattern!r}. Domain must be one of "
            "graph/schema/codegen/meta and op must be a snake_case identifier."
        )
    return name


def _node_settings_to_tool_spec(node_type: str, settings_cls: type) -> ToolSpec:
    """Project a node settings Pydantic class into a ``ToolSpec``.

    Pydantic v2's ``model_json_schema()`` already emits a JSON-Schema-2020-12
    compatible document with ``$defs``/``properties``/``required``. We inject
    the explicit ``$schema`` dialect URI per D004 so MCP consumers see the
    declared dialect.
    """
    schema = dict(settings_cls.model_json_schema())
    # Pydantic doesn't emit $schema by default; declare the 2020-12 dialect
    # explicitly so MCP clients (and future MCP server shim) don't have to
    # guess the dialect.
    schema.setdefault("$schema", JSON_SCHEMA_DIALECT)
    description = (settings_cls.__doc__ or f"Create a {node_type} node.").strip()
    # Some settings classes have multi-line docstrings; collapse to the first
    # paragraph to keep the prompt cheap.
    description = description.split("\n\n", 1)[0].strip()
    return ToolSpec(
        name=mcp_tool_name("graph", f"add_{node_type}"),
        description=description,
        long_description=NODE_LONG_DESCRIPTIONS.get(node_type, ""),
        user_instructions=_compose_user_instructions(node_type),
        parameters=schema,
    )


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

    The custom-node store is imported lazily so this module stays importable
    in isolation (mirrors W11/W12/W13's lazy-litellm contract — no eager
    side-effect imports).
    """
    seen: set[str] = set()
    types: list[str] = []
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        if node_type not in seen:
            seen.add(node_type)
            types.append(node_type)
    # Lazy import — keeps ``import flowfile_core.ai.tools`` cheap and avoids
    # pulling the configs package during catalog-only callers (e.g. tests).
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
            # Defensive: _iter_node_types already filters to known types, but
            # if a custom node falls out of the store between enumeration and
            # lookup we skip rather than crash the whole catalog.
            continue
        tools.append(_node_settings_to_tool_spec(node_type, settings_cls))
    return tools


# Universal ops that apply to every category — the executor needs schema
# introspection and basic graph mutation regardless of which category was
# picked. ``meta`` is excluded from this set so the agent doesn't loop on
# pick_category mid-stream.
_UNIVERSAL_OP_NAMES: Final[frozenset[str]] = frozenset({tool.name for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS)})


def _node_tool_name(node_type: str) -> str:
    return mcp_tool_name("graph", f"add_{node_type}")


# Per-surface presets (D002 (C)). Frozensets so callers can't mutate them.
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
    "agent": frozenset({"flowfile.meta.pick_category"}),
    # ``agent_complex`` exposes the full catalog in a single call — used when
    # context budgets and provider quality both allow it (e.g. Opus 4.7 on a
    # paid tier). The executor decides which surface to pick at runtime.
    "agent_complex": frozenset(),  # populated below after _build_node_type_tools
    "docgen": frozenset(
        {
            "flowfile.schema.read_node_schema",
            "flowfile.schema.read_node_preview",
        }
    ),
    # Settings autocomplete (W34) is text-only — the LLM emits formula
    # expressions / join-key pairs as JSON, never invoking tools. The empty
    # frozenset is intentional: ``build_tool_catalog(surface="settings_autocomplete")``
    # returns ``[]`` so callers can short-circuit cleanly.
    "settings_autocomplete": frozenset(),
    # Lineage Q&A (W51) is a read-only Assist surface — same posture as
    # ``docgen`` and ``explain``. The route passes ``tools=None`` to the
    # provider; the empty frozenset keeps the surface lockstep happy
    # without surfacing tools the LLM would never call.
    "lineage": frozenset(),
    # W58 intent classifier — single-shot judgement; the route passes
    # ``tools=None`` and reads strict JSON. No tool catalog needed.
    "intent_classifier": frozenset(),
}


# Two-stage agent: each ``pick_category`` outcome → the tool surface for the
# next call. Always augmented with ``_UNIVERSAL_OP_NAMES`` at lookup so the
# executor doesn't have to remember to merge.
_NODE_CATEGORY_MEMBERS: Final[dict[str, tuple[str, ...]]] = {
    "transformations": (
        "filter",
        "select",
        "sort",
        "formula",
        "unique",
        "record_id",
        "sample",
        "random_split",
        "text_to_rows",
        "graph_solver",
    ),
    "joins": (
        "join",
        "cross_join",
        "fuzzy_match",
        "union",
    ),
    "aggregations": (
        "group_by",
        "pivot",
        "unpivot",
        "record_count",
    ),
    "io": (
        "read",
        "output",
        "manual_input",
        "database_reader",
        "database_writer",
        "cloud_storage_reader",
        "cloud_storage_writer",
        "catalog_reader",
        "catalog_writer",
        "kafka_source",
        "google_analytics_reader",
        "external_source",
    ),
    "code": (
        "polars_code",
        "python_script",
        "sql_query",
    ),
    "ml": (
        "train_model",
        "apply_model",
        "evaluate_model",
    ),
    "meta": (
        "explore_data",
        "wait_for",
    ),
    "graph": (),  # graph category exposes only the universal graph-ops surface
}


def _category_to_tool_names(category: str) -> frozenset[str]:
    members = _NODE_CATEGORY_MEMBERS.get(category, ())
    names = {_node_tool_name(nt) for nt in members}
    if category == "code":
        names |= {tool.name for tool in CODEGEN_OPS_TOOLS}
    if category == "meta":
        names |= {tool.name for tool in META_OPS_TOOLS}
    # Universal graph + schema ops are always available.
    names |= _UNIVERSAL_OP_NAMES
    return frozenset(names)


CATEGORY_PRESETS: Final[dict[str, frozenset[str]]] = {name: _category_to_tool_names(name) for name in CATEGORY_NAMES}


# Heuristic keyword → category map for ``pick_category``. The values are the
# substrings we look for in a (lowercased) intent string. First match wins
# in declaration order, so put the most specific matches first.
_CATEGORY_KEYWORDS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    (
        "joins",
        ("join", "merge", "lookup", "fuzzy match", "cross join", "union"),
    ),
    (
        "aggregations",
        (
            "group by",
            "groupby",
            "aggregate",
            "aggregation",
            "summarise",
            "summarize",
            "pivot",
            "unpivot",
            "count by",
            "average",
            "mean",
            "sum",
        ),
    ),
    (
        "io",
        (
            "read csv",
            "read parquet",
            "read file",
            "load from",
            "open file",
            "write to",
            "write csv",
            "write parquet",
            "save to",
            "from database",
            "to database",
            "from s3",
            "from gcs",
            "from kafka",
            "from google analytics",
            "import data",
            "export data",
        ),
    ),
    (
        "code",
        (
            "polars code",
            "python script",
            "python code",
            "sql query",
            "run sql",
            "custom code",
        ),
    ),
    (
        "ml",
        (
            "train model",
            "predict",
            "apply model",
            "evaluate model",
            "score data",
        ),
    ),
    (
        "graph",
        (
            "delete node",
            "remove node",
            "connect node",
            "wire up",
            "rerun",
            "run flow",
            "stage diff",
            "propose subgraph",
        ),
    ),
    (
        "transformations",
        (
            "filter",
            "where ",
            "rename",
            "select",
            "drop column",
            "sort",
            "order by",
            "unique",
            "deduplicate",
            "sample",
            "random split",
            "split rows",
            "record id",
            "row number",
            "formula",
            "expression",
            "compute column",
            "derive column",
            "text to rows",
            "split text",
        ),
    ),
)


def pick_category(intent: str, *, default: ToolCategory = "transformations") -> ToolCategory:
    """Heuristic first-stage categoriser used as the fallback for the two-stage flow.

    The production planner (W40) replaces this with an LLM call against
    ``flowfile.meta.pick_category``; this implementation lets W31's executor
    exercise the two-stage path in tests and degrade gracefully when no
    provider is configured.

    Returns the matched ``ToolCategory`` or ``default`` (``"transformations"``)
    when no keyword matches.
    """
    if not intent or not intent.strip():
        return default
    needle = intent.lower()
    for category, keywords in _CATEGORY_KEYWORDS:
        for kw in keywords:
            if kw in needle:
                # Cast is safe — every category in the heuristic is a member
                # of ``CATEGORY_NAMES``.
                return category  # type: ignore[return-value]
    return default


def _full_catalog() -> list[ToolSpec]:
    """Internal: the complete deduplicated tool list."""
    tools = list(_build_node_type_tools())
    seen = {tool.name for tool in tools}
    for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS, *CODEGEN_OPS_TOOLS, *META_OPS_TOOLS):
        if tool.name not in seen:
            seen.add(tool.name)
            tools.append(tool)
    return tools


def build_tool_catalog(*, surface: str | None = None) -> list[ToolSpec]:
    """Return the tool catalog, optionally filtered by surface or category.

    With ``surface=None`` returns the full deduplicated catalog (every
    node-type tool + every ops-surface tool). Otherwise filters by the
    union of ``SURFACE_PRESETS`` and ``CATEGORY_PRESETS`` lookup —
    ``"agent"`` is the first-stage surface (just ``pick_category``) and
    ``"transformations"`` (etc.) is a second-stage category.

    Raises ``KeyError`` for unknown surface names so callers get a hard
    failure rather than a silently empty catalog.
    """
    catalog = _full_catalog()
    if surface is None:
        return catalog
    if surface == "agent_complex":
        # ``agent_complex`` is documented as the full catalog; resolve it
        # explicitly so callers don't have to know about the SURFACE_PRESETS
        # quirk.
        return catalog
    if surface in SURFACE_PRESETS:
        names = SURFACE_PRESETS[surface]
    elif surface in CATEGORY_PRESETS:
        names = CATEGORY_PRESETS[surface]
    else:
        valid = sorted(set(SURFACE_PRESETS) | set(CATEGORY_PRESETS) | {"agent_complex"})
        raise KeyError(
            f"Unknown surface or category {surface!r}. Valid: {valid}. "
            "(Use surface=None to return the full catalog.)"
        )
    return [tool for tool in catalog if tool.name in names]


# Sanity check at import time: every literal in the public type aliases
# corresponds to a real preset key. This catches the "I added a surface
# but forgot the preset entry" class of bug at module load.
def _check_preset_coverage() -> None:
    surface_keys = set(get_args(SurfaceLiteral))
    category_keys = set(get_args(ToolCategory))
    preset_keys = set(SURFACE_PRESETS) | {"agent_complex"}
    if surface_keys != preset_keys:
        missing = surface_keys - preset_keys
        extra = preset_keys - surface_keys
        raise RuntimeError(f"SurfaceLiteral / SURFACE_PRESETS coverage mismatch: missing={missing}, extra={extra}")
    if category_keys != set(CATEGORY_PRESETS):
        missing = category_keys - set(CATEGORY_PRESETS)
        extra = set(CATEGORY_PRESETS) - category_keys
        raise RuntimeError(f"ToolCategory / CATEGORY_PRESETS coverage mismatch: missing={missing}, extra={extra}")


_check_preset_coverage()


__all__ = [
    "MCP_TOOL_NAMESPACE",
    "JSON_SCHEMA_DIALECT",
    "ToolCategory",
    "SurfaceLiteral",
    "SURFACE_PRESETS",
    "CATEGORY_PRESETS",
    "build_tool_catalog",
    "mcp_tool_name",
    "pick_category",
]
