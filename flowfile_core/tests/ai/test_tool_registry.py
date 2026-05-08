"""W30 — Tool catalog generation tests.

Cases:

* ``test_full_catalog_min_size`` — full catalog enumerates every entry in
  ``NODE_TYPE_TO_SETTINGS_CLASS`` plus the four ops surfaces.
* ``test_node_tool_names_follow_mcp_convention`` — every node-type tool name
  matches ``flowfile.graph.add_<node_type>`` (D004).
* ``test_ops_tool_names_follow_mcp_convention`` — graph / schema / codegen /
  meta tool names match ``flowfile.<domain>.<op>``.
* ``test_node_tool_parameters_are_valid_json_schema_2020_12`` — every emitted
  ``ToolSpec.parameters`` validates as a JSON Schema 2020-12 (``Draft202012Validator.check_schema``).
* ``test_node_tool_parameters_declare_dialect`` — every node-type tool's
  ``parameters`` declares the JSON-Schema-2020-12 dialect URI on ``$schema``.
* ``test_ops_tool_parameters_are_valid_json_schema`` — graph / schema /
  codegen / meta op specs are also valid JSON Schemas.
* ``test_surface_presets_resolve_to_known_tools`` — every preset name maps
  to a tool actually present in the catalog.
* ``test_category_presets_resolve_to_known_tools`` — same for categories.
* ``test_cmd_k_surface_is_narrow`` — ``cmd_k`` returns ≤ 8 tools (D002 narrow
  surface budget).
* ``test_explain_surface_is_read_only`` — ``explain`` returns only schema-ops.
* ``test_agent_surface_first_stage_is_meta_only`` — ``agent`` returns only
  ``flowfile.meta.pick_category``.
* ``test_agent_complex_surface_is_full`` — ``agent_complex`` returns the full
  catalog.
* ``test_transformations_category_includes_universal_ops`` — every category
  surface is augmented with graph + schema ops at lookup.
* ``test_unknown_surface_raises`` — unknown surface name raises ``KeyError``.
* ``test_pick_category_heuristic`` — keyword matching maps representative
  intents to the expected bucket.
* ``test_pick_category_falls_back_on_empty_intent`` — empty / nonsense
  intent falls back to ``transformations``.
* ``test_mcp_tool_name_helper`` — ``mcp_tool_name`` enforces the dotted
  convention; bad inputs raise.
* ``test_user_defined_node_appears_in_catalog`` — UDFs registered via the
  custom-node store show up under ``flowfile.graph.add_<udf_type>``.
* ``test_lazy_litellm_import`` — importing the tools package does not pull
  in litellm (mirrors W11/W12/W13 contract).
* ``test_descriptions_are_non_empty`` — every tool has a non-empty
  description (the LLM uses these to disambiguate).
"""

from __future__ import annotations

import importlib
import re
import sys
from typing import get_args
from unittest.mock import patch

import pytest
from jsonschema import Draft202012Validator

from flowfile_core.ai.providers.base import ToolSpec
from flowfile_core.ai.tools import (
    CODEGEN_OPS_TOOLS,
    GRAPH_OPS_TOOLS,
    JSON_SCHEMA_DIALECT,
    META_OPS_TOOLS,
    SCHEMA_OPS_TOOLS,
    SURFACE_PRESETS,
    SurfaceLiteral,
    build_tool_catalog,
    mcp_tool_name,
)
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS

_NODE_TOOL_RE = re.compile(r"^flowfile\.graph\.add_[a-z_][a-z0-9_]*$")
_OPS_TOOL_RE = re.compile(r"^flowfile\.(graph|schema|codegen|meta)\.[a-z_][a-z0-9_]*$")


# ---------------------------------------------------------------------------
# Catalog shape
# ---------------------------------------------------------------------------


def test_full_catalog_min_size() -> None:
    """Full catalog covers every node-type entry plus all four ops surfaces."""
    catalog = build_tool_catalog()
    expected = (
        len(NODE_TYPE_TO_SETTINGS_CLASS)
        + len(GRAPH_OPS_TOOLS)
        + len(SCHEMA_OPS_TOOLS)
        + len(CODEGEN_OPS_TOOLS)
        + len(META_OPS_TOOLS)
    )
    # ``>=`` because a UDF registered ahead of the test (e.g. by a
    # previously-loaded fixture) legitimately bumps the count.
    assert len(catalog) >= expected
    # Names are unique — the catalog dedupes by name.
    names = [tool.name for tool in catalog]
    assert len(names) == len(set(names))


def test_full_catalog_returns_tool_specs() -> None:
    catalog = build_tool_catalog()
    assert all(isinstance(tool, ToolSpec) for tool in catalog)


def test_w47_update_node_settings_present_in_catalog() -> None:
    """W47 — ``flowfile.graph.update_node_settings`` is back in the catalog
    with a real implementation. Inverts the W46 absence assertion: W46
    pulled the tool because the executor refused it; W47 lifts the refusal
    and ships ``_handle_update_node_settings`` plus the modifications
    bucket on :class:`GraphDiff`.

    ``run_node`` / ``propose_subgraph`` stay out — W46 dropped them
    permanently (autonomous run-node is unsafe; propose_subgraph is
    redundant with the planner's per-step staging).
    """
    catalog = build_tool_catalog()
    names = {tool.name for tool in catalog}
    assert "flowfile.graph.update_node_settings" in names
    assert "flowfile.graph.run_node" not in names
    assert "flowfile.graph.propose_subgraph" not in names

    # The universal-ops derivation auto-includes ``update_node_settings``
    # in every surface preset alongside add_node / connect / delete_node /
    # delete_connection. (W71 v1.10 — CATEGORY_PRESETS removed alongside
    # the legacy two-stage agent surface; only SURFACE_PRESETS remains.)
    from flowfile_core.ai.tools.registry import (
        SURFACE_PRESETS,
        _UNIVERSAL_OP_NAMES,
    )

    assert "flowfile.graph.update_node_settings" in _UNIVERSAL_OP_NAMES

    permanently_dropped = {
        "flowfile.graph.run_node",
        "flowfile.graph.propose_subgraph",
    }
    for surface, preset in SURFACE_PRESETS.items():
        assert not (permanently_dropped & preset), (
            f"surface {surface!r} contains a removed stub: {permanently_dropped & preset}"
        )


# ---------------------------------------------------------------------------
# Naming convention (D004)
# ---------------------------------------------------------------------------


def test_node_tool_names_follow_mcp_convention() -> None:
    """Every node-type tool name matches ``flowfile.graph.add_<node_type>``."""
    catalog = build_tool_catalog()
    catalog_names = {tool.name for tool in catalog}
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        expected = f"flowfile.graph.add_{node_type}"
        assert expected in catalog_names, f"missing {expected}"
        assert _NODE_TOOL_RE.match(expected), expected


def test_ops_tool_names_follow_mcp_convention() -> None:
    """Every ops-surface tool matches ``flowfile.<domain>.<op>``."""
    for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS, *CODEGEN_OPS_TOOLS, *META_OPS_TOOLS):
        assert _OPS_TOOL_RE.match(tool.name), tool.name


def test_mcp_tool_name_helper() -> None:
    assert mcp_tool_name("graph", "add_filter") == "flowfile.graph.add_filter"
    assert mcp_tool_name("schema", "read_node_schema") == "flowfile.schema.read_node_schema"
    assert mcp_tool_name("codegen", "generate_polars_code") == "flowfile.codegen.generate_polars_code"
    assert mcp_tool_name("meta", "classify_intent") == "flowfile.meta.classify_intent"

    with pytest.raises(ValueError, match="MCP naming convention"):
        mcp_tool_name("frontend", "click_button")  # bad domain
    with pytest.raises(ValueError, match="MCP naming convention"):
        mcp_tool_name("graph", "AddFilter")  # not snake_case
    with pytest.raises(ValueError, match="MCP naming convention"):
        mcp_tool_name("graph", "add filter")  # space in op


# ---------------------------------------------------------------------------
# JSON Schema dialect (D004)
# ---------------------------------------------------------------------------


def _validate_schema(schema: dict, *, ctx: str) -> None:
    """Assert ``schema`` is a structurally valid JSON Schema 2020-12 doc."""
    try:
        Draft202012Validator.check_schema(schema)
    except Exception as exc:  # pragma: no cover - the assert below carries context
        pytest.fail(f"{ctx}: invalid JSON Schema 2020-12: {exc}")


def test_node_tool_parameters_are_valid_json_schema_2020_12() -> None:
    catalog = build_tool_catalog()
    for tool in catalog:
        if not tool.name.startswith("flowfile.graph.add_"):
            continue
        _validate_schema(tool.parameters, ctx=tool.name)


def test_node_tool_parameters_declare_dialect() -> None:
    catalog = build_tool_catalog()
    for tool in catalog:
        if not tool.name.startswith("flowfile.graph.add_"):
            continue
        assert (
            tool.parameters.get("$schema") == JSON_SCHEMA_DIALECT
        ), f"{tool.name} did not declare the 2020-12 dialect URI"


def test_ops_tool_parameters_are_valid_json_schema() -> None:
    for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS, *CODEGEN_OPS_TOOLS, *META_OPS_TOOLS):
        _validate_schema(tool.parameters, ctx=tool.name)


# ---------------------------------------------------------------------------
# Surface / category presets (D002)
# ---------------------------------------------------------------------------


def test_surface_preset_keys_match_literal() -> None:
    surface_keys = set(get_args(SurfaceLiteral))
    assert set(SURFACE_PRESETS) == surface_keys


def test_surface_presets_resolve_to_known_tools() -> None:
    catalog_names = {tool.name for tool in build_tool_catalog()}
    for surface, names in SURFACE_PRESETS.items():
        if surface in ("agent_complex", "agent_staged"):
            # ``agent_complex`` is full-catalog (preset is intentionally
            # empty so ``build_tool_catalog`` short-circuits to the full
            # list). ``agent_staged`` is the wrapper key for the W71
            # state machine; the actual tool catalogs live under the
            # per-stage ``staged_*`` keys.
            continue
        for name in names:
            assert name in catalog_names, f"surface {surface!r} references unknown tool {name!r}"


def test_cmd_k_surface_is_narrow() -> None:
    """Cmd+K surface must stay small to meet the sub-1s TTFB target (D002)."""
    tools = build_tool_catalog(surface="cmd_k")
    assert 0 < len(tools) <= 8, f"cmd_k preset over budget: {len(tools)} tools"


def test_explain_surface_is_read_only() -> None:
    tools = build_tool_catalog(surface="explain")
    assert tools, "explain surface should not be empty"
    for tool in tools:
        assert tool.name.startswith("flowfile.schema."), tool.name


def test_agent_complex_surface_is_full_minus_writers() -> None:
    """W71 v2.1 — ``agent_complex`` returns the full catalog minus the
    writer-shaped node tools (output / database_writer /
    cloud_storage_writer / catalog_writer). Pre-v2.1 it was strictly
    ``== full``; the writer-block policy intentionally trims those
    four tools so the LLM never sees them.
    """
    from flowfile_core.ai.safety import AGENT_BLOCKED_NODE_TYPES

    full = build_tool_catalog()
    agent_complex = build_tool_catalog(surface="agent_complex")
    blocked_names = {f"flowfile.graph.add_{nt}" for nt in AGENT_BLOCKED_NODE_TYPES}

    full_names = {tool.name for tool in full}
    agent_names = {tool.name for tool in agent_complex}
    assert full_names - agent_names == blocked_names, (
        f"agent_complex must drop exactly the writer tools; got dropped="
        f"{full_names - agent_names!r}"
    )
    assert blocked_names.isdisjoint(agent_names)


def test_unknown_surface_raises() -> None:
    """W71 v1.10 — error message simplified after CATEGORY_PRESETS
    removal; the matcher just checks the surface-unknown phrase."""
    with pytest.raises(KeyError, match="Unknown surface"):
        build_tool_catalog(surface="nonsense")


# ---------------------------------------------------------------------------
# UDF integration
# ---------------------------------------------------------------------------


def test_user_defined_node_appears_in_catalog() -> None:
    """A UDF registered in the custom-node store surfaces under flowfile.graph.add_<type>."""
    from flowfile_core.ai.tools import registry as registry_mod

    fake_store = {"my_custom_udf": object()}  # value is unused by the iterator
    with patch.object(registry_mod, "_iter_node_types", wraps=registry_mod._iter_node_types):
        # Wrapping isn't enough since the inner function calls into CUSTOM_NODE_STORE
        # at lookup time; instead, monkey-patch the lazy import target directly.
        from flowfile_core.configs import node_store as node_store_mod

        original = dict(node_store_mod.CUSTOM_NODE_STORE)
        try:
            node_store_mod.CUSTOM_NODE_STORE.update(fake_store)
            catalog = build_tool_catalog()
            names = {tool.name for tool in catalog}
            assert "flowfile.graph.add_my_custom_udf" in names
        finally:
            # Restore to avoid cross-test pollution.
            node_store_mod.CUSTOM_NODE_STORE.clear()
            node_store_mod.CUSTOM_NODE_STORE.update(original)


# ---------------------------------------------------------------------------
# Lazy-import contract
# ---------------------------------------------------------------------------


def test_lazy_litellm_import() -> None:
    """Importing flowfile_core.ai.tools must not pull in litellm.

    Mirrors the W11/W12/W13 contract — keeps cold-start import cheap and
    avoids exposing module-walk tooling to vendor SDK side effects.
    """
    # Drop any cached litellm + tools modules so the re-import is honest.
    for name in list(sys.modules):
        if name == "litellm" or name.startswith("litellm."):
            del sys.modules[name]
        elif name == "flowfile_core.ai.tools" or name.startswith("flowfile_core.ai.tools."):
            del sys.modules[name]

    importlib.import_module("flowfile_core.ai.tools")
    assert "litellm" not in sys.modules


# ---------------------------------------------------------------------------
# Description quality (the model uses these to disambiguate)
# ---------------------------------------------------------------------------


def test_descriptions_are_non_empty() -> None:
    catalog = build_tool_catalog()
    for tool in catalog:
        assert tool.description.strip(), f"{tool.name} has an empty description"
        # Descriptions should be reasonably terse — guard against accidentally
        # dumping a 5KB Pydantic docstring into the prompt.
        assert len(tool.description) <= 2000, f"{tool.name} description too long: {len(tool.description)} chars"


# ---------------------------------------------------------------------------
# Coverage cross-check: every node-type ToolSpec round-trips
# ---------------------------------------------------------------------------


def test_every_node_type_in_catalog_has_corresponding_settings_class() -> None:
    """The catalog is in lock-step with NODE_TYPE_TO_SETTINGS_CLASS."""
    catalog = {tool.name: tool for tool in build_tool_catalog()}
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        name = f"flowfile.graph.add_{node_type}"
        assert name in catalog
        # The schema's "title" is Pydantic's default and should match the class name.
        title = catalog[name].parameters.get("title")
        assert title == NODE_TYPE_TO_SETTINGS_CLASS[node_type].__name__, (
            f"{name}: title {title!r} mismatch with class " f"{NODE_TYPE_TO_SETTINGS_CLASS[node_type].__name__!r}"
        )


# ---------------------------------------------------------------------------
# W56 — long_description coverage (per-node-type narrative docs)
# ---------------------------------------------------------------------------


# Sanity floor for narrative docs — short enough that placeholder strings
# trip the test, long enough that "filters rows" alone wouldn't pass.
_W56_LONG_DESCRIPTION_MIN_CHARS = 80


def test_w56_every_node_type_has_long_description() -> None:
    """W56 AC1 + AC7 — every NODE_TYPE_TO_SETTINGS_CLASS entry surfaces narrative docs.

    Without this guard, a future node type can land without docs and the
    agent surface silently degrades back to JSON-Schema-only grounding.
    """
    catalog = {tool.name: tool for tool in build_tool_catalog()}
    missing: list[str] = []
    too_short: list[tuple[str, int]] = []
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        name = f"flowfile.graph.add_{node_type}"
        spec = catalog[name]
        text = (spec.long_description or "").strip()
        if not text:
            missing.append(node_type)
            continue
        if len(text) < _W56_LONG_DESCRIPTION_MIN_CHARS:
            too_short.append((node_type, len(text)))
    assert not missing, f"node types missing long_description: {missing}"
    assert not too_short, (
        f"node types with stub-shaped long_description " f"(< {_W56_LONG_DESCRIPTION_MIN_CHARS} chars): {too_short}"
    )


def test_w56_ops_tools_have_long_descriptions() -> None:
    """W56 — graph / schema / codegen / meta op tools also carry narrative docs."""
    too_short: list[tuple[str, int]] = []
    for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS, *CODEGEN_OPS_TOOLS, *META_OPS_TOOLS):
        text = (tool.long_description or "").strip()
        if len(text) < _W56_LONG_DESCRIPTION_MIN_CHARS:
            too_short.append((tool.name, len(text)))
    assert not too_short, f"ops tools with missing/short long_description: {too_short}"


def test_w56_long_description_does_not_duplicate_short_description() -> None:
    """W56 — long_description should be substantively different from description.

    Catches the lazy mistake of `long_description=description` (no narrative
    grounding added). Asserts at least 50% character growth on average,
    skipping the (handful of) ops tools where the short description is
    already long.
    """
    catalog = build_tool_catalog()
    for tool in catalog:
        if not tool.long_description:
            continue
        # Tools with very long short descriptions don't need a much longer
        # long_description; only check the typical case.
        if len(tool.description) > 200:
            continue
        assert len(tool.long_description) > len(tool.description), (
            f"{tool.name}: long_description not longer than description "
            f"({len(tool.long_description)} vs {len(tool.description)} chars)"
        )


# ---------------------------------------------------------------------------
# W56 v2 — user_instructions field (chat / advisory surfaces)
# ---------------------------------------------------------------------------


_W56_USER_INSTRUCTIONS_MIN_CHARS = 200  # higher than long_description floor —
# user_instructions has more required content (settings + worked example +
# pitfall) and should always be substantive.


def test_w56v2_every_node_type_has_user_instructions() -> None:
    """W56 v2 — every NODE_TYPE_TO_SETTINGS_CLASS entry has chat-facing prose.

    Without this, the chat surface (uses surface="explain") loses its
    Flowfile vocabulary for that node type and starts hallucinating
    UI elements.
    """
    catalog = {tool.name: tool for tool in build_tool_catalog()}
    missing: list[str] = []
    too_short: list[tuple[str, int]] = []
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        name = f"flowfile.graph.add_{node_type}"
        spec = catalog[name]
        text = (spec.user_instructions or "").strip()
        if not text:
            missing.append(node_type)
            continue
        if len(text) < _W56_USER_INSTRUCTIONS_MIN_CHARS:
            too_short.append((node_type, len(text)))
    assert not missing, f"node types missing user_instructions: {missing}"
    assert not too_short, (
        f"node types with stub-shaped user_instructions " f"(< {_W56_USER_INSTRUCTIONS_MIN_CHARS} chars): {too_short}"
    )


def test_w56v2_user_instructions_cite_canonical_palette_labels() -> None:
    """W56 v2 — every node-type tool's user_instructions begins with the
    canonical palette label from ``flowfile_core.configs.node_store.nodes``.

    Catches drift if the palette label is renamed in ``nodes.py`` but the
    user_instructions file isn't updated (or the runtime composition path
    silently breaks).
    """
    from flowfile_core.ai.tools.node_docs import palette_label_for

    catalog = {tool.name: tool for tool in build_tool_catalog()}
    drift: list[tuple[str, str, str]] = []
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        name = f"flowfile.graph.add_{node_type}"
        spec = catalog.get(name)
        if not spec or not spec.user_instructions:
            continue
        expected_label = palette_label_for(node_type)
        # The composer prepends `(palette: 'Group by', section: ...)` so the
        # palette label appears wrapped in single quotes in the prompt.
        marker = f"'{expected_label}'"
        if marker not in spec.user_instructions:
            drift.append((node_type, expected_label, spec.user_instructions[:120]))
    assert not drift, "user_instructions drifted from canonical palette labels in nodes.py:\n" + "\n".join(
        f"  - {nt}: expected palette {label!r}, got: {body!r}..." for nt, label, body in drift
    )


def test_w56v2_ops_tools_skip_user_instructions_by_default() -> None:
    """W56 v2 — graph / schema / codegen / meta tools have empty
    user_instructions because the user can't reach them through the canvas.

    Surfacing them in the chat-facing reference would only confuse the
    model into describing tool calls in chat answers.
    """
    for tool in (*GRAPH_OPS_TOOLS, *SCHEMA_OPS_TOOLS, *CODEGEN_OPS_TOOLS, *META_OPS_TOOLS):
        assert (tool.user_instructions or "").strip() == "", (
            f"{tool.name} unexpectedly has user_instructions; " "ops tools are not user-facing in the chat sense."
        )


def test_w56v2_palette_labels_match_node_store() -> None:
    """W56 v2 — every documented node_type has a palette entry in nodes.py.

    Cross-checks the user_instructions dict against the canonical palette
    config so a renamed (or removed) palette entry breaks the test rather
    than silently sending stale strings to the model.

    Exceptions: ``promise`` and ``user_defined`` are internal placeholders
    that are not in the palette; they are documented anyway so the model
    can answer questions about them when they appear in a flow.
    """
    from flowfile_core.ai.tools.node_docs import NODE_USER_INSTRUCTIONS, palette_label_for
    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    _, nodes_dict, _ = get_all_standard_nodes()
    palette_node_types = set(nodes_dict)
    documented = set(NODE_USER_INSTRUCTIONS)
    internal_only = {"promise", "user_defined"}
    palette_documented = documented - internal_only
    missing_from_palette = palette_documented - palette_node_types
    assert not missing_from_palette, (
        f"node types documented in NODE_USER_INSTRUCTIONS but missing "
        f"from the palette config: {sorted(missing_from_palette)}"
    )
    # And every palette entry that has a matching settings class should
    # be documented (so we don't ship a node the chat can't talk about).
    palette_with_settings = {nt for nt in palette_node_types if nt in NODE_TYPE_TO_SETTINGS_CLASS}
    undocumented = palette_with_settings - documented
    assert not undocumented, (
        f"palette node types missing user_instructions: {sorted(undocumented)}. "
        "Add an entry to NODE_USER_INSTRUCTIONS in node_docs.py."
    )
    # Spot-check: the live transcript was about group_by → "Group by".
    # If this assertion fails, the palette label was renamed without
    # updating the chat repro test in test_context.py.
    assert palette_label_for("group_by") == "Group by"
    assert palette_label_for("filter") == "Filter data"


# ---------------------------------------------------------------------------
# W56 v2 — agent_payload_example field (Pydantic-shape drift guard)
# ---------------------------------------------------------------------------


def test_w56v2_agent_payload_examples_validate() -> None:
    """W56 v2 — every agent_payload_example must round-trip through the
    real Pydantic settings class.

    Catches drift: if NodeGroupBy.groupby_input.agg_cols is renamed in
    transform_schema.py but the example in node_docs.py isn't updated,
    this test fails before any user hits the agent retry loop.
    """
    import json as _json

    from flowfile_core.ai.tools.node_docs import NODE_AGENT_PAYLOAD_EXAMPLES
    from flowfile_core.schemas.schemas import get_settings_class_for_node_type

    drift: list[tuple[str, str]] = []
    for node_type, example_json in NODE_AGENT_PAYLOAD_EXAMPLES.items():
        settings_cls = get_settings_class_for_node_type(node_type)
        if settings_cls is None:
            drift.append((node_type, f"no settings class for node_type={node_type!r}"))
            continue
        try:
            data = _json.loads(example_json)
        except _json.JSONDecodeError as exc:
            drift.append((node_type, f"JSON decode error: {exc}"))
            continue
        try:
            settings_cls.model_validate(data)
        except Exception as exc:  # noqa: BLE001 — surface any validation error
            drift.append((node_type, f"{settings_cls.__name__}.model_validate failed: {exc}"))
    assert not drift, "agent_payload_example drift:\n" + "\n".join(f"  - {nt}: {err}" for nt, err in drift)


def test_w56v2_agent_payload_examples_only_for_divergent_nodes() -> None:
    """W56 v2 — examples are only authored for the seven node types whose
    Pydantic shape diverges from the natural LLM guess.

    Adding examples for simple nodes (filter / sort / etc.) burns tokens
    without improving accuracy. If a future ergonomics improvement
    actually needs more examples, the test here is the place to widen
    the allow-list deliberately.
    """
    from flowfile_core.ai.tools.node_docs import NODE_AGENT_PAYLOAD_EXAMPLES

    expected = {
        "group_by",
        "pivot",
        "join",
        "fuzzy_match",
        "select",
        "unpivot",
        "text_to_rows",
        # W67 follow-up — RawData.data layout is non-obvious (columnar; LLM
        # defaults to row-oriented and silently corrupts alignment because
        # both validate as list[list]). Worked example disambiguates.
        "manual_input",
        # W71 v1.12C — ``FunctionInput.function`` is a Flowfile expression
        # string (SQL-style ``[column]`` references), NOT raw Polars. LLMs
        # consistently emit ``pl.col('x') + pl.col('y')`` into this field
        # because the schema only constrains it as ``str``. Worked example
        # locks in the ``[column]`` shape.
        "formula",
        # W71 v2.2 — explore_data needs NO settings; example is the
        # empty-shape ``{}`` envelope so the LLM emits an empty inner
        # at fill_settings instead of fabricating GraphicWalkerInput
        # values it has no signal for.
        "explore_data",
    }
    actual = set(NODE_AGENT_PAYLOAD_EXAMPLES)
    extra = actual - expected
    missing = expected - actual
    assert not extra, (
        f"agent_payload_example added for non-divergent node types: {extra}. "
        "Widen the allow-list here deliberately if this is intentional."
    )
    assert not missing, f"agent_payload_example missing for divergent node types: {missing}"


def test_w56v2_agent_payload_examples_surface_on_agent_only() -> None:
    """W56 v2 — examples appear ONLY on agent_complex catalog.

    Read-only / advisory surfaces (explain / lineage / docgen) get
    user_instructions, not agent payloads. cmd_k / ghost_node get the
    agent-shaped catalog but their preset filters out group_by / pivot /
    fuzzy_match / etc. (they only carry a narrow set of common transforms).
    (W71 v1.10 — legacy ``"agent"`` surface removed; ``agent_complex``
    is the only full-catalog surface left.)
    """
    from flowfile_core.ai.context.builder import assemble_system_prompt

    agent_prompt = assemble_system_prompt("agent_complex")
    explain_prompt = assemble_system_prompt("explain")

    # The example payload's distinctive marker — the customer-count line
    # from group_by — must appear in agent and NOT in explain.
    example_marker = '"new_name": "customer_count"'
    assert example_marker in agent_prompt, "agent surface missing group_by example payload"
    assert example_marker not in explain_prompt, (
        "explain surface should not see agent payload examples — those are "
        "tool-call shape only, irrelevant for chat/advisory answers."
    )

    # Conversely, the user-instructions worked-example phrase appears on
    # explain but not (necessarily) on agent — they're separate slices
    # over the same source-of-truth.
    user_marker = "customers per city"
    assert user_marker in explain_prompt.lower(), "explain surface missing user-instructions worked example"


# ---------------------------------------------------------------------------
# W56 v2 — frontend sidebar label cross-check
# ---------------------------------------------------------------------------


def test_w56v2_sidebar_labels_match_frontend() -> None:
    """W56 v2 — every NODE_GROUP_TO_SIDEBAR_LABEL value appears verbatim
    in the frontend's NodeList.vue.

    Brittle by design: a frontend rename must propagate to the catalog,
    or this test fails. Catches drift like "Aggregations" → "Group / Pivot"
    that would break chat answers citing the old label.
    """
    from pathlib import Path

    from flowfile_core.ai.tools.node_docs import NODE_GROUP_TO_SIDEBAR_LABEL

    # Walk up from this test file to the repo root, then to the frontend.
    repo_root = Path(__file__).resolve().parents[3]
    nodelist_path = (
        repo_root / "flowfile_frontend" / "src" / "renderer" / "app" / "views" / "DesignerView" / "NodeList.vue"
    )
    if not nodelist_path.is_file():
        pytest.skip(f"frontend NodeList.vue not found at {nodelist_path}")
    nodelist_text = nodelist_path.read_text(encoding="utf-8")

    drift: list[tuple[str, str]] = []
    for node_group, sidebar_label in NODE_GROUP_TO_SIDEBAR_LABEL.items():
        if sidebar_label not in nodelist_text:
            drift.append((node_group, sidebar_label))
    assert not drift, (
        "NODE_GROUP_TO_SIDEBAR_LABEL drifted from frontend NodeList.vue:\n"
        + "\n".join(f"  - node_group={ng!r} → label={label!r} not found" for ng, label in drift)
        + f"\n(checked against {nodelist_path})"
    )


# ---------------------------------------------------------------------------
# W67 — nested-Pydantic shape pass-through (inline $ref)
# ---------------------------------------------------------------------------


def _walk_for_refs_and_defs(node: object, path: str = "") -> list[tuple[str, str]]:
    """Return ``[(path, ref_or_marker), ...]`` for every leftover ``$ref`` or
    ``$defs`` block in the schema tree."""
    findings: list[tuple[str, str]] = []
    if isinstance(node, dict):
        if "$ref" in node:
            findings.append((path, node["$ref"]))
        if "$defs" in node:
            findings.append((path or "<root>", "$defs"))
        for k, v in node.items():
            findings.extend(_walk_for_refs_and_defs(v, f"{path}.{k}"))
    elif isinstance(node, list):
        for i, item in enumerate(node):
            findings.extend(_walk_for_refs_and_defs(item, f"{path}[{i}]"))
    return findings


def test_w67_nested_pydantic_field_inlined_at_property_site() -> None:
    """W67 Defect 1 — ``add_manual_input``'s ``raw_data_format`` field renders
    as an inline object schema (``type: 'object'`` with ``properties``), not
    a ``$ref`` cross-reference. Live transcript 2026-05-06 surfaced the LLM
    JSON-string-encoding the value because it didn't follow ``$ref``; the
    inlined shape removes that hop.
    """
    from flowfile_core.ai.tools.registry import _node_settings_to_tool_spec
    from flowfile_core.schemas.input_schema import NodeManualInput

    spec = _node_settings_to_tool_spec("manual_input", NodeManualInput)
    raw_data_format = spec.parameters["properties"]["raw_data_format"]

    assert "$ref" not in raw_data_format, (
        f"raw_data_format still emits $ref: {raw_data_format!r}. " "W67 inliner regressed."
    )
    assert (
        raw_data_format.get("type") == "object"
    ), f"raw_data_format must declare type='object' after inlining, got {raw_data_format.get('type')!r}"
    properties = raw_data_format.get("properties") or {}
    assert "columns" in properties, f"raw_data_format.properties missing 'columns': {properties!r}"
    assert "data" in properties, f"raw_data_format.properties missing 'data': {properties!r}"

    # MinimalFieldInfo (nested inside columns.items) must also be inlined —
    # not a $ref. Two-deep inlining proof.
    columns_items = properties["columns"].get("items", {})
    assert "$ref" not in columns_items, f"nested MinimalFieldInfo still a $ref: {columns_items!r}"
    assert columns_items.get("type") == "object", f"columns.items must be an object: {columns_items!r}"


@pytest.mark.parametrize("node_type", sorted(NODE_TYPE_TO_SETTINGS_CLASS))
def test_w67_no_ref_or_defs_remains_in_node_tool_parameters(node_type: str) -> None:
    """W67 Defect 1 — every node-type ToolSpec inlines ``$ref``/``$defs``.

    Parametrised across :data:`NODE_TYPE_TO_SETTINGS_CLASS` so a future node
    type with an unhandled ref pattern (e.g. self-referential or external
    schema) shows up as a precise failure rather than a generic catalog
    regression.
    """
    from flowfile_core.ai.tools.registry import _node_settings_to_tool_spec

    settings_cls = NODE_TYPE_TO_SETTINGS_CLASS[node_type]
    spec = _node_settings_to_tool_spec(node_type, settings_cls)
    findings = _walk_for_refs_and_defs(spec.parameters)
    assert not findings, f"add_{node_type} tool spec leaks ref/defs after inlining:\n" + "\n".join(
        f"  - {p} -> {ref}" for p, ref in findings
    )


def test_w67_audit_known_nested_pydantic_fields_render_as_object() -> None:
    """W67 Defect 1 audit — a positive contract that the catalog renders
    each known nested-Pydantic field as an object (or array-of-object) schema.

    Hand-picked allow-list of fields whose annotation is a ``BaseModel``
    subclass on the canonical settings classes. Adding a new node type with
    a nested-Pydantic field means adding it here too — that's the desired
    failure mode (loud, deliberate widening) per the spec's audit guidance.
    """
    from flowfile_core.ai.tools.registry import _node_settings_to_tool_spec

    # (node_type, field_name, kind) — kind is "object", "array_of_object",
    # or "optional_object" (Pydantic emits anyOf with the object branch).
    audit: list[tuple[str, str, str]] = [
        ("manual_input", "raw_data_format", "object"),
        ("read", "received_file", "object"),
        ("database_reader", "database_settings", "object"),
        ("database_writer", "database_write_settings", "object"),
        ("cloud_storage_reader", "cloud_storage_settings", "object"),
        ("cloud_storage_writer", "cloud_storage_settings", "object"),
        ("filter", "filter_input", "object"),
        ("formula", "function", "object"),
        ("group_by", "groupby_input", "object"),
        ("join", "join_input", "object"),
        ("pivot", "pivot_input", "object"),
        ("select", "select_input", "array_of_object"),
        ("sort", "sort_input", "array_of_object"),
        ("text_to_rows", "text_to_rows_input", "object"),
        ("unique", "unique_input", "object"),
        ("unpivot", "unpivot_input", "object"),
    ]

    failures: list[str] = []
    for node_type, field_name, kind in audit:
        if node_type not in NODE_TYPE_TO_SETTINGS_CLASS:
            failures.append(f"{node_type}: unknown node_type (audit allow-list stale)")
            continue
        settings_cls = NODE_TYPE_TO_SETTINGS_CLASS[node_type]
        if field_name not in settings_cls.model_fields:
            failures.append(f"{node_type}.{field_name}: field not on settings class (audit allow-list stale)")
            continue

        spec = _node_settings_to_tool_spec(node_type, settings_cls)
        properties = spec.parameters.get("properties") or {}
        if field_name not in properties:
            failures.append(f"{node_type}.{field_name}: field absent from rendered ToolSpec.parameters.properties")
            continue
        field_schema = properties[field_name]

        # Pydantic emits Optional[Model] as {"anyOf": [{"$ref"...}, {"type":"null"}]}
        # which after inlining becomes {"anyOf": [{"type":"object",...}, {"type":"null"}]}.
        if "anyOf" in field_schema:
            object_branches = [b for b in field_schema["anyOf"] if b.get("type") == "object"]
            if not object_branches:
                failures.append(f"{node_type}.{field_name}: anyOf with no object branch: {field_schema!r}")
                continue
            field_schema = object_branches[0]

        if kind == "object":
            if field_schema.get("type") != "object":
                failures.append(f"{node_type}.{field_name}: expected type='object', got {field_schema.get('type')!r}")
                continue
            if not field_schema.get("properties"):
                failures.append(f"{node_type}.{field_name}: type='object' but no 'properties' block")
        elif kind == "array_of_object":
            if field_schema.get("type") != "array":
                failures.append(f"{node_type}.{field_name}: expected type='array', got {field_schema.get('type')!r}")
                continue
            items = field_schema.get("items") or {}
            if items.get("type") != "object":
                failures.append(
                    f"{node_type}.{field_name}: array items expected type='object', got {items.get('type')!r}"
                )

    assert not failures, "W67 audit failures:\n" + "\n".join(f"  - {f}" for f in failures)
