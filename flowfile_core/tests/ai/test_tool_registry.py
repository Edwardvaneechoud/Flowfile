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
    CATEGORY_NAMES,
    CATEGORY_PRESETS,
    CODEGEN_OPS_TOOLS,
    GRAPH_OPS_TOOLS,
    JSON_SCHEMA_DIALECT,
    META_OPS_TOOLS,
    SCHEMA_OPS_TOOLS,
    SURFACE_PRESETS,
    SurfaceLiteral,
    ToolCategory,
    build_tool_catalog,
    mcp_tool_name,
    pick_category,
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


def test_w46_unimplemented_stubs_absent_from_catalog() -> None:
    """W46 — the three W31 stub tools must not be in the catalog.

    ``update_node_settings`` / ``run_node`` / ``propose_subgraph`` were
    advertised in the catalog but the executor refused them. The LLM kept
    burning retries on stubs. W46 dropped the entries; the executor's
    rejection branch stays as defence-in-depth.
    """
    catalog = build_tool_catalog()
    names = {tool.name for tool in catalog}
    assert "flowfile.graph.update_node_settings" not in names
    assert "flowfile.graph.run_node" not in names
    assert "flowfile.graph.propose_subgraph" not in names

    # And not in any preset / category lookup either (defence vs
    # `_UNIVERSAL_OP_NAMES` derivation drift).
    from flowfile_core.ai.tools.registry import (
        CATEGORY_PRESETS,
        SURFACE_PRESETS,
    )

    forbidden = {
        "flowfile.graph.update_node_settings",
        "flowfile.graph.run_node",
        "flowfile.graph.propose_subgraph",
    }
    for surface, preset in SURFACE_PRESETS.items():
        assert not (forbidden & preset), f"surface {surface!r} contains a removed stub: {forbidden & preset}"
    for category, preset in CATEGORY_PRESETS.items():
        assert not (forbidden & preset), f"category {category!r} contains a removed stub: {forbidden & preset}"


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
    assert mcp_tool_name("meta", "pick_category") == "flowfile.meta.pick_category"

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


def test_category_preset_keys_match_literal() -> None:
    category_keys = set(get_args(ToolCategory))
    assert set(CATEGORY_PRESETS) == category_keys
    assert set(CATEGORY_PRESETS) == set(CATEGORY_NAMES)


def test_surface_presets_resolve_to_known_tools() -> None:
    catalog_names = {tool.name for tool in build_tool_catalog()}
    for surface, names in SURFACE_PRESETS.items():
        if surface == "agent_complex":
            # agent_complex is documented as full-catalog; preset entry is
            # intentionally empty so build_tool_catalog short-circuits.
            continue
        for name in names:
            assert name in catalog_names, f"surface {surface!r} references unknown tool {name!r}"


def test_category_presets_resolve_to_known_tools() -> None:
    catalog_names = {tool.name for tool in build_tool_catalog()}
    for category, names in CATEGORY_PRESETS.items():
        for name in names:
            assert name in catalog_names, f"category {category!r} references unknown tool {name!r}"


def test_cmd_k_surface_is_narrow() -> None:
    """Cmd+K surface must stay small to meet the sub-1s TTFB target (D002)."""
    tools = build_tool_catalog(surface="cmd_k")
    assert 0 < len(tools) <= 8, f"cmd_k preset over budget: {len(tools)} tools"


def test_explain_surface_is_read_only() -> None:
    tools = build_tool_catalog(surface="explain")
    assert tools, "explain surface should not be empty"
    for tool in tools:
        assert tool.name.startswith("flowfile.schema."), tool.name


def test_agent_surface_first_stage_is_meta_only() -> None:
    """The first stage of D002's two-stage flow exposes only pick_category."""
    tools = build_tool_catalog(surface="agent")
    assert [tool.name for tool in tools] == ["flowfile.meta.pick_category"]


def test_agent_complex_surface_is_full() -> None:
    full = build_tool_catalog()
    agent_complex = build_tool_catalog(surface="agent_complex")
    assert {tool.name for tool in full} == {tool.name for tool in agent_complex}


def test_transformations_category_includes_universal_ops() -> None:
    """Every category surface is augmented with graph + schema ops at lookup."""
    tools = build_tool_catalog(surface="transformations")
    names = {tool.name for tool in tools}
    # At least one transformation node tool
    assert "flowfile.graph.add_filter" in names
    # Universal graph op is always present
    assert "flowfile.graph.connect" in names
    # Universal schema op is always present
    assert "flowfile.schema.read_node_schema" in names
    # ML-only category tool is *not* present
    assert "flowfile.graph.add_train_model" not in names


def test_io_category_includes_io_node_tools() -> None:
    tools = build_tool_catalog(surface="io")
    names = {tool.name for tool in tools}
    assert "flowfile.graph.add_read" in names
    assert "flowfile.graph.add_database_reader" in names
    assert "flowfile.graph.add_cloud_storage_reader" in names
    # Transformation tools are *not* in the io category
    assert "flowfile.graph.add_filter" not in names


def test_code_category_includes_codegen_ops() -> None:
    tools = build_tool_catalog(surface="code")
    names = {tool.name for tool in tools}
    assert "flowfile.codegen.generate_polars_code" in names
    assert "flowfile.codegen.generate_python_script" in names
    assert "flowfile.codegen.generate_sql_query" in names
    # The corresponding node-type tools are also present so the executor
    # can stage the generated body via add_polars_code etc.
    assert "flowfile.graph.add_polars_code" in names


def test_unknown_surface_raises() -> None:
    with pytest.raises(KeyError, match="Unknown surface or category"):
        build_tool_catalog(surface="nonsense")


# ---------------------------------------------------------------------------
# pick_category (heuristic)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "intent,expected",
    [
        ("filter rows where region == 'EU'", "transformations"),
        ("rename column foo to bar", "transformations"),
        ("sort by revenue desc", "transformations"),
        ("join customers and orders", "joins"),
        ("merge on customer_id", "joins"),
        ("group by region and average revenue", "aggregations"),
        ("pivot the table on quarter", "aggregations"),
        ("read parquet from s3", "io"),
        ("write csv to disk", "io"),
        ("run polars code to compute lag", "code"),
        ("python script to call api", "code"),
        ("train model on this dataset", "ml"),
        ("predict scores", "ml"),
        ("delete node 17", "graph"),
        ("connect node 12 to node 18", "graph"),
    ],
)
def test_pick_category_heuristic(intent: str, expected: str) -> None:
    assert pick_category(intent) == expected


def test_pick_category_falls_back_on_empty_intent() -> None:
    assert pick_category("") == "transformations"
    assert pick_category("   ") == "transformations"
    # Pure noise that hits no keyword falls back to default.
    assert pick_category("xyzzy plugh") == "transformations"


def test_pick_category_respects_default_argument() -> None:
    assert pick_category("xyzzy plugh", default="meta") == "meta"


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
