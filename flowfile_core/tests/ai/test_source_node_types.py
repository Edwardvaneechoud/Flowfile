"""The source-node-type list stays derived from the registry and AI-creatable.

Guards the de-duplication of the old hard-coded list: every prose mention of
"source nodes" derives from ``get_source_node_types()`` (the registry truth),
every such node must be creatable by the AI (so the LLM is never told about a
node it cannot add), and the static prompt files can't silently drift.
"""

from __future__ import annotations

from flowfile_core.ai.context.builder import _PROMPTS_DIR
from flowfile_core.ai.safety import AGENT_BLOCKED_NODE_TYPES
from flowfile_core.ai.tools.classification import _NODE_CLASS_MAP
from flowfile_core.configs.node_store.nodes import get_source_node_types
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS

EXPECTED_SOURCE_TYPES = {
    "catalog_reader",
    "cloud_storage_reader",
    "database_reader",
    "external_source",
    "google_analytics_reader",
    "kafka_source",
    "manual_input",
    "read",
    "rest_api_reader",
}

# LLM-facing prompts that inline the source list. They're static text (no
# templating), so this drift guard turns a silent desync into a CI failure.
_PROMPT_FILES_WITH_SOURCE_LIST = ("base", "planner", "stage_classify")


def test_get_source_node_types_matches_registry():
    sources = get_source_node_types()
    assert set(sources) == EXPECTED_SOURCE_TYPES
    # Sorted, de-duplicated, and the internal dict-only node is excluded.
    assert list(sources) == sorted(sources)
    assert "polars_lazy_frame" not in sources


def test_source_nodes_are_ai_creatable():
    """Every source mentioned to the LLM must be a node the AI can actually add."""
    creatable = set(NODE_TYPE_TO_SETTINGS_CLASS) - set(AGENT_BLOCKED_NODE_TYPES)
    not_creatable = set(get_source_node_types()) - creatable
    assert not not_creatable, (
        f"source nodes not AI-creatable: {sorted(not_creatable)} — either expose them "
        "to the AI or filter the prose lists so the LLM isn't told about nodes it can't add"
    )


def test_node_class_map_sources_match_registry():
    """The hand-maintained classification map must agree with the registry."""
    class_map_sources = {nt for nt, cls in _NODE_CLASS_MAP.items() if cls == "source"}
    assert class_map_sources == set(get_source_node_types())


def test_prompt_files_mention_every_source_type():
    """Each LLM prompt that lists source nodes must list all of them (no drift)."""
    for name in _PROMPT_FILES_WITH_SOURCE_LIST:
        text = (_PROMPTS_DIR / f"{name}.md").read_text(encoding="utf-8")
        missing = [s for s in get_source_node_types() if f"``{s}``" not in text]
        assert not missing, f"{name}.md is missing source types: {missing}"
