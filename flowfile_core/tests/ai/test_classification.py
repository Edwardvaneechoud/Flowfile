"""Node-type classification tests."""

from __future__ import annotations

from flowfile_core.ai.tools.classification import (
    classify_node_type,
    is_predictable_via_mirror,
)
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS


def test_every_known_node_type_classified() -> None:
    """Every entry in the production registry must have an explicit class."""
    for node_type in NODE_TYPE_TO_SETTINGS_CLASS:
        cls = classify_node_type(node_type)
        assert cls in {"static", "dynamic", "source", "passthrough"}, f"unknown classification for {node_type}: {cls}"


def test_dynamic_set_matches_design() -> None:
    expected_dynamic = {
        "pivot",
        "unpivot",
        "text_to_rows",
        "graph_solver",
        "python_script",
        "polars_code",
        "sql_query",
        "user_defined",
    }
    actual_dynamic = {nt for nt in NODE_TYPE_TO_SETTINGS_CLASS if classify_node_type(nt) == "dynamic"}
    assert actual_dynamic == expected_dynamic


def test_source_set_includes_all_readers() -> None:
    expected_source = {
        "manual_input",
        "read",
        "database_reader",
        "cloud_storage_reader",
        "catalog_reader",
        "kafka_source",
        "google_analytics_reader",
        "rest_api_reader",
        "external_source",
    }
    actual_source = {nt for nt in NODE_TYPE_TO_SETTINGS_CLASS if classify_node_type(nt) == "source"}
    assert actual_source == expected_source


def test_static_includes_writers_and_models() -> None:
    """Writers (database_writer / cloud_storage_writer / catalog_writer) and ML
    nodes (train/apply/evaluate_model) are static — schema is passthrough or
    fixed."""
    static_set = {nt for nt in NODE_TYPE_TO_SETTINGS_CLASS if classify_node_type(nt) == "static"}
    assert {"database_writer", "cloud_storage_writer", "catalog_writer"} <= static_set
    assert {"train_model", "apply_model", "evaluate_model"} <= static_set


def test_unknown_node_type_defaults_to_dynamic() -> None:
    """Unknown types default to dynamic — fail-closed under the kernel-dry-run
    refusal path is safer than silently returning a wrong static prediction."""
    assert classify_node_type("definitely_not_a_real_node_type") == "dynamic"


def test_is_predictable_via_mirror_excludes_dynamic() -> None:
    assert is_predictable_via_mirror("filter")
    assert is_predictable_via_mirror("manual_input")
    assert is_predictable_via_mirror("promise")
    assert not is_predictable_via_mirror("polars_code")
    assert not is_predictable_via_mirror("python_script")
    assert not is_predictable_via_mirror("pivot")


def test_classify_lazy_litellm() -> None:
    """Importing the classification module must not pull litellm."""
    import sys

    # Module is already imported by the test runner; just assert litellm
    # didn't sneak in on its dependency chain.
    leaked = [m for m in sys.modules if m == "litellm" or m.startswith("litellm.")]
    assert not leaked, f"litellm leaked from classification import: {leaked}"
