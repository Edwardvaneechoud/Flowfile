"""One case per placeholder predicate, each with its negative control.

The negative controls carry the weight: a predicate that placeholders
everything is as broken as one that placeholders nothing.
"""

import pytest

from flowfile_core.flowfile.share.compatibility import (
    PLACEHOLDER_SENTINEL_SUFFIX,
    classify_node,
    placeholder_type,
)
from flowfile_core.flowfile.share.filter_translation import rewrite_filter_settings, translate_advanced_filter


def _classify(node_type: str, settings: dict | None = None):
    return classify_node(1, node_type, settings)


def _identity_select(*names):
    return {"renames": [{"old_name": name, "new_name": name, "keep": True} for name in names]}


SUPPORTED_CASES = [
    ("basic filter", "filter", {"filter_input": {"mode": "basic", "basic_filter": {"field": "a"}}}),
    (
        "inner identity join",
        "join",
        {
            "join_input": {
                "how": "inner",
                "join_mapping": [{"left_col": "id", "right_col": "id"}],
                "left_select": _identity_select("id", "name"),
                "right_select": _identity_select("id", "city"),
            }
        },
    ),
    (
        "plain rename select",
        "select",
        {"keep_missing": False, "select_input": [{"old_name": "a", "new_name": "b", "keep": True}]},
    ),
    (
        # The browser select casts like core does (non-strict, before rename).
        "select changing a type",
        "select",
        {
            "keep_missing": False,
            "select_input": [{"old_name": "a", "new_name": "a", "keep": True, "data_type": "String", "data_type_change": True}],
        },
    ),
    (
        "whitelisted group_by aggregations",
        "group_by",
        {
            "groupby_input": {
                "agg_cols": [
                    {"old_name": "city", "agg": "groupby"},
                    {"old_name": "amount", "agg": "n_unique"},
                    {"old_name": "amount", "agg": "concat"},
                ]
            }
        },
    ),
    ("whitelisted pivot aggregations", "pivot", {"pivot_input": {"aggregations": ["sum", "median"]}}),
    ("ungrouped record id", "record_id", {"record_id_input": {"group_by": False}}),
    ("csv read", "read", {"received_file": {"file_type": "csv", "scan_mode": "single_file"}}),
    ("parquet write", "output", {"output_settings": {"file_type": "parquet"}}),
    ("sort", "sort", {"sort_input": [{"column": "a"}]}),
    ("manual input", "manual_input", {"raw_data_format": {"columns": [], "data": []}}),
]

PLACEHOLDER_CASES = [
    ("locked node type", "database_reader", {}, "Runs only in the full Flowfile app"),
    ("absent node type", "run_flow", {}, "Not available in the browser version"),
    ("catalog reader", "catalog_reader", {}, "browser catalog"),
    ("catalog writer", "catalog_writer", {}, "browser catalog"),
    ("polars code", "polars_code", {"polars_code_input": {"polars_code": "output_df = input_df"}}, "Custom Python"),
    ("flow input", "flow_input", {}, "Subflow inputs"),
    (
        "advanced filter",
        "filter",
        {"filter_input": {"mode": "advanced", "advanced_filter": "[a] > 1"}},
        "executable code",
    ),
    (
        "split filter",
        "filter",
        {"filter_input": {"mode": "basic", "basic_filter": {"field": "a"}}, "split_mode": True},
        "Two-output",
    ),
    (
        "non-inner join",
        "join",
        {"join_input": {"how": "left", "left_select": _identity_select("id"), "right_select": _identity_select("id")}},
        "inner joins",
    ),
    (
        "join dropping a column",
        "join",
        {
            "join_input": {
                "how": "inner",
                "left_select": {"renames": [{"old_name": "id", "new_name": "id", "keep": False}]},
                "right_select": _identity_select("id"),
            }
        },
        "renames or drops",
    ),
    (
        "join renaming a column",
        "join",
        {
            "join_input": {
                "how": "inner",
                "left_select": _identity_select("id"),
                "right_select": {"renames": [{"old_name": "id", "new_name": "id_right", "keep": True}]},
            }
        },
        "renames or drops",
    ),
    ("select keeping unlisted columns", "select", {"keep_missing": True, "select_input": []}, "drops columns"),
    ("per-group record id", "record_id", {"record_id_input": {"group_by": True}}, "Per-group"),
    (
        "unknown group_by aggregation",
        "group_by",
        {"groupby_input": {"agg_cols": [{"old_name": "a", "agg": "std"}]}},
        "'std' aggregation",
    ),
    ("pivot n_unique", "pivot", {"pivot_input": {"aggregations": ["n_unique"]}}, "'n_unique' aggregation"),
    ("json read", "read", {"received_file": {"file_type": "json"}}, "cannot read json"),
    (
        "directory read",
        "read",
        {"received_file": {"file_type": "csv", "scan_mode": "directory"}},
        "whole directory",
    ),
    ("json write", "output", {"output_settings": {"file_type": "json"}}, "cannot write json"),
]


@pytest.mark.parametrize(
    "node_type,settings", [(t, s) for _, t, s in SUPPORTED_CASES], ids=[case[0] for case in SUPPORTED_CASES]
)
def test_supported_nodes_travel_as_themselves(node_type, settings):
    status = _classify(node_type, settings)
    assert status.status == "supported"
    assert status.reason is None


@pytest.mark.parametrize(
    "node_type,settings,reason_fragment",
    [(t, s, r) for _, t, s, r in PLACEHOLDER_CASES],
    ids=[case[0] for case in PLACEHOLDER_CASES],
)
def test_incompatible_nodes_become_placeholders(node_type, settings, reason_fragment):
    status = _classify(node_type, settings)
    assert status.status == "placeholder"
    assert reason_fragment in status.reason


def test_n_unique_splits_group_by_from_pivot():
    """The browser's pivot implements fewer aggregations than its group_by."""
    group_by = _classify("group_by", {"groupby_input": {"agg_cols": [{"old_name": "a", "agg": "n_unique"}]}})
    pivot = _classify("pivot", {"pivot_input": {"aggregations": ["n_unique"]}})
    assert group_by.status == "supported"
    assert pivot.status == "placeholder"


def test_missing_settings_never_crash_a_predicate():
    for node_type in ("filter", "join", "select", "record_id", "group_by", "pivot", "read", "output"):
        assert _classify(node_type, None).status in {"supported", "placeholder"}


def test_placeholder_type_uses_the_sentinel_suffix():
    assert placeholder_type("join") == f"join{PLACEHOLDER_SENTINEL_SUFFIX}"


TRANSLATABLE_FILTERS = [
    ("greater than a whole number", "[quantity] > 7", "quantity", "greater_than", "7"),
    ("no spaces", "[quantity]>7", "quantity", "greater_than", "7"),
    ("single equals", '[city] = "Amsterdam"', "city", "equals", "Amsterdam"),
    ("double equals", '[city] == "Amsterdam"', "city", "equals", "Amsterdam"),
    ("single-quoted text", "[city] = 'Amsterdam'", "city", "equals", "Amsterdam"),
    ("not equals", "[quantity] != 7", "quantity", "not_equals", "7"),
    ("greater than or equals", "[quantity] >= 7", "quantity", "greater_than_or_equals", "7"),
    ("less than", "[quantity] < 7", "quantity", "less_than", "7"),
    ("less than or equals", "[quantity] <= 7", "quantity", "less_than_or_equals", "7"),
    ("negative bound", "[balance] > -5", "balance", "greater_than", "-5"),
    ("parenthesised", "([quantity] > 7)", "quantity", "greater_than", "7"),
    ("column name with a space", "[unit price] > 7", "unit price", "greater_than", "7"),
    ("empty text", '[city] = ""', "city", "equals", ""),
]

UNTRANSLATABLE_FILTERS = [
    ("two conditions", "[a] > 1 and [b] < 2"),
    ("a function call", 'contains([city], "dam")'),
    ("a computed left side", "[a] + 1 > 2"),
    ("a wrapped left side", "length([a]) > 3"),
    ("a negated comparison", "not([a] > 1)"),
    ("column against column", "[a] = [b]"),
    ("a literal on the left", "7 < [a]"),
    # int() rejects a fractional bound on the browser's integer columns.
    ("a fractional bound", "[price] > 7.5"),
    ("a boolean literal", "[flag] = true"),
    ("a null literal", "[a] = null"),
    ("a flow parameter", "[a] > ${threshold}"),
    ("an unparseable expression", "[a] > "),
    ("nothing at all", ""),
]


@pytest.mark.parametrize(
    "expression,field,operator,value",
    [(e, f, o, v) for _, e, f, o, v in TRANSLATABLE_FILTERS],
    ids=[case[0] for case in TRANSLATABLE_FILTERS],
)
def test_plain_comparisons_become_basic_filters(expression, field, operator, value):
    assert translate_advanced_filter(expression) == {
        "mode": "basic",
        "basic_filter": {"field": field, "operator": operator, "value": value},
        "advanced_filter": "",
    }


@pytest.mark.parametrize(
    "expression", [e for _, e in UNTRANSLATABLE_FILTERS], ids=[case[0] for case in UNTRANSLATABLE_FILTERS]
)
def test_anything_beyond_one_comparison_is_not_translated(expression):
    assert translate_advanced_filter(expression) is None


def test_a_translated_filter_classifies_as_supported():
    """The end of the chain: the rewritten settings are what the check reads."""
    settings = {"filter_input": {"mode": "advanced", "advanced_filter": "[quantity] > 7"}}
    assert _classify("filter", settings).status == "placeholder"

    rewritten = rewrite_filter_settings(settings)
    assert _classify("filter", rewritten).status == "supported"
    assert "advanced_filter" not in str(rewritten["filter_input"]["basic_filter"])


def test_translation_never_touches_a_basic_filter():
    settings = {"filter_input": {"mode": "basic", "basic_filter": {"field": "a", "operator": "equals", "value": "1"}}}
    assert rewrite_filter_settings(settings) is None


def test_a_translatable_split_filter_stays_a_placeholder():
    """Translation removes one reason to demote a node, never all of them."""
    settings = {"filter_input": {"mode": "advanced", "advanced_filter": "[a] > 1"}, "split_mode": True}
    assert _classify("filter", rewrite_filter_settings(settings)).status == "placeholder"
