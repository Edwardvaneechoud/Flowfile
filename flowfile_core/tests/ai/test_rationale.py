from flowfile_core.ai.agents.planner.rationale import _arg_summary_for_add


def _entry(name: str) -> dict:
    return {"field": {"name": name, "data_type": "Auto"}, "function": "[a] + 1"}


def test_single_formula_names_its_output_column():
    assert _arg_summary_for_add("formula", {"functions": [_entry("total")]}).endswith("`total`")
    assert _arg_summary_for_add("formula", {"function": _entry("total")}).endswith("`total`")


def test_multi_entry_formula_counts_columns():
    assert _arg_summary_for_add("formula", {"functions": [_entry("a"), _entry("b")]}).endswith("2 columns")
