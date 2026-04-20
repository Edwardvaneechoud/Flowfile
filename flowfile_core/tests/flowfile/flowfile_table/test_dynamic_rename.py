import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import transform_schema


def _engine() -> FlowDataEngine:
    return FlowDataEngine(
        pl.DataFrame(
            {
                "name": ["a", "b"],
                "age": [1, 2],
                "score": [0.1, 0.2],
            }
        )
    )


def test_prefix_all_renames_every_column():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(rename_mode="prefix", prefix="src_")
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["src_name", "src_age", "src_score"]


def test_suffix_list_only_renames_listed_columns():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="suffix",
        suffix="_raw",
        selection_mode="list",
        selected_columns=["name", "does_not_exist"],
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    # Only 'name' is renamed; missing columns are silently skipped; others pass through.
    assert result == ["name_raw", "age", "score"]


def test_data_type_numeric_group():
    engine = _engine()
    # data_type_group: name=String, age=Numeric, score=Numeric
    settings = transform_schema.DynamicRenameInput(
        rename_mode="prefix",
        prefix="num_",
        selection_mode="data_type",
        selected_data_type="Numeric",
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "num_age", "num_score"]


def test_data_type_string_group():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="suffix",
        suffix="_text",
        selection_mode="data_type",
        selected_data_type="String",
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name_text", "age", "score"]


def test_data_type_date_group_matches_date_columns():
    from datetime import date

    engine = FlowDataEngine(
        pl.DataFrame(
            {
                "id": [1, 2],
                "booked_on": [date(2024, 1, 1), date(2024, 1, 2)],
            }
        )
    )
    settings = transform_schema.DynamicRenameInput(
        rename_mode="prefix",
        prefix="ts_",
        selection_mode="data_type",
        selected_data_type="Date",
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["id", "ts_booked_on"]


def test_data_type_group_none_is_noop():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="prefix",
        prefix="x_",
        selection_mode="data_type",
        selected_data_type=None,
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "age", "score"]


def test_formula_uppercase_uses_column_name_reference():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="formula",
        formula="uppercase([column_name])",
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["NAME", "AGE", "SCORE"]


def test_formula_literal_concat():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="formula",
        formula='"v2_" + [column_name]',
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["v2_name", "v2_age", "v2_score"]


def test_duplicate_between_renamed_columns_raises():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="formula",
        formula='"x"',
    )
    with pytest.raises(ValueError, match="duplicate column name"):
        engine.apply_dynamic_rename(settings)


def test_duplicate_with_passthrough_column_raises():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="suffix",
        suffix="",  # no-op suffix
        selection_mode="list",
        selected_columns=["name"],
    )
    # No-op renames produce no conflicts — verify the happy path survives.
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "age", "score"]

    # Now rename `age` to collide with the passthrough `name` column.
    collide = transform_schema.DynamicRenameInput(
        rename_mode="formula",
        formula='"name"',
        selection_mode="list",
        selected_columns=["age"],
    )
    with pytest.raises(ValueError, match="duplicate column name"):
        engine.apply_dynamic_rename(collide)


def test_empty_list_selection_is_noop():
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="prefix",
        prefix="src_",
        selection_mode="list",
        selected_columns=[],
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "age", "score"]


def test_resolve_map_matches_apply_output():
    engine = _engine()
    columns = [(c.column_name, c.data_type_group) for c in engine.schema]
    settings = transform_schema.DynamicRenameInput(rename_mode="prefix", prefix="p_")
    rename_map = FlowDataEngine.resolve_dynamic_rename_map(columns, settings)
    assert rename_map == {"name": "p_name", "age": "p_age", "score": "p_score"}


def test_empty_formula_is_noop():
    """An empty formula string must leave column names untouched (no `to_expr` call)."""
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(rename_mode="formula", formula="")
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "age", "score"]


def test_whitespace_only_formula_is_noop():
    """A whitespace-only formula is treated the same as an empty one."""
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(rename_mode="formula", formula="   \t\n")
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "age", "score"]


def test_scalar_literal_broadcasts_to_single_target():
    """A scalar/literal formula result (length 1) should broadcast when there is
    exactly one target column; the test verifies the broadcast path in
    `_compute_renamed_names` produces a valid single-column rename."""
    engine = _engine()
    settings = transform_schema.DynamicRenameInput(
        rename_mode="formula",
        formula='"renamed"',
        selection_mode="list",
        selected_columns=["age"],
    )
    result = engine.apply_dynamic_rename(settings).data_frame.collect_schema().names()
    assert result == ["name", "renamed", "score"]


def test_formula_with_no_targets_returns_empty_map():
    """If selection yields zero targets, `resolve_dynamic_rename_map` must short-circuit
    before invoking the formula engine (so an otherwise-invalid formula is never parsed)."""
    columns = [("name", "String"), ("age", "Numeric")]
    settings = transform_schema.DynamicRenameInput(
        rename_mode="formula",
        formula="this would not parse (((",
        selection_mode="list",
        selected_columns=["does_not_exist"],
    )
    assert FlowDataEngine.resolve_dynamic_rename_map(columns, settings) == {}
