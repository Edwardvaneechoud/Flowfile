"""Engine-level tests for the multi-field formula operation.

Covers both the pure expression builder and the applied `FlowDataEngine` path.

Run with:
    pytest flowfile_core/tests/flowfile/flowfile_table/test_multi_field_formula.py -v
"""
from datetime import date, datetime

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import transform_schema

BASE_DATA = {"name": ["ada", "bob"], "age": [30, 40], "score": [0.5, 1.5]}

MIXED_DATA = {
    "label": ["x", "y"],
    "qty": [1, 2],
    "booked_on": [date(2024, 1, 1), date(2024, 1, 2)],
    "seen_at": [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0)],
    "active": [True, False],
}

MONTH_DATA = {"jan": [10.0, 20.0], "feb": [30.0, 40.0], "Total ": [100.0, 200.0]}


def _engine(data: dict | None = None) -> FlowDataEngine:
    """Wrap a LazyFrame so every assertion exercises the lazy path core uses."""
    return FlowDataEngine(pl.LazyFrame(data if data is not None else BASE_DATA))


def _apply(data: dict | None = None, **kwargs) -> FlowDataEngine:
    return _engine(data).apply_multi_field_formula(transform_schema.MultiFieldFormulaInput(**kwargs))


def _names(data: dict | None = None, **kwargs) -> list[str]:
    return _apply(data, **kwargs).data_frame.collect_schema().names()


def _rows(data: dict | None = None, **kwargs) -> list[dict]:
    return _apply(data, **kwargs).data_frame.collect().to_dicts()


def _schema(data: dict | None = None, **kwargs) -> pl.Schema:
    return _apply(data, **kwargs).data_frame.collect_schema()


# Output modes


def test_replace_all_overwrites_every_column_in_place():
    result = _apply({"a": [1, 2], "b": [10, 20]}, formula="[_CurrentField_] + 1", selection_mode="all")
    assert result.data_frame.collect_schema().names() == ["a", "b"]
    assert result.data_frame.collect().to_dicts() == [{"a": 2, "b": 11}, {"a": 3, "b": 21}]


def test_new_mode_with_prefix_appends_after_the_existing_columns():
    kwargs = dict(
        formula="[_CurrentField_] * 2",
        selection_mode="data_type",
        selected_data_type="Numeric",
        output_mode="new",
        output_prefix="copy_",
    )
    assert _names(**kwargs) == ["name", "age", "score", "copy_age", "copy_score"]
    assert _rows(**kwargs) == [
        {"name": "ada", "age": 30, "score": 0.5, "copy_age": 60, "copy_score": 1.0},
        {"name": "bob", "age": 40, "score": 1.5, "copy_age": 80, "copy_score": 3.0},
    ]


def test_new_mode_suffix_keeps_a_leading_space():
    kwargs = dict(
        formula="[_CurrentField_] / [Total ] * 100",
        selection_mode="list",
        selected_columns=["jan", "feb"],
        output_mode="new",
        output_suffix=" % Total",
    )
    assert _names(MONTH_DATA, **kwargs) == ["jan", "feb", "Total ", "jan % Total", "feb % Total"]
    assert _rows(MONTH_DATA, **kwargs) == [
        {"jan": 10.0, "feb": 30.0, "Total ": 100.0, "jan % Total": 10.0, "feb % Total": 30.0},
        {"jan": 20.0, "feb": 40.0, "Total ": 200.0, "jan % Total": 10.0, "feb % Total": 20.0},
    ]


# Selection


def test_list_mode_keeps_selected_column_order_and_skips_unknown_names():
    names = _names(
        formula="[_CurrentField_]",
        selection_mode="list",
        selected_columns=["score", "does_not_exist", "name"],
        output_mode="new",
        output_suffix="_v",
    )
    # New columns follow `selected_columns` order, not schema order; the unknown name is dropped.
    assert names == ["name", "age", "score", "score_v", "name_v"]


def test_list_mode_replace_leaves_unselected_columns_untouched():
    assert _rows(
        formula="[_CurrentField_] + 1",
        selection_mode="list",
        selected_columns=["age"],
    ) == [
        {"name": "ada", "age": 31, "score": 0.5},
        {"name": "bob", "age": 41, "score": 1.5},
    ]


def test_data_type_mode_string_group():
    rows = _rows(
        MIXED_DATA,
        formula="uppercase([_CurrentField_])",
        selection_mode="data_type",
        selected_data_type="String",
    )
    assert [r["label"] for r in rows] == ["X", "Y"]
    # Every non-String column passes through untouched.
    assert [r["qty"] for r in rows] == [1, 2]
    assert [r["booked_on"] for r in rows] == [date(2024, 1, 1), date(2024, 1, 2)]
    assert [r["seen_at"] for r in rows] == [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0)]
    assert [r["active"] for r in rows] == [True, False]


def test_data_type_mode_numeric_group():
    assert _names(
        MIXED_DATA,
        formula="[_CurrentField_] * 10",
        selection_mode="data_type",
        selected_data_type="Numeric",
        output_mode="new",
        output_suffix="_n",
    ) == ["label", "qty", "booked_on", "seen_at", "active", "qty_n"]


def test_data_type_mode_date_group_covers_date_and_datetime():
    assert _names(
        MIXED_DATA,
        formula="[_CurrentFieldType_]",
        selection_mode="data_type",
        selected_data_type="Date",
        output_mode="new",
        output_suffix="_t",
    ) == ["label", "qty", "booked_on", "seen_at", "active", "booked_on_t", "seen_at_t"]


def test_data_type_mode_boolean_group():
    rows = _rows(
        MIXED_DATA,
        formula="[_CurrentField_] == false",
        selection_mode="data_type",
        selected_data_type="Boolean",
    )
    assert [r["active"] for r in rows] == [False, True]
    assert [r["label"] for r in rows] == ["x", "y"]


def test_data_type_mode_with_no_selected_type_is_a_no_op():
    result = _apply(formula="[_CurrentField_] + 1", selection_mode="data_type", selected_data_type=None)
    assert result.data_frame.collect().to_dicts() == [
        {"name": "ada", "age": 30, "score": 0.5},
        {"name": "bob", "age": 40, "score": 1.5},
    ]


# Placeholder binding


def test_current_field_name_binds_the_column_name_as_a_literal():
    assert _rows(
        formula="[_CurrentFieldName_]",
        selection_mode="list",
        selected_columns=["age"],
        output_mode="new",
        output_suffix="_label",
    ) == [
        {"name": "ada", "age": 30, "score": 0.5, "age_label": "age"},
        {"name": "bob", "age": 40, "score": 1.5, "age_label": "age"},
    ]


def test_current_field_type_binds_the_polars_dtype_base_token():
    rows = _rows(
        MIXED_DATA,
        formula="[_CurrentFieldType_]",
        selection_mode="all",
        output_mode="new",
        output_suffix="_t",
    )
    assert {k: v for k, v in rows[0].items() if k.endswith("_t")} == {
        "label_t": "String",
        "qty_t": "Int64",
        # Parameterised dtypes bind only the base token, never "Datetime(time_unit='us', ...)".
        "booked_on_t": "Date",
        "seen_at_t": "Datetime",
        "active_t": "Boolean",
    }


def test_current_field_name_and_type_combine_in_one_formula():
    assert _rows(
        formula='[_CurrentFieldName_] + ":" + [_CurrentFieldType_]',
        selection_mode="list",
        selected_columns=["score"],
        output_mode="new",
        output_prefix="meta_",
    ) == [
        {"name": "ada", "age": 30, "score": 0.5, "meta_score": "score:Float64"},
        {"name": "bob", "age": 40, "score": 1.5, "meta_score": "score:Float64"},
    ]


def test_formula_without_placeholders_writes_the_same_value_to_every_target():
    """No placeholder is a valid formula: every target gets the same constant, dtype included."""
    kwargs = dict(formula="1", selection_mode="all")
    assert _schema({"a": [1], "b": ["x"]}, **kwargs) == pl.Schema([("a", pl.Int32), ("b", pl.Int32)])
    assert _rows({"a": [1], "b": ["x"]}, **kwargs) == [{"a": 1, "b": 1}]

    new_kwargs = dict(kwargs, output_mode="new", output_suffix="_c")
    assert _schema({"a": [1], "b": ["x"]}, **new_kwargs) == pl.Schema(
        [("a", pl.Int64), ("b", pl.String), ("a_c", pl.Int32), ("b_c", pl.Int32)]
    )


def test_placeholders_are_case_insensitive():
    assert _rows(
        formula="[_currentfield_] + 1",
        selection_mode="list",
        selected_columns=["age"],
    ) == [
        {"name": "ada", "age": 31, "score": 0.5},
        {"name": "bob", "age": 41, "score": 1.5},
    ]
    assert _rows(
        formula='[_CURRENTFIELDNAME_] + "/" + [_currentFIELDtype_]',
        selection_mode="list",
        selected_columns=["age"],
        output_mode="new",
        output_suffix="_meta",
    )[0]["age_meta"] == "age/Int64"


@pytest.mark.parametrize(
    "column_name,expected",
    [
        ("plain", '"plain"'),
        ("it's", '"it\'s"'),
    ],
)
def test_bind_current_field_name_quoting(column_name: str, expected: str):
    """Pure binding rule for `[_CurrentFieldName_]` (spec 1.4).

    Names are always double-quoted; a name carrying a single quote is safe inside them.
    """
    assert transform_schema.bind_multi_field_formula("[_CurrentFieldName_]", column_name, "String") == expected


def test_current_field_name_with_a_single_quote_runs_end_to_end():
    """The double-quoted literal has to survive polars-expr-transformer, not just the binder."""
    assert _rows({"it's": [1]}, formula="[_CurrentFieldName_]", selection_mode="all") == [{"it's": "it's"}]


def test_a_double_quoted_column_name_cannot_be_bound_as_a_literal():
    """No literal form survives the pinned parser, so the friendly error fires instead."""
    with pytest.raises(ValueError, match="cannot be written as a string literal"):
        transform_schema.bind_multi_field_formula("[_CurrentFieldName_]", 'he said "hi"', "String")
    with pytest.raises(ValueError, match="cannot be written as a string literal"):
        _apply({'he said "hi"': [1]}, formula="[_CurrentFieldName_]", selection_mode="all")


def test_bind_current_field_inserts_backslashes_literally():
    assert transform_schema.bind_multi_field_formula("[_CurrentField_]", r"a\b", "String") == r"[a\b]"


def test_bind_current_field_type_strips_dtype_parameters():
    bound = transform_schema.bind_multi_field_formula(
        "[_CurrentFieldType_]", "seen_at", "Datetime(time_unit='us', time_zone=None)"
    )
    assert bound == '"Datetime"'


# Cross references


def test_every_expression_reads_the_original_input_values():
    """One `with_columns` means a target referenced by another target is never the rewritten value."""
    # `Total ` is deliberately processed FIRST: a sequential per-column loop would then divide
    # jan/feb by the already-overwritten 100.0 and produce different numbers.
    rows = _rows(
        MONTH_DATA,
        formula="[_CurrentField_] / [Total ] * 100",
        selection_mode="list",
        selected_columns=["Total ", "jan", "feb"],
    )
    assert rows == [
        {"jan": 10.0, "feb": 30.0, "Total ": 100.0},
        {"jan": 10.0, "feb": 20.0, "Total ": 100.0},
    ]


# Output cast


def test_output_cast_to_float64():
    assert _schema(formula="[_CurrentField_]", selection_mode="data_type", selected_data_type="Numeric",
                   output_data_type="Float64") == pl.Schema(
        [("name", pl.String), ("age", pl.Float64), ("score", pl.Float64)]
    )


def test_output_cast_to_int64_truncates():
    result = _apply(
        formula="[_CurrentField_]",
        selection_mode="list",
        selected_columns=["score"],
        output_data_type="Int64",
    )
    assert result.data_frame.collect_schema()["score"] == pl.Int64
    assert result.data_frame.collect().to_dicts() == [
        {"name": "ada", "age": 30, "score": 0},
        {"name": "bob", "age": 40, "score": 1},
    ]


def test_output_cast_to_decimal():
    schema = _schema(
        formula="[_CurrentField_]",
        selection_mode="list",
        selected_columns=["score"],
        output_data_type="Decimal",
    )
    assert isinstance(schema["score"], pl.Decimal)


def test_output_cast_applies_to_new_columns_too():
    schema = _schema(
        formula="[_CurrentField_] * 2",
        selection_mode="list",
        selected_columns=["age"],
        output_mode="new",
        output_prefix="d_",
        output_data_type="String",
    )
    assert schema["age"] == pl.Int64
    assert schema["d_age"] == pl.String


def test_output_cast_that_cannot_succeed_fails_on_collect_not_on_build():
    """Parity with the `formula` node: the cast is lazy, so a bad cast surfaces at collect time."""
    result = _apply({"a": ["x", "y"]}, formula="[_CurrentField_]", selection_mode="all", output_data_type="Int64")
    assert result.data_frame.collect_schema()["a"] == pl.Int64
    with pytest.raises(pl.exceptions.InvalidOperationError):
        result.data_frame.collect()


@pytest.mark.parametrize("output_data_type", [None, "Auto"])
def test_auto_and_none_skip_the_cast(output_data_type):
    schema = _schema(
        formula="[_CurrentField_] * 2",
        selection_mode="data_type",
        selected_data_type="Numeric",
        output_data_type=output_data_type,
    )
    assert schema["age"] == pl.Int64
    assert schema["score"] == pl.Float64


# Validation


def test_empty_formula_raises_even_with_zero_targets():
    with pytest.raises(ValueError, match=r"^Multi-field formula: no formula configured$"):
        _apply(formula="   ", selection_mode="data_type", selected_data_type=None)


def test_new_mode_without_prefix_or_suffix_raises():
    with pytest.raises(
        ValueError, match=r"^Multi-field formula: writing to new columns requires a prefix or a suffix$"
    ):
        _apply(formula="[_CurrentField_]", selection_mode="all", output_mode="new")


def test_new_mode_without_affix_raises_before_the_no_targets_short_circuit():
    with pytest.raises(
        ValueError, match=r"^Multi-field formula: writing to new columns requires a prefix or a suffix$"
    ):
        _apply(formula="[_CurrentField_]", selection_mode="data_type", selected_data_type=None, output_mode="new")


def test_output_name_colliding_with_an_existing_column_raises():
    with pytest.raises(ValueError, match=r"^Multi-field formula: output column 'new_age' already exists$"):
        _apply(
            {"age": [1], "new_age": [2]},
            formula="[_CurrentField_]",
            selection_mode="list",
            selected_columns=["age"],
            output_mode="new",
            output_prefix="new_",
        )


def test_duplicate_target_raises_in_replace_mode():
    with pytest.raises(
        ValueError,
        match=r"^Multi-field formula: output column 'a' is produced by more than one selected column$",
    ):
        _apply({"a": [1]}, formula="[_CurrentField_]", selection_mode="list", selected_columns=["a", "a"])


def test_duplicate_target_raises_in_new_mode():
    with pytest.raises(
        ValueError,
        match=r"^Multi-field formula: output column 'p_a' is produced by more than one selected column$",
    ):
        _apply(
            {"a": [1]},
            formula="[_CurrentField_]",
            selection_mode="list",
            selected_columns=["a", "a"],
            output_mode="new",
            output_prefix="p_",
        )


def test_unquotable_column_name_raises():
    with pytest.raises(
        ValueError, match=r"""^Multi-field formula: column 'a"b'c' cannot be written as a string literal$"""
    ):
        _apply({"""a"b'c""": [1]}, formula="[_CurrentFieldName_]", selection_mode="all")


# No-op and laziness


def test_zero_targets_is_a_no_op_that_preserves_the_schema():
    engine = _engine()
    result = engine.apply_multi_field_formula(
        transform_schema.MultiFieldFormulaInput(
            formula="[_CurrentField_] + 1", selection_mode="list", selected_columns=["nope"]
        )
    )
    assert [c.column_name for c in result.schema] == ["name", "age", "score"]
    assert [c.data_type for c in result.schema] == [c.data_type for c in engine.schema]
    assert result.data_frame.collect().to_dicts() == [
        {"name": "ada", "age": 30, "score": 0.5},
        {"name": "bob", "age": 40, "score": 1.5},
    ]


def test_result_stays_lazy():
    result = _apply(formula="[_CurrentField_]", selection_mode="all")
    assert isinstance(result.data_frame, pl.LazyFrame)


def test_no_op_result_stays_lazy():
    result = _apply(formula="[_CurrentField_]", selection_mode="data_type", selected_data_type=None)
    assert isinstance(result.data_frame, pl.LazyFrame)


# Pure expression builder


def test_build_expressions_returns_one_expression_per_target():
    exprs = FlowDataEngine.build_multi_field_formula_expressions(
        [("name", "String", "String"), ("age", "Int64", "Numeric")],
        transform_schema.MultiFieldFormulaInput(formula="[_CurrentField_]", selection_mode="all"),
    )
    assert len(exprs) == 2
    assert all(isinstance(e, pl.Expr) for e in exprs)
    assert [e.meta.output_name() for e in exprs] == ["name", "age"]


def test_build_expressions_aliases_new_columns():
    exprs = FlowDataEngine.build_multi_field_formula_expressions(
        [("age", "Int64", "Numeric")],
        transform_schema.MultiFieldFormulaInput(
            formula="[_CurrentField_]", selection_mode="all", output_mode="new", output_prefix="p_",
            output_suffix="_s",
        ),
    )
    assert [e.meta.output_name() for e in exprs] == ["p_age_s"]


def test_build_expressions_is_empty_without_targets():
    exprs = FlowDataEngine.build_multi_field_formula_expressions(
        [("age", "Int64", "Numeric")],
        transform_schema.MultiFieldFormulaInput(
            formula="[_CurrentField_]", selection_mode="list", selected_columns=["missing"]
        ),
    )
    assert exprs == []
