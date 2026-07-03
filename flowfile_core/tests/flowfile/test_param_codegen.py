"""Unit tests for sentinel-based parameter substitution in generated code."""

from flowfile_core.flowfile.code_generator.param_codegen import (
    apply_param_sentinels,
    codegen_parameters,
    param_sentinel,
    parameter_default_repr,
    resolve_param_sentinels,
    restore_param_sentinels,
    restore_sentinels_to_refs,
)
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.schemas import input_schema


def _params(*specs) -> list[FlowParameter]:
    return [FlowParameter(**spec) for spec in specs]


def test_whole_string_sentinel_becomes_identifier():
    code = f"df = input_df.head('{param_sentinel('limit')}')"
    resolved, leaked = resolve_param_sentinels(code, {"limit"})
    assert resolved == "df = input_df.head(limit)"
    assert leaked == set()


def test_embedded_sentinel_becomes_fstring_with_brace_escaping():
    code = 'x = ff.lit("value {a} __FF_PARAM_test__ end")'
    resolved, leaked = resolve_param_sentinels(code, {"test"})
    assert resolved == 'x = ff.lit(f"value {{a}} {test} end")'
    assert leaked == set()


def test_name_position_sentinel_becomes_identifier():
    code = "df = input_df.head(__FF_PARAM_limit__)"
    resolved, leaked = resolve_param_sentinels(code, {"limit"})
    assert resolved == "df = input_df.head(limit)"
    assert leaked == set()


def test_unknown_sentinel_name_left_untouched():
    code = "x = '__FF_PARAM_other__'"
    resolved, leaked = resolve_param_sentinels(code, {"limit"})
    assert resolved == code
    assert leaked == set()


def test_multiline_string_degrades_to_literal_ref():
    code = 'x = """line1\n__FF_PARAM_limit__\n"""'
    resolved, leaked = resolve_param_sentinels(code, {"limit"})
    assert "${limit}" in resolved
    assert "__FF_PARAM_" not in resolved
    assert leaked == {"limit"}


def test_raw_string_degrades_to_literal_ref():
    code = "x = r'__FF_PARAM_limit__ \\d+'"
    resolved, leaked = resolve_param_sentinels(code, {"limit"})
    assert "${limit}" in resolved
    assert leaked == {"limit"}


def test_restore_sentinels_to_refs():
    assert restore_sentinels_to_refs("a __FF_PARAM_x__ b") == "a ${x} b"


def test_codegen_parameters_filters_invalid_identifiers():
    params = _params(
        {"name": "ok_name", "default_value": "1", "type": "integer"},
        {"name": "not ok", "default_value": ""},
        {"name": "1bad", "default_value": ""},
    )
    assert [p.name for p in codegen_parameters(params)] == ["ok_name"]


def test_parameter_default_repr_typed():
    assert parameter_default_repr(FlowParameter(name="i", default_value="5", type="integer")) == "5"
    assert parameter_default_repr(FlowParameter(name="b", default_value="true", type="boolean")) == "True"
    assert parameter_default_repr(FlowParameter(name="s", default_value="x y")) == "'x y'"


def test_apply_and_restore_sentinels_on_settings():
    settings = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=1,
        polars_code_input={"polars_code": "output_df = input_df.head(${limit}) # ${unknown}"},
    )
    params = _params({"name": "limit", "default_value": "10", "type": "integer"})
    restorations = apply_param_sentinels([settings, None], params)
    assert param_sentinel("limit") in settings.polars_code_input.polars_code
    assert "${unknown}" in settings.polars_code_input.polars_code  # unknown refs untouched
    restore_param_sentinels(restorations)
    assert settings.polars_code_input.polars_code == "output_df = input_df.head(${limit}) # ${unknown}"
