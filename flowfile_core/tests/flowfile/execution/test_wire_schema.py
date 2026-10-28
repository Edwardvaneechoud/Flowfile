"""Core reads back every dtype a worker sends in a wire reference, extension dtypes included."""

import polars as pl
import pytest

from flowfile_core.flowfile.execution.protocol import Schema
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import safe_eval_pl_type
from flowfile_core.schemas.output_model import FileColumn
from shared.compute_protocol import ResultRef, SubmitResult, WireColumn, WorkerLocation, schema_to_wire

DTYPES = {
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "int128": pl.Int128,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "boolean": pl.Boolean,
    "string": pl.String,
    "binary": pl.Binary,
    "date": pl.Date,
    "time": pl.Time,
    "null": pl.Null,
    "object": pl.Object,
    "datetime_naive": pl.Datetime("us"),
    "datetime_utc": pl.Datetime("ns", "UTC"),
    "datetime_amsterdam": pl.Datetime("ms", "Europe/Amsterdam"),
    "duration": pl.Duration("ms"),
    "decimal": pl.Decimal(10, 2),
    "list": pl.List(pl.Int64),
    "nested_list": pl.List(pl.List(pl.String)),
    "array_1d": pl.Array(pl.Float64, 3),
    "array_2d": pl.Array(pl.Int32, (2, 3)),
    "struct_spaced_field": pl.Struct({"first name": pl.String, "age": pl.Int32}),
    "categorical": pl.Categorical(),
    "enum_quoted": pl.Enum(["it's", 'say "hi"', "plain"]),
    "geoarrow_wkb": pl.Extension("geoarrow.wkb", pl.Binary, "{}"),
    "arrow_uuid": pl.Extension("arrow.uuid", pl.Binary),
    "geoarrow_point": pl.Extension("geoarrow.point", pl.Struct({"x": pl.Float64, "y": pl.Float64}), "{}"),
    "geoarrow_wkt": pl.Extension("geoarrow.wkt", pl.String, '{"crs": "EPSG:4326"}'),
}


def _through_the_wire(columns: list[WireColumn]) -> list[WireColumn]:
    location = WorkerLocation(kind="file", path="/cache/flow/abc.arrow").model_dump_json().encode()
    ref = ResultRef(result_id="abc", columns=columns, row_count=0, state="materialised", location=location)
    sent = SubmitResult(ref=ref, job_id="job-1").model_dump_json()
    return SubmitResult.model_validate_json(sent).ref.columns


@pytest.mark.parametrize("dtype", list(DTYPES.values()), ids=list(DTYPES))
def test_a_polars_dtype_comes_back_from_the_wire_unchanged(dtype):
    schema = pl.Schema({"column": dtype})

    [column] = _through_the_wire(schema_to_wire(schema))

    assert column.name == "column"
    assert safe_eval_pl_type(column.data_type) == schema["column"]


def test_core_schema_round_trips_through_file_column_and_the_wire():
    schema: Schema = [
        FlowfileColumn.create_from_polars_dtype(name, dtype, col_index=index)
        for index, (name, dtype) in enumerate(DTYPES.items())
    ]
    wire_columns = [
        WireColumn.model_validate(
            FileColumn.model_validate(column.get_column_repr()).model_dump(include={"name", "data_type"})
        )
        for column in schema
    ]

    rebuilt = [
        FlowfileColumn.create_from_polars_dtype(column.name, safe_eval_pl_type(column.data_type), col_index=index)
        for index, column in enumerate(_through_the_wire(wire_columns))
    ]

    assert [column.get_column_repr() for column in rebuilt] == [column.get_column_repr() for column in schema]
    by_name = {column.column_name: column for column in rebuilt}
    assert by_name["geoarrow_wkb"].semantic_type == "geometry"
    assert by_name["geoarrow_wkb"].data_type_group == "Binary"
    assert by_name["geoarrow_point"].semantic_type == "geometry"
    assert by_name["geoarrow_point"].data_type_group == "Complex"
    assert by_name["geoarrow_wkt"].data_type_group == "String"
    assert by_name["arrow_uuid"].semantic_type is None
