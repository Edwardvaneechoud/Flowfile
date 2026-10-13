"""The worker's side of the wire models: what it encodes survives JSON unchanged, without importing core."""

import io

import polars as pl
import pytest
from pydantic import ValidationError

from shared.compute_protocol import (
    ResultRef,
    SubmitHints,
    SubmitResult,
    WireColumn,
    WorkerLocation,
    schema_to_wire,
)

GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")


def _arrow_stream(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_ipc_stream(buffer)
    return buffer.getvalue()


def test_schema_to_wire_keeps_column_order_and_spells_each_dtype_as_str():
    schema = pl.Schema(
        {
            "geom": GEOARROW_WKB,
            "at": pl.Datetime("us", "Europe/Amsterdam"),
            "point": pl.Struct({"x": pl.Float64, "y": pl.Float64}),
            "size": pl.Enum(["small", "large"]),
        }
    )

    columns = schema_to_wire(schema)

    assert [column.name for column in columns] == ["geom", "at", "point", "size"]
    assert [column.data_type for column in columns] == [str(dtype) for dtype in schema.values()]
    assert columns[0] == WireColumn(name="geom", data_type="Extension('geoarrow.wkb', Binary, '{}')")


def test_a_materialised_result_survives_json_with_its_file_location_and_sample():
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    sample = _arrow_stream(df)
    with pytest.raises(UnicodeDecodeError):
        sample.decode("utf-8")
    location = WorkerLocation(kind="file", path="/cache/flow/abc.arrow").model_dump_json().encode()
    result = SubmitResult(
        ref=ResultRef(
            result_id="abc",
            columns=schema_to_wire(df.schema),
            row_count=2,
            state="materialised",
            location=location,
        ),
        sample=sample,
        job_id="job-1",
    )

    back = SubmitResult.model_validate_json(result.model_dump_json())

    assert back == result
    assert back.sample == sample
    assert back.ref.row_count == 2
    assert WorkerLocation.model_validate_json(back.ref.location) == WorkerLocation(
        kind="file", path="/cache/flow/abc.arrow"
    )


def test_a_deferred_result_survives_json_with_its_plan_bytes():
    lf = pl.LazyFrame({"id": [1, 2, 3]}).filter(pl.col("id") > 1)
    plan = lf.serialize()
    result = SubmitResult(
        ref=ResultRef(
            result_id="def",
            columns=schema_to_wire(pl.Schema({"id": pl.Int64})),
            row_count=None,
            state="deferred",
            location=WorkerLocation(kind="plan", plan=plan).model_dump_json().encode(),
        ),
        sample=None,
        job_id="job-2",
    )

    back = SubmitResult.model_validate_json(result.model_dump_json())

    assert back == result
    assert back.sample is None
    assert back.ref.row_count is None
    location = WorkerLocation.model_validate_json(back.ref.location)
    assert location.kind == "plan"
    assert location.path is None
    assert location.plan == plan


def test_submit_hints_without_preview_rows_survive_json():
    hints = SubmitHints(must_materialise=True, wide=False, fanout=2, preview_rows=None)

    assert SubmitHints.model_validate_json(hints.model_dump_json()) == hints


def test_a_state_outside_the_two_literals_is_refused():
    with pytest.raises(ValidationError):
        ResultRef(result_id="x", columns=[], row_count=None, state="cached", location=b"")


def test_a_location_kind_outside_file_and_plan_is_refused():
    with pytest.raises(ValidationError):
        WorkerLocation(kind="s3", path="s3://bucket/key")
