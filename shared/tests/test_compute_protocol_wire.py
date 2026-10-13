"""The worker's side of the wire models: what it encodes survives JSON unchanged, without importing core.

A blob's bytes are copied to and from a ``file://`` URI here; the presigned ``https://`` side runs in the worker's route
tests against MinIO.
"""

import io
import socket

import polars as pl
import pytest
from pydantic import ValidationError

from shared.compute_protocol import (
    ResultRef,
    SubmitHints,
    SubmitResult,
    WireColumn,
    WorkerLocation,
    get_blob,
    put_blob,
    schema_to_wire,
)

GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")
BLOB = bytes(range(256)) * 64


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


def test_a_location_kind_outside_file_plan_and_blob_is_refused():
    with pytest.raises(ValidationError):
        WorkerLocation(kind="s3", path="s3://bucket/key")


def test_a_blob_reference_survives_json_with_its_kind_and_the_path_of_its_bytes():
    location = WorkerLocation(kind="blob", path="/cache/-1/abc.blob").model_dump_json().encode()
    ref = ResultRef(result_id="abc", columns=[], row_count=None, state="materialised", location=location, kind="blob")

    back = ResultRef.model_validate_json(ref.model_dump_json())

    assert back == ref
    assert back.kind == "blob"
    assert WorkerLocation.model_validate_json(back.location) == WorkerLocation(kind="blob", path="/cache/-1/abc.blob")


def test_a_reference_without_a_kind_is_a_table():
    sent = {"result_id": "abc", "columns": [], "row_count": 0, "state": "deferred", "location": ""}

    assert ResultRef.model_validate(sent).kind == "table"


def test_a_kind_outside_table_and_blob_is_refused():
    with pytest.raises(ValidationError):
        ResultRef(result_id="x", columns=[], row_count=None, state="materialised", location=b"", kind="model")


def test_a_blob_is_copied_to_and_from_a_file_uri(tmp_path):
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)

    put_blob(source, f"file://{tmp_path}/out.bin")
    get_blob(f"file://{tmp_path}/out.bin", tmp_path / "back" / "model.bin")

    assert (tmp_path / "out.bin").read_bytes() == BLOB
    assert (tmp_path / "back" / "model.bin").read_bytes() == BLOB


def test_a_put_into_a_missing_folder_fails_and_makes_none(tmp_path):
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)

    with pytest.raises(FileNotFoundError):
        put_blob(source, f"file://{tmp_path}/missing/out.bin")
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize("uri", ["s3://bucket/model.bin", "http://host/model.bin", "/plain/path.bin"])
def test_a_blob_uri_of_another_scheme_is_refused_before_anything_is_read_or_written(tmp_path, uri):
    with pytest.raises(ValueError, match="file:// and https:// only"):
        put_blob(tmp_path / "never-read.bin", uri)
    with pytest.raises(ValueError, match="file:// and https:// only"):
        get_blob(uri, tmp_path / "never" / "written.bin")
    assert not (tmp_path / "never").exists()


def test_a_failed_get_leaves_no_file_and_names_no_query(tmp_path):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    target = tmp_path / "held" / "model.blob"

    with pytest.raises(OSError) as raised:
        get_blob(f"https://127.0.0.1:{port}/bucket/model.bin?X-Amz-Signature=secret", target)

    assert str(raised.value) == f"GET https://127.0.0.1:{port}/bucket/model.bin failed: ConnectionError"
    assert raised.value.__suppress_context__, "the client's error, which repeats the URL, is not chained"
    assert not target.exists()


def test_a_failed_file_copy_leaves_no_file(tmp_path):
    target = tmp_path / "held" / "model.blob"

    with pytest.raises(FileNotFoundError):
        get_blob(f"file://{tmp_path}/missing.bin", target)

    assert not target.exists()
