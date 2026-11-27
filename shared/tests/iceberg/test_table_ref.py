"""A table named by its catalog's address and its name is read and written by a process that was given nothing else.

``scan_iceberg_table`` builds a plan in one process; a fresh process with no storage key of its own runs it and asks
the catalog for the keys itself. ``write_iceberg_table`` creates a missing table, appends and overwrites, and what it
wrote is what DuckDB and PyIceberg see, also after DuckDB deleted and inserted rows in between.

Every test skips unless the catalog runs: ``python -m test_utils.iceberg_rest start``.
"""

import base64
import dataclasses
import json

import polars as pl
import pytest

from shared import iceberg_utils
from shared.iceberg_utils import IcebergTableRef, scan_iceberg_table, storage_properties
from shared.tests.iceberg.table_child import expected, rows_frame
from shared.tests.iceberg.test_rest_catalog import _seed_through_duckdb
from test_utils.iceberg_rest import fixtures

pytestmark = pytest.mark.iceberg_rest

CHILD = "shared.tests.iceberg.table_child"
CHILD_AWS_ENV = ["AWS_CONFIG_FILE", "AWS_EC2_METADATA_DISABLED", "AWS_ENDPOINT_URL", "AWS_SHARED_CREDENTIALS_FILE"]
SECRET_KEY_NAMES = [b"s3.secret-access-key", b"s3.session-token"]


def _ref(namespace: str, name: str) -> IcebergTableRef:
    return IcebergTableRef(fixtures.CATALOG_URI, f"{namespace}.{name}", fixtures.WAREHOUSE)


def _ref_payload(namespace: str, name: str) -> dict:
    return dataclasses.asdict(_ref(namespace, name))


def _plan(frame: pl.LazyFrame) -> str:
    return base64.b64encode(frame.serialize()).decode()


class _KeyValues(list):
    """Key values a test compares against; its ``repr``, which pytest prints for a failing test's arguments, holds none."""

    def __repr__(self) -> str:
        return f"<{len(self)} key values>"


@pytest.fixture
def handed_keys(monkeypatch):
    """Every key value the catalog hands this process while the test builds a plan."""
    seen = _KeyValues()
    load = iceberg_utils.load_table

    def recording(ref):
        table = load(ref)
        seen.extend(fixtures.key_values(storage_properties(table)))
        return table

    monkeypatch.setattr(iceberg_utils, "load_table", recording)
    return seen


def _read(run_child, child_env, frame: pl.LazyFrame, keys=(), **extra) -> dict:
    return run_child(CHILD, {"op": "collect_plan", "plan": _plan(frame), **extra}, child_env, secrets=keys)


def test_a_plan_built_here_holds_no_key_and_a_fresh_process_reads_the_table_with_it(
    rest_catalog, namespace, seed_table, child_env, run_child, handed_keys
):
    seed_table("by_ref", 1000, files=2)

    frame = scan_iceberg_table(_ref(namespace, "by_ref"))
    blob = frame.serialize()

    handed = len(handed_keys)
    assert handed == 3, "the catalog handed out no keys while the plan was built"
    leaked = [index for index, value in enumerate(handed_keys) if value.encode() in blob]
    assert leaked == []
    named = [name for name in SECRET_KEY_NAMES if name in blob]
    assert named == []
    names_the_table = f"{namespace}.by_ref".encode() in blob
    assert names_the_table

    answer = _read(run_child, child_env, frame, handed_keys, bound=900)
    print(json.dumps(answer))
    assert (answer["rows"], answer["id_sum"]) == expected(1000)
    assert answer["filtered_rows"] == 99
    assert any(f"Reading {namespace}.by_ref from {fixtures.S3_ENDPOINT}" in note and "valid for" in note
               for note in answer["notes"]), answer["notes"]
    assert answer["aws_env"] == CHILD_AWS_ENV
    assert [w for w in answer["warnings"] if "PerformanceWarning" in w] == []


def test_rows_another_engine_deleted_stay_deleted_and_the_run_log_says_why_the_read_was_slower(
    rest_catalog, namespace, seed_table, child_env, run_child, duck
):
    seed_table("deleted_v2", 1000, files=2, properties={"format-version": "2"})
    fixtures.attach_lake(duck)
    duck.execute(f"DELETE FROM lake.{namespace}.deleted_v2 WHERE id % 7 = 0")

    answer = _read(run_child, child_env, scan_iceberg_table(_ref(namespace, "deleted_v2")))

    print(json.dumps(answer))
    kept = [i for i in range(1000) if i % 7]
    assert (answer["rows"], answer["id_sum"]) == (857, sum(kept))
    fell_back = [note for note in answer["notes"] if "position delete file not limited to one data file" in note]
    assert len(fell_back) == 1 and "PyIceberg" in fell_back[0], answer["notes"]


def test_a_table_with_deletion_vectors_is_read_by_polars_itself(
    rest_catalog, namespace, child_env, run_child, duck
):
    _seed_through_duckdb(rest_catalog, duck, f"{namespace}.deleted_v3", "3")
    fixtures.attach_lake(duck)
    duck.execute(f"DELETE FROM lake.{namespace}.deleted_v3 WHERE id % 7 = 0")

    answer = _read(run_child, child_env, scan_iceberg_table(_ref(namespace, "deleted_v3")))

    print(json.dumps(answer))
    assert (answer["rows"], answer["id_sum"]) == (857, sum(i for i in range(1000) if i % 7))
    assert [note for note in answer["notes"] if "PyIceberg" in note] == []


def test_a_credential_about_to_expire_is_refused_before_anything_is_read(
    rest_catalog, namespace, seed_table, child_env, run_child
):
    seed_table("short_credential", 10)
    frame = scan_iceberg_table(_ref(namespace, "short_credential"), minimum_seconds=10**7)

    with pytest.raises(AssertionError) as failed:
        _read(run_child, child_env, frame)

    assert f"credential for {namespace}.short_credential runs out in" in str(failed.value)
    assert "too soon to read it" in str(failed.value)


def test_the_columns_come_from_the_catalog_while_storage_is_out_of_reach(
    rest_catalog, namespace, seed_table, child_env, run_child
):
    """With the store frozen, the columns still come back: they are in the catalog's answer, not in a file."""
    seed_table("columns_only", 10)

    with fixtures.minio_paused():
        answer = run_child(CHILD, {"op": "schema_ref", "ref": _ref_payload(namespace, "columns_only")}, child_env)

    assert answer["schema"] == {name: str(dtype) for name, dtype in rows_frame(1).collect_schema().items()}
    assert answer["aws_env"] == CHILD_AWS_ENV


def _write(run_child, child_env, ref: dict, rows: int, start: int, mode: str) -> dict:
    return run_child(CHILD, {"op": "write_ref", "ref": ref, "rows": rows, "start": start, "mode": mode}, child_env)


def _engines_agree(rest_catalog, duck, run_child, child_env, namespace: str, name: str) -> tuple[int, int]:
    """(rows, id sum) as DuckDB, a PyIceberg scan and a fresh Polars child read them, asserted equal."""
    identifier = f"{namespace}.{name}"
    fixtures.attach_lake(duck)
    duck_rows, duck_sum = duck.execute(f"SELECT count(*), coalesce(sum(id), 0) FROM lake.{identifier}").fetchone()
    scanned = rest_catalog.load_table(identifier).scan().to_arrow()
    pyiceberg = (scanned.num_rows, sum(scanned.column("id").to_pylist()))
    polars = _read(run_child, child_env, scan_iceberg_table(_ref(namespace, name)))
    seen = {"duckdb": (duck_rows, int(duck_sum)), "pyiceberg": pyiceberg, "polars": (polars["rows"], polars["id_sum"])}
    assert len(set(seen.values())) == 1, seen
    return pyiceberg


def _objects_under(table) -> int:
    bucket, _, prefix = table.metadata.location.removeprefix("s3://").partition("/")
    listing = fixtures.s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix + "/")
    return listing.get("KeyCount", 0)


def test_a_write_creates_a_missing_table_and_another_engine_sees_its_rows(
    rest_catalog, namespace, child_env, run_child, duck
):
    answer = _write(run_child, child_env, _ref_payload(namespace, "created"), 1000, 0, "append")

    print(json.dumps(answer))
    assert (answer["rows"], answer["id_sum"]) == expected(1000)
    assert any(note.startswith(f"Writing {namespace}.created to ") for note in answer["notes"]), answer["notes"]
    assert answer["aws_env"] == CHILD_AWS_ENV
    assert _engines_agree(rest_catalog, duck, run_child, child_env, namespace, "created") == expected(1000)
    assert rest_catalog.load_table(f"{namespace}.created").metadata.format_version == 2


def test_appends_and_overwrites_survive_another_engines_deletes_and_inserts(
    rest_catalog, namespace, child_env, run_child, duck
):
    ref, name = _ref_payload(namespace, "mixed"), "mixed"
    table = f"lake.{namespace}.mixed"
    rows: set[int] = set()

    def check(stage: str) -> None:
        seen = _engines_agree(rest_catalog, duck, run_child, child_env, namespace, name)
        assert seen == (len(rows), sum(rows)), stage

    _write(run_child, child_env, ref, 1000, 0, "append")
    rows |= set(range(1000))
    check("1. written")
    duck.execute(f"DELETE FROM {table} WHERE grp = 3")
    rows -= {i for i in rows if i % 10 == 3}
    check("2. DuckDB deleted grp 3")
    _write(run_child, child_env, ref, 300, 5000, "overwrite")
    rows = set(range(5000, 5300))
    check("3. overwritten")
    _write(run_child, child_env, ref, 200, 6000, "append")
    rows |= set(range(6000, 6200))
    check("4. appended")
    seed = rows_frame(50, 7000).collect().to_arrow(compat_level=pl.CompatLevel.oldest())
    duck.register("seed_rows", seed)
    duck.execute(f"INSERT INTO {table} SELECT * FROM seed_rows")
    duck.unregister("seed_rows")
    rows |= set(range(7000, 7050))
    check("5. DuckDB inserted")
    duck.execute(f"DELETE FROM {table} WHERE id % 5 = 0")
    rows = {i for i in rows if i % 5}
    check("6. DuckDB deleted id % 5 = 0")


def test_a_frame_with_a_count_column_is_cast_to_the_tables_types(rest_catalog, namespace):
    """``pl.len()`` is an unsigned count; the table gets a 64-bit column, and a second write fits it by casting."""
    from shared.iceberg_utils import write_iceberg_table

    ref = _ref(namespace, "counted")
    frame = rows_frame(100).group_by("grp").agg(pl.len().alias("n")).select("n", "grp")
    write_iceberg_table(ref, frame, "append")
    write_iceberg_table(ref, frame.select("grp", "n"), "append")

    scanned = rest_catalog.load_table(f"{namespace}.counted").scan().to_arrow()
    assert scanned.num_rows == 20 and sum(scanned.column("n").to_pylist()) == 200
    assert str(scanned.schema.field("n").type) == "int64"


def test_a_frame_with_other_columns_is_refused_and_nothing_is_written(rest_catalog, namespace, seed_table):
    from shared.iceberg_utils import write_iceberg_table

    table = seed_table("other_columns", 10)
    before = _objects_under(table)

    with pytest.raises(ValueError, match="a write here does not change a table's columns"):
        write_iceberg_table(_ref(namespace, "other_columns"), pl.LazyFrame({"id": [1], "extra": ["x"]}), "append")

    assert _objects_under(rest_catalog.load_table(f"{namespace}.other_columns")) == before


def test_a_version_3_table_is_refused_and_nothing_is_written(rest_catalog, namespace, duck):
    from shared.iceberg_utils import write_iceberg_table

    _seed_through_duckdb(rest_catalog, duck, f"{namespace}.v3_write", "3")
    table = rest_catalog.load_table(f"{namespace}.v3_write")
    before = _objects_under(table)

    with pytest.raises(ValueError, match="version 3 table, which this Flowfile reads and cannot write yet"):
        write_iceberg_table(_ref(namespace, "v3_write"), rows_frame(10), "append")

    assert _objects_under(table) == before


def test_a_failed_commit_names_the_files_it_left(rest_catalog, namespace, duck):
    """``_sink`` is called past the version 3 refusal on purpose: a version 3 table is the one commit that is certain
    to fail after Polars wrote a data file (it writes no manifest for version 3)."""
    from shared.iceberg_utils import _sink

    _seed_through_duckdb(rest_catalog, duck, f"{namespace}.v3_sink", "3")
    table = rest_catalog.load_table(f"{namespace}.v3_sink")

    with pytest.raises(RuntimeError) as failed:
        _sink(table, rows_frame(10), "append")

    message = str(failed.value)
    print(message)
    assert message.startswith(f"The write to {namespace}.v3_sink did not commit (")
    assert "nothing removes them." in message
    folders = message.split("since this write began: ", 1)[1].split(". If no other", 1)[0].split(", ")
    assert len(folders) == 1 and "/data/" in folders[0]
    bucket, _, prefix = folders[0].removeprefix("s3://").partition("/")
    listing = fixtures.s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/")
    assert listing.get("KeyCount", 0) == 1
