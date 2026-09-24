"""The cloud-writer bug report's flow (run-1390 snapshot), replayed over HTTP against a real core + worker.

Every test runs in remote and local execution; connection tests use ``stack``, "No connection" tests ``ambient_stack``.
"""

import re
import time
import uuid

import polars as pl
import pytest
from deltalake import DeltaTable

from .helpers import MINIO_OPTIONS, NAMED_PROFILE, list_keys

pytestmark = pytest.mark.cloud_e2e

LOCATIONS = pytest.mark.parametrize("location", ["remote", "local"])
READER_ID, WRITER_ID, IFNULL_ID, LITERAL_ID = 1, 3, 4, 5
WRITER_NO_PATH = "Cloud storage writer has no target path."
READER_NO_PATH = "Cloud storage reader has no source path."
CRASH = re.compile(r"TypeError|is not an instance")


def _succeeded(run: dict) -> None:
    errors = {node_id: result["error"] for node_id, result in run["nodes"].items() if not result["success"]}
    assert run["success"] is True and not errors, errors
    assert set(run["nodes"]) == {READER_ID, IFNULL_ID, LITERAL_ID, WRITER_ID}


def _assert_user_rows(frame: pl.DataFrame, rows: int = 6) -> None:
    assert frame.height == rows
    assert frame["category"].null_count() == 0
    assert sorted(frame["category"].unique().to_list()) == ["A", "B", "C", "na"]
    assert (frame["category"] == "na").sum() == 2 * rows // 6
    assert frame["output_field"].unique().to_list() == ["test"]


@LOCATIONS
def test_user_flow_appends_partitioned_delta(stack, user_flow, minio_connection, new_target, location):
    target = new_target("table")
    writer = {"auth_mode": "access_key", "connection_name": minio_connection, "resource_path": target}
    first = stack.run_flow(user_flow(location, minio_connection, writer=writer))
    _succeeded(first)
    _assert_user_rows(pl.read_delta(target, storage_options=MINIO_OPTIONS))
    table = DeltaTable(target, storage_options=MINIO_OPTIONS)
    assert table.metadata().partition_columns == ["output_field"]
    assert list_keys(f"{target}/output_field=test/")

    second = stack.run(first["flow_id"])
    _succeeded(second)
    assert DeltaTable(target, storage_options=MINIO_OPTIONS).version() == 1
    _assert_user_rows(pl.read_delta(target, storage_options=MINIO_OPTIONS), rows=12)


@LOCATIONS
def test_exact_user_writer_fails_on_missing_path(stack, user_flow, minio_connection, location):
    run = stack.run_flow(user_flow(location, minio_connection))
    writer = run["nodes"][WRITER_ID]
    assert run["success"] is False
    assert writer["success"] is False
    assert WRITER_NO_PATH in writer["error"]
    assert re.search("path", writer["error"], re.IGNORECASE)
    assert not CRASH.search(writer["error"]), writer["error"]
    assert all(run["nodes"][node_id]["success"] for node_id in (READER_ID, IFNULL_ID, LITERAL_ID))


@LOCATIONS
@pytest.mark.parametrize(
    "path, message",
    [("", WRITER_NO_PATH), ("output/table", "Cloud storage path 'output/table' is not a URI.")],
    ids=["empty", "relative"],
)
def test_connection_without_uri_fails_instead_of_writing_locally(
    stack, user_flow, minio_connection, location, path, message
):
    writer = {"auth_mode": "access_key", "connection_name": minio_connection, "resource_path": path}
    run = stack.run_flow(user_flow(location, minio_connection, writer=writer))
    error = run["nodes"][WRITER_ID]["error"]
    assert run["nodes"][WRITER_ID]["success"] is False
    assert message in error, error


NO_CONNECTION_WRITES = {
    "append": {},
    "append_track_changes": {"track_changes": True},
    "upsert": {"write_mode": "upsert", "merge_keys": ["id"], "partition_by": []},
}


@LOCATIONS
@pytest.mark.parametrize("case", list(NO_CONNECTION_WRITES))
def test_no_connection_writes_with_local_aws_profile(
    ambient_stack, user_flow, ambient_minio_connection, new_target, location, case
):
    """The reporter's writer shapes (runs 1386-1388 tracked changes) on the local AWS profile."""
    target = new_target("table")
    writer = {"resource_path": target, **NO_CONNECTION_WRITES[case]}
    run = ambient_stack.run_flow(user_flow(location, ambient_minio_connection, writer=writer))
    _succeeded(run)
    _assert_user_rows(pl.read_delta(target, storage_options=MINIO_OPTIONS))


@LOCATIONS
def test_no_connection_reader_reads_with_local_aws_profile(
    ambient_stack, user_flow, ambient_minio_connection, new_target, location
):
    """Run 1382: a "No connection" reader built aws-cli options that polars rejected."""
    target = new_target("table")
    reader = {"auth_mode": "aws-cli", "connection_name": None}
    writer = {"auth_mode": "access_key", "connection_name": ambient_minio_connection, "resource_path": target}
    run = ambient_stack.run_flow(user_flow(location, ambient_minio_connection, reader=reader, writer=writer))
    _succeeded(run)
    _assert_user_rows(pl.read_delta(target, storage_options=MINIO_OPTIONS))


@pytest.fixture(scope="module")
def aws_cli_connection(stack):
    """A saved aws-cli connection naming the ``minio`` profile and MinIO's endpoint."""
    name = f"minio aws-cli {uuid.uuid4().hex[:6]}"
    stack.create_minio_connection(
        name, auth_method="aws-cli", aws_profile=NAMED_PROFILE, aws_access_key_id=None, aws_secret_access_key=None
    )
    yield name
    stack.delete_connection(name)


@LOCATIONS
def test_aws_cli_connection_uses_its_profile_and_endpoint(stack, user_flow, aws_cli_connection, new_target, location):
    """Only the named profile holds MinIO's keys and only the connection points at MinIO over HTTP."""
    target = new_target("table")
    writer = {"auth_mode": "aws-cli", "connection_name": aws_cli_connection, "resource_path": target}
    run = stack.run_flow(user_flow(location, aws_cli_connection, writer=writer))
    _succeeded(run)
    _assert_user_rows(pl.read_delta(target, storage_options=MINIO_OPTIONS))


def test_connection_stack_has_no_ambient_route_to_minio(stack, user_flow, minio_connection, new_target):
    """Guards the harness: a connection without an endpoint must not reach MinIO through the environment."""
    name = f"minio no endpoint {uuid.uuid4().hex[:6]}"
    stack.create_minio_connection(name, endpoint_url=None, aws_allow_unsafe_html=False)
    try:
        target = new_target("table")
        writer = {"auth_mode": "access_key", "connection_name": name, "resource_path": target}
        run = stack.run_flow(user_flow("remote", minio_connection, writer=writer), timeout=240)
        assert run["nodes"][WRITER_ID]["success"] is False
        assert not list_keys(target)
    finally:
        stack.delete_connection(name)


@LOCATIONS
@pytest.mark.timeout(120)
def test_reader_empty_directory_path_fails_fast(stack, user_flow, minio_connection, location):
    flow = user_flow(location, minio_connection, reader={"resource_path": "", "scan_mode": "directory"})
    started = time.monotonic()
    run = stack.run_flow(flow)
    elapsed = time.monotonic() - started
    reader = run["nodes"][READER_ID]
    assert reader["success"] is False
    assert READER_NO_PATH in reader["error"], reader["error"]
    assert not CRASH.search(reader["error"])
    assert elapsed < 30, f"empty reader path took {elapsed:.1f}s"


def _read_back(file_format: str, target: str) -> pl.DataFrame:
    if file_format == "delta":
        return pl.read_delta(target, storage_options=MINIO_OPTIONS)
    if file_format == "parquet":
        return pl.read_parquet(target, storage_options=MINIO_OPTIONS)
    if file_format == "csv":
        return pl.scan_csv(target, separator=";", storage_options=MINIO_OPTIONS).collect()
    return pl.read_ndjson(target, storage_options=MINIO_OPTIONS)


# Non-delta settings are what the drawer leaves after switching the saved Delta writer's format.
_FILE_DEFAULTS = {"write_mode": "overwrite", "partition_by": [], "merge_keys": [], "track_changes": False}
WRITE_CASES = {
    "delta_overwrite": ({"file_format": "delta", "write_mode": "overwrite"}, "table", 6),
    "delta_upsert": (
        {"file_format": "delta", "write_mode": "upsert", "merge_keys": ["id"], "partition_by": []},
        "table",
        6,
    ),
    "delta_append_track_changes": (
        {"file_format": "delta", "write_mode": "append", "track_changes": True},
        "table",
        12,
    ),
    "parquet_overwrite": ({"file_format": "parquet", **_FILE_DEFAULTS}, "out.parquet", 6),
    "csv_overwrite": ({"file_format": "csv", **_FILE_DEFAULTS}, "out.csv", 6),
    "json_overwrite": ({"file_format": "json", **_FILE_DEFAULTS}, "out.json", 6),
}


@LOCATIONS
@pytest.mark.parametrize("case", list(WRITE_CASES))
def test_writer_formats_and_modes(stack, user_flow, minio_connection, new_target, location, case):
    settings, name, rows_after_rerun = WRITE_CASES[case]
    target = new_target(name)
    writer = {"auth_mode": "access_key", "connection_name": minio_connection, "resource_path": target, **settings}
    first = stack.run_flow(user_flow(location, minio_connection, writer=writer))
    _succeeded(first)
    _assert_user_rows(_read_back(settings["file_format"], target))

    _succeeded(stack.run(first["flow_id"]))
    _assert_user_rows(_read_back(settings["file_format"], target), rows=rows_after_rerun)
    if settings["file_format"] != "delta":
        return
    table = DeltaTable(target, storage_options=MINIO_OPTIONS)
    last_commit = table.history(1)[0]
    if settings["write_mode"] == "upsert":
        assert last_commit["operation"] == "MERGE"
    elif settings["write_mode"] == "overwrite":
        assert last_commit["operationParameters"]["mode"] == "Overwrite"
    if settings.get("track_changes"):
        assert table.metadata().configuration.get("delta.enableChangeDataFeed") == "true"
        changes = pl.from_arrow(table.load_cdf(starting_version=table.version()).read_all())
        assert changes.height == 6
        assert changes["_change_type"].unique().to_list() == ["insert"]
