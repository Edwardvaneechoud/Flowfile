"""A cloud reader/writer without an object-storage path fails before any I/O, instead of resolving against the cwd."""

import os
import re
import time

import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.schemas import input_schema
from shared.cloud_storage.utils import ensure_path_has_wildcard_pattern
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

WRITER_NO_PATH = (
    "Cloud storage writer has no target path. Enter an object-storage URI such as s3://bucket/folder/table."
)
READER_NO_PATH = (
    "Cloud storage reader has no source path. Enter an object-storage URI such as s3://bucket/folder/file.parquet."
)
MISLEADING_ERROR = re.compile("TypeError|is not an instance of")

# The writer shape the UI saves: "No connection", empty path, format moved off CSV.
USER_WRITER_SETTINGS = {
    "resource_path": "",
    "write_mode": "append",
    "file_format": "delta",
    "parquet_compression": "snappy",
    "csv_delimiter": ";",
    "csv_encoding": "utf8-lossy",
    "partition_by": ["output_field"],
    "merge_keys": [],
    "track_changes": False,
    "auth_mode": "aws-cli",
    "connection_name": None,
}
ROWS = [
    {"id": 1, "category": "A", "output_field": "test"},
    {"id": 2, "category": "na", "output_field": "test"},
]


@pytest.fixture
def isolated_cwd(tmp_path, monkeypatch):
    """Run from an empty directory, so a write that resolves against the cwd is visible."""
    workdir = tmp_path / "cwd"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    return workdir


@pytest.fixture
def static_aws_profile(tmp_path, monkeypatch):
    """Give aws-cli static credentials aimed at the local MinIO mock, so only the path guard can stop the write."""
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    aws_dir = tmp_path / "aws"
    aws_dir.mkdir()
    (aws_dir / "credentials").write_text(
        "[default]\naws_access_key_id = minioadmin\naws_secret_access_key = minioadmin\n"
    )
    (aws_dir / "config").write_text("[default]\nregion = us-east-1\n")
    (aws_dir / "boto.cfg").write_text("")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(aws_dir / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(aws_dir / "config"))
    monkeypatch.setenv("BOTO_CONFIG", str(aws_dir / "boto.cfg"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ALLOW_HTTP", "true")


def _writer_graph(execution_location: str, **overrides):
    graph = create_test_graph(flow_id=1, execution_location=execution_location)
    add_test_manual_input(graph, ROWS, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="cloud_storage_writer"))
    graph.add_cloud_storage_writer(
        input_schema.NodeCloudStorageWriter.model_validate(
            {
                "flow_id": 1,
                "node_id": 2,
                "user_id": 1,
                "depending_on_id": 1,
                "cloud_storage_settings": {**USER_WRITER_SETTINGS, **overrides},
            }
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
    return graph


def _failed_step(run_info, node_id: int):
    step = next(step for step in run_info.node_step_result if step.node_id == node_id)
    assert not step.success, f"node {node_id} should have failed"
    return step


def test_user_writer_settings_fail_with_the_path_error(execution_location, isolated_cwd, static_aws_profile):
    graph = _writer_graph(execution_location)
    assert graph.get_node(2).setting_input.get_default_description() == "Write (no path set) (delta, append)"

    run_info = graph.run_graph()

    assert not run_info.success
    error = _failed_step(run_info, 2).error
    assert WRITER_NO_PATH in error
    assert not MISLEADING_ERROR.search(error)
    assert list(isolated_cwd.iterdir()) == []


@pytest.mark.parametrize("path", ["output_folder/table", "table"])
def test_writer_rejects_a_relative_path(path, isolated_cwd, static_aws_profile):
    run_info = _writer_graph("local", resource_path=path).run_graph()

    error = _failed_step(run_info, 2).error
    assert f"Cloud storage path '{path}' is not a URI" in error
    assert list(isolated_cwd.iterdir()) == []


def test_directory_reader_with_empty_path_fails_fast(execution_location, isolated_cwd, static_aws_profile):
    graph = create_test_graph(flow_id=1, execution_location=execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="cloud_storage_reader"))
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=1,
            node_id=1,
            user_id=1,
            cloud_storage_settings=input_schema.CloudStorageReadSettings(
                resource_path="",
                auth_mode="aws-cli",
                scan_mode="directory",
                file_format="parquet",
            ),
        )
    )
    assert graph.get_node(1).setting_input.get_default_description() == "Read (no path set) (parquet)"

    started = time.monotonic()
    run_info = graph.run_graph()
    elapsed = time.monotonic() - started

    error = _failed_step(run_info, 1).error
    assert READER_NO_PATH in error
    assert elapsed < 5, f"an empty path should fail before any listing, took {elapsed:.1f}s"


def test_change_read_target_guards_the_path_before_resolving_a_connection():
    from flowfile_core.flowfile.flow_graph import _cloud_change_read_target

    settings = input_schema.CloudStorageReadSettings(
        resource_path="",
        connection_name="no-such-connection",
        file_format="delta",
        cdc_mode="since_version",
        cdc_from_version=0,
    )
    with pytest.raises(ValueError, match=re.escape(READER_NO_PATH)):
        _cloud_change_read_target(settings, user_id=1)


def test_wildcard_pattern_rejects_an_empty_path():
    with pytest.raises(ValueError, match=re.escape(READER_NO_PATH)):
        ensure_path_has_wildcard_pattern("", "parquet")
