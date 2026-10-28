"""Failures typed by what went wrong, and leftovers a failure must not leave.

A task-route poll that loses its worker is the backend being out of reach (``ComputeUnavailable`` naming it), not the
task's own failure. A custom node run in this process whose later output fails to be written leaves none of its
earlier outputs held. A plan of an ``aws-cli`` read carries its profile's region when the connection names none, and
never its credentials.
"""

import os
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import _run
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.execution.payloads import CustomNodePayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import BaseFetcher
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

TEAM = "worker:team"


def test_a_poll_that_loses_its_worker_is_unavailable_naming_the_backend(closed_port_url):
    handshake.reset_handshake(closed_port_url)
    fetcher = BaseFetcher("t-lost-poll", worker_url=closed_port_url)

    def start() -> BaseFetcher:
        fetcher.running = True
        return fetcher

    with pytest.raises(ComputeUnavailable) as excinfo:
        _run(start, backend_id=TEAM)

    assert excinfo.value.backend_id == TEAM
    assert str(excinfo.value).startswith(f"Lost the worker at {closed_port_url} while following task t-lost-poll")
    assert fetcher.error_kind == "connection"


class TwoOutputs:
    """A custom node with no node file whose second output fails when it is written."""

    @classmethod
    def from_settings(cls, values):
        return cls()

    def process(self, *inputs):
        good = pl.LazyFrame({"a": [1, 2]})
        bad = pl.LazyFrame({"a": ["1", "x"]}).select(pl.col("a").cast(pl.Int64))
        return {"good": good, "bad": bad}


def test_a_custom_node_whose_later_output_fails_holds_none_of_its_outputs(held):
    from flowfile_worker.results import result_path

    job = CustomNodePayload(
        inputs=(),
        custom_node=TwoOutputs(),
        node_source=None,
        class_name=None,
        settings_values={},
        output_names=["good", "bad"],
        user_id=None,
        flow_id=-9,
        node_id=1,
        job_id="t-two-outputs",
    )

    with pytest.raises(Exception, match="conversion"):
        registry.backend(registry.LOCAL_BACKEND_ID).run_op(OperationKind.custom_node, op_payload(job))

    assert held() == set()
    assert not os.path.exists(result_path(-9, "t-two-outputs_0"))
    assert not os.path.exists(result_path(-9, "t-two-outputs_1"))


@pytest.fixture
def aws_profile(tmp_path: Path, monkeypatch) -> str:
    """An AWS config with a profile that names a region and a credentials file that holds a key for it."""
    config = tmp_path / "config"
    config.write_text("[profile flowfile-region]\nregion = eu-west-3\n")
    credentials = tmp_path / "credentials"
    credentials.write_text("[flowfile-region]\naws_access_key_id = AKIATEST\naws_secret_access_key = not-a-secret\n")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    return "flowfile-region"


def aws_cli(profile: str | None, region: str | None = None) -> FullCloudStorageConnection:
    return FullCloudStorageConnection(
        storage_type="s3", auth_method="aws-cli", connection_name="team", aws_profile=profile, aws_region=region
    )


def test_an_aws_cli_plan_carries_its_profiles_region_and_no_credential(aws_profile):
    options = CloudStorageReader.get_plan_storage_options(aws_cli(aws_profile))

    assert options["aws_region"] == "eu-west-3"
    assert not {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"} & options.keys()
    assert "not-a-secret" not in repr(options)


def test_an_aws_cli_plan_keeps_the_region_its_connection_names(aws_profile):
    assert CloudStorageReader.get_plan_storage_options(aws_cli(aws_profile, "us-east-2"))["aws_region"] == "us-east-2"


def test_an_aws_cli_plan_of_a_profile_that_does_not_exist_names_no_region(aws_profile):
    assert "aws_region" not in CloudStorageReader.get_plan_storage_options(aws_cli("no-such-profile"))
