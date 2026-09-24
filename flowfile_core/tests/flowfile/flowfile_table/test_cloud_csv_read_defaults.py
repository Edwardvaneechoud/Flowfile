"""A cloud CSV read with unset CSV options uses the drawer's defaults instead of crashing.

The three options are Optional on ``CloudStorageReadSettings`` (the drawer clears them when the
format moves off CSV), and Polars raises a TypeError for ``None`` on each of them.
"""

import uuid

import boto3
import pytest

from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettings, CloudStorageReadSettingsInternal
from tests.flowfile.conftest import create_test_graph
from tests.flowfile_core_test_utils import is_docker_available
from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection, get_cloud_connection

pytestmark = pytest.mark.skipif(not is_docker_available(), reason="MinIO needs Docker")

BUCKET = "flowfile-test"
FILES = ("a.csv", "b.csv")
CSV_BODY = b"id,name\n1,alpha\n2,beta\n3,gamma\n"
EXPECTED_ROWS = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}, {"id": 3, "name": "gamma"}]
CSV_OPTIONS = ("csv_has_header", "csv_delimiter", "csv_encoding")


@pytest.fixture(scope="module")
def csv_prefix():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    prefix = f"csv-read-defaults-{uuid.uuid4().hex[:12]}"
    for name in FILES:
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/{name}", Body=CSV_BODY)
    try:
        yield f"s3://{BUCKET}/{prefix}"
    finally:
        for name in FILES:
            s3.delete_object(Bucket=BUCKET, Key=f"{prefix}/{name}")


def _unset(options) -> dict:
    return {option: None for option in options}


@pytest.mark.parametrize("unset", [*((o,) for o in CSV_OPTIONS), CSV_OPTIONS], ids=lambda u: "+".join(u))
def test_engine_reads_csv_with_unset_options(csv_prefix, unset):
    read_settings = CloudStorageReadSettings(
        resource_path=f"{csv_prefix}/a.csv", file_format="csv", scan_mode="single_file", **_unset(unset)
    )
    engine = FlowDataEngine.from_cloud_storage_obj(
        CloudStorageReadSettingsInternal(connection=get_cloud_connection(), read_settings=read_settings)
    )

    assert engine.data_frame.collect().to_dicts() == EXPECTED_ROWS


def test_reader_node_runs_with_unset_options(csv_prefix, execution_location):
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()
    graph = create_test_graph(flow_id=1, execution_location=execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="cloud_storage_reader"))
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=1,
            node_id=1,
            user_id=1,
            cloud_storage_settings=CloudStorageReadSettings(
                resource_path=csv_prefix,
                file_format="csv",
                scan_mode="directory",
                connection_name=conn.connection_name,
                **_unset(CSV_OPTIONS),
            ),
        )
    )

    run_info = graph.run_graph()

    assert run_info.success, [step.error for step in run_info.node_step_result]
    data = graph.get_node(1).get_resulting_data().data_frame.collect()
    assert data.columns == ["id", "name"]
    assert sorted(data.to_dicts(), key=lambda row: row["id"]) == sorted(EXPECTED_ROWS * 2, key=lambda row: row["id"])


def test_flowframe_export_reads_csv_with_unset_options(csv_prefix):
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()
    graph = create_test_graph(flow_id=1, execution_location="local")
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="cloud_storage_reader"))
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=1,
            node_id=1,
            user_id=1,
            cloud_storage_settings=CloudStorageReadSettings(
                resource_path=f"{csv_prefix}/a.csv",
                file_format="csv",
                connection_name=conn.connection_name,
                **_unset(CSV_OPTIONS),
            ),
        )
    )
    code = export_flow_to_flowframe(graph)
    namespace = {}
    exec(code, namespace)

    exported = namespace["run_etl_pipeline"]().collect()

    assert exported.to_dicts() == EXPECTED_ROWS
    assert exported.equals(graph.get_node(1).get_resulting_data().data_frame.collect())
