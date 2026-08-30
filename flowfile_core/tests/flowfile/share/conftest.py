"""Flow builders for the share-link tests.

The canary builders plant a unique sentinel string in every settings field that
must not survive into a share link — hostnames, connection names, bucket paths,
API tokens, catalog ids, absolute paths, and the expression/code bodies that are
``eval``'d on import. Auto-generated node descriptions embed several of those,
so the sentinels double as a description-leak check.
"""

from pathlib import Path

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import cloud_storage_schemas, input_schema, schemas, transform_schema

CANARIES = {
    "db_host": "CANARYDBHOST",
    "db_user": "CANARYDBUSER",
    "db_name": "CANARYDBNAME",
    "db_table": "CANARYDBTABLE",
    "db_connection": "CANARYDBCONNECTION",
    "cloud_connection": "CANARYCLOUDCONNECTION",
    "cloud_path": "CANARYCLOUDBUCKET",
    "api_url": "CANARYAPIHOST",
    "api_token": "CANARYAPITOKEN",
    "kafka_connection": "CANARYKAFKACONNECTION",
    "kafka_topic": "CANARYKAFKATOPIC",
    "ga_connection": "CANARYGACONNECTION",
    "ga_property": "CANARYGAPROPERTY",
    "catalog_table": "CANARYCATALOGTABLE",
    "catalog_namespace": "CANARYCATALOGNAMESPACE",
    "local_dir": "CANARYLOCALDIR",
    "filter_expression": "CANARYFILTEREXPRESSION",
    "polars_code": "CANARYPOLARSCODE",
}

SAMPLE_ROWS = [{"id": 1, "city": "Amsterdam"}, {"id": 2, "city": "Berlin"}]


def make_graph(flow_id: int = 1, name: str = "share_test") -> FlowGraph:
    """An empty in-memory FlowGraph, never written to disk."""
    return FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=flow_id, name=name, path=f"/tmp/{name}.flowfile", execution_mode="Development"
        ),
        name=name,
    )


def _promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def add_manual_input(graph: FlowGraph, node_id: int = 1, data: list[dict] | None = None) -> None:
    _promise(graph, node_id, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data or SAMPLE_ROWS),
        )
    )


def add_read(graph: FlowGraph, node_id: int, path: str, file_type: str = "csv", **kwargs) -> None:
    _promise(graph, node_id, "read")
    graph.add_read(
        input_schema.NodeRead(
            flow_id=graph.flow_id,
            node_id=node_id,
            received_file=input_schema.ReceivedTable(
                name=Path(path).name,
                path=path,
                file_type=file_type,
                table_settings=input_schema.InputCsvTable(),
                **kwargs,
            ),
        )
    )


def add_filter(graph: FlowGraph, node_id: int, depends_on: int, filter_input, split_mode: bool = False) -> None:
    _promise(graph, node_id, "filter")
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depends_on,
            filter_input=filter_input,
            split_mode=split_mode,
        )
    )


def add_polars_code(graph: FlowGraph, node_id: int, depends_on: int, code: str) -> None:
    _promise(graph, node_id, "polars_code")
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=[depends_on],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        )
    )


def build_canary_flow() -> FlowGraph:
    """A flow whose every connection-bearing node carries a sentinel string."""
    graph = make_graph(name="canaries")

    _promise(graph, 1, "database_reader")
    graph.add_database_reader(
        input_schema.NodeDatabaseReader(
            flow_id=1,
            node_id=1,
            database_settings=input_schema.DatabaseSettings(
                connection_mode="inline",
                database_connection=input_schema.DatabaseConnection(
                    database_type="postgresql",
                    username=CANARIES["db_user"],
                    host=CANARIES["db_host"],
                    port=5432,
                    database=CANARIES["db_name"],
                ),
                table_name=CANARIES["db_table"],
                query_mode="table",
            ),
        )
    )

    _promise(graph, 2, "cloud_storage_writer")
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_cloud_storage_writer(
        input_schema.NodeCloudStorageWriter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            cloud_storage_settings=cloud_storage_schemas.CloudStorageWriteSettings(
                auth_mode="access_key",
                connection_name=CANARIES["cloud_connection"],
                resource_path=f"s3://{CANARIES['cloud_path']}/out.parquet",
                write_mode="overwrite",
                file_format="parquet",
            ),
        )
    )

    _promise(graph, 3, "rest_api_reader")
    graph.add_rest_api_reader(
        input_schema.NodeRestApiReader(
            flow_id=1,
            node_id=3,
            rest_api_settings=input_schema.RestApiSettings(
                url=f"https://{CANARIES['api_url']}/v1/records",
                headers={"Authorization": f"Bearer {CANARIES['api_token']}"},
            ),
        )
    )

    _promise(graph, 4, "kafka_source")
    graph.add_kafka_source(
        input_schema.NodeKafkaSource(
            flow_id=1,
            node_id=4,
            kafka_settings=input_schema.KafkaSourceSettings(
                kafka_connection_name=CANARIES["kafka_connection"],
                topic_name=CANARIES["kafka_topic"],
                sync_name="canary_sync",
            ),
        )
    )

    _promise(graph, 5, "google_analytics_reader")
    graph.add_google_analytics_reader(
        input_schema.NodeGoogleAnalyticsReader(
            flow_id=1,
            node_id=5,
            google_analytics_settings=input_schema.GoogleAnalyticsSettings(
                ga_connection_name=CANARIES["ga_connection"],
                property_id=CANARIES["ga_property"],
                metrics=["sessions"],
                dimensions=["date"],
            ),
        )
    )

    _promise(graph, 6, "catalog_reader")
    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=1,
            node_id=6,
            catalog_table_id=4242,
            catalog_table_name=CANARIES["catalog_table"],
            catalog_full_table_name=f"{CANARIES['catalog_namespace']}.{CANARIES['catalog_table']}",
            catalog_namespace_id=99,
        )
    )

    _promise(graph, 7, "catalog_writer")
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(6, 7))
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=1,
            node_id=7,
            depending_on_id=6,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=CANARIES["catalog_table"],
                namespace_id=99,
                namespace_full_name=CANARIES["catalog_namespace"],
            ),
        )
    )

    add_read(graph, 8, f"/{CANARIES['local_dir']}/sales.csv")

    add_filter(
        graph,
        9,
        8,
        # A function call, so the expression is never translated into a basic
        # filter — the canary has to stay on the placeholder path it guards.
        transform_schema.FilterInput(
            mode="advanced", advanced_filter=f"contains([city], '{CANARIES['filter_expression']}')"
        ),
    )

    add_polars_code(graph, 10, 9, f"output_df = input_df.filter(pl.col('city') != '{CANARIES['polars_code']}')")
    return graph


@pytest.fixture
def canary_flow() -> FlowGraph:
    return build_canary_flow()
