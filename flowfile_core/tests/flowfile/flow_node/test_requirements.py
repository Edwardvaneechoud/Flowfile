"""What a node needs from its compute, derived from its template and settings, per node family.

Each test builds a family's settings model and passes it with the family's template from the node store (a custom
node's template is built by hand), so no graph, worker or Docker is involved.
"""

import pytest

from flowfile_core.catalog.storage_backend import CatalogStorageTarget
from flowfile_core.configs import node_store
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_node.requirements import NodeRequirements, requirements_for
from flowfile_core.schemas import cloud_storage_schemas, input_schema, transform_schema
from flowfile_core.schemas.schemas import NodeTemplate
from shared.compute_protocol import PLUGIN_PACKAGES

pytestmark = pytest.mark.core

NOTHING = NodeRequirements()
ON_KERNEL = NodeRequirements(operation=OperationKind.file_write, write_schemes=frozenset({"file"}), needs_kernel=True)
LOCAL_CATALOG = CatalogStorageTarget(is_cloud=False, base="/data/catalog_tables")
CLOUD_CATALOG = CatalogStorageTarget(is_cloud=True, base="s3://lake/catalog", connection_name="lake")


def _requirements(item: str, settings, **kwargs) -> NodeRequirements:
    return requirements_for(node_store.node_dict[item], settings, **kwargs)


def _ids(**fields) -> dict:
    return {"flow_id": 1, "node_id": 2, **fields}


def _custom_template(execution_environment: str | None) -> NodeTemplate:
    return NodeTemplate(
        name="Upper case",
        item="upper_case",
        input=1,
        output=1,
        image="user-defined-icon.png",
        node_type="process",
        transform_type="wide",
        node_group="custom",
        custom_node=True,
        execution_environment=execution_environment,
    )


def _read(path: str, file_type: str, **table_settings) -> input_schema.NodeRead:
    received = input_schema.ReceivedTable(path=path, file_type=file_type, table_settings=table_settings)
    return input_schema.NodeRead(**_ids(received_file=received))


@pytest.mark.parametrize(
    ("file_type", "table_settings", "operation"),
    [
        ("parquet", {}, None),
        ("ipc", {}, None),
        ("ndjson", {}, None),
        ("csv", {"encoding": "utf-8"}, None),
        ("csv", {"encoding": "latin-1"}, OperationKind.file_read),
        ("excel", {}, OperationKind.file_read),
        ("json", {}, OperationKind.file_read),
        ("avro", {}, OperationKind.file_read),
    ],
)
def test_a_read_runs_file_read_only_for_a_format_polars_cannot_scan(tmp_path, file_type, table_settings, operation):
    settings = _read(str(tmp_path / f"data.{file_type}"), file_type, **table_settings)

    assert _requirements("read", settings) == NodeRequirements(operation=operation, read_schemes=frozenset({"file"}))


@pytest.mark.parametrize(
    ("path", "scheme"),
    [
        ("https://example.com/data.parquet", "https"),
        ("http://example.com/data.parquet", "http"),
        ("s3://bucket/data.parquet", "s3"),
        ("gs://bucket/data.parquet", "gs"),
    ],
)
def test_a_read_names_the_scheme_of_its_path(path, scheme):
    assert _requirements("read", _read(path, "parquet")).read_schemes == frozenset({scheme})


def test_an_excel_read_from_a_url_runs_file_read_over_https():
    settings = _read("https://example.com/book.xlsx", "excel")

    assert _requirements("read", settings) == NodeRequirements(
        operation=OperationKind.file_read, read_schemes=frozenset({"https"})
    )


def test_an_output_writes_a_local_file(tmp_path):
    output = input_schema.OutputSettings(
        name="out.csv", directory=str(tmp_path), file_type="csv", table_settings=input_schema.OutputCsvTable()
    )
    settings = input_schema.NodeOutput(**_ids(output_settings=output))

    assert _requirements("output", settings) == NodeRequirements(
        operation=OperationKind.file_write, write_schemes=frozenset({"file"})
    )


@pytest.mark.parametrize(
    ("resource_path", "scheme"),
    [
        ("s3://bucket/data.parquet", "s3"),
        ("s3a://bucket/data.parquet", "s3"),
        ("az://container/data.parquet", "az"),
        ("abfss://container@account.dfs.core.windows.net/data.parquet", "az"),
        ("gs://bucket/data.parquet", "gs"),
    ],
)
def test_a_cloud_read_is_a_plan_over_its_object_storage_scheme(resource_path, scheme):
    cloud = cloud_storage_schemas.CloudStorageReadSettings(resource_path=resource_path, connection_name="minio")
    settings = input_schema.NodeCloudStorageReader(**_ids(cloud_storage_settings=cloud))

    assert _requirements("cloud_storage_reader", settings) == NodeRequirements(
        read_schemes=frozenset({scheme}), credentials=frozenset({"minio"})
    )


def test_a_cloud_change_read_is_a_plan_too():
    cloud = cloud_storage_schemas.CloudStorageReadSettings(
        resource_path="s3://bucket/table",
        connection_name="minio",
        file_format="delta",
        cdc_mode="since_version",
        cdc_from_version=1,
    )
    settings = input_schema.NodeCloudStorageReader(**_ids(cloud_storage_settings=cloud))

    assert _requirements("cloud_storage_reader", settings) == NodeRequirements(
        read_schemes=frozenset({"s3"}), credentials=frozenset({"minio"})
    )


def test_a_cloud_read_without_a_connection_names_none():
    cloud = cloud_storage_schemas.CloudStorageReadSettings(resource_path="s3://bucket/data.parquet")
    settings = input_schema.NodeCloudStorageReader(**_ids(cloud_storage_settings=cloud))

    assert _requirements("cloud_storage_reader", settings).credentials == frozenset()


@pytest.mark.parametrize(
    ("write", "operation"),
    [
        ({"file_format": "parquet"}, OperationKind.cloud_write),
        ({"file_format": "delta", "write_mode": "append"}, OperationKind.cloud_write),
        ({"file_format": "delta", "write_mode": "append", "track_changes": True}, OperationKind.delta_write),
        ({"file_format": "delta", "write_mode": "upsert", "merge_keys": ["id"]}, OperationKind.delta_merge),
        ({"file_format": "delta", "write_mode": "delete", "merge_keys": ["id"]}, OperationKind.delta_merge),
    ],
)
def test_a_cloud_write_runs_the_delta_operations_only_for_a_merge_or_a_tracked_write(write, operation):
    cloud = cloud_storage_schemas.CloudStorageWriteSettings(
        resource_path="az://container/table", connection_name="adls", **write
    )
    settings = input_schema.NodeCloudStorageWriter(**_ids(cloud_storage_settings=cloud))

    assert _requirements("cloud_storage_writer", settings) == NodeRequirements(
        operation=operation, write_schemes=frozenset({"az"}), credentials=frozenset({"adls"})
    )


def _cloud_nodes(resource_path: str) -> tuple[NodeRequirements, NodeRequirements]:
    read = cloud_storage_schemas.CloudStorageReadSettings(resource_path=resource_path, connection_name="minio")
    write = cloud_storage_schemas.CloudStorageWriteSettings(resource_path=resource_path, connection_name="minio")
    reader = input_schema.NodeCloudStorageReader(**_ids(cloud_storage_settings=read))
    writer = input_schema.NodeCloudStorageWriter(**_ids(cloud_storage_settings=write))
    return _requirements("cloud_storage_reader", reader), _requirements("cloud_storage_writer", writer)


@pytest.mark.parametrize(
    "resource_path",
    ["", "   ", "output/table", "https://example.com/data.parquet"],
    ids=["empty", "blank", "relative", "url"],
)
def test_a_cloud_read_or_write_claims_no_scheme_for_a_path_it_refuses_itself(resource_path):
    reader, writer = _cloud_nodes(resource_path)

    assert reader == NodeRequirements(credentials=frozenset({"minio"}))
    assert writer == NodeRequirements(operation=OperationKind.cloud_write, credentials=frozenset({"minio"}))


def test_a_cloud_read_or_write_on_an_absolute_local_path_claims_file(tmp_path):
    reader, writer = _cloud_nodes(str(tmp_path / "table"))

    assert reader.read_schemes == frozenset({"file"})
    assert writer.write_schemes == frozenset({"file"})


@pytest.mark.parametrize(
    ("resource_path", "scheme"),
    [("${root}/table", "file"), ("s3://${bucket}/table", "s3")],
    ids=["local", "object_storage"],
)
def test_a_cloud_read_or_write_on_a_parameterised_path_claims_the_scheme_of_its_text(resource_path, scheme):
    reader, writer = _cloud_nodes(resource_path)

    assert reader.read_schemes == frozenset({scheme})
    assert writer.write_schemes == frozenset({scheme})


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (LOCAL_CATALOG, NodeRequirements(read_schemes=frozenset({"file"}))),
        (CLOUD_CATALOG, NodeRequirements(read_schemes=frozenset({"s3"}), credentials=frozenset({"lake"}))),
        (None, NOTHING),
    ],
)
def test_a_catalog_read_takes_its_scheme_and_connection_from_the_storage_target(target, expected):
    settings = input_schema.NodeCatalogReader(**_ids(catalog_table_id=7))

    assert _requirements("catalog_reader", settings, catalog_target=target) == expected


@pytest.mark.parametrize(
    ("write", "operation"),
    [
        ({"write_mode": "overwrite"}, OperationKind.delta_write),
        ({"write_mode": "append", "track_changes": True}, OperationKind.delta_write),
        ({"write_mode": "error"}, OperationKind.delta_write),
        ({"write_mode": "upsert", "merge_keys": ["id"]}, OperationKind.delta_merge),
        ({"write_mode": "scd2", "merge_keys": ["id"]}, OperationKind.delta_scd2),
    ],
)
def test_a_catalog_write_runs_the_delta_operation_of_its_mode(write, operation):
    settings = input_schema.NodeCatalogWriter(
        **_ids(catalog_write_settings=input_schema.CatalogWriteSettings(table_name="orders", **write))
    )

    assert _requirements("catalog_writer", settings, catalog_target=CLOUD_CATALOG) == NodeRequirements(
        operation=operation, write_schemes=frozenset({"s3"}), credentials=frozenset({"lake"})
    )
    assert _requirements("catalog_writer", settings, catalog_target=LOCAL_CATALOG) == NodeRequirements(
        operation=operation, write_schemes=frozenset({"file"})
    )
    assert _requirements("catalog_writer", settings) == NodeRequirements(operation=operation)


def test_a_virtual_catalog_write_needs_nothing_from_its_compute():
    settings = input_schema.NodeCatalogWriter(
        **_ids(catalog_write_settings=input_schema.CatalogWriteSettings(table_name="orders", write_mode="virtual"))
    )

    assert _requirements("catalog_writer", settings, catalog_target=CLOUD_CATALOG) == NOTHING


def _database_settings(mode: str | None, database_type: str = "postgresql", **fields) -> dict:
    if mode != "inline":
        return {"connection_mode": mode, "database_connection_name": "warehouse", **fields}
    connection = input_schema.DatabaseConnection(database_type=database_type, database="/data/local.db")
    return {"connection_mode": "inline", "database_connection": connection, **fields}


@pytest.mark.parametrize(
    ("mode", "database_type", "schemes", "credentials"),
    [
        ("reference", "postgresql", frozenset(), frozenset({"warehouse"})),
        (None, "postgresql", frozenset(), frozenset({"warehouse"})),
        ("inline", "postgresql", frozenset(), frozenset()),
        ("inline", "sqlite", frozenset({"file"}), frozenset()),
        ("inline", "duckdb", frozenset({"file"}), frozenset()),
    ],
)
def test_a_database_read_names_its_stored_connection_or_reads_its_local_file(mode, database_type, schemes, credentials):
    database = input_schema.DatabaseSettings(**_database_settings(mode, database_type, table_name="orders"))
    settings = input_schema.NodeDatabaseReader(**_ids(database_settings=database))

    assert _requirements("database_reader", settings) == NodeRequirements(
        operation=OperationKind.database_read, read_schemes=schemes, credentials=credentials
    )


@pytest.mark.parametrize(
    ("mode", "database_type", "schemes", "credentials"),
    [
        ("reference", "postgresql", frozenset(), frozenset({"warehouse"})),
        (None, "postgresql", frozenset(), frozenset({"warehouse"})),
        ("inline", "mysql", frozenset(), frozenset()),
        ("inline", "sqlite", frozenset({"file"}), frozenset()),
    ],
)
def test_a_database_write_names_its_stored_connection_or_writes_its_local_file(
    mode, database_type, schemes, credentials
):
    database = input_schema.DatabaseWriteSettings(**_database_settings(mode, database_type, table_name="orders"))
    settings = input_schema.NodeDatabaseWriter(**_ids(database_write_settings=database))

    assert _requirements("database_writer", settings) == NodeRequirements(
        operation=OperationKind.database_write, write_schemes=schemes, credentials=credentials
    )


@pytest.mark.parametrize(("connection_name", "credentials"), [("broker", frozenset({"broker"})), (None, frozenset())])
def test_a_kafka_source_runs_kafka_read_with_its_connection(connection_name, credentials):
    kafka = input_schema.KafkaSourceSettings(kafka_connection_name=connection_name, topic_name="orders")
    settings = input_schema.NodeKafkaSource(**_ids(kafka_settings=kafka))

    assert _requirements("kafka_source", settings) == NodeRequirements(
        operation=OperationKind.kafka_read, credentials=credentials
    )


def test_a_google_analytics_read_names_its_connection():
    ga = input_schema.GoogleAnalyticsSettings(ga_connection_name="marketing", property_id="123")
    settings = input_schema.NodeGoogleAnalyticsReader(**_ids(google_analytics_settings=ga))

    assert _requirements("google_analytics_reader", settings) == NodeRequirements(
        operation=OperationKind.google_analytics_read, credentials=frozenset({"marketing"})
    )


def test_a_rest_api_read_names_no_connection_for_its_secret():
    rest = input_schema.RestApiSettings(
        url="https://api.example.com/items",
        auth=input_schema.RestApiAuthSettings(auth_type="bearer", secret_name="api_token"),
    )
    settings = input_schema.NodeRestApiReader(**_ids(rest_api_settings=rest))

    assert _requirements("rest_api_reader", settings) == NodeRequirements(operation=OperationKind.rest_api_read)


def test_a_fuzzy_match_runs_fuzzy_join_and_needs_polars_simed():
    join = transform_schema.FuzzyMatchInput(
        join_mapping=[transform_schema.FuzzyMap(left_col="name", right_col="fname", fuzzy_type="levenshtein")],
        left_select=[transform_schema.SelectInput("name")],
        right_select=[transform_schema.SelectInput("fname")],
    )
    settings = input_schema.NodeFuzzyMatch(**_ids(depending_on_ids=[1, 3], join_input=join))

    requirements = _requirements("fuzzy_match", settings)

    assert requirements == NodeRequirements(operation=OperationKind.fuzzy_join, plugins=frozenset({"polars_simed"}))
    assert requirements.plugins <= set(PLUGIN_PACKAGES)


def test_a_random_split_runs_random_split():
    settings = input_schema.NodeRandomSplit(**_ids(depending_on_id=1))

    assert _requirements("random_split", settings) == NodeRequirements(operation=OperationKind.random_split)


@pytest.mark.parametrize(
    ("item", "settings"),
    [
        (
            "graph_solver",
            input_schema.NodeGraphSolver(
                **_ids(graph_solver_input=transform_schema.GraphSolverInput(col_from="a", col_to="b"))
            ),
        ),
        (
            "explode_hierarchy",
            input_schema.NodeExplodeHierarchy(
                **_ids(
                    explode_hierarchy_input=transform_schema.ExplodeHierarchyInput(
                        parent_column="parent", child_column="child"
                    )
                )
            ),
        ),
    ],
)
def test_graph_solver_and_explode_hierarchy_need_polars_grouper(item, settings):
    requirements = _requirements(item, settings)

    assert requirements == NodeRequirements(plugins=frozenset({"polars_grouper"}))
    assert requirements.plugins <= set(PLUGIN_PACKAGES)


def _formula(expression: str) -> transform_schema.FunctionInput:
    return transform_schema.FunctionInput(field=transform_schema.FieldInput(name="out"), function=expression)


@pytest.mark.parametrize(
    ("item", "settings"),
    [
        ("formula", input_schema.NodeFormula(**_ids(function=_formula("[a] + 1")))),
        ("formula", input_schema.NodeFormula(**_ids(functions=[]))),
        ("multi_field_formula", input_schema.NodeMultiFieldFormula(**_ids())),
        (
            "filter",
            input_schema.NodeFilter(
                **_ids(filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > 1"))
            ),
        ),
        (
            "gate",
            input_schema.NodeGate(
                **_ids(gate_input=transform_schema.GateInput(condition_source="formula", formula="[a] > 1"))
            ),
        ),
        (
            "dynamic_rename",
            input_schema.NodeDynamicRename(
                **_ids(
                    dynamic_rename_input=transform_schema.DynamicRenameInput(
                        rename_mode="formula", formula="uppercase([column_name])"
                    )
                )
            ),
        ),
    ],
)
def test_a_node_that_carries_a_formula_needs_polars_ds(item, settings):
    requirements = _requirements(item, settings)

    assert requirements == NodeRequirements(plugins=frozenset({"polars_ds"}))
    assert requirements.plugins <= set(PLUGIN_PACKAGES)


@pytest.mark.parametrize(
    ("item", "settings"),
    [
        (
            "filter",
            input_schema.NodeFilter(
                **_ids(
                    filter_input=transform_schema.FilterInput(
                        mode="basic", basic_filter=transform_schema.BasicFilter(field="a", value="1")
                    )
                )
            ),
        ),
        (
            "gate",
            input_schema.NodeGate(
                **_ids(gate_input=transform_schema.GateInput(condition_source="parameter", parameter="region"))
            ),
        ),
        (
            "dynamic_rename",
            input_schema.NodeDynamicRename(
                **_ids(dynamic_rename_input=transform_schema.DynamicRenameInput(rename_mode="prefix", prefix="v2_"))
            ),
        ),
    ],
)
def test_the_same_node_without_a_formula_needs_nothing(item, settings):
    assert _requirements(item, settings) == NOTHING


@pytest.mark.parametrize(
    ("publish_to_catalog", "artifact_scheme", "write_schemes"),
    [(False, "file", set()), (True, "file", {"file"}), (True, "https", {"https"}), (True, None, set())],
)
def test_train_model_writes_the_artifact_store_only_when_it_publishes(
    publish_to_catalog, artifact_scheme, write_schemes
):
    train_input = input_schema.TrainModelSettings(model_name="m", publish_to_catalog=publish_to_catalog)
    settings = input_schema.NodeTrainModel(**_ids(depending_on_id=1, train_input=train_input))

    assert _requirements("train_model", settings, artifact_scheme=artifact_scheme) == NodeRequirements(
        operation=OperationKind.ml_train, write_schemes=frozenset(write_schemes)
    )


@pytest.mark.parametrize(
    ("source", "artifact_scheme", "read_schemes"),
    [
        ("upstream", "file", set()),
        ("catalog", "file", {"file"}),
        ("catalog", "https", {"https"}),
        ("catalog", None, set()),
    ],
)
def test_apply_model_reads_the_artifact_store_only_from_the_catalog(source, artifact_scheme, read_schemes):
    apply_input = input_schema.ApplyModelSettings(source=source, upstream_node_id=1, model_name="m")
    settings = input_schema.NodeApplyModel(**_ids(depending_on_id=1, apply_input=apply_input))

    assert _requirements("apply_model", settings, artifact_scheme=artifact_scheme) == NodeRequirements(
        operation=OperationKind.ml_apply, read_schemes=frozenset(read_schemes)
    )


@pytest.mark.parametrize("kernel_id", ["analytics", None])
def test_a_python_script_runs_on_a_kernel_by_its_template(kernel_id):
    settings = input_schema.NodePythonScript(
        **_ids(python_script_input=input_schema.PythonScriptInput(code="x = 1", kernel_id=kernel_id))
    )

    assert _requirements("python_script", settings) == ON_KERNEL


@pytest.mark.parametrize(
    ("execution_environment", "kernel_id", "expected"),
    [
        ("local", None, NodeRequirements(operation=OperationKind.custom_node)),
        (None, None, NodeRequirements(operation=OperationKind.custom_node)),
        ("kernel", None, ON_KERNEL),
        ("kernel", "analytics", ON_KERNEL),
        ("local", "analytics", ON_KERNEL),
    ],
)
def test_a_custom_node_runs_on_a_kernel_when_its_manifest_or_its_settings_say_so(
    execution_environment, kernel_id, expected
):
    settings = input_schema.UserDefinedNode(**_ids(kernel_id=kernel_id, is_user_defined=True))

    assert requirements_for(_custom_template(execution_environment), settings) == expected


@pytest.mark.parametrize(
    ("template", "needs_kernel"),
    [
        (node_store.node_dict["python_script"], True),
        (_custom_template("kernel"), True),
        (_custom_template("local"), False),
        (node_store.node_dict["fuzzy_match"], False),
        (node_store.node_dict["cloud_storage_reader"], False),
        (node_store.node_dict["catalog_writer"], False),
    ],
)
def test_a_promise_answers_only_whether_its_template_runs_on_a_kernel(template, needs_kernel):
    settings = input_schema.NodePromise(**_ids(node_type=template.item))

    assert requirements_for(template, settings, catalog_target=CLOUD_CATALOG) == NodeRequirements(
        needs_kernel=needs_kernel
    )


@pytest.mark.parametrize(
    ("item", "settings"),
    [
        ("select", input_schema.NodeSelect(**_ids())),
        ("sort", input_schema.NodeSort(**_ids())),
        ("union", input_schema.NodeUnion(**_ids())),
        ("group_by", input_schema.NodeGroupBy(**_ids())),
        ("pivot", input_schema.NodePivot(**_ids())),
        ("unpivot", input_schema.NodeUnpivot(**_ids())),
        ("record_count", input_schema.NodeRecordCount(**_ids())),
        ("sample", input_schema.NodeSample(**_ids())),
        (
            "sql_query",
            input_schema.NodeSqlQuery(
                **_ids(sql_query_input=transform_schema.SqlQueryInput(sql_code="SELECT * FROM input_1"))
            ),
        ),
        (
            "manual_input",
            input_schema.NodeManualInput(**_ids(raw_data_format=input_schema.RawData(columns=[], data=[]))),
        ),
        ("list_files", input_schema.NodeListFiles(**_ids(path="/data"))),
        ("explore_data", input_schema.NodeExploreData(**_ids())),
        ("evaluate_model", input_schema.NodeEvaluateModel(**_ids())),
        ("wait_for", input_schema.NodeWaitFor(**_ids())),
        ("api_response", input_schema.NodeApiResponse(**_ids())),
        ("flow_output", input_schema.NodeFlowOutput(**_ids())),
    ],
)
def test_a_plain_transform_or_a_node_working_in_core_needs_nothing(item, settings):
    assert _requirements(item, settings) == NOTHING


def test_a_polars_code_node_runs_the_polars_code_operation():
    settings = input_schema.NodePolarsCode(
        **_ids(polars_code_input=transform_schema.PolarsCodeInput(polars_code="output_df = input_df"))
    )

    assert _requirements("polars_code", settings) == NodeRequirements(operation=OperationKind.polars_code)
