from base64 import b64decode
from io import BytesIO

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars_grouper import graph_solver

from flowfile_worker import main, models
from flowfile_worker.external_sources.s3_source.models import (
    WriteSettings,
)
from flowfile_worker.secrets import encrypt_secret

client = TestClient(main.app)


try:
    # noinspection PyUnresolvedReferences
    from test_utils.s3.fixtures import get_minio_client
    from tests.utils import cloud_storage_connection_settings, find_parent_directory, is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    # noinspection PyUnresolvedReferences
    from utils import find_parent_directory, is_docker_available


@pytest.fixture
def pw():
    return encrypt_secret('testpass')


@pytest.fixture
def create_fuzzy_data() -> models.FuzzyJoinInput:
    fuzzy_maps = [
        models.FuzzyMapping(left_col='name', right_col='name_right', threshold_score=60.0,
                            fuzzy_type='levenshtein'),
        models.FuzzyMapping(left_col='city', right_col='city_right', threshold_score=60.0, fuzzy_type='levenshtein')
    ]

    # Create the left and right DataFrames with the provided data
    left_df = pl.LazyFrame(
        data=[
            ('John', 'weert'),
            ('Johan', 'eindhoven'),
            ('Johannes', 'eindhoven'),
            ('Edward', 'denbosch'),
            ('Edwin', 'utrecht')
        ],
        orient='row',
        schema=['name', 'city']
    )

    right_df = pl.LazyFrame(
        data=[
            ('John', 'weert'),
            ('Johan', 'eindhoven'),
            ('Johannes', 'eindhoven'),
            ('Edward', 'denbosch'),
            ('Edwin', 'utrecht')
        ],
        orient='row',
        schema=['name_right', 'city_right']
    )

    # Serialize DataFrames - use raw bytes, Pydantic handles base64 for JSON transport
    left_serializable_object = models.PolarsOperation(
        operation=left_df.serialize(),
        flowfile_flow_id=1
    )
    right_serializable_object = models.PolarsOperation(
        operation=right_df.serialize(),
        flowfile_flow_id=1
    )

    # Return the FuzzyJoinInput
    return models.FuzzyJoinInput(
        left_df_operation=left_serializable_object,
        right_df_operation=right_serializable_object,
        fuzzy_maps=fuzzy_maps
    )


@pytest.fixture
def create_grouper_data():
    df = pl.DataFrame(
        {
            "from": ["A", "B", "C", "E", "F", "G", "I", "I", 'AA'],
            "to": ["B", "C", "D", "F", "G", "J", "K", "J", 'Z']
        }
    )
    return df.select(graph_solver(pl.col("from"), pl.col("to")).alias('group')).lazy()


def test_external_package(create_grouper_data):
    df = create_grouper_data
    # Send raw bytes with metadata in headers
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Operation-Type": "store",
        "X-Flow-Id": "1",
        "X-Node-Id": "-1",
    }
    v = client.post('/submit_query/', content=df.serialize(), headers=headers)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    # Results are base64-encoded string in JSON response, decode once
    result_df = pl.LazyFrame.deserialize(BytesIO(b64decode(status.results))).collect()
    assert result_df.equals(df.collect()), f'Expected:\n{df.collect()}\n\nResult:\n{result_df}'


def test_add_fuzzy_join(create_fuzzy_data):
    load = create_fuzzy_data
    # Use model_dump_json() - Pydantic handles single base64 encoding for bytes in JSON
    v = client.post('/add_fuzzy_join', data=load.model_dump_json())
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    # Results are base64-encoded string in JSON response, decode once
    pl.LazyFrame.deserialize(BytesIO(b64decode(status.results))).collect()


def test_sample():
    lf = pl.LazyFrame({'value': [i for i in range(1000)]})
    # Send raw bytes with metadata in headers
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Operation-Type": "store_sample",
        "X-Sample-Size": "10",
        "X-Flow-Id": "1",
        "X-Node-Id": "-1",
    }
    v = client.post('/store_sample/', content=lf.serialize(), headers=headers)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    result_df = pl.read_ipc(status.file_ref)
    assert result_df.equals(lf.collect().limit(10)), f'Expected:\n{lf.collect()}\n\nResult:\n{result_df}'


def test_polars_transformation():
    df = (pl.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]).lazy()
          .select((pl.col('a') + pl.col('b')).alias('total'))
          )
    # Send raw bytes with metadata in headers
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Operation-Type": "store",
        "X-Flow-Id": "1",
        "X-Node-Id": "-1",
    }
    v = client.post('/submit_query/', content=df.serialize(), headers=headers)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    # Results are base64-encoded string in JSON response, decode once
    result_df = pl.LazyFrame.deserialize(BytesIO(b64decode(status.results))).collect()
    assert result_df.equals(df.collect()), f'Expected:\n{df.collect()}\n\nResult:\n{result_df}'


def test_create_func():
    file_type = 'csv'
    from flowfile_core.schemas import input_schema
    path = str(find_parent_directory("Flowfile") / "flowfile_core/tests/inputFile/Mall_Customers.csv")
    received_table = input_schema.ReceivedTable(name="Mall_Customers.csv",
                                                path=path,
                                                file_type="csv",
                                                table_settings=input_schema.InputCsvTable(
                                                    delimiter=",",
                                                    has_headers=True,
                                                    encoding="ISO-8859-1",
                                                    infer_schema_length=260000,
                                                    truncate_ragged_lines=False,
                                                    ignore_errors=False
                                                )).model_dump()

    v = client.post(f'/create_table/{file_type}', json=received_table)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    # Results are base64-encoded string in JSON response, decode once
    try:
        result_df = pl.LazyFrame.deserialize(BytesIO(b64decode(status.results)))
    except Exception:
        raise Exception('Error with deserializing the DataFrame')


def test_write_output_csv():
    lf = pl.LazyFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    # Use raw bytes - Pydantic model handles JSON serialization with single base64 encoding
    polars_script_write = models.PolarsScriptWrite(
        operation=lf.serialize(),
        data_type='csv',
        path="flowfile_core/tests/inputFile/Mall_Customers.csv",
        write_mode='overwrite',
        sheet_name='Sheet1',
        delimiter=','
    )
    v = client.post('/write_results/', data=polars_script_write.model_dump_json())
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    df = pl.read_csv(status.file_ref)
    assert df.count()[0, 0] == 3, f'Expected 3 records, got {df.count()[0, 0]}'


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_store_sql_result(pw):
    database_connection = dict(host='localhost', password=pw, username='testuser', port=5433, database='testdb')
    sql_source_settings = dict(connection=database_connection, query='SELECT * FROM public.movies')
    v = client.post('/store_database_read_result', json=sql_source_settings)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status = models.Status.model_validate(v.json())
    assert status.status == 'Starting', 'Expected status to be Starting'
    r = client.get(f'/status/{status.background_task_id}')
    assert r.status_code == 200, r.text
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    assert status.status == 'Completed', 'Expected status to be Completed'
    # Results are base64-encoded string in JSON response, decode once
    try:
        result_df = pl.LazyFrame.deserialize(BytesIO(b64decode(status.results))).collect()
    except Exception:
        raise Exception('Error with deserializing the DataFrame')
    assert result_df.shape[0] > 0, 'Expected to get some data from the database'


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_store_in_database(pw):
    lf = pl.LazyFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    # Use raw bytes - Base64Bytes will encode to base64 string for JSON
    from base64 import b64encode
    settings_data = {
        'connection': {'username': 'testuser', 'password': pw, 'host': 'localhost', 'port': 5433,
                       'database': 'testdb', 'database_type': 'postgresql', 'url': None},
        'table_name': 'public.test_output',
        'if_exists': 'replace',
        'flowfile_flow_id': 1,
        'flowfile_node_id': -1,
        'operation': b64encode(lf.serialize()).decode('ascii')  # Base64 string for JSON
    }
    v = client.post('/store_database_write_result/', json=settings_data)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status = models.Status.model_validate(v.json())
    assert status.status == 'Starting', 'Expected status to be Starting'
    r = client.get(f'/status/{status.background_task_id}')
    assert r.status_code == 200, r.text
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    assert status.status == 'Completed', 'Expected status to be Completed'


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_store_in_cloud_storage(cloud_storage_connection_settings):
    lf = pl.LazyFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    # Create model and serialize manually to preserve SecretStr values
    from base64 import b64encode
    cloud_write_settings = models.CloudStorageScriptWrite(
        connection=cloud_storage_connection_settings,
        write_settings=WriteSettings(
            resource_path="s3://worker-test-bucket/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy"
        ),
        operation=lf.serialize()
    )
    # Use model_dump and manually handle SecretStr to preserve secret values
    settings_data = cloud_write_settings.model_dump()
    settings_data["connection"]["aws_secret_access_key"] = (
        settings_data["connection"]["aws_secret_access_key"].get_secret_value()
        if hasattr(settings_data["connection"]["aws_secret_access_key"], 'get_secret_value')
        else settings_data["connection"]["aws_secret_access_key"]
    )
    settings_data["operation"] = b64encode(settings_data["operation"]).decode('ascii')
    v = client.post('/write_data_to_cloud/', json=settings_data)
    assert v.status_code == 200, v.text
    assert models.Status.model_validate(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.model_validate(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.model_validate(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    assert status.status == 'Completed', 'Expected status to be Completed'
