from fastapi.testclient import TestClient
import polars as pl
import base64
from io import BytesIO
from flowfile_worker import main
from flowfile_worker import models
from polars_grouper import graph_solver
from flowfile_worker.external_sources.airbyte_sources.models import AirbyteSettings

client = TestClient(main.app)


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

    # Serialize DataFrames
    left_serializable_object = models.PolarsOperation(
        operation=base64.encodebytes(left_df.serialize())
    )
    right_serializable_object = models.PolarsOperation(
        operation=base64.encodebytes(right_df.serialize())
    )

    # Return the FuzzyJoinInput
    return models.FuzzyJoinInput(
        left_df_operation=left_serializable_object,
        right_df_operation=right_serializable_object,
        fuzzy_maps=fuzzy_maps
    )


def create_grouper_data():
    df = pl.DataFrame(
        {
            "from": ["A", "B", "C", "E", "F", "G", "I", "I", 'AA'],
            "to": ["B", "C", "D", "F", "G", "J", "K", "J", 'Z']
        }
    )
    return df.select(graph_solver(pl.col("from"), pl.col("to")).alias('group')).lazy()


def test_external_package():
    df = create_grouper_data()
    load = models.PolarsScript(operation=base64.encodebytes(df.serialize()), operation_type='store')
    v = client.post('/submit_query', data=load.json())
    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    lf_test = base64.decodebytes(status.results.encode())
    result_df = pl.LazyFrame.deserialize(BytesIO(lf_test)).collect()
    assert result_df.equals(df.collect()), f'Expected:\n{df.collect()}\n\nResult:\n{result_df}'


def test_add_fuzzy_join():
    load = create_fuzzy_data()
    v = client.post('/add_fuzzy_join', data=load.json())
    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    lf_test = base64.decodebytes(status.results.encode())
    pl.LazyFrame.deserialize(BytesIO(lf_test)).collect()


def test_sample():
    lf = pl.LazyFrame({'value': [i for i in range(1000)]})
    serialized_df = lf.serialize()
    polars_script = models.PolarsScriptSample(operation=base64.encodebytes(serialized_df),
                                              operation_type='store_sample', sample_size=10)
    v = client.post('/store_sample', data=polars_script.json())
    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    result_df = pl.read_ipc(status.file_ref)
    assert result_df.equals(lf.collect().limit(10)), f'Expected:\n{lf.collect()}\n\nResult:\n{result_df}'


def test_polars_transformation():
    df = (pl.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]).lazy()
          .select((pl.col('a') + pl.col('b')).alias('total'))
          )
    serialized_df = df.serialize()
    load = models.PolarsScript(operation=base64.encodebytes(serialized_df), operation_type='store')
    # import requests

    v = client.post('/submit_query', data=load.json())
    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    lf_test = base64.decodebytes(status.results.encode())
    result_df = pl.LazyFrame.deserialize(BytesIO(lf_test)).collect()
    assert result_df.equals(df.collect()), f'Expected:\n{df.collect()}\n\nResult:\n{result_df}'


def test_create_func():
    received_table = '{"id": null, "name": "cross-verified-database.csv", "path": "/Users/username/Downloads/cross-verified-database.csv", "directory": null, "analysis_file_available": false, "status": null, "file_type": "csv", "fields": [], "reference": "", "starting_from_line": 0, "delimiter": ",", "has_headers": true, "encoding": "ISO-8859-1", "parquet_ref": null, "row_delimiter": "", "quote_char": "", "infer_schema_length": 260000, "truncate_ragged_lines": false, "ignore_errors": false, "sheet_name": null, "start_row": 0, "start_column": 0, "end_row": 0, "end_column": 0, "type_inference": false}'
    file_type = 'csv'

    v = client.post(f'/create_table/{file_type}', data=received_table)

    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    lf_test = base64.decodebytes(status.results.encode())
    try:
        result_df = pl.LazyFrame.deserialize(BytesIO(lf_test))
    except:
        raise Exception(f'Error with deserializing the DataFrame')


def test_write_output_csv():
    data = {
        'operation': 'oWJJUqJndmVyc2lvbhggY2RzbKFtRGF0YUZyYW1lU2NhbqJiZGahZ2NvbHVtbnOBpGRuYW1lZG5h\nbWVoZGF0YXR5cGVmU3RyaW5nbGJpdF9zZXR0aW5ncwBmdmFsdWVzg2dlZHV3YXJkZmVkd2FyZGhj\nb3VydG5leWZzY2hlbWGhZmZpZWxkc6FkbmFtZWZTdHJpbmc=\n',
        'data_type': 'csv', 'path': '/Users/username/FlowfileDesigner/backend/tests/data/output_csv.csv',
        'write_mode': 'overwrite', 'sheet_name': 'Sheet1', 'delimiter': ','}
    v = client.post('/write_results/', json=data)
    polars_script_write = models.PolarsScriptWrite(**data)
    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    df = pl.read_csv(status.file_ref)
    assert df.count() == 3, f'Expected 3 records, got {df.count()}'


def test_store_airbyte_result():
    airbyte_settings = AirbyteSettings(**{'source_name': 'source-faker', 'stream': 'users', 'config_ref': None,
                                          'config': {'count': 1000, 'seed': -1, 'records_per_slice': 1000,
                                                     'always_updated': True, 'parallelism': 4}, 'fields': None,
                                          'enforce_full_refresh': True})
    v = client.post('/store_airbyte_result', json=airbyte_settings.dict())
    assert v.status_code == 200, v.text
    assert models.Status.parse_obj(v.json()), 'Error with parsing the response to Status'
    status: models.Status = models.Status.parse_obj(v.json())
    r = client.get(f'/status/{status.background_task_id}')
    status = models.Status.parse_obj(r.json())
    if status.error_message is not None:
        raise Exception(f'Error message: {status.error_message}')
    lf_test = base64.decodebytes(status.results.encode())
    result_df = pl.LazyFrame.deserialize(BytesIO(lf_test)).collect()

