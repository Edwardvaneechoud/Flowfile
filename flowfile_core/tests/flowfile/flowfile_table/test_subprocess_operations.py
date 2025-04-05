import polars as pl
from flowfile_core.flowfile.flowfile_table.subprocess_operations import (trigger_database_read_collector,
                                                                         DatabaseExternalReadSettings,
                                                                         ExternalDatabaseFetcher)
from flowfile_core.flowfile.sources.external_sources.sql_source.models import ExtDataBaseConnection


def test_trigger_database_read_collector():
    database_connection = ExtDataBaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password='testpass',
                                                host='localhost',
                                                port=5433,
                                                database='testdb',
                                                )
    database_external_read_settings = DatabaseExternalReadSettings(connection=database_connection, node_id=1,
                                                                   flow_id=1, query="SELECT * FROM public.movies")
    result_status = trigger_database_read_collector(database_external_read_settings)
    assert result_status.status == "Starting", "Failed to trigger database read collector"


def test_external_database_fetcher_wait_on_completion():
    database_connection = ExtDataBaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password='testpass',
                                                host='localhost',
                                                port=5433,
                                                database='testdb',
                                                )
    database_external_read_settings = DatabaseExternalReadSettings(connection=database_connection, node_id=1,
                                                                   flow_id=1, query="SELECT * FROM public.movies")
    external_database_fetcher = ExternalDatabaseFetcher(database_external_read_settings,
                                                        wait_on_completion=True)
    assert external_database_fetcher.started, "Status should be started"
    assert external_database_fetcher.error_description is None, "Error description should be None"
    assert not external_database_fetcher.running, "Fetcher should not be running"
    assert isinstance(external_database_fetcher.result, pl.LazyFrame), "Result should be a Polars LazyFrame"
    df = external_database_fetcher.result.collect()
    assert len(df)>0, "DataFrame should not be empty"


def test_external_database_fetcher_not_wait_on_completion():
    database_connection = ExtDataBaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password='testpass',
                                                host='localhost',
                                                port=5433,
                                                database='testdb',
                                                )
    database_external_read_settings = DatabaseExternalReadSettings(connection=database_connection, node_id=1,
                                                                   flow_id=1, query="SELECT * FROM public.movies")
    external_database_fetcher = ExternalDatabaseFetcher(database_external_read_settings,
                                                        wait_on_completion=False)
    assert external_database_fetcher.result is None, "Do not expect a result yet"
    assert isinstance(external_database_fetcher.get_result(), pl.LazyFrame), "Result should be a Polars LazyFrame"
    df = external_database_fetcher.get_result().collect()
    assert len(df) > 0, "DataFrame should not be empty"
