import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    ExternalDatabaseFetcher,
    ExternalDatabaseWriter,
    trigger_database_read_collector,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
    ExtDatabaseConnection,
)
from flowfile_core.secret_manager.secret_manager import encrypt_secret

try:
    from tests.flowfile_core_test_utils import ensure_password_is_available, is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import is_docker_available


@pytest.fixture
def pw():
    return encrypt_secret('testpass', user_id=1)


def test_trigger_database_read_collector(pw):
    database_connection = ExtDatabaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password=pw,
                                                host='localhost',
                                                port=5433,
                                                database='testdb',
                                                )
    database_external_read_settings = DatabaseExternalReadSettings(connection=database_connection, node_id=1,
                                                                   flow_id=1, query="SELECT * FROM public.movies")
    result_status = trigger_database_read_collector(database_external_read_settings)
    assert result_status.status == "Starting", "Failed to trigger database read collector"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_external_database_fetcher_wait_on_completion(pw):
    database_connection = ExtDatabaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password=pw,
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
    assert len(df) > 0, "DataFrame should not be empty"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_external_database_fetcher_not_wait_on_completion(pw):
    database_connection = ExtDatabaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password=pw,
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


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_external_database_writer(pw):
    lf = pl.LazyFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    # Use raw bytes - the DatabaseExternalWriteSettings model handles serialization
    settings_data = {'connection': {'username': 'testuser', 'password': pw, 'host': 'localhost', 'port': 5433,
                                    'database': 'testdb', 'database_type': 'postgresql', 'url': None},
                     'table_name': 'public.test_output', 'if_exists': 'replace', 'flowfile_flow_id': 1,
                     'flowfile_node_id': -1,
                     'operation': lf.serialize()}
    database_external_read_settings = DatabaseExternalWriteSettings(**settings_data)
    external_database_fetcher = ExternalDatabaseWriter(database_external_read_settings, wait_on_completion=False)
    assert external_database_fetcher.result is None, "Do not expect a result yet"
    external_database_fetcher.get_result()
    assert not external_database_fetcher.running, "Fetcher should not be running"
    assert external_database_fetcher.error_code == 0, "Error code should be 0"
    assert external_database_fetcher.error_description is None, "Error description should be None"

    database_connection = ExtDatabaseConnection(database_type='postgresql',
                                                username='testuser',
                                                password=pw,
                                                host='localhost',
                                                port=5433,
                                                database='testdb',
                                                )
    database_external_read_settings = DatabaseExternalReadSettings(connection=database_connection, node_id=1,
                                                                   flow_id=1, query="SELECT * FROM public.test_output")
    external_database_fetcher = ExternalDatabaseFetcher(database_external_read_settings,
                                                        wait_on_completion=False)
    assert external_database_fetcher.result is None, "Do not expect a result yet"
    assert isinstance(external_database_fetcher.get_result(), pl.LazyFrame), "Result should be a Polars LazyFrame"
    df: pl.DataFrame = external_database_fetcher.get_result().collect()
    df.equals(lf.collect())
