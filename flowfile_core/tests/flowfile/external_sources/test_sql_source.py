from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (SqlSource,
                                                                                   get_query_columns,
                                                                                   get_table_column_types,
                                                                                   get_polars_type)
import pytest
import polars as pl
from flowfile_core.schemas.input_schema import MinimalFieldInfo
from flowfile_core.flowfile.flowfile_table.flow_file_column.main import FlowfileColumn
from tests.utils import is_docker_available
from sqlalchemy import create_engine


@pytest.fixture
def expected_schema() -> list[FlowfileColumn]:
    minimal_schema = [MinimalFieldInfo(name='movie_id', data_type='Int64'),
                      MinimalFieldInfo(name='title', data_type='String'),
                      MinimalFieldInfo(name='cast', data_type='String'),
                      MinimalFieldInfo(name='crew', data_type='String')]
    return [FlowfileColumn.create_from_minimal_field_info(field) for field in minimal_schema]


@pytest.fixture
def engine():
    return create_engine("postgresql://testuser:testpass@localhost:5433/testdb")


@pytest.fixture
def sql_source():
    return SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                     table_name='credits')


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_sql_source_with_table_and_schema(expected_schema):
    sql_source = SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                           table_name='credits',
                           schema_name='public')
    assert sql_source.get_schema() == expected_schema, "Schema does not match expected schema"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_sql_source_with_table_and_no_schema(expected_schema):
    sql_source = SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                           table_name='public.credits')
    assert sql_source.get_schema() == expected_schema, "Schema does not match expected schema"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_sql_source_with_query(expected_schema):
    sql_source = SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
                           query="SELECT * FROM credits")
    expected_columns = ['movie_id', 'title', 'cast', 'crew']
    assert [s.column_name for s in sql_source.get_schema()] == expected_columns, "Schema does not match expected schema"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_query_columns(engine):
    expected_columns = ['movie_id', 'title', 'cast', 'crew']

    result_columns = get_query_columns(engine, query_text="SELECT * FROM credits")
    assert result_columns == expected_columns, "Query columns do not match expected columns"

@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_table_column_types(engine):
    from sqlalchemy.sql.sqltypes import INTEGER, TEXT, Text
    from sqlalchemy.dialects.postgresql import JSONB
    expected = [
        ('movie_id', INTEGER()),
        ('title', TEXT()),
        ('cast', JSONB(astext_type=Text())),
        ('crew', JSONB(astext_type=Text()))  # Assuming the fourth column is named 'crew'
    ]
    result = get_table_column_types(engine, table_name='credits')
    assert str(result) == str(expected), "Table column types do not match expected types"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_pl_df(sql_source):
    """Test that get_pl_df returns a polars DataFrame and caches the result."""
    df = sql_source.get_pl_df()
    assert isinstance(df, pl.DataFrame), "get_pl_df should return a polars DataFrame"

    # Test caching
    cached_df = sql_source.read_result
    assert cached_df is not None, "The result should be cached in read_result"
    assert cached_df is df, "Cached DataFrame should be the same object as returned DataFrame"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_df(sql_source, monkeypatch):
    """Test that get_df converts polars DataFrame to pandas DataFrame."""
    # Mock the get_pl_df method
    mock_pl_df = pl.DataFrame({'col1': [1, 2, 3]})
    monkeypatch.setattr(sql_source, 'get_pl_df', lambda: mock_pl_df)

    # Call get_df
    result = sql_source.get_df()

    # Check it's a pandas DataFrame
    import pandas as pd
    assert isinstance(result, pd.DataFrame), "get_df should return a pandas DataFrame"
    assert len(result) == 3, "DataFrame should have the correct number of rows"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_data_getter(sql_source, monkeypatch):
    """Test that data_getter yields dictionaries from the DataFrame."""
    # Mock the get_pl_df method
    mock_pl_df = pl.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    monkeypatch.setattr(sql_source, 'get_pl_df', lambda: mock_pl_df)

    # Get results from data_getter
    results = list(sql_source.data_getter())

    # Check the results
    assert len(results) == 2, "Should have 2 rows of data"
    assert results[0] == {'col1': 1, 'col2': 'a'}, "First row should match expected values"
    assert results[1] == {'col1': 2, 'col2': 'b'}, "Second row should match expected values"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_sample_with_table(monkeypatch):
    """Test get_sample with table mode."""
    # Create SqlSource with table mode
    sql_source = SqlSource(
        connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
        table_name='credits'
    )

    # Mock pl.read_database_uri
    expected_data = [{'col1': 1, 'col2': 'a'}, {'col1': 2, 'col2': 'b'}]
    mock_df = pl.DataFrame(expected_data)
    monkeypatch.setattr(pl, 'read_database_uri', lambda query, conn: mock_df)

    # Get samples
    samples = list(sql_source.get_sample(n=10))

    # Check results
    assert len(samples) == 2, "Should return all rows in the sample DataFrame"
    assert samples == expected_data, "Sample data should match expected data"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_sample_with_query(monkeypatch):
    """Test get_sample with query mode."""
    # Create SqlSource with query mode
    sql_source = SqlSource(
        connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
        query="SELECT * FROM credits"
    )

    mock_df = pl.DataFrame({'col1': [1, 2, 3, 4, 5], 'col2': ['a', 'b', 'c', 'd', 'e']})
    monkeypatch.setattr(sql_source, 'get_pl_df', lambda: mock_df)

    # Get samples with n=3
    samples = list(sql_source.get_sample(n=3))

    # Check results
    assert len(samples) == 3, "Should return n rows in the sample"
    assert samples[0] == {'col1': 1, 'col2': 'a'}, "First sample should match expected data"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_initial_data(sql_source):
    """Test that get_initial_data returns an empty list."""
    result = sql_source.get_initial_data()
    assert result == [], "get_initial_data should return an empty list"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_iter(sql_source, monkeypatch):
    """Test that get_iter yields data from data_getter."""
    # Mock data_getter
    expected_data = [{'col1': 1}, {'col1': 2}]
    monkeypatch.setattr(sql_source, 'data_getter', lambda: (item for item in expected_data))

    # Get results from get_iter
    results = list(sql_source.get_iter())

    # Check results
    assert results == expected_data, "get_iter should yield data from data_getter"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_invalid_initialization():
    """Test that SqlSource raises exceptions for invalid initialization parameters."""
    # Neither query nor table_name
    with pytest.raises(ValueError) as exc_info:
        SqlSource(connection_string="postgresql://testuser:testpass@localhost:5433/testdb")
    assert "Either table_name or query must be provided" in str(exc_info.value)

    # Both query and table_name
    with pytest.raises(ValueError) as exc_info:
        SqlSource(
            connection_string="postgresql://testuser:testpass@localhost:5433/testdb",
            query="SELECT * FROM credits",
            table_name="credits"
        )
    assert "Only one of table_name or query can be provided" in str(exc_info.value)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_parse_table_name():
    """Test the _parse_table_name static method."""
    # Table with schema
    schema, table = SqlSource._parse_table_name("public.credits")
    assert schema == "public", "Schema should be 'public'"
    assert table == "credits", "Table should be 'credits'"

    # Table without schema
    schema, table = SqlSource._parse_table_name("credits")
    assert schema is None, "Schema should be None"
    assert table == "credits", "Table should be 'credits'"

    # Multi-part schema
    schema, table = SqlSource._parse_table_name("analytics.public.credits")
    assert schema == "analytics.public", "Schema should be 'analytics.public'"
    assert table == "credits", "Table should be 'credits'"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_parse_schema(sql_source, expected_schema, monkeypatch):
    """Test that parse_schema calls get_flow_file_columns."""
    monkeypatch.setattr(sql_source, 'get_flow_file_columns', lambda: expected_schema)
    result = sql_source.parse_schema()
    assert result == expected_schema, "parse_schema should return result from get_flow_file_columns"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_get_polars_type():
    """Test the get_polars_type function converts SQLAlchemy types to polars types."""
    from sqlalchemy.sql.sqltypes import INTEGER, FLOAT, VARCHAR, BOOLEAN, DATE, DATETIME
    import polars as pl

    # Test INTEGER type
    int_type = get_polars_type(INTEGER())
    assert isinstance(int_type(), pl.Int64), "INTEGER should convert to polars Int64"

    # Test FLOAT type
    float_type = get_polars_type(FLOAT())
    assert isinstance(float_type(), pl.Float64), "FLOAT should convert to polars Float64"

    # Test VARCHAR type
    string_type = get_polars_type(VARCHAR())
    assert isinstance(string_type(), pl.String), "VARCHAR should convert to polars String"

    # Test BOOLEAN type
    bool_type = get_polars_type(BOOLEAN())
    assert isinstance(bool_type(), pl.Boolean), "BOOLEAN should convert to polars Boolean"

    # Test DATE type
    date_type = get_polars_type(DATE())
    assert isinstance(date_type(), pl.Date), "DATE should convert to polars Date"

    # Test DATETIME type
    datetime_type = get_polars_type(DATETIME())
    assert isinstance(datetime_type(), pl.Datetime), "DATETIME should convert to polars Datetime"

    # Test unknown type (should default to String)
    from sqlalchemy.sql.sqltypes import Enum
    unknown_type = get_polars_type(Enum("A", "B"))
    assert isinstance(unknown_type(), pl.String), "Unknown type should convert to polars String"