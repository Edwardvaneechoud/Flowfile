import polars as pl
import pytest
from sqlalchemy import Boolean, Column, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    SqlSource,
    get_polars_type,
    get_query_columns,
    get_table_column_types,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import (
    construct_sql_uri,
    get_sqlalchemy_uri,
)

Base = declarative_base()


class Movie(Base):
    __tablename__ = "movies"
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    rating = Column(Float)
    votes = Column(Integer)
    description = Column(Text)
    is_active = Column(Boolean, default=True)


class Actor(Base):
    __tablename__ = "actors"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    birth_year = Column(Integer)


@pytest.fixture
def sqlite_db(tmp_path):
    """Create a temporary SQLite database with sample data."""
    db_path = str(tmp_path / "test.db")
    connection_string = f"sqlite:///{db_path}"
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine)
    session: Session = session_factory()

    movies = [
        Movie(id=1, title="The Matrix", rating=8.7, votes=1800000, description="A sci-fi classic", is_active=True),
        Movie(id=2, title="Inception", rating=8.8, votes=2200000, description="Mind-bending thriller", is_active=True),
        Movie(id=3, title="The Shawshank Redemption", rating=9.3, votes=2500000, description="Drama", is_active=True),
    ]
    actors = [
        Actor(id=1, name="Keanu Reeves", birth_year=1964),
        Actor(id=2, name="Leonardo DiCaprio", birth_year=1974),
        Actor(id=3, name="Morgan Freeman", birth_year=1937),
    ]

    session.add_all(movies + actors)
    session.commit()
    session.close()
    engine.dispose()

    yield db_path


# ============================================================================
# URI Construction Tests
# ============================================================================


class TestSQLiteURIConstruction:
    """Tests for SQLite URI construction."""

    def test_construct_sqlite_uri_with_path(self):
        """Test that construct_sql_uri builds correct SQLite URIs."""
        uri = construct_sql_uri(database_type="sqlite", database="/tmp/test.db")
        assert uri == "sqlite:////tmp/test.db"

    def test_construct_sqlite_uri_default_path(self):
        """Test SQLite URI with default database path."""
        uri = construct_sql_uri(database_type="sqlite")
        assert uri == "sqlite:///./database.db"

    def test_construct_sqlite_uri_relative_path(self):
        """Test SQLite URI with relative path."""
        uri = construct_sql_uri(database_type="sqlite", database="./my_data.db")
        assert uri == "sqlite:///./my_data.db"

    def test_get_sqlalchemy_uri_sqlite_unchanged(self):
        """Test that SQLite URIs pass through get_sqlalchemy_uri unchanged."""
        sqlite_uri = "sqlite:///path/to/db"
        assert get_sqlalchemy_uri(sqlite_uri) == sqlite_uri

    def test_get_sqlalchemy_uri_sqlite_absolute_unchanged(self):
        """Test absolute SQLite paths are unchanged."""
        sqlite_uri = "sqlite:////absolute/path/to/db"
        assert get_sqlalchemy_uri(sqlite_uri) == sqlite_uri


# ============================================================================
# SQLite Integration Tests (no Docker required)
# ============================================================================


class TestSQLiteIntegration:
    """Integration tests using a temporary SQLite file."""

    def test_sql_source_with_table(self, sqlite_db):
        """Test SqlSource can read a SQLite table."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="movies")
        schema = sql_source.get_schema()
        column_names = [col.column_name for col in schema]
        assert "id" in column_names
        assert "title" in column_names
        assert "rating" in column_names

    def test_sql_source_with_query(self, sqlite_db):
        """Test SqlSource with a custom SELECT query against SQLite."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(
            connection_string=conn_str,
            query="SELECT id, title, rating FROM movies WHERE rating > 8.5",
        )
        columns = [s.column_name for s in sql_source.get_schema()]
        assert columns == ["id", "title", "rating"]

    def test_get_pl_df(self, sqlite_db):
        """Test reading SQLite data into a Polars DataFrame."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="movies")
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3
        assert "title" in df.columns

    def test_get_pl_df_caching(self, sqlite_db):
        """Test that DataFrame results are cached."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="movies")
        df1 = sql_source.get_pl_df()
        df2 = sql_source.get_pl_df()
        assert df1 is df2

    def test_get_query_columns(self, sqlite_db):
        """Test getting column names from a SQLite query."""
        conn_str = f"sqlite:///{sqlite_db}"
        engine = create_engine(conn_str)
        columns = get_query_columns(engine, "SELECT id, title FROM movies")
        assert columns == ["id", "title"]
        engine.dispose()

    def test_get_table_column_types(self, sqlite_db):
        """Test getting column types from a SQLite table."""
        conn_str = f"sqlite:///{sqlite_db}"
        engine = create_engine(conn_str)
        column_types = get_table_column_types(engine, "movies")
        column_names = [name for name, _ in column_types]
        assert "id" in column_names
        assert "title" in column_names
        assert "rating" in column_names
        engine.dispose()

    def test_get_sample(self, sqlite_db):
        """Test getting a sample of data from SQLite."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="movies")
        samples = list(sql_source.get_sample(n=2))
        assert len(samples) <= 2
        assert "title" in samples[0]

    def test_validate_succeeds(self, sqlite_db):
        """Test that validation succeeds for a valid SQLite table."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="movies")
        sql_source.validate()  # Should not raise

    def test_validate_fails_for_missing_table(self, sqlite_db):
        """Test that validation fails for a non-existent SQLite table."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="nonexistent_table")
        with pytest.raises((ValueError, Exception)):  # noqa: B017
            sql_source.validate()

    def test_read_actors_table(self, sqlite_db):
        """Test reading the actors table."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="actors")
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert "name" in df.columns
        assert "birth_year" in df.columns
        assert len(df) == 3

    def test_data_getter(self, sqlite_db):
        """Test that data_getter yields dictionaries."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(connection_string=conn_str, table_name="movies")
        results = list(sql_source.data_getter())
        assert len(results) == 3
        assert isinstance(results[0], dict)
        assert "title" in results[0]

    def test_query_with_join(self, sqlite_db):
        """Test a query joining two tables."""
        conn_str = f"sqlite:///{sqlite_db}"
        sql_source = SqlSource(
            connection_string=conn_str,
            query="SELECT m.title, a.name FROM movies m, actors a WHERE m.id = a.id",
        )
        df = sql_source.get_pl_df()
        assert isinstance(df, pl.DataFrame)
        assert "title" in df.columns
        assert "name" in df.columns
        assert len(df) == 3

    def test_nonexistent_db_file(self, tmp_path):
        """Test behavior with a non-existent SQLite file for read."""
        db_path = str(tmp_path / "does_not_exist.db")
        conn_str = f"sqlite:///{db_path}"
        sql_source = SqlSource(connection_string=conn_str, table_name="some_table")
        with pytest.raises((ValueError, Exception)):  # noqa: B017
            sql_source.validate()


# ============================================================================
# SQLite Type Mapping Tests
# ============================================================================


class TestSQLiteTypeMappings:
    """Tests for SQLite type mappings to Polars types."""

    def test_integer_type(self):
        assert get_polars_type("integer") == pl.Int64

    def test_text_type(self):
        assert get_polars_type("text") == pl.Utf8

    def test_real_type(self):
        assert get_polars_type("real") == pl.Float32

    def test_blob_type(self):
        assert get_polars_type("blob") == pl.Binary

    def test_boolean_type(self):
        assert get_polars_type("boolean") == pl.Boolean

    def test_varchar_type(self):
        assert get_polars_type("varchar") == pl.Utf8

    def test_float_type(self):
        assert get_polars_type("float") == pl.Float64

    def test_date_type(self):
        assert get_polars_type("date") == pl.Date

    def test_datetime_type(self):
        assert get_polars_type("datetime") == pl.Datetime
