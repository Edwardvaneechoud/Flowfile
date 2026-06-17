"""Tests for the database schema/table browsing functionality."""

import pytest
from sqlalchemy import create_engine, inspect


class TestDbBrowserSQLite:
    """Test schema and table listing using SQLite (no Docker required)."""

    def test_list_schemas(self, sqlite_db):
        """SQLite should return at least the 'main' schema."""
        engine = create_engine(f"sqlite:///{sqlite_db}")
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        assert "main" in schemas
        engine.dispose()

    def test_list_tables(self, sqlite_db):
        """SQLite should list the tables created by the fixture."""
        engine = create_engine(f"sqlite:///{sqlite_db}")
        inspector = inspect(engine)
        tables = sorted(inspector.get_table_names())
        assert "movies" in tables
        assert "actors" in tables
        engine.dispose()

    def test_list_tables_with_schema(self, sqlite_db):
        """Listing tables with explicit 'main' schema should work."""
        engine = create_engine(f"sqlite:///{sqlite_db}")
        inspector = inspect(engine)
        tables = sorted(inspector.get_table_names(schema="main"))
        assert "movies" in tables
        assert "actors" in tables
        engine.dispose()

    def test_list_tables_empty_db(self, tmp_path):
        """An empty database should return no tables."""
        db_path = str(tmp_path / "empty.db")
        engine = create_engine(f"sqlite:///{db_path}")
        # Create the database file by connecting
        with engine.connect():
            pass
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert tables == []
        engine.dispose()


class TestResolveConnectionString:
    """Test the _resolve_connection_string helper."""

    def test_sqlite_inline(self, sqlite_db):
        """SQLite inline connection should resolve without password lookup."""
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            _resolve_connection_string,
        )
        from flowfile_core.schemas.input_schema import DatabaseConnection, DatabaseSettings

        db_conn = DatabaseConnection(
            database_type="sqlite",
            database=f"sqlite:///{sqlite_db}",
        )
        settings = DatabaseSettings(
            connection_mode="inline",
            database_connection=db_conn,
            query_mode="table",
        )
        conn_str = _resolve_connection_string(settings, user_id=1)
        assert conn_str.startswith("sqlite:///")
        assert sqlite_db in conn_str


class TestCreateEngineFromDbSettings:
    """Test the create_engine_from_db_settings helper."""

    def test_sqlite_engine(self, sqlite_db):
        """Should create a working engine for SQLite."""
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
            create_engine_from_db_settings,
        )
        from flowfile_core.schemas.input_schema import DatabaseConnection, DatabaseSettings

        db_conn = DatabaseConnection(
            database_type="sqlite",
            database=f"sqlite:///{sqlite_db}",
        )
        settings = DatabaseSettings(
            connection_mode="inline",
            database_connection=db_conn,
            query_mode="table",
        )
        engine = create_engine_from_db_settings(settings, user_id=1)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "movies" in tables
        assert "actors" in tables
        engine.dispose()
