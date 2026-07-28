"""Pins the dialect vocabulary contract between core's models and shared.db_dialects."""

import pytest
from pydantic import ValidationError

from flowfile_core.schemas.input_schema import DatabaseConnection
from shared.db_dialects import KNOWN_DIALECT_NAMES


@pytest.mark.parametrize("name", KNOWN_DIALECT_NAMES)
def test_database_connection_accepts_every_registered_dialect(name):
    assert DatabaseConnection(database_type=name).database_type == name


def test_database_connection_rejects_unknown_dialect():
    with pytest.raises(ValidationError, match="Unsupported database type"):
        DatabaseConnection(database_type="not-a-database")


def test_database_connection_default_is_postgresql():
    assert DatabaseConnection().database_type == "postgresql"


def test_database_connection_accepts_mixed_case_and_preserves_it():
    """Vocabulary matching is case-insensitive (the registry lowercases lookups);
    the raw spelling is preserved on the model."""
    assert DatabaseConnection(database_type="PostgreSQL").database_type == "PostgreSQL"
    assert DatabaseConnection(database_type="SQLITE").database_type == "SQLITE"


class TestSqlSourceDialectInference:
    """SqlSource resolves its dialect from the explicit database_type, else the URI scheme."""

    def test_infers_from_uri_scheme(self):
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource

        source = SqlSource(connection_string="sqlite:///some.db", table_name="t")
        assert source.dialect.name == "sqlite"
        assert source.database_type == "sqlite"

    def test_strips_sqlalchemy_driver_suffix(self):
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource

        source = SqlSource(connection_string="mysql+pymysql://u@h/d", table_name="t")
        assert source.dialect.name == "mysql"

    def test_explicit_database_type_wins_over_scheme(self):
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource

        source = SqlSource(connection_string="sqlite:///some.db", table_name="t", database_type="postgresql")
        assert source.dialect.name == "postgresql"

    def test_unknown_scheme_falls_back_to_generic_limit_behavior(self):
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource

        source = SqlSource(connection_string="redshift://u@h/d", table_name="t")
        assert source.get_sample_query() == "SELECT * FROM t LIMIT 1"
