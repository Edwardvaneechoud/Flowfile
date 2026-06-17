"""Tests for flowfile/database_connection_manager/models module."""

from flowfile_core.flowfile.database_connection_manager.models import DatabaseConnectionOutput


class TestDatabaseConnectionOutput:
    """Test DatabaseConnectionOutput model."""

    def test_create_model(self):
        conn = DatabaseConnectionOutput(
            id=1,
            name="test_db",
            type="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            username="user",
            password="pass",
            ssl_mode="require",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        assert conn.id == 1
        assert conn.name == "test_db"
        assert conn.type == "postgresql"
        assert conn.host == "localhost"
        assert conn.port == 5432
        assert conn.database == "testdb"
        assert conn.username == "user"
        assert conn.password == "pass"
        assert conn.ssl_mode == "require"

    def test_optional_password(self):
        conn = DatabaseConnectionOutput(
            id=1,
            name="test_db",
            type="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            username="user",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        assert conn.password is None

    def test_optional_ssl_mode(self):
        conn = DatabaseConnectionOutput(
            id=1,
            name="test_db",
            type="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            username="user",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        assert conn.ssl_mode is None

    def test_model_serialization(self):
        conn = DatabaseConnectionOutput(
            id=1,
            name="test_db",
            type="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            username="user",
            password="secret",
            ssl_mode="require",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        data = conn.model_dump()
        assert data["id"] == 1
        assert data["name"] == "test_db"
        assert data["password"] == "secret"
