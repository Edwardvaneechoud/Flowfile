"""Tests for connection_manager module."""

import pytest

from flowfile_core.flowfile.connection_manager.models import Connection
from flowfile_core.flowfile.connection_manager._connection_manager import ConnectionManager


class TestConnection:
    """Test Connection dataclass."""

    def test_create_connection(self):
        conn = Connection(group="db", name="postgres-main", config_setting={"host": "localhost"})
        assert conn.group == "db"
        assert conn.name == "postgres-main"
        assert conn.config_setting == {"host": "localhost"}
        assert conn.type is None

    def test_create_connection_with_type(self):
        conn = Connection(group="api", name="rest-api", config_setting={}, type="http")
        assert conn.type == "http"


class TestConnectionManager:
    """Test ConnectionManager class."""

    @pytest.fixture
    def manager(self):
        cm = ConnectionManager()
        cm.connections = {}
        return cm

    @pytest.fixture
    def sample_connection(self):
        return Connection(group="db", name="postgres-main", config_setting={"host": "localhost"})

    def test_add_connection_new_group(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        assert "db" in manager.connections
        assert "postgres-main" in manager.connections["db"]

    def test_add_connection_existing_group(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        conn2 = Connection(group="db", name="mysql", config_setting={"host": "mysql-host"})
        manager.add_connection("db", "mysql", conn2)
        assert len(manager.connections["db"]) == 2

    def test_add_connection_duplicate_raises(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        with pytest.raises(Exception, match="already exists"):
            manager.add_connection("db", "postgres-main", sample_connection)

    def test_get_connection(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        result = manager.get_connection("db", "postgres-main")
        assert result == sample_connection

    def test_get_connection_not_found_raises(self, manager):
        with pytest.raises(Exception, match="does not exist"):
            manager.get_connection("db", "nonexistent")

    def test_check_if_connection_exists_true(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        assert manager.check_if_connection_exists("db", "postgres-main") is True

    def test_check_if_connection_exists_false(self, manager):
        assert manager.check_if_connection_exists("db", "postgres-main") is False

    def test_check_if_connection_exists_wrong_group(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        assert manager.check_if_connection_exists("api", "postgres-main") is False

    def test_raise_if_connection_exists(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        with pytest.raises(Exception, match="already exists"):
            manager.raise_if_connection_exists("db", "postgres-main")

    def test_raise_if_connection_exists_no_error(self, manager):
        # Should not raise
        manager.raise_if_connection_exists("db", "nonexistent")

    def test_raise_if_connection_does_not_exist(self, manager):
        with pytest.raises(Exception, match="does not exist"):
            manager.raise_if_connection_does_not_exist("db", "nonexistent")

    def test_raise_if_connection_does_not_exist_no_error(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        # Should not raise
        manager.raise_if_connection_does_not_exist("db", "postgres-main")

    def test_update_connection(self, manager, sample_connection):
        manager.add_connection("db", "postgres-main", sample_connection)
        new_conn = Connection(group="db", name="postgres-main", config_setting={"host": "new-host"})
        manager.update_connection("db", "postgres-main", new_conn)
        result = manager.get_connection("db", "postgres-main")
        assert result.config_setting == {"host": "new-host"}

    def test_update_connection_not_found_raises(self, manager, sample_connection):
        with pytest.raises(Exception, match="does not exist"):
            manager.update_connection("db", "nonexistent", sample_connection)

    def test_insert_settings_raw(self, manager):
        settings = {"host": "localhost", "port": 5432}
        manager.insert_settings_raw("db", "pg", settings)
        result = manager.get_connection("db", "pg")
        assert result.group == "db"
        assert result.name == "pg"
        assert result.config_setting == settings

    def test_connection_groups(self, manager, sample_connection):
        manager.add_connection("db", "pg", sample_connection)
        conn2 = Connection(group="api", name="rest", config_setting={})
        manager.add_connection("api", "rest", conn2)
        groups = manager.connection_groups()
        assert set(groups) == {"db", "api"}

    def test_connection_groups_empty(self, manager):
        assert manager.connection_groups() == []

    def test_get_available_connections_in_group(self, manager, sample_connection):
        manager.add_connection("db", "pg", sample_connection)
        conn2 = Connection(group="db", name="mysql", config_setting={})
        manager.add_connection("db", "mysql", conn2)
        connections = manager.get_available_connections_in_group("db")
        assert set(connections) == {"pg", "mysql"}

    def test_get_available_connections_in_group_empty(self, manager):
        result = manager.get_available_connections_in_group("nonexistent")
        assert result == []
