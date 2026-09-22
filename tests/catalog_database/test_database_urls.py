"""Catalog URL precedence and driver options, without importing core."""

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from shared import database as shared_database
from shared.database import SQLITE_POOL_ARGS, create_catalog_engine, get_catalog_engine, sqlite_database_path
from shared.storage_config import get_database_url, get_legacy_database_path, storage


@pytest.fixture(autouse=True)
def isolate(monkeypatch, tmp_path):
    monkeypatch.delenv("FLOWFILE_DATABASE_URL", raising=False)
    monkeypatch.delenv("FLOWFILE_DB_PATH", raising=False)
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.setattr(storage, "_base_dir", tmp_path)


def test_precedence(monkeypatch):
    assert get_database_url() == f"sqlite:///{storage.database_directory / 'flowfile_catalog.db'}"
    monkeypatch.setenv("TESTING", "True")
    assert get_database_url() == f"sqlite:///{storage.temp_directory / 'test_flowfile_catalog.db'}"
    monkeypatch.setenv("FLOWFILE_DB_PATH", "/tmp/explicit.db")
    assert get_database_url() == "sqlite:////tmp/explicit.db"
    url = "postgresql+psycopg2://sqlite_user:p%25ss@localhost/catalog"
    monkeypatch.setenv("FLOWFILE_DATABASE_URL", url)
    assert get_database_url() == url
    engine = create_catalog_engine()
    assert engine.url == make_url(url)
    assert engine.dialect.name == "postgresql"
    assert sqlite_database_path() is None
    engine.dispose()


@pytest.mark.parametrize("variable", ["FLOWFILE_DATABASE_URL", "FLOWFILE_DB_PATH"])
@pytest.mark.parametrize("url", ["postgresql://user:pass@localhost/catalog", "sqlite+pysqlite:///:memory:"])
def test_full_urls_and_no_legacy_import(monkeypatch, variable, url):
    storage.database_directory.mkdir()
    (storage.database_directory / "flowfile.db").touch()
    assert get_legacy_database_path() is not None
    monkeypatch.setenv(variable, url)
    assert get_database_url() == url
    assert get_legacy_database_path() is None


@pytest.mark.parametrize("url", ["sqlite://", "sqlite:///:memory:", "sqlite:///file:memdb?mode=memory&uri=true"])
def test_memory_database_has_no_backup_path(url):
    assert sqlite_database_path(url) is None


def test_sqlite_driver_and_query_options(tmp_path):
    path = tmp_path / "catalog.db"
    url = f"sqlite+pysqlite:///{path}?timeout=5"
    assert sqlite_database_path(url) == path
    engine = create_catalog_engine(url)
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT 1")) == 1
    engine.dispose()


def test_file_sqlite_gets_tuned_pool_and_wal(tmp_path):
    engine = create_catalog_engine(f"sqlite:///{tmp_path / 'catalog.db'}")
    assert engine.pool.size() == SQLITE_POOL_ARGS["pool_size"]
    assert engine.pool._max_overflow == SQLITE_POOL_ARGS["max_overflow"]
    assert engine.pool._timeout == SQLITE_POOL_ARGS["pool_timeout"]
    with engine.connect() as conn:
        assert conn.scalar(text("PRAGMA journal_mode")) == "wal"
    engine.dispose()


def test_memory_sqlite_keeps_default_pool():
    engine = create_catalog_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT 1")) == 1
    engine.dispose()


def test_get_catalog_engine_is_shared_per_url(tmp_path, monkeypatch):
    monkeypatch.setattr(shared_database, "_engines", {})
    url_a = f"sqlite:///{tmp_path / 'a.db'}"
    url_b = f"sqlite:///{tmp_path / 'b.db'}"
    engine = get_catalog_engine(url_a)
    assert get_catalog_engine(url_a) is engine
    assert get_catalog_engine(url_b) is not engine
    monkeypatch.setenv("FLOWFILE_DATABASE_URL", url_a)
    assert get_catalog_engine() is engine
    for e in {engine, get_catalog_engine(url_b)}:
        e.dispose()
