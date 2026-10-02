"""The notebook kernel's copy of the catalog database (no Docker)."""

import hashlib
import sqlite3
import threading
from contextlib import closing

from flowfile_core.kernel import notebook_db


def _digest(path):
    """The database file and its WAL; readers touch only the ``-shm`` index."""
    files = (path, path.with_name(path.name + "-wal"))
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.exists()}


def _rows(path) -> list[int]:
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        return [row[0] for row in conn.execute("SELECT v FROM t ORDER BY v")]


def test_the_copy_follows_the_source_and_leaves_it_untouched(tmp_path, monkeypatch):
    source = tmp_path / "db" / "flowfile_catalog.db"
    source.parent.mkdir()
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(source))
    writer = sqlite3.connect(source)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE t (v INTEGER)")
    writer.execute("INSERT INTO t VALUES (1)")
    writer.commit()
    shared = tmp_path / "shared"
    try:
        before = _digest(source)
        copy = notebook_db.refresh(str(shared), "k")
        assert _digest(source) == before
        assert copy == notebook_db.copy_path(str(shared), "k")
        assert _rows(copy) == [1]
        with closing(sqlite3.connect(copy)) as conn:
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert sorted(p.name for p in copy.parent.iterdir()) == [copy.name]

        unchanged = copy.stat().st_mtime_ns
        assert notebook_db.refresh(str(shared), "k") == copy
        assert copy.stat().st_mtime_ns == unchanged

        writer.execute("INSERT INTO t VALUES (2)")
        writer.commit()
        before = _digest(source)
        notebook_db.refresh(str(shared), "k")
        assert _digest(source) == before
        assert _rows(copy) == [1, 2]
    finally:
        writer.close()
    notebook_db.remove(str(shared), "k")
    assert not notebook_db.copy_dir(str(shared), "k").exists()


def test_no_copy_without_a_sqlite_file(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_DB_PATH", "sqlite:///:memory:")
    assert notebook_db.refresh(str(tmp_path), "k") is None


def test_one_kernels_copy_never_waits_on_anothers(tmp_path, monkeypatch):
    source = tmp_path / "flowfile_catalog.db"
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(source))
    with closing(sqlite3.connect(source)) as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.commit()
    shared = str(tmp_path / "shared")
    copied = threading.Event()
    with notebook_db._lock("busy"):
        other = threading.Thread(target=lambda: notebook_db.refresh(shared, "other") and copied.set(), daemon=True)
        other.start()
        assert copied.wait(10), "a copy for one kernel waited on another kernel's lock"
    other.join(10)
    assert notebook_db.copy_path(shared, "other").exists()
