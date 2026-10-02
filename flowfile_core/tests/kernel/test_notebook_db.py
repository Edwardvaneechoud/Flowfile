"""The notebook kernel's copy of the catalog database (no Docker)."""

import hashlib
import re
import sqlite3
import threading
from contextlib import closing

import pytest

from flowfile_core.kernel import notebook_db

COPY_NAME = re.compile(r"flowfile_catalog\.[0-9a-f]{32}\.db")


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
        assert copy.parent == notebook_db.copy_dir(str(shared), "k") and COPY_NAME.fullmatch(copy.name)
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
        newer = notebook_db.refresh(str(shared), "k")
        assert _digest(source) == before
        assert newer != copy and COPY_NAME.fullmatch(newer.name)
        assert _rows(newer) == [1, 2]
        assert sorted(p.name for p in newer.parent.iterdir()) == [newer.name]
    finally:
        writer.close()
    notebook_db.remove(str(shared), "k")
    assert not notebook_db.copy_dir(str(shared), "k").exists()


def test_a_copy_is_never_written_over_a_name_the_kernel_has_seen(tmp_path, monkeypatch):
    """Docker Desktop serves a replaced file's old entry for a moment: every copy is a new name, the fixed one
    (``FLOWFILE_DB_PATH``) is never written, and what an earlier copy left behind is swept."""
    source = tmp_path / "flowfile_catalog.db"
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(source))
    with closing(sqlite3.connect(source)) as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.commit()
    shared = str(tmp_path / "shared")
    directory = notebook_db.copy_dir(shared, "k")
    directory.mkdir(parents=True)
    leftovers = [notebook_db.copy_path(shared, "k"), directory / "flowfile_catalog.db-journal"]
    leftovers.append(directory / ".flowfile_catalog.0123.db.4567.tmp")
    for leftover in leftovers:
        leftover.write_bytes(b"stale")
    names = []
    for value in range(3):
        with closing(sqlite3.connect(source)) as conn:
            conn.execute("INSERT INTO t VALUES (?)", (value,))
            conn.commit()
        copy = notebook_db.refresh(shared, "k")
        names.append(copy.name)
        assert [p.name for p in directory.iterdir()] == [copy.name]
        assert _rows(copy) == list(range(value + 1))
        notebook_db.remove(shared, "k") if value == 1 else None
    assert len(set(names)) == 3 and all(COPY_NAME.fullmatch(name) for name in names)


def test_no_copy_without_a_sqlite_file(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_DB_PATH", "sqlite:///:memory:")
    assert notebook_db.refresh(str(tmp_path), "k") is None


@pytest.mark.parametrize("kernel_id", ["..", ".", "../artifacts", "{outside}"])
def test_a_kernel_id_that_leaves_the_copies_folder_is_refused(tmp_path, monkeypatch, kernel_id):
    source = tmp_path / "flowfile_catalog.db"
    monkeypatch.setenv("FLOWFILE_DB_PATH", str(source))
    with closing(sqlite3.connect(source)) as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.commit()
    shared, outside = tmp_path / "shared", tmp_path / "outside"
    kept = [shared / "artifacts" / "model.bin", outside / "data.csv"]
    for path in kept:
        path.parent.mkdir(parents=True)
        path.write_text("kept")
    copy = notebook_db.refresh(str(shared), "k")
    kernel_id = kernel_id.format(outside=outside)

    for call in (notebook_db.refresh, notebook_db.remove, notebook_db.copy_path):
        with pytest.raises(ValueError, match="Kernel id"):
            call(str(shared), kernel_id)
    assert copy.exists() and all(path.read_text() == "kept" for path in kept)
    assert sorted(p.name for p in outside.iterdir()) == ["data.csv"]


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
    assert [COPY_NAME.fullmatch(p.name) is not None for p in notebook_db.copy_dir(shared, "other").iterdir()] == [True]
