"""The temp sweep and the kernels' shared folder, which lives in the temp folder: the folder itself is never
removed, stored artifacts and people's files in it are kept, and the rest goes once nothing in it was written for
a day. Real files in a temp folder the storage singleton is pointed at."""

import os
import time
from pathlib import Path

import pytest

from shared.storage_config import storage

DAYS = 24 * 3600


@pytest.fixture
def shared(tmp_path, monkeypatch) -> Path:
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.delenv("FLOWFILE_SHARED_DIR", raising=False)
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    storage._ensure_directories()
    return storage.shared_directory


def _write(path: Path, age_days: float = 0.0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    _age(path, age_days)
    return path


def _age(path: Path, days: float) -> None:
    stamp = time.time() - days * DAYS
    os.utime(path, (stamp, stamp))


def _age_tree(folder: Path, days: float) -> None:
    for path in [*folder.rglob("*"), folder]:
        _age(path, days)


def test_the_shared_folder_outlives_a_sweep_however_old_it_is(shared):
    inode = shared.stat().st_ino
    _age_tree(shared, 3)

    storage.cleanup_directories()

    assert shared.stat().st_ino == inode


def test_stored_artifacts_and_peoples_files_are_kept(shared):
    kept = [
        _write(storage.global_artifacts_directory / "7" / "model.pkl"),
        _write(shared / "artifacts" / "my-kernel" / "9001" / "model" / "data.artifact"),
        _write(shared / "user_files" / "reports" / "monthly.csv"),
    ]
    _age_tree(shared, 30)

    storage.cleanup_directories()

    assert all(path.exists() for path in kept)


def test_what_nothing_wrote_to_for_a_day_goes(shared):
    exchange = _write(shared / "9001" / "3" / "inputs" / "main_0.parquet")
    staged = _write(storage.artifact_staging_directory / "12_model.pkl")
    snapshot = _write(storage.shared_virtual_results_directory / "fvt-4-abc.arrow")
    _age_tree(shared, 2)

    storage.cleanup_directories()

    assert not (shared / "9001").exists()
    assert not exchange.exists() and not staged.exists() and not snapshot.exists()
    assert storage.artifact_staging_directory.is_dir()
    assert storage.shared_virtual_results_directory.is_dir()


def test_a_flows_folder_stays_while_one_file_in_it_is_fresh(shared):
    old = _write(shared / "9001" / "3" / "inputs" / "main_0.parquet")
    _age_tree(shared, 2)
    fresh = _write(shared / "9001" / "4" / "outputs" / "main.parquet")
    _age(shared / "9001", 2)

    storage.cleanup_directories()

    assert old.exists() and fresh.exists()


def test_the_rest_of_the_temp_folder_is_swept_as_before(shared):
    stale = _write(storage.temp_directory / "leftover.tmp", age_days=2)
    fresh = _write(storage.temp_directory / "in_use.tmp")

    storage.cleanup_directories()

    assert not stale.exists()
    assert fresh.exists()
