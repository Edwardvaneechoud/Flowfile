"""Byte-identical SQLite DDL across the four dialect-portability edits."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = Path("flowfile_core/flowfile_core/alembic")


def test_sqlite_schema_unchanged(tmp_path):
    baseline = os.environ.get("CATALOG_BASELINE_REF", "HEAD")
    dumps = []
    for variant in ("before", "after"):
        scripts = tmp_path / variant
        shutil.copytree(ROOT / MIGRATIONS, scripts, ignore=shutil.ignore_patterns("__pycache__"))
        if variant == "before":
            for prefix in ("002_", "016_", "020_", "026_"):
                migration = next((scripts / "versions").glob(f"{prefix}*.py"))
                source = subprocess.check_output(
                    ["git", "show", f"{baseline}:{MIGRATIONS}/versions/{migration.name}"], cwd=ROOT
                )
                migration.write_bytes(source)
        db = tmp_path / f"{variant}.db"
        env = dict(
            os.environ,
            FLOWFILE_DATABASE_URL=f"sqlite:///{db}",
            FLOWFILE_SKIP_STARTUP_MIGRATION="1",
            FLOWFILE_SKIP_INIT_DB="1",
            FLOWFILE_MODE="docker",
            FLOWFILE_STORAGE_DIR=str(tmp_path / "storage"),
            FLOWFILE_USER_DATA_DIR=str(tmp_path / "user"),
            TEST_MODE="1",
            TESTING="True",
            JWT_SECRET_KEY="catalog-test-jwt-secret",
            FLOWFILE_INTERNAL_TOKEN="catalog-test-internal-token",
            SECURE_STORAGE_PATH=str(tmp_path / "secrets"),
        )
        subprocess.run(
            [sys.executable, str(Path(__file__).with_name("schema_probe.py")), str(scripts)],
            env=env,
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        dump = subprocess.check_output(["sqlite3", str(db), ".schema"])
        (tmp_path / f"{variant}.sql").write_bytes(dump)
        dumps.append(dump)
    assert dumps[0] == dumps[1], "SQLite .schema changed; inspect before.sql and after.sql in the test directory"
