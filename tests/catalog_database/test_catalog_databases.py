"""Full catalog migration, seed, constraint and backup contracts on both databases."""

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[2]


def run_probe(tmp_path, url, scenario):
    env = dict(
        os.environ,
        FLOWFILE_DATABASE_URL=url,
        FLOWFILE_MODE="docker",
        FLOWFILE_STORAGE_DIR=str(tmp_path / "storage"),
        FLOWFILE_USER_DATA_DIR=str(tmp_path / "user"),
        FLOWFILE_ADMIN_USER="catalog_admin",
        FLOWFILE_ADMIN_PASSWORD="catalog-test-password",
        TESTING="True",
        TEST_MODE="1",
        FLOWFILE_MASTER_KEY="MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
        JWT_SECRET_KEY="catalog-test-jwt-secret",
        FLOWFILE_INTERNAL_TOKEN="catalog-test-internal-token",
        SECURE_STORAGE_PATH=str(tmp_path / "secrets"),
    )
    result = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("catalog_probe.py")), scenario],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("scenario", ["fresh", "populated"])
def test_sqlite_catalog(tmp_path, scenario):
    run_probe(tmp_path, f"sqlite:///{tmp_path / 'catalog.db'}", scenario)


@pytest.fixture(scope="module")
def postgres_server():
    url = os.environ.get("TEST_CATALOG_POSTGRES_URL")
    if url:
        yield url
    else:
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgres:16") as postgres:
            yield postgres.get_connection_url()


@pytest.mark.docker_integration
@pytest.mark.parametrize("scenario", ["fresh", "populated"])
def test_postgres_catalog(tmp_path, postgres_server, scenario):
    admin = create_engine(postgres_server, isolation_level="AUTOCOMMIT")
    name = "catalog_" + uuid.uuid4().hex
    try:
        with admin.connect() as conn:
            assert conn.scalar(text("SHOW server_version_num")).startswith("16")
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        url = make_url(postgres_server).set(database=name).render_as_string(hide_password=False)
        run_probe(tmp_path, url, scenario)
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()
