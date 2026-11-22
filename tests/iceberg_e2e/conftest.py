"""Real core and worker processes for the Iceberg end-to-end suite, against the local Iceberg REST catalog.

The stack is ``test_utils.iceberg_rest.flow_demo.running_stack``: desktop mode, free ports, a private database, storage
directory, secure store and HOME, and no AWS identity (empty credential files, no metadata service, a closed endpoint).
Nothing here imports flowfile_core. Every test skips unless the catalog runs: ``python -m test_utils.iceberg_rest
start``.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator

import pytest

from test_utils.iceberg_rest import fixtures, flow_demo
from test_utils.services import Services


@pytest.fixture(scope="session")
def namespace() -> Iterator[str]:
    if not fixtures.catalog_ready():
        pytest.skip(fixtures.SKIP_REASON)
    name = f"e2e_{uuid.uuid4().hex[:8]}"
    yield name
    lake = flow_demo.catalog()
    for identifier in lake.list_tables(name):
        with contextlib.suppress(Exception):
            lake.purge_table(identifier)
    with contextlib.suppress(Exception):
        lake.drop_namespace(name)


@pytest.fixture(scope="session")
def stack(namespace, tmp_path_factory) -> Iterator[Services]:
    with flow_demo.running_stack(tmp_path_factory.mktemp("iceberg_stack")) as running:
        yield running


@pytest.fixture(scope="session")
def duck(namespace, tmp_path_factory):
    try:
        connection = flow_demo.duck(tmp_path_factory.mktemp("duckdb_extensions"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"DuckDB's iceberg extension could not be installed or attached: {fixtures.mask(str(exc))[:300]}")
    yield connection
    connection.close()
