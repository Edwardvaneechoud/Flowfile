"""Fixtures for the Iceberg REST catalog tests: the catalog, a namespace, seeded tables and clean child processes.

Nothing here starts a container. Without a catalog at ``test_utils.iceberg_rest.fixtures.CATALOG_URI`` every test
skips, so the folder collects and skips on machines and CI runners that have none.
"""

import contextlib
import uuid

import pytest

from test_utils.iceberg_rest import fixtures
from test_utils.iceberg_rest.pytest_fixtures import clean_aws  # noqa: F401  (autouse for this folder)

SKIP_REASON = fixtures.SKIP_REASON


@pytest.fixture(scope="session")
def rest_catalog():
    if not fixtures.catalog_ready():
        pytest.skip(SKIP_REASON)
    from pyiceberg.catalog import load_catalog

    return load_catalog("proof", **fixtures.catalog_properties())


@pytest.fixture(scope="session")
def namespace(rest_catalog):
    name = f"proof_{uuid.uuid4().hex[:8]}"
    rest_catalog.create_namespace(name)
    yield name
    for identifier in rest_catalog.list_tables(name):
        with contextlib.suppress(Exception):
            rest_catalog.purge_table(identifier)
    with contextlib.suppress(Exception):
        rest_catalog.drop_namespace(name)


@pytest.fixture
def seed_table(rest_catalog, namespace):
    """Create ``<namespace>.<name>`` and append ``rows`` deterministic rows in ``files`` appends from this process."""
    from shared.tests.iceberg.table_child import rows_frame

    def seed(name: str, rows: int, files: int = 1, properties: dict | None = None):
        import polars as pl

        arrow = rows_frame(rows).collect().to_arrow(compat_level=pl.CompatLevel.oldest())
        table = rest_catalog.create_table(f"{namespace}.{name}", schema=arrow.schema, properties=properties or {})
        step = -(-rows // files)
        for offset in range(0, rows, step):
            table.append(arrow.slice(offset, step))
        return rest_catalog.load_table(f"{namespace}.{name}")

    return seed


@pytest.fixture
def child_env(tmp_path) -> dict[str, str]:
    """The whole environment of a child process: no AWS key, no profile, an empty home and a closed AWS endpoint."""
    return fixtures.clean_env(tmp_path / "child_home")


@pytest.fixture
def run_child():
    """Run a module in a fresh Python with exactly the given environment; returns its last JSON line."""
    return fixtures.run_child


@pytest.fixture(scope="session")
def duck(rest_catalog, tmp_path_factory):
    """A DuckDB connection with the catalog attached as ``lake``: an engine other than Polars and PyIceberg."""
    import duckdb

    connection = duckdb.connect()
    try:
        fixtures.attach_lake(connection, tmp_path_factory.mktemp("duckdb_extensions"))
    except duckdb.Error as exc:
        pytest.skip(f"DuckDB's iceberg extension could not be installed or attached: {fixtures.mask(str(exc))[:300]}")
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def signed_ready():
    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)


@pytest.fixture(scope="session")
def principal(signed_ready):
    """A principal of the signed catalog that may manage its content; removed after the session."""
    made = fixtures.new_principal()
    yield made
    with contextlib.suppress(Exception):
        fixtures.drop_principal(made.name)


@pytest.fixture(scope="session")
def signed(principal):
    """A PyIceberg client on the signed catalog as the principal, for seeding and for checking what was written."""
    return fixtures.signed_catalog(principal)


@pytest.fixture
def signed_namespace(signed):
    name = f"signed_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    yield name
    for child in signed.list_namespaces(name):
        with contextlib.suppress(Exception):
            signed.drop_namespace(child)
    for identifier in signed.list_tables(name):
        with contextlib.suppress(Exception):
            signed.drop_table(identifier)  # Polaris refuses a purge unless configured to; the stack is throwaway
    with contextlib.suppress(Exception):
        signed.drop_namespace(name)


@pytest.fixture
def seed_signed(signed, signed_namespace):
    """Create ``<signed_namespace>.<name>`` on the signed catalog with ``rows`` deterministic rows."""
    from shared.iceberg_utils import write_table
    from shared.tests.iceberg.table_child import rows_frame

    def seed(name: str, rows: int):
        import polars as pl

        frame = rows_frame(rows)
        arrow = frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest())
        table = signed.create_table(f"{signed_namespace}.{name}", schema=arrow.schema)
        if rows:
            write_table(table, frame, "append")
        return signed.load_table(f"{signed_namespace}.{name}")

    return seed


@pytest.fixture
def sealing(monkeypatch):
    """Seals a sign-in as stand-in ciphertext this process decrypts, as core and the worker decrypt a stored one."""
    from shared import cloud_credential_provider

    monkeypatch.setattr(cloud_credential_provider, "_decrypt", cloud_credential_provider._decrypt)
    cloud_credential_provider.register_secret_decryptor(fixtures.open_sealed)
    return fixtures.seal
