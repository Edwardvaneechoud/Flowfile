"""Pytest fixtures and helpers that the Iceberg REST tests of core, the frame and the worker share.

A test module imports the fixtures it uses by name (``from test_utils.iceberg_rest.pytest_fixtures import
clean_aws, principal, signed``). ``clean_aws`` is autouse, so importing it turns it on for that module only. Nothing
here starts a container: without the rig (``python -m test_utils.iceberg_rest start``) the fixtures skip. Core is
imported inside the helpers that need it, so the frame's and the worker's tests can import this module.
"""

import contextlib
import re
import uuid
from collections.abc import Iterator

import polars as pl
import pytest

from test_utils.iceberg_rest import fixtures


SECRET_SHAPES = {
    "ciphertext": re.compile(r"\$ffsec\$"),
    "storage key name": re.compile(r"s3\.(secret-access-key|session-token|access-key-id)"),
    "AWS-style access key": re.compile(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b"),
    "bearer header": re.compile(r"Bearer\s+[A-Za-z0-9._-]{20,}"),
    "client secret field": re.compile(r"client_secret"),
}


class SecretValues(list):
    """Secret values a test searches for; its ``repr`` holds none of them."""

    def __repr__(self) -> str:
        return f"<{len(self)} secret values>"


def secret_findings(text: str, values: list[str]) -> list[str]:
    """Names of the secret shapes and indexes of the secret values found in ``text``; never a value."""
    found = [name for name, shape in SECRET_SHAPES.items() if shape.search(text)]
    return found + [f"value #{index}" for index, value in enumerate(values) if value and value in text]


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    """No ambient AWS key or profile, and a closed endpoint for anything that falls back to the environment."""
    from test_utils.s3.aws_profiles import isolate_aws

    aws = tmp_path / "aws"
    aws.mkdir()
    isolate_aws(monkeypatch, aws)


@pytest.fixture(scope="module")
def principal():
    """A principal of the signed catalog (Apache Polaris) that may manage its content; dropped after the module."""
    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)
    made = fixtures.new_principal()
    yield made
    with contextlib.suppress(Exception):
        fixtures.drop_principal(made.name)


@pytest.fixture(scope="module")
def signed(principal):
    """A PyIceberg client on the signed catalog as ``principal``, for seeding and for checking what was written."""
    return fixtures.signed_catalog(principal)


def drop_namespace(catalog, name: str) -> None:
    """Drop ``name`` and its tables; Polaris refuses a purge unless configured to, so the tables are dropped."""
    for identifier in catalog.list_tables(name):
        with contextlib.suppress(Exception):
            catalog.drop_table(identifier)
    with contextlib.suppress(Exception):
        catalog.drop_namespace(name)


@contextlib.contextmanager
def namespace_on(catalog, prefix: str) -> Iterator[str]:
    """A namespace ``<prefix>_<hex>`` of the caller's own on ``catalog``, dropped with its tables afterwards."""
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(name)
    try:
        yield name
    finally:
        drop_namespace(catalog, name)


def orders_frame(rows: int, start: int = 0) -> pl.LazyFrame:
    """``rows`` deterministic rows ``id``, ``grp``, ``val`` and ``name``, ids from ``start``."""
    return (
        pl.LazyFrame()
        .select(pl.int_range(start, start + rows, dtype=pl.Int64).alias("id"))
        .with_columns(
            grp=pl.col("id") % 10, val=pl.col("id").cast(pl.Float64) * 0.5, name=pl.format("name-{}", pl.col("id"))
        )
    )


def seed_orders(catalog, identifier: str, rows: int = 1000, start: int = 0):
    """Create ``identifier`` with ``orders_frame``'s columns and append its rows from this process (PyIceberg)."""
    from shared.iceberg_utils import write_table

    frame = orders_frame(rows, start)
    table = catalog.create_table(
        identifier, schema=frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest()).schema
    )
    if rows:
        write_table(table, frame, "append")
    return table


def polaris_body(principal, name: str | None = None, **overrides) -> dict:
    """The body that adds the signed catalog to Flowfile's catalog with the principal's client id and secret."""
    return {
        "name": name or f"polaris_{uuid.uuid4().hex[:6]}",
        "uri": fixtures.SIGNED_CATALOG_URI,
        "warehouse": fixtures.SIGNED_WAREHOUSE,
        "sign_in": "client_credentials",
        "client_id": principal.client_id,
        "client_secret": principal.client_secret,
        "scope": fixtures.SIGNED_SCOPE,
        **overrides,
    }


def add_catalog(principal, owner_id: int = 1, **overrides) -> tuple[str, int]:
    """Add the signed catalog through the catalog service as ``owner_id``; ``(name, catalog id)``."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate

    body = ExternalCatalogCreate(**polaris_body(principal, **overrides))
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).create_external_catalog(body, owner_id)
        catalog_id = db.query(CatalogNamespace).filter_by(name=body.name, parent_id=None).one().id
    return body.name, catalog_id


def remove_catalog(name: str) -> None:
    """Remove the catalog kept elsewhere named ``name`` from Flowfile's catalog, when it is still there."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace

    with get_db_context() as db:
        row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
        if row is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)


def schema_id(catalog_id: int, schema: str) -> int:
    """The id of the schema row ``schema`` under the catalog kept elsewhere ``catalog_id``."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace

    with get_db_context() as db:
        return db.query(CatalogNamespace).filter_by(parent_id=catalog_id, name=schema).one().id


@pytest.fixture
def kernel_of():
    """Makes TestClients each signed as a running kernel of a user, with that kernel's own token; the rows go after."""
    from fastapi.testclient import TestClient

    from flowfile_core import main
    from flowfile_core.auth.jwt import create_kernel_token
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Kernel

    made: list[str] = []

    def _client(user_id: int) -> TestClient:
        kernel_id, token_id = f"ext-kernel-{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
        with get_db_context() as db:
            db.add(Kernel(id=kernel_id, name="Outside catalog", user_id=user_id, token_id=token_id))
            db.commit()
        made.append(kernel_id)
        client = TestClient(main.app)
        client.headers = {
            "X-Internal-Token": create_kernel_token(user_id, kernel_id, token_id),
            "X-Kernel-Id": kernel_id,
        }
        return client

    yield _client
    with get_db_context() as db:
        db.query(Kernel).filter(Kernel.id.in_(made)).delete(synchronize_session=False)
        db.commit()


@contextlib.contextmanager
def added_catalog(principal, schema: str | None = None, owner_id: int = 1, **overrides) -> Iterator[tuple]:
    """The signed catalog added by ``owner_id`` for the block: ``(name, catalog id, schema id or None)``."""
    name, catalog_id = add_catalog(principal, owner_id, **overrides)
    try:
        yield name, catalog_id, schema_id(catalog_id, schema) if schema is not None else None
    finally:
        remove_catalog(name)
