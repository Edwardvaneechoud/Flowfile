"""Project-test fixtures.

Other modules (tests/flowfile/conftest.py, test_catalog*.py, test_code_generator.py)
wipe all CatalogNamespace rows to start clean. The seeded public 'General' catalog and
its system schemas are created once per session by init_db() and never restored, so when
those modules run first the seed is gone. The project tests depend on it (auto_register_flow,
namespace projection), so re-seed it (idempotent get-or-create) before every project test.
"""
import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import (
    create_default_catalog_namespace,
    create_default_local_user,
)


@pytest.fixture(autouse=True)
def _seed_default_catalog_namespaces():
    with get_db_context() as db:
        create_default_local_user(db)        # ensures local_user (id 1) exists; no-op if present
        create_default_catalog_namespace(db)  # re-creates General + default/Unnamed Flows/Local Flows
    yield
