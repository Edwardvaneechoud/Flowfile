"""Override parent conftest fixtures that require the worker to be running."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def flowfile_worker():
    """Skip worker requirement for API schema/client tests."""
    yield
