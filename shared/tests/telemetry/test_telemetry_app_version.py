"""app_version resolution must survive the Docker image shape (no dist-info).

Both service Dockerfiles run ``poetry install --only=main --no-root`` with the
source on PYTHONPATH, so ``importlib.metadata`` has nothing to look up.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError

import pytest

from shared import _version, telemetry


@pytest.fixture
def no_dist_info(monkeypatch):
    """The Docker shape: the flowfile distribution is not installed."""
    import importlib.metadata

    def _missing(name: str, *args, **kwargs):
        raise PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", _missing)


def test_app_version_falls_back_to_the_canonical_version(no_dist_info):
    assert telemetry._app_version() == _version.__version__


def test_envelope_carries_the_real_version_without_dist_info(no_dist_info, enabled, posts, monkeypatch):
    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)
    telemetry.emit("app_started")
    telemetry.flush()

    assert posts.events[0]["app_version"] == _version.__version__


def test_collector_accepts_the_canonical_version():
    from tools.telemetry_collector.app import APP_VERSION_RE

    assert APP_VERSION_RE.fullmatch(_version.__version__)
