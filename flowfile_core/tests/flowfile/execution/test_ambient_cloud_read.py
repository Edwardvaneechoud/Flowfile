"""A cloud read built for a backend that declares ``ambient`` credential resolution carries no Python credential provider.

The backend is a stand-in that only answers ``capabilities()``; what is checked is the provider the read is built with
(``NATIVE_CREDENTIALS``, which ``get_scan_kwargs`` turns into ``credential_provider=None``), read from the backend's
cached handshake verdict when there is one and asked of the backend once otherwise.
"""

from types import SimpleNamespace

import polars as pl
import pytest

from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import NATIVE_CREDENTIALS, CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettings,
    CloudStorageReadSettingsInternal,
    FullCloudStorageConnection,
)
from shared.compute_protocol import local_capabilities

PLAIN = {"aws_region": "nbg1", "endpoint_url": "https://objects.test"}


def capabilities(resolution: str):
    return local_capabilities().model_copy(
        update={"credential_resolution": resolution, "read_schemes": frozenset({"s3"})}
    )


class Backend:
    """A backend that only declares how it resolves credentials; ``reachable=False`` makes it unreachable."""

    def __init__(self, backend_id: str, resolution: str, *, reachable: bool = True) -> None:
        self.id = backend_id
        self._capabilities = capabilities(resolution)
        self.reachable = reachable
        self.asked = 0

    def capabilities(self):
        self.asked += 1
        if not self.reachable:
            raise ComputeUnavailable("down", backend_id=self.id)
        return self._capabilities


@pytest.fixture
def chosen(monkeypatch) -> list:
    """The credential provider each parquet read is built with, in order."""
    chosen: list = []
    monkeypatch.setattr(
        FlowDataEngine,
        "_read_parquet_from_cloud",
        classmethod(lambda cls, path, options, provider, *a, **k: chosen.append(provider)),
    )
    return chosen


def settings() -> CloudStorageReadSettingsInternal:
    connection = FullCloudStorageConnection(
        storage_type="s3",
        auth_method="env_vars",
        connection_name="ambient",
        aws_region="nbg1",
        endpoint_url=PLAIN["endpoint_url"],
    )
    return CloudStorageReadSettingsInternal(
        read_settings=CloudStorageReadSettings(resource_path="s3://source/data.parquet", file_format="parquet"),
        connection=connection,
    )


def test_an_ambient_backend_gets_no_provider_and_any_other_keeps_the_default(chosen):
    FlowDataEngine.from_cloud_storage_obj(settings(), user_id=1, backend=Backend("worker:polars", "ambient"))
    FlowDataEngine.from_cloud_storage_obj(settings(), user_id=1, backend=None)
    FlowDataEngine.from_cloud_storage_obj(settings(), user_id=1, backend=Backend("worker:team", "master_key"))

    assert chosen == [NATIVE_CREDENTIALS, None, None]


def test_a_cached_verdict_answers_without_asking_the_backend(chosen, monkeypatch):
    backend = Backend("worker:polars", "ambient")
    monkeypatch.setattr(
        handshake,
        "last_verdict",
        lambda backend_id: SimpleNamespace(backend=capabilities("ambient")) if backend_id == backend.id else None,
    )

    FlowDataEngine.from_cloud_storage_obj(settings(), user_id=1, backend=backend)

    assert chosen == [NATIVE_CREDENTIALS]
    assert backend.asked == 0


def test_an_unreachable_backend_leaves_the_read_as_it_was(chosen):
    FlowDataEngine.from_cloud_storage_obj(
        settings(), user_id=1, backend=Backend("worker:down", "ambient", reachable=False)
    )

    assert chosen == [None]


def test_native_credentials_put_no_python_provider_in_the_plan():
    kwargs = CloudStorageReader.get_scan_kwargs(PLAIN, NATIVE_CREDENTIALS)

    assert kwargs == {"credential_provider": None, "storage_options": PLAIN}
    assert b"CredentialProvider" not in pl.scan_parquet("s3://source/data.parquet", **kwargs).serialize()
    assert b"CredentialProvider" in pl.scan_parquet("s3://source/data.parquet", storage_options=PLAIN).serialize()
