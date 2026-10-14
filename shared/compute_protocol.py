"""The compute protocol's version and what a compute reports about itself.

Core and the worker import this one definition. The worker answers ``GET /capabilities``
with the ``Capabilities`` that ``local_capabilities()`` builds for its process; core builds
the same for its own process, so the two can be compared field by field. The wire models
(``SubmitHints``, ``SubmitResult``, ``ResultRef``) are what the two exchange about one result.
"""

import importlib.metadata
import importlib.util
import platform
import sys
from pathlib import Path
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

from shared._version import get_version

PROTOCOL_VERSION = 1
PROTOCOL_VERSION_HEADER = "X-Protocol-Version"

# Packages whose installed version a compute reports.
PLUGIN_PACKAGES = ("polars_grouper", "polars_simed", "polars_ds", "deltalake", "pl_fuzzy_frame_match")
# The subset that registers Polars expression plugins: a plan that uses one carries the
# plugin's library path as Polars resolved it, relative to the sending process's sys.prefix.
EXPRESSION_PLUGINS = ("polars_grouper", "polars_ds")


class Capabilities(BaseModel):
    """What one compute process can run.

    A handshake compares every field but ``operations``: the values of the ``OperationKind``s a backend's
    ``run_op`` runs, which core's backend fills in from its own table. A compute answers it empty.
    """

    app_version: str
    polars_version: str
    python_version: str
    protocol_version: int
    platform: str
    plugins: dict[str, str]
    plugin_libs: dict[str, str]
    operations: frozenset[str] = frozenset()


def plugin_versions() -> dict[str, str]:
    """Installed version per package in ``PLUGIN_PACKAGES``; a package that is not installed is absent."""
    versions: dict[str, str] = {}
    for name in PLUGIN_PACKAGES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return versions


def plugin_libs() -> dict[str, str]:
    """The dynamic library path per expression plugin, exactly as Polars serialises it into a plan.

    Each plugin passes its package directory to ``polars.plugins.register_plugin_function``,
    which finds the library inside it and keeps the path relative to ``sys.prefix``. Reusing
    Polars' own resolver means two processes that disagree here would disagree on the plan
    bytes. A plugin that is not installed is absent.
    """
    from polars.plugins import _resolve_plugin_path

    libs: dict[str, str] = {}
    for name in EXPRESSION_PLUGINS:
        spec = importlib.util.find_spec(name)
        if spec is None or spec.origin is None:
            continue
        try:
            libs[name] = str(_resolve_plugin_path(Path(spec.origin).parent))
        except FileNotFoundError:
            continue
    return libs


def local_capabilities() -> Capabilities:
    """The capabilities of the running process."""
    return Capabilities(
        app_version=get_version(),
        polars_version=pl.__version__,
        python_version=platform.python_version(),
        protocol_version=PROTOCOL_VERSION,
        platform=f"{sys.platform}-{platform.machine()}",
        plugins=plugin_versions(),
        plugin_libs=plugin_libs(),
    )


# A serialised plan or an Arrow stream is not UTF-8, so bytes travel as base64 in JSON.
_BASE64_BYTES = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")


class WireColumn(BaseModel):
    """One column of a result: the ``name`` and ``data_type`` of ``FileColumn``, the dtype spelled ``str(dtype)``."""

    name: str
    data_type: str


class WorkerLocation(BaseModel):
    """Where a worker keeps a result: the IPC file it wrote, or the serialised plan that computes it on read.

    A ``ResultRef``'s ``location`` is this model's JSON, which core carries without reading it.
    """

    model_config = _BASE64_BYTES

    kind: Literal["file", "plan"]
    path: str | None = None
    plan: bytes | None = None


class ResultRef(BaseModel):
    """A result as the worker names it, without its rows.

    ``columns`` is the result's schema in order and ``location`` the JSON of a ``WorkerLocation``.
    There is no backend id: only core knows what it calls the worker, so core's backend adds its own.
    """

    model_config = _BASE64_BYTES

    result_id: str
    columns: list[WireColumn]
    row_count: int | None
    state: Literal["materialised", "deferred"]
    location: bytes


class SubmitHints(BaseModel):
    """What core knows about a submitted plan, for the worker's materialisation policy."""

    must_materialise: bool
    wide: bool
    fanout: int
    preview_rows: int | None


class SubmitResult(BaseModel):
    """The worker's answer to a submit: the reference, the inline Arrow IPC sample when one was asked for, the job."""

    model_config = _BASE64_BYTES

    ref: ResultRef
    sample: bytes | None = None
    job_id: str


def schema_to_wire(schema: pl.Schema) -> list[WireColumn]:
    """The schema's columns in order, each dtype spelled ``str(dtype)``, which core reads back with its dtype parser."""
    return [WireColumn(name=name, data_type=str(dtype)) for name, dtype in schema.items()]
