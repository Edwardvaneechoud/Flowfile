"""The compute protocol's version and what a compute reports about itself.

Core and the worker import this one definition. The worker answers ``GET /capabilities``
with the ``Capabilities`` that ``local_capabilities()`` builds for its process; core builds
the same for its own process, so the two can be compared field by field.
"""

import importlib.metadata
import importlib.util
import platform
import sys
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from shared._version import get_version

PROTOCOL_VERSION = 1
PROTOCOL_VERSION_HEADER = "X-Protocol-Version"

# Packages whose installed version a compute reports.
PLUGIN_PACKAGES = ("polars_grouper", "polars_simed", "polars_ds", "deltalake", "pl_fuzzy_frame_match")
# The subset that registers Polars expression plugins: a plan that uses one carries the
# plugin's library path as Polars resolved it, relative to the sending process's sys.prefix.
EXPRESSION_PLUGINS = ("polars_grouper", "polars_ds")


class Capabilities(BaseModel):
    """What one compute process can run, in the fields a handshake compares."""

    app_version: str
    polars_version: str
    python_version: str
    protocol_version: int
    platform: str
    plugins: dict[str, str]
    plugin_libs: dict[str, str]


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
