"""The compute protocol's version and what a compute reports about itself.

Core and the worker import this one definition. ``local_capabilities()`` builds the
``Capabilities`` of the running process; the worker answers ``GET /capabilities`` with it and
the operations and schemes it declares, and core builds the same for its own process, so the
two can be compared field by field. The wire models (``SubmitHints``, ``SubmitResult``,
``ResultRef``) are what the two exchange about one result. A result is a table or a blob, a file of bytes such as a
fitted model that is never read as rows; ``put_blob`` and ``get_blob`` copy a blob's bytes to and from a ``file://`` URI
or a presigned ``https://`` URL, in whichever process holds it. ``file_uri`` writes a local path's ``file:`` URI and
``file_uri_path`` reads the path back, in one form on every platform.
"""

import importlib.metadata
import importlib.util
import platform
import shutil
import sys
from pathlib import Path
from typing import BinaryIO, Literal
from urllib.parse import urlsplit

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

# The object-storage schemes every Flowfile process reads and writes; ``file`` and ``https`` are the declarer's to add.
CLOUD_SCHEMES = frozenset({"s3", "az", "gs"})


class Capabilities(BaseModel):
    """What one compute process can run.

    A handshake compares the four versions, ``platform``, ``plugins`` and ``plugin_libs``. The other four fields
    are what a compute declares and are never compared: ``operations``, the values of the ``OperationKind``s it
    runs; ``read_schemes`` and ``write_schemes``, the URI schemes it reads and writes; and
    ``credential_resolution``, how it resolves a connection a plan names.
    """

    app_version: str
    polars_version: str
    python_version: str
    protocol_version: int
    platform: str
    plugins: dict[str, str]
    plugin_libs: dict[str, str]
    operations: frozenset[str] = frozenset()
    read_schemes: frozenset[str] = frozenset()
    write_schemes: frozenset[str] = frozenset()
    credential_resolution: Literal["master_key", "vended", "ambient"]


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
    """The capabilities of the running process.

    A Flowfile process, core or worker, resolves a connection with the master key it holds. ``operations`` and
    the schemes are left empty, for the process that declares them to fill.
    """
    return Capabilities(
        app_version=get_version(),
        polars_version=pl.__version__,
        python_version=platform.python_version(),
        protocol_version=PROTOCOL_VERSION,
        platform=f"{sys.platform}-{platform.machine()}",
        plugins=plugin_versions(),
        plugin_libs=plugin_libs(),
        credential_resolution="master_key",
    )


# A serialised plan or an Arrow stream is not UTF-8, so bytes travel as base64 in JSON.
_BASE64_BYTES = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")


class WireColumn(BaseModel):
    """One column of a result: the ``name`` and ``data_type`` of ``FileColumn``, the dtype spelled ``str(dtype)``."""

    name: str
    data_type: str


class WorkerLocation(BaseModel):
    """Where a worker keeps a result: the IPC file it wrote, the serialised plan that computes it on read, or a blob.

    A blob's ``path`` is the file of its bytes, which are not rows. A ``ResultRef``'s ``location`` is this model's JSON,
    which core carries without reading it.
    """

    model_config = _BASE64_BYTES

    kind: Literal["file", "plan", "blob"]
    path: str | None = None
    plan: bytes | None = None


class ResultRef(BaseModel):
    """A result as the worker names it, without its rows.

    ``columns`` is the result's schema in order and ``location`` the JSON of a ``WorkerLocation``. ``kind`` is
    ``"table"`` unless the result is a blob, which has no columns and no row count; an answer without it is a table.
    There is no backend id: only core knows what it calls the worker, so core's backend adds its own.
    """

    model_config = _BASE64_BYTES

    result_id: str
    columns: list[WireColumn]
    row_count: int | None
    state: Literal["materialised", "deferred"]
    location: bytes
    kind: Literal["table", "blob"] = "table"


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


_BLOB_TIMEOUT = 600
"""Seconds a blob's ``PUT`` or ``GET`` waits to connect and between bytes, what a kernel's presigned transfer gets."""


def file_uri(path: str | Path) -> str:
    """The ``file:`` URI of the absolute ``path`` as ``Path.as_uri()`` writes it (RFC 8089), read by ``file_uri_path``.

    The path is percent-encoded: ``/tmp/a b`` is ``file:///tmp/a%20b``, ``C:\\Users\\me`` is ``file:///C:/Users/me``
    and a UNC path ``\\\\server\\share\\x`` is ``file://server/share/x``.

    Raises:
        ValueError: ``path`` is relative.
    """
    return Path(path).as_uri()


def file_uri_path(uri: str) -> str:
    """The local path a ``file:`` URI names, percent-decoded as ``file_uri`` wrote it; another URI or a path as it is.

    A URI with a host names a UNC share, and the host is kept before the path as ``//<host>``.
    """
    parts = urlsplit(uri)
    if parts.scheme != "file":
        return uri
    from urllib.request import url2pathname

    return url2pathname(f"//{parts.netloc}{parts.path}" if parts.netloc else parts.path)


def put_blob(path: Path, uri: str) -> None:
    """Copy the file at ``path`` to ``uri``: a copy to a ``file://`` URI, a ``PUT`` to a presigned ``https://`` URL.

    The ``PUT`` streams the open file with its length. A ``file://`` target's folder is the caller's and is not made.

    Raises:
        ValueError: ``uri`` is of another scheme; nothing was read.
        OSError: The copy failed, or the ``PUT`` was answered with no 2xx or did not complete; the message names the
            URL without its query, which holds a presigned URL's signature.
    """
    if _blob_scheme(uri) == "file":
        shutil.copyfile(path, file_uri_path(uri))
        return
    with open(path, "rb") as body:
        _transfer("PUT", uri, data=body)


def get_blob(uri: str, path: Path) -> None:
    """Copy what is at ``uri`` to ``path``: a copy from a ``file://`` URI, a ``GET`` of a presigned ``https://`` URL.

    The ``GET`` is written to ``path`` as it arrives. ``path``'s folder is made, and a fetch that fails removes what
    it wrote.

    Raises:
        ValueError: ``uri`` is of another scheme; nothing was written.
        OSError: The copy failed, or the ``GET`` was answered with no 2xx or did not complete; the message names the
            URL without its query, which holds a presigned URL's signature.
    """
    scheme = _blob_scheme(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "wb") as out:
            if scheme == "file":
                with open(file_uri_path(uri), "rb") as source:
                    shutil.copyfileobj(source, out)
            else:
                _transfer("GET", uri, out=out)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _blob_scheme(uri: str) -> str:
    """The scheme of a blob's ``uri``: ``file`` or ``https``, else ``ValueError`` naming the scheme and no more."""
    scheme, separator, _ = uri.partition("://")
    if not separator or scheme not in ("file", "https"):
        named = f"{scheme}://" if separator else "a path without a scheme"
        raise ValueError(f"A blob is copied to and from file:// and https:// only, not {named}")
    return scheme


def _transfer(method: str, uri: str, *, data: BinaryIO | None = None, out: BinaryIO | None = None) -> None:
    """Send ``data`` to ``uri`` (``PUT``) or write what it answers to ``out`` (``GET``).

    No message names the URL's query: a presigned URL's signature is there, and ``requests``' own messages repeat
    the whole URL, so they are not passed on.
    """
    import requests

    parts = urlsplit(uri)
    where = f"{parts.scheme}://{parts.netloc.rpartition('@')[2]}{parts.path}"
    try:
        with requests.request(method, uri, data=data, stream=out is not None, timeout=_BLOB_TIMEOUT) as response:
            if not response.ok:
                raise OSError(f"{method} {where} answered {response.status_code} {response.reason}")
            if out is not None:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    out.write(chunk)
    except requests.RequestException as e:
        raise OSError(f"{method} {where} failed: {type(e).__name__}") from None
