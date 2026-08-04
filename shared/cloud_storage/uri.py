"""Path arithmetic for object-storage URIs.

``pathlib.Path`` and ``os.path`` collapse the double slash in ``scheme://``
(``Path("s3://b/k")`` becomes ``s3:/b/k``), so object-storage locations need
their own join/parent/split helpers. This module is the single place that knows
the scheme list and how a URI decomposes.

A URI is modelled as ``<scheme>://<container>/<key>`` where *container* is an S3
bucket, an ADLS container, or a GCS bucket. ``<scheme>://`` on its own is the
container-listing root.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

CLOUD_URI_SCHEMES: Final[tuple[str, ...]] = (
    "s3://",
    "s3a://",
    "az://",
    "abfs://",
    "abfss://",
    "adl://",
    "gs://",
    "gcs://",
)

# The scheme each storage type reads and writes by default. ADLS also accepts
# abfs(s)://; delta-rs needs abfss://, which normalize_delta_path handles at read time.
_CANONICAL_SCHEMES: Final[dict[str, str]] = {"s3": "s3://", "adls": "az://", "gcs": "gs://"}


@dataclass(frozen=True)
class ParsedUri:
    """A cloud URI split into its scheme, container, and key."""

    scheme: str
    container: str
    key: str

    @property
    def is_root(self) -> bool:
        """True when the URI addresses the container list rather than a container."""
        return not self.container


def is_cloud_uri(value: str) -> bool:
    """Return True when *value* is an object-storage URI rather than a local path."""
    return value.startswith(CLOUD_URI_SCHEMES)


def canonical_scheme(storage_type: str) -> str:
    """Return the URI scheme Flowfile emits for a storage type (``s3`` -> ``s3://``)."""
    try:
        return _CANONICAL_SCHEMES[storage_type]
    except KeyError:
        raise ValueError(f"Unsupported storage type: {storage_type}") from None


def scheme_of(uri: str) -> str:
    """Return the ``scheme://`` prefix of *uri*, or "" when it is not a cloud URI."""
    for scheme in CLOUD_URI_SCHEMES:
        if uri.startswith(scheme):
            return scheme
    return ""


def parse_uri(uri: str) -> ParsedUri:
    """Split a cloud URI into scheme, container, and key.

    Trailing slashes are dropped from the key so ``s3://b/k/`` and ``s3://b/k``
    parse identically.
    """
    scheme = scheme_of(uri)
    if not scheme:
        raise ValueError(f"Not a cloud storage URI: {uri}")
    remainder = uri[len(scheme) :].strip("/")
    if not remainder:
        return ParsedUri(scheme=scheme, container="", key="")
    container, _, key = remainder.partition("/")
    return ParsedUri(scheme=scheme, container=container, key=key)


def build_uri(scheme: str, container: str = "", key: str = "") -> str:
    """Rebuild a URI from its parts. Inverse of :func:`parse_uri`."""
    if not container:
        return scheme
    key = key.strip("/")
    return f"{scheme}{container}/{key}" if key else f"{scheme}{container}"


def uri_is_root(uri: str) -> bool:
    """True when *uri* addresses the container list (``s3://``)."""
    return parse_uri(uri).is_root


def uri_parent(uri: str) -> str:
    """Return the parent location of *uri*.

    Walks key segments, then the container, and finally stops at the
    container-listing root — never falling back to a local-looking path.
    """
    parsed = parse_uri(uri)
    if not parsed.container:
        return parsed.scheme
    if not parsed.key:
        return parsed.scheme
    parent_key = parsed.key.rsplit("/", 1)[0] if "/" in parsed.key else ""
    return build_uri(parsed.scheme, parsed.container, parent_key)


def uri_join(base: str, child: str) -> str:
    """Append a single path segment to *base*, keeping the ``scheme://`` intact."""
    parsed = parse_uri(base)
    child = child.strip("/")
    if not child:
        return build_uri(parsed.scheme, parsed.container, parsed.key)
    if not parsed.container:
        return build_uri(parsed.scheme, child)
    key = f"{parsed.key}/{child}" if parsed.key else child
    return build_uri(parsed.scheme, parsed.container, key)


def uri_basename(uri: str) -> str:
    """Return the display name of the object, prefix, or container *uri* points at."""
    parsed = parse_uri(uri)
    if parsed.key:
        return parsed.key.rsplit("/", 1)[-1]
    return parsed.container or parsed.scheme
