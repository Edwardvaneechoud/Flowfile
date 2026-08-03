"""One-level listing of object storage, for the storage browser UI.

``directory.py`` answers "what is the first file under this prefix" for schema
inference. This module answers "what is directly under this prefix" so a person
can click their way to a path. Providers are reached through the dependencies
already in the tree (boto3, azure-storage-blob, gcsfs) and every heavy import
stays function-local, matching ``directory.py``.

Two behaviours are worth knowing before consuming a :class:`BrowseResult`:

* Being refused permission to enumerate buckets/containers is **not** an error.
  It is reported as ``root_listing_denied`` so the caller can offer a
  "type a bucket name" affordance instead of a dead end — least-privilege keys
  routinely grant ``s3:ListBucket`` while withholding ``s3:ListAllMyBuckets``.
* Entries are sorted within the returned page only. S3 and ADLS hand back
  prefixes and objects as separate per-page arrays, so a later page can contain
  directories that sort before an earlier page's files.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from shared.cloud_storage.uri import (
    ParsedUri,
    build_uri,
    canonical_scheme,
    is_cloud_uri,
    parse_uri,
    uri_join,
)

DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 1000

_CONNECT_TIMEOUT_SECONDS = 5
_READ_TIMEOUT_SECONDS = 15
_GCS_MAX_ENTRIES = 10_000
_DELTA_LOG_DIR = "_delta_log"
_MAX_PATH_LENGTH = 2048

EntryKind = Literal["container", "prefix", "object"]


class BrowseError(Exception):
    """Base for every failure the browser can report. Carries its own HTTP mapping."""

    status_code = 502
    error_code = "PROVIDER_ERROR"


class BrowseInvalidPath(BrowseError):
    status_code = 400
    error_code = "INVALID_PATH"


class BrowseAccessDenied(BrowseError):
    status_code = 403
    error_code = "ACCESS_DENIED"


class BrowseNotFound(BrowseError):
    status_code = 404
    error_code = "PATH_NOT_FOUND"


class BrowseUnsupported(BrowseError):
    status_code = 409
    error_code = "BROWSE_UNSUPPORTED_AUTH"


class BrowseTimeout(BrowseError):
    status_code = 504
    error_code = "PROVIDER_TIMEOUT"


@dataclass(frozen=True)
class BrowseEntry:
    """One row in the browser: a bucket/container, a prefix, or an object."""

    name: str
    uri: str
    is_directory: bool
    entry_kind: EntryKind
    size: int | None = None
    last_modified: datetime | None = None

    @property
    def is_hidden(self) -> bool:
        """Object stores have no hidden flag; treat dot- and underscore-prefixed names as internal."""
        return self.name.startswith((".", "_"))


@dataclass(frozen=True)
class BrowseResult:
    """A single page of a listing, plus what the caller needs to render it honestly."""

    path: str
    entries: list[BrowseEntry] = field(default_factory=list)
    truncated: bool = False
    next_token: str | None = None
    root_listing_denied: bool = False
    current_is_delta_table: bool = False


_ADLS_UNSUPPORTED_AUTH = {
    "service_principal": (
        "Browsing Azure storage with service-principal auth is not supported. "
        "Use an account key or SAS token, or enter the path manually."
    ),
    "managed_identity": (
        "Browsing Azure storage with managed-identity auth is not supported. "
        "Use an account key or SAS token, or enter the path manually."
    ),
}


def browse_support(storage_type: str, auth_method: str) -> tuple[bool, str | None]:
    """Return ``(browsable, reason_when_not)`` without touching the network.

    Lets a caller grey out a connection before the user clicks Browse. The
    listing functions enforce the same rule, so this is a courtesy, not the gate.
    """
    if storage_type not in ("s3", "adls", "gcs"):
        return False, f"Browsing is not supported for {storage_type} storage."
    if storage_type == "adls" and auth_method in _ADLS_UNSUPPORTED_AUTH:
        return False, _ADLS_UNSUPPORTED_AUTH[auth_method]
    return True, None


def list_cloud_uri(
    storage_type: str,
    uri: str,
    storage_options: dict[str, Any] | None = None,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    page_token: str | None = None,
) -> BrowseResult:
    """List one level of *uri*. An empty *uri* means "the bucket/container list".

    Raises only :class:`BrowseError` subclasses — provider exceptions are
    translated so callers never need botocore/azure/gcsfs imports.
    """
    page_size = max(1, min(page_size, MAX_PAGE_SIZE))
    parsed = _normalize_browse_uri(uri, storage_type)

    if storage_type == "s3":
        return _list_s3(parsed, storage_options, page_size, page_token)
    if storage_type == "adls":
        return _list_adls(parsed, storage_options, page_size, page_token)
    if storage_type == "gcs":
        return _list_gcs(parsed, storage_options, page_size, page_token)
    raise BrowseUnsupported(f"Browsing is not supported for {storage_type} storage.")


def _normalize_browse_uri(uri: str, storage_type: str) -> ParsedUri:
    """Validate *uri* and re-root it onto the canonical scheme for *storage_type*.

    ``az://``, ``abfs://`` and ``abfss://`` all converge on ``az://`` so a path
    typed in any spelling browses the same place. Rejects traversal, wildcards,
    and control characters before any provider client is built.
    """
    scheme = canonical_scheme(storage_type)
    uri = (uri or "").strip()
    if not uri:
        return ParsedUri(scheme=scheme, container="", key="")
    if len(uri) > _MAX_PATH_LENGTH:
        raise BrowseInvalidPath("Path is too long.")
    # A SAS token rides in the query string; drop it before it can reach a log line.
    uri = uri.split("?", 1)[0]
    if not is_cloud_uri(uri):
        raise BrowseInvalidPath(f"Not a cloud storage URI: {uri}")
    if ".." in uri or "*" in uri or any(char < " " or char == "\x7f" for char in uri):
        raise BrowseInvalidPath("Path contains characters that are not allowed.")
    parsed = parse_uri(uri)
    return ParsedUri(scheme=scheme, container=parsed.container, key=parsed.key)


def _sort_entries(entries: list[BrowseEntry]) -> list[BrowseEntry]:
    return sorted(entries, key=lambda entry: (not entry.is_directory, entry.name.lower()))


def _encode_token(provider: str, raw: str) -> str:
    return base64.urlsafe_b64encode(f"{provider}:{raw}".encode()).decode()


def _decode_token(provider: str, token: str | None) -> str | None:
    """Decode a page token, refusing one minted for a different provider or page kind."""
    if not token:
        return None
    try:
        decoded = base64.urlsafe_b64decode(token.encode()).decode()
    except (binascii.Error, UnicodeDecodeError, ValueError):
        raise BrowseInvalidPath("Invalid page token.") from None
    prefix, separator, raw = decoded.partition(":")
    if not separator or prefix != provider:
        raise BrowseInvalidPath("Invalid page token.")
    return raw


def _slice_page(
    path: str,
    entries: list[BrowseEntry],
    page_size: int,
    page_token: str | None,
    provider: str,
    *,
    current_is_delta_table: bool = False,
) -> BrowseResult:
    """Offset-paginate a fully materialised listing (bucket lists, gcsfs)."""
    raw = _decode_token(provider, page_token)
    try:
        offset = int(raw) if raw is not None else 0
    except ValueError:
        raise BrowseInvalidPath("Invalid page token.") from None
    if offset < 0:
        raise BrowseInvalidPath("Invalid page token.")
    ordered = _sort_entries(entries)
    window = ordered[offset : offset + page_size]
    next_offset = offset + page_size
    truncated = next_offset < len(ordered)
    return BrowseResult(
        path=path,
        entries=window,
        truncated=truncated,
        next_token=_encode_token(provider, str(next_offset)) if truncated else None,
        current_is_delta_table=current_is_delta_table,
    )


# --- S3 ---------------------------------------------------------------------

# Object-store option keys that boto3 would reject; anything not listed is dropped.
_S3_CLIENT_KEYS = ("aws_access_key_id", "aws_secret_access_key", "aws_session_token", "endpoint_url")
_S3_DENIED_CODES = {
    "AccessDenied",
    "AllAccessDisabled",
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "ExpiredToken",
    "InvalidToken",
    "UnauthorizedOperation",
}
_S3_MISSING_CODES = {"NoSuchBucket", "NoSuchKey", "404", "NotFound"}


def _build_s3_client(storage_options: dict[str, Any] | None):
    """Build a boto3 S3 client from Polars-shaped storage options, by allow-list.

    Deliberately separate from ``directory.py::_create_s3_client``, which is on the
    read hot path: that one forwards unknown keys and treats the string ``"False"``
    that ``build_s3_storage_options`` emits for ``verify`` as truthy.
    """
    import boto3
    from botocore.config import Config

    options = storage_options or {}
    kwargs: dict[str, Any] = {key: options[key] for key in _S3_CLIENT_KEYS if options.get(key)}

    region = options.get("aws_region") or options.get("region_name")
    if region:
        kwargs["region_name"] = region

    verify = options.get("verify")
    if verify is not None:
        kwargs["verify"] = verify not in (False, "False", "false", "0", 0)

    config = Config(
        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        read_timeout=_READ_TIMEOUT_SECONDS,
        retries={"max_attempts": 2, "mode": "standard"},
    )
    if kwargs.get("endpoint_url"):
        # MinIO and other S3-compatible endpoints do not serve virtual-hosted-style URLs.
        config = config.merge(Config(s3={"addressing_style": "path"}))
    return boto3.client("s3", config=config, **kwargs)


def _s3_error_code(exc: Any) -> str:
    return str(exc.response.get("Error", {}).get("Code", "")) if getattr(exc, "response", None) else ""


def _translate_s3_error(exc: Exception, location: str) -> BrowseError:
    from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

    if isinstance(exc, NoCredentialsError | PartialCredentialsError):
        return BrowseAccessDenied("No usable credentials for this storage.")
    if isinstance(exc, ClientError):
        code = _s3_error_code(exc)
        if code in _S3_DENIED_CODES:
            return BrowseAccessDenied(f"Access denied listing {location}.")
        if code in _S3_MISSING_CODES:
            return BrowseNotFound(f"{location} does not exist.")
        return BrowseError(f"Storage provider error ({code or type(exc).__name__}) listing {location}.")
    return _translate_generic_error(exc, location)


def _translate_generic_error(exc: Exception, location: str) -> BrowseError:
    """Map transport-level failures without echoing provider text (it can carry endpoints/tokens)."""
    name = type(exc).__name__
    if isinstance(exc, TimeoutError) or "Timeout" in name:
        return BrowseTimeout(f"Timed out listing {location}.")
    if isinstance(exc, PermissionError):
        return BrowseAccessDenied(f"Access denied listing {location}.")
    if isinstance(exc, FileNotFoundError):
        return BrowseNotFound(f"{location} does not exist.")
    return BrowseError(f"Storage provider error ({name}) listing {location}.")


def _list_s3(
    parsed: ParsedUri, storage_options: dict[str, Any] | None, page_size: int, page_token: str | None
) -> BrowseResult:
    client = _build_s3_client(storage_options)
    if parsed.is_root:
        return _list_s3_buckets(client, parsed.scheme, page_size, page_token)

    base = build_uri(parsed.scheme, parsed.container, parsed.key)
    prefix = f"{parsed.key}/" if parsed.key else ""
    kwargs: dict[str, Any] = {
        "Bucket": parsed.container,
        "Prefix": prefix,
        "Delimiter": "/",
        "MaxKeys": page_size,
    }
    continuation = _decode_token("s3o", page_token)
    if continuation:
        kwargs["ContinuationToken"] = continuation

    try:
        response = client.list_objects_v2(**kwargs)
    except Exception as exc:
        raise _translate_s3_error(exc, base) from None

    entries: list[BrowseEntry] = []
    is_delta = False
    for common in response.get("CommonPrefixes", []):
        name = str(common.get("Prefix", "")).rstrip("/").rsplit("/", 1)[-1]
        if not name:
            continue
        if name == _DELTA_LOG_DIR:
            is_delta = True
        entries.append(BrowseEntry(name=name, uri=uri_join(base, name), is_directory=True, entry_kind="prefix"))

    for obj in response.get("Contents", []):
        key = str(obj.get("Key", ""))
        # Console-created folders leave a zero-byte marker object; it is not a file.
        if not key or key == prefix or key.endswith("/"):
            continue
        entries.append(
            BrowseEntry(
                name=key.rsplit("/", 1)[-1],
                uri=build_uri(parsed.scheme, parsed.container, key),
                is_directory=False,
                entry_kind="object",
                size=obj.get("Size"),
                last_modified=obj.get("LastModified"),
            )
        )

    next_raw = response.get("NextContinuationToken")
    return BrowseResult(
        path=base,
        entries=_sort_entries(entries),
        truncated=bool(response.get("IsTruncated")),
        next_token=_encode_token("s3o", next_raw) if next_raw else None,
        current_is_delta_table=is_delta,
    )


def _list_s3_buckets(client: Any, scheme: str, page_size: int, page_token: str | None) -> BrowseResult:
    try:
        response = client.list_buckets()
    except Exception as exc:
        translated = _translate_s3_error(exc, scheme)
        if isinstance(translated, BrowseAccessDenied):
            return BrowseResult(path=scheme, root_listing_denied=True)
        raise translated from None

    buckets = [
        BrowseEntry(
            name=str(bucket["Name"]),
            uri=build_uri(scheme, str(bucket["Name"])),
            is_directory=True,
            entry_kind="container",
            last_modified=bucket.get("CreationDate"),
        )
        for bucket in response.get("Buckets", [])
        if bucket.get("Name")
    ]
    return _slice_page(scheme, buckets, page_size, page_token, "s3b")


# --- ADLS -------------------------------------------------------------------


def _build_blob_service_client(storage_options: dict[str, Any] | None):
    from azure.core.credentials import AzureSasCredential
    from azure.storage.blob import BlobServiceClient

    options = storage_options or {}
    account_key = options.get("account_key")
    sas_token = options.get("sas_token")
    if account_key:
        credential: Any = account_key
    elif sas_token:
        # A bare string credential is interpreted as an account key unless it parses
        # as a SAS query string; be explicit so a malformed token fails clearly.
        credential = AzureSasCredential(sas_token)
    else:
        raise BrowseUnsupported(_ADLS_UNSUPPORTED_AUTH["service_principal"])

    account = options.get("account_name") or "devstoreaccount1"
    endpoint = options.get("azure_storage_endpoint") or f"https://{account}.blob.core.windows.net"
    return BlobServiceClient(
        account_url=endpoint,
        credential=credential,
        connection_timeout=_CONNECT_TIMEOUT_SECONDS,
        read_timeout=_READ_TIMEOUT_SECONDS,
    )


def _translate_azure_error(exc: Exception, location: str) -> BrowseError:
    from azure.core.exceptions import (
        ClientAuthenticationError,
        HttpResponseError,
        ResourceNotFoundError,
        ServiceRequestError,
    )

    if isinstance(exc, ResourceNotFoundError):
        return BrowseNotFound(f"{location} does not exist.")
    if isinstance(exc, ClientAuthenticationError):
        return BrowseAccessDenied(f"Access denied listing {location}.")
    if isinstance(exc, ServiceRequestError):
        return BrowseTimeout(f"Could not reach the storage account for {location}.")
    if isinstance(exc, HttpResponseError):
        status = getattr(exc, "status_code", None)
        if status == 403:
            return BrowseAccessDenied(f"Access denied listing {location}.")
        if status == 404:
            return BrowseNotFound(f"{location} does not exist.")
        return BrowseError(f"Storage provider error ({status}) listing {location}.")
    return _translate_generic_error(exc, location)


def _list_adls(
    parsed: ParsedUri, storage_options: dict[str, Any] | None, page_size: int, page_token: str | None
) -> BrowseResult:
    client = _build_blob_service_client(storage_options)
    if parsed.is_root:
        return _list_adls_containers(client, parsed.scheme, page_size, page_token)
    return _list_adls_blobs(client, parsed, page_size, page_token)


def _list_adls_containers(client: Any, scheme: str, page_size: int, page_token: str | None) -> BrowseResult:
    try:
        pages = client.list_containers(results_per_page=page_size).by_page(_decode_token("azc", page_token))
        page = next(pages, None)
        containers = list(page) if page is not None else []
        next_raw = pages.continuation_token
    except BrowseError:
        raise
    except Exception as exc:
        translated = _translate_azure_error(exc, scheme)
        if isinstance(translated, BrowseAccessDenied):
            return BrowseResult(path=scheme, root_listing_denied=True)
        raise translated from None

    entries = [
        BrowseEntry(
            name=container.name,
            uri=build_uri(scheme, container.name),
            is_directory=True,
            entry_kind="container",
            last_modified=getattr(container, "last_modified", None),
        )
        for container in containers
    ]
    return BrowseResult(
        path=scheme,
        entries=_sort_entries(entries),
        truncated=bool(next_raw),
        next_token=_encode_token("azc", next_raw) if next_raw else None,
    )


def _list_adls_blobs(client: Any, parsed: ParsedUri, page_size: int, page_token: str | None) -> BrowseResult:
    from azure.storage.blob import BlobPrefix

    base = build_uri(parsed.scheme, parsed.container, parsed.key)
    prefix = f"{parsed.key}/" if parsed.key else ""
    try:
        walker = client.get_container_client(parsed.container).walk_blobs(
            name_starts_with=prefix, delimiter="/", results_per_page=page_size
        )
        pages = walker.by_page(_decode_token("azb", page_token))
        page = next(pages, None)
        items = list(page) if page is not None else []
        next_raw = pages.continuation_token
    except BrowseError:
        raise
    except Exception as exc:
        raise _translate_azure_error(exc, base) from None

    entries: list[BrowseEntry] = []
    is_delta = False
    for item in items:
        if isinstance(item, BlobPrefix):
            name = str(item.name).rstrip("/").rsplit("/", 1)[-1]
            if not name:
                continue
            if name == _DELTA_LOG_DIR:
                is_delta = True
            entries.append(BrowseEntry(name=name, uri=uri_join(base, name), is_directory=True, entry_kind="prefix"))
            continue
        key = str(item.name)
        name = key.rsplit("/", 1)[-1]
        if not name:
            continue
        entries.append(
            BrowseEntry(
                name=name,
                uri=build_uri(parsed.scheme, parsed.container, key),
                is_directory=False,
                entry_kind="object",
                size=getattr(item, "size", None),
                last_modified=getattr(item, "last_modified", None),
            )
        )

    return BrowseResult(
        path=base,
        entries=_sort_entries(entries),
        truncated=bool(next_raw),
        next_token=_encode_token("azb", next_raw) if next_raw else None,
        current_is_delta_table=is_delta,
    )


# --- GCS --------------------------------------------------------------------


def _parse_gcs_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _normalize_gcs_options(storage_options: dict[str, Any] | None) -> dict[str, Any]:
    """Turn a service-account key into the dict gcsfs expects.

    ``build_gcs_storage_options`` puts the raw service-account JSON in ``token``.
    gcsfs only reads a string token as a file path or a raw OAuth2 access token, so
    handing it the JSON verbatim authenticates with the key text as a bearer token.
    """
    options = dict(storage_options or {})
    token = options.get("token")
    if isinstance(token, str) and token.lstrip().startswith("{"):
        try:
            options["token"] = json.loads(token)
        except ValueError:
            raise BrowseAccessDenied("The service account key for this connection is not valid JSON.") from None
    return options


def _list_gcs(
    parsed: ParsedUri, storage_options: dict[str, Any] | None, page_size: int, page_token: str | None
) -> BrowseResult:
    import gcsfs

    options = _normalize_gcs_options(storage_options)
    try:
        fs = gcsfs.GCSFileSystem(**options)
    except Exception as exc:
        raise _translate_generic_error(exc, parsed.scheme) from None

    if parsed.is_root:
        return _list_gcs_buckets(fs, parsed.scheme, page_size, page_token)

    base = build_uri(parsed.scheme, parsed.container, parsed.key)
    path = f"{parsed.container}/{parsed.key}" if parsed.key else parsed.container
    try:
        # gcsfs caches listings; without this a just-written object stays invisible.
        fs.invalidate_cache(path)
        items = fs.ls(path, detail=True)
    except Exception as exc:
        raise _translate_gcs_error(exc, base) from None

    entries: list[BrowseEntry] = []
    is_delta = False
    for item in items[:_GCS_MAX_ENTRIES]:
        raw_name = str(item.get("name", "")).rstrip("/")
        if not raw_name or raw_name == path:
            continue
        name = raw_name.rsplit("/", 1)[-1]
        if not name:
            continue
        key = raw_name.split("/", 1)[1] if "/" in raw_name else ""
        if item.get("type") == "directory":
            if name == _DELTA_LOG_DIR:
                is_delta = True
            entries.append(BrowseEntry(name=name, uri=uri_join(base, name), is_directory=True, entry_kind="prefix"))
            continue
        entries.append(
            BrowseEntry(
                name=name,
                uri=build_uri(parsed.scheme, parsed.container, key),
                is_directory=False,
                entry_kind="object",
                size=item.get("size"),
                last_modified=_parse_gcs_timestamp(item.get("updated")),
            )
        )

    return _slice_page(base, entries, page_size, page_token, "gso", current_is_delta_table=is_delta)


def _list_gcs_buckets(fs: Any, scheme: str, page_size: int, page_token: str | None) -> BrowseResult:
    try:
        items = fs.ls("", detail=True)
    except Exception as exc:
        translated = _translate_gcs_error(exc, scheme)
        if isinstance(translated, BrowseAccessDenied):
            return BrowseResult(path=scheme, root_listing_denied=True)
        raise translated from None

    entries = []
    for item in items:
        name = str(item.get("name", "")).strip("/")
        if not name:
            continue
        entries.append(BrowseEntry(name=name, uri=build_uri(scheme, name), is_directory=True, entry_kind="container"))
    return _slice_page(scheme, entries, page_size, page_token, "gsb")


def _translate_gcs_error(exc: Exception, location: str) -> BrowseError:
    status = getattr(exc, "code", None) or getattr(exc, "status", None)
    if status == 403:
        return BrowseAccessDenied(f"Access denied listing {location}.")
    if status == 404:
        return BrowseNotFound(f"{location} does not exist.")
    return _translate_generic_error(exc, location)
