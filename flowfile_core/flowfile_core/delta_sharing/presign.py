"""The URL a recipient downloads one data file of a shared table from.

A table on local disk gets a signed URL on core's own protocol route (``file_tokens``). A table on S3 gets an S3
presigned URL signed with the access key held by the catalog's storage connection. A table on ADLS gets a read-only
blob SAS from the connection's account key, or the connection's own SAS token. A connection without keys of its
own (one that runs as this server's identity), and GCS, cannot sign here: :func:`signing_refusal` refuses the table
when it is added to a share, so no link is ever signed with the server's ambient credentials.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

from flowfile_core.catalog.storage_backend import CatalogStorageTarget
from flowfile_core.delta_sharing import file_tokens
from shared.cloud_storage.storage_options import build_s3_client
from shared.cloud_storage.uri import parse_uri, storage_type_for_uri

FILES_ROUTE = "files"

_KEYLESS = (
    "Its catalog's storage connection holds no access key of its own, so Flowfile cannot hand out "
    "download links for it."
)


def signing_refusal(target: CatalogStorageTarget, table_path: str) -> str | None:
    """Why file URLs cannot be signed for a table at *table_path*, or ``None`` when they can."""
    if not target.is_cloud:
        return None
    options = target.storage_options
    kind = storage_type_for_uri(table_path)
    if kind == "s3":
        return None if options.get("aws_access_key_id") and options.get("aws_secret_access_key") else _KEYLESS
    if kind == "adls":
        return None if options.get("account_key") or options.get("sas_token") else _KEYLESS
    if kind == "gcs":
        return "Tables on Google Cloud Storage cannot be shared yet."
    return "Its storage location cannot hand out download links."


def _join_key(prefix: str, rel_path: str) -> str:
    return f"{prefix.rstrip('/')}/{rel_path}" if prefix else rel_path


def _s3_url(target: CatalogStorageTarget, table_path: str, rel_path: str, ttl: int) -> str:
    parsed = parse_uri(table_path)
    client = build_s3_client(target.storage_options, path_style=True, signature_version="s3v4")
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": parsed.container, "Key": _join_key(parsed.key, rel_path)},
        ExpiresIn=ttl,
    )


def _adls_parts(table_path: str, options: dict[str, str]) -> tuple[str, str, str]:
    """``(account, container, blob prefix)`` of an ``az://``/``abfs(s)://`` table path."""
    parsed = parse_uri(table_path)
    container, _, host = parsed.container.partition("@")
    account = host.split(".", 1)[0] if host else options.get("account_name", "")
    if not account:
        raise ValueError("The table's storage connection names no Azure account.")
    return account, container, parsed.key


def _adls_url(target: CatalogStorageTarget, table_path: str, rel_path: str, expires: datetime) -> str:
    from azure.storage.blob import BlobSasPermissions, generate_blob_sas

    options = target.storage_options
    account, container, prefix = _adls_parts(table_path, options)
    blob = _join_key(prefix, rel_path)
    endpoint = options.get("azure_storage_endpoint") or f"https://{account}.blob.core.windows.net"
    if options.get("account_key"):
        sas = generate_blob_sas(
            account_name=account,
            container_name=container,
            blob_name=blob,
            account_key=options["account_key"],
            permission=BlobSasPermissions(read=True),
            expiry=expires,
        )
    else:
        sas = options["sas_token"].lstrip("?")
    return f"{endpoint.rstrip('/')}/{container}/{quote(blob)}?{sas}"


def file_url(
    *,
    target: CatalogStorageTarget,
    table_path: str,
    rel_path: str,
    share_table_id: int,
    protocol_base_url: str,
    ttl: int,
    now: float,
) -> tuple[str, int]:
    """``(url, expiration in epoch milliseconds)`` for one file of a shared table."""
    expires_at = int(now) + ttl
    if not target.is_cloud:
        token = file_tokens.mint(share_table_id, rel_path, table_path, expires_at)
        return f"{protocol_base_url.rstrip('/')}/{FILES_ROUTE}/{token}", expires_at * 1000
    refusal = signing_refusal(target, table_path)
    if refusal is not None:
        raise ValueError(refusal)
    kind = storage_type_for_uri(table_path)
    if kind == "s3":
        return _s3_url(target, table_path, rel_path, ttl), expires_at * 1000
    return _adls_url(target, table_path, rel_path, datetime.fromtimestamp(expires_at, UTC)), expires_at * 1000


def local_file(table_path: str, rel_path: str) -> Path | None:
    """The file *rel_path* names inside the table directory, or ``None`` when it would leave the directory."""
    if not rel_path or rel_path.startswith(("/", "\\")) or "\x00" in rel_path:
        return None
    root = Path(table_path).resolve()
    candidate = (root / rel_path).resolve()
    if candidate == root or root not in candidate.parents:
        return None
    return candidate if candidate.is_file() else None
