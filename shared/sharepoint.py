"""Shared Microsoft Graph client-credential helpers."""

import base64
import logging
import re
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Annotated, Literal
from urllib.parse import quote
from uuid import UUID, uuid4

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import Base64Bytes, Field, model_validator

from shared.wire_model import WireModel

DELEGATED_SCOPE = "https://graph.microsoft.com/Files.Read.All https://graph.microsoft.com/Sites.Read.All offline_access"
EXCEL_PREVIEW_PERMISSION = "Files.ReadWrite"


def delegated_scope(excel_column_preview: bool = False) -> str:
    if excel_column_preview:
        return f"{DELEGATED_SCOPE} https://graph.microsoft.com/{EXCEL_PREVIEW_PERMISSION}"
    return DELEGATED_SCOPE


def has_excel_preview_scope(scope: object) -> bool:
    return isinstance(scope, str) and any(
        value in {EXCEL_PREVIEW_PERMISSION, f"https://graph.microsoft.com/{EXCEL_PREVIEW_PERMISSION}"}
        for value in scope.split()
    )


SharePointSiteId = Annotated[str, Field(max_length=512, pattern=r"^[A-Za-z0-9.-]+,[A-Za-z0-9-]+,[A-Za-z0-9-]+$")]
SharePointListId = Annotated[str, Field(max_length=128, pattern=r"^[A-Za-z0-9-]+$")]
SharePointColumnName = Annotated[str, Field(max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]


class SharePointListSource(WireModel):
    site_id: SharePointSiteId
    list_id: SharePointListId
    columns: list[SharePointColumnName] = Field(min_length=1, max_length=100)
    odata_filter: str | None = Field(default=None, max_length=2048, repr=False)


class SharePointCredential(WireModel):
    tenant_id: UUID
    client_id: UUID
    auth_method: Literal["client_secret", "certificate", "delegated"] = "client_secret"
    client_secret_encrypted: str = Field(min_length=1, repr=False)


class SharePointDeltaState(WireModel):
    link: str = Field(min_length=1, max_length=32768, repr=False)
    item_id: str | None = Field(default=None, max_length=512)
    empty_ipc: Base64Bytes = Field(max_length=262144, repr=False)


class SharePointReadSettings(SharePointCredential):
    drive_id: str | None = Field(default=None, min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$")
    file_path: str | None = Field(default=None, min_length=1)
    list_source: SharePointListSource | None = None
    schema_only: bool = False
    incremental: bool = False
    delta_state: SharePointDeltaState | None = Field(default=None, repr=False)
    excel_column_preview: bool = False
    sheet_name: str | None = Field(default=None, min_length=1)
    csv_delimiter: str = Field(default=",", pattern=r"^[\x20-\x7e\t]$")
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1

    @model_validator(mode="after")
    def validate_source(self):
        validate_read_target(self.drive_id, self.file_path, self.list_source)
        if self.incremental and (self.schema_only or not is_incremental_file(self.file_path)):
            raise ValueError("Incremental reads require an exact CSV, JSON or Parquet file.")
        return self


class SharePointListTarget(WireModel):
    site_id: SharePointSiteId
    list_id: SharePointListId
    column_mapping: dict[Annotated[str, Field(min_length=1)], SharePointColumnName] = Field(
        min_length=1, max_length=100,
    )

    @model_validator(mode="after")
    def unique_columns(self):
        if len(set(self.column_mapping.values())) != len(self.column_mapping):
            raise ValueError("Map each SharePoint list column at most once.")
        return self


class SharePointWriteTarget(WireModel):
    drive_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_!.-]{1,512}$")
    file_path: str | None = Field(default=None, min_length=1, max_length=2048)
    format: Literal["csv", "xlsx", "parquet"] | None = None
    conflict_mode: Literal["overwrite", "error", "append_suffix", "update", "append"] = "error"
    sheet_name: str | None = Field(default=None, min_length=1, max_length=31, pattern=r"^[^\[\]:*?/\\\x00-\x1f]+$")
    write_when_empty: bool = False
    list_target: SharePointListTarget | None = None

    @model_validator(mode="after")
    def validate_target(self):
        if self.list_target is None and self.drive_id and self.file_path and self.format:
            if self.format == "xlsx" or (self.conflict_mode not in {"update", "append"} and self.sheet_name is None):
                return self
            raise ValueError("Sheet name, Update sheet and Append rows are only available for Excel output.")
        if (self.list_target is not None and self.drive_id is None and self.file_path is None
                and self.format is None and self.conflict_mode == "error" and self.sheet_name is None
                and not self.write_when_empty):
            return self
        raise ValueError("Select either a SharePoint output file or an append-only list target.")


class SharePointWriteSettings(SharePointCredential, SharePointWriteTarget):
    flowfile_flow_id: int = -1
    flowfile_node_id: int | str = -1


class SharePointWriteRequest(SharePointWriteSettings):
    operation: Base64Bytes = Field(repr=False)


def validate_read_target(drive_id, file_path, list_source):
    if list_source is None and drive_id and file_path:
        return
    if list_source is not None and drive_id is None and file_path is None:
        return
    raise ValueError("Select either a SharePoint file or a list, not both.")


class SharePointReadError(ValueError):
    """A SharePoint read failed; the message is safe to expose to callers."""


def request(client: httpx.Client, method: str, url: str, label: str, **kwargs) -> httpx.Response:
    log_scope = _private_request.set(True)
    try:
        return client.request(method, url, follow_redirects=False, **kwargs)
    except (httpx.RequestError, httpx.InvalidURL):
        raise SharePointReadError(f"{label} could not be completed.") from None
    finally:
        _private_request.reset(log_scope)


def check_status(response: httpx.Response, label: str) -> None:
    if not response.is_success:
        raise SharePointReadError(f"{label} failed (HTTP {response.status_code}).")


def graph_listing(client: httpx.Client, path: str, fields: str | None, token: str, label: str, **params):
    if fields is not None:
        params["$select"] = fields
    url = httpx.URL(f"https://graph.microsoft.com{path}", params=params)
    for _ in range(10):
        response = request(client, "GET", str(url), label, headers={"Authorization": f"Bearer {token}"})
        check_status(response, label)
        try:
            page = response.json()
            entries = page["value"]
            if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
                raise ValueError
            next_link = page.get("@odata.nextLink")
            continuation = httpx.URL(next_link) if next_link is not None else None
        except (ValueError, TypeError, KeyError, httpx.InvalidURL):
            raise SharePointReadError("SharePoint returned invalid listing metadata.") from None
        if continuation is not None and (
            continuation.scheme != "https"
            or continuation.netloc != b"graph.microsoft.com"
            or continuation.userinfo
            or continuation.path != url.path
        ):
            raise SharePointReadError("SharePoint returned an invalid listing continuation.")
        yield from entries
        if continuation is None:
            return
        url = continuation
    raise SharePointReadError("The SharePoint listing exceeds the browsing limit.")


def is_file_glob(file_path: str) -> bool:
    # SharePoint does not allow these two characters in literal names.
    return "*" in file_path or "?" in file_path


def is_incremental_file(file_path: str | None) -> bool:
    return bool(file_path and not is_file_glob(file_path) and file_path.lower().endswith((".csv", ".json", ".parquet")))


def graph_drive_delta(client: httpx.Client, drive_id: str, token: str, link: str | None):
    base = httpx.URL(f"https://graph.microsoft.com/v1.0/drives/{quote(drive_id, safe='')}/root/delta")
    label = "SharePoint delta query"
    paths = (base.path, base.path.removesuffix("/root/delta") + "/delta")

    def checked(value: str) -> httpx.URL:
        try:
            if not isinstance(value, str) or not value or len(value) > 32768:
                raise ValueError
            url = httpx.URL(value)
            query_form = url.path in paths and bool(url.query)
            function_form = not url.query and any(
                re.fullmatch(re.escape(path) + r"\(token=[^/\\?#()]+\)", url.path) for path in paths
            )
            if (url.scheme != "https" or url.netloc != base.netloc or url.userinfo or
                    url.fragment or not (query_form or function_form)):
                raise ValueError
            return url
        except (TypeError, ValueError, httpx.InvalidURL):
            raise SharePointReadError(
                "Microsoft Graph returned an invalid continuation for SharePoint delta."
            ) from None

    url = checked(link) if link is not None else base.copy_merge_params({"token": "latest"})
    entries = {}
    for _ in range(100):
        response = request(client, "GET", str(url), label, headers={"Authorization": f"Bearer {token}"})
        if response.status_code == 410:
            raise SharePointReadError(
                "The SharePoint delta cursor expired. Reset it explicitly to read the file again."
            )
        check_status(response, label)
        try:
            page = response.json()
            items = page["value"]
            if not isinstance(items, list):
                raise ValueError
            for item in items:
                if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
                    raise ValueError
                entries[item["id"]] = item
            has_next = "@odata.nextLink" in page
            if has_next == ("@odata.deltaLink" in page):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise SharePointReadError("Microsoft Graph returned invalid SharePoint delta metadata.") from None
        if not has_next:
            return entries, str(checked(page["@odata.deltaLink"]))
        url = checked(page["@odata.nextLink"])
    raise SharePointReadError("The SharePoint delta query exceeded its page limit; the cursor was not advanced.")


def _certificate_assertion(pem: str, endpoint: str, client_id: str) -> str:
    try:
        certificate_pem = re.search(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", pem, re.S)
        key_pem = re.search(r"-----BEGIN (RSA PRIVATE KEY|PRIVATE KEY)-----.*?-----END \1-----", pem, re.S)
        if certificate_pem is None or key_pem is None:
            raise ValueError
        certificate = x509.load_pem_x509_certificate(certificate_pem[0].encode())
        key = serialization.load_pem_private_key(key_pem[0].encode(), password=None)
        if not isinstance(key, rsa.RSAPrivateKey) or key.public_key() != certificate.public_key():
            raise ValueError
        thumbprint = base64.urlsafe_b64encode(certificate.fingerprint(hashes.SHA256())).decode().rstrip("=")
        now = int(datetime.now(UTC).timestamp())
        return jwt.encode(
            {
                "aud": endpoint,
                "iss": client_id,
                "sub": client_id,
                "jti": str(uuid4()),
                "iat": now,
                "nbf": now,
                "exp": now + 300,
            },
            key,
            algorithm="PS256",
            headers={"x5t#S256": thumbprint},
        )
    except (ValueError, TypeError, jwt.PyJWTError):
        raise SharePointReadError("A valid SharePoint certificate and matching RSA private key are required.") from None


def acquire_token(
    client: httpx.Client, tenant_id: str, client_id: str, credential: str, *, auth_method: str = "client_secret",
    excel_column_preview: bool = False,
) -> str:
    try:
        tenant = UUID(tenant_id)
    except ValueError:
        raise SharePointReadError("A valid Microsoft tenant ID is required.") from None

    endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": client_id, "scope": "https://graph.microsoft.com/.default"}
    if auth_method == "client_secret":
        data["client_secret"] = credential
    elif auth_method == "certificate":
        data["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
        data["client_assertion"] = _certificate_assertion(credential, endpoint, client_id)
    elif auth_method == "delegated":
        data.update(grant_type="refresh_token", refresh_token=credential, scope=delegated_scope(excel_column_preview))
    else:
        raise SharePointReadError("Unsupported SharePoint authentication method.")

    label = "Microsoft token request"
    token_response = request(
        client,
        "POST",
        endpoint,
        label,
        data=data,
    )
    if auth_method == "delegated" and token_response.status_code == 400:
        try:
            error = token_response.json()
        except ValueError:
            error = None
        if isinstance(error, dict) and error.get("error") == "invalid_grant":
            raise SharePointReadError(
                "Microsoft sign-in is no longer valid. "
                "Sign in again using Reconnect on the saved SharePoint connection."
            )
    check_status(token_response, label)
    try:
        payload = token_response.json()
        token = payload["access_token"]
    except (ValueError, KeyError, TypeError):
        raise SharePointReadError("Microsoft token response did not contain an access token.") from None
    if not isinstance(token, str) or not token or not token.isascii() or not token.isprintable() or " " in token:
        raise SharePointReadError("Microsoft token response did not contain a valid access token.")
    if excel_column_preview and not has_excel_preview_scope(payload.get("scope")):
        raise SharePointReadError("Microsoft did not grant Excel preview permission.")

    return token


_private_request = ContextVar("sharepoint_private_request", default=False)


def _allow_http_log(record: logging.LogRecord) -> bool:
    return not _private_request.get()


# HTTPX logs full URLs (including upload capabilities); HTTP Core debug traces
# can include redirect headers. Suppress only while making SharePoint requests.
for _logger_name in ("httpx", "httpcore.http11", "httpcore.http2"):
    logging.getLogger(_logger_name).addFilter(_allow_http_log)


def download_file(
    client: httpx.Client,
    token: str,
    drive_id: str,
    parts: list[str],
    *,
    limit: int,
    limit_message: str,
    partial: bool = False,
    tail: bool = False,
    trim_partial_line: bool = True,
    item_id: str | None = None,
) -> bytes:
    if item_id is None:
        path = "/".join(quote(part, safe="") for part in parts)
        target = f"root:/{path}:"
    else:
        target = f"items/{quote(item_id, safe='')}"
    url = f"https://graph.microsoft.com/v1.0/drives/{quote(drive_id, safe='')}/{target}/content"
    label = "SharePoint file download"
    headers = {"Authorization": f"Bearer {token}"}
    log_scope = _private_request.set(True)
    try:
        if partial or tail:
            headers["Accept-Encoding"] = "identity"
        for attempt in range(2):
            with client.stream("GET", url, headers=headers, auth=None, follow_redirects=False) as response:
                if attempt == 0 and response.status_code == 302:
                    target = httpx.URL(response.headers.get("Location", ""))
                    if target.scheme != "https" or not target.host or target.userinfo:
                        raise SharePointReadError("Microsoft Graph returned an invalid download URL.")
                    url = str(target)
                    headers.pop("Authorization", None)
                    if tail:
                        headers["Range"] = f"bytes=-{limit}"
                    elif partial:
                        headers["Range"] = f"bytes=0-{limit - 1}"
                    continue
                if (partial or tail) and response.headers.get("Content-Encoding", "identity") != "identity":
                    raise SharePointReadError(limit_message)

                expected_size = None
                prefix_total = None
                if tail:
                    match = re.fullmatch(
                        r"bytes (\d{1,12})-(\d{1,12})/(\d{1,12})", response.headers.get("Content-Range", "")
                    )
                    if response.status_code != 206 or not match:
                        raise SharePointReadError(limit_message)
                    start, end, total = map(int, match.groups())
                    if not (max(0, total - limit) <= start <= end == total - 1):
                        raise SharePointReadError(limit_message)
                    expected_size = end - start + 1
                elif partial:
                    match = re.fullmatch(r"bytes 0-(\d{1,12})/(\d{1,12})", response.headers.get("Content-Range", ""))
                    if response.status_code != 206 or not match:
                        raise SharePointReadError(limit_message)
                    end, total = map(int, match.groups())
                    if not end < min(total, limit):
                        raise SharePointReadError(limit_message)
                    expected_size = end + 1
                    prefix_total = total
                check_status(response, label)
                data = bytearray()
                for chunk in response.iter_bytes(chunk_size=65536):
                    if len(data) + len(chunk) > limit:
                        raise SharePointReadError(limit_message)
                    data.extend(chunk)
                if expected_size is not None and len(data) != expected_size:
                    raise SharePointReadError(limit_message)
                if trim_partial_line and prefix_total is not None and len(data) < prefix_total:
                    # A range can end mid-record, including inside a quoted field.
                    data = data[: data.rfind(b"\n") + 1]
                return bytes(data)
    except (httpx.HTTPError, httpx.InvalidURL):
        raise SharePointReadError(f"{label} could not be completed.") from None
    finally:
        _private_request.reset(log_scope)
    raise SharePointReadError(f"{label} could not be completed.")
