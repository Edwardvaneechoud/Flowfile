"""Shared Microsoft Graph client-credential helpers."""

import re
from urllib.parse import quote
from uuid import UUID

import httpx
from pydantic import Field

from shared.wire_model import WireModel


class SharePointReadSettings(WireModel):
    tenant_id: UUID
    client_id: UUID
    client_secret_encrypted: str = Field(min_length=1, repr=False)
    drive_id: str = Field(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$")
    file_path: str = Field(min_length=1)
    schema_only: bool = False
    sheet_name: str | None = Field(default=None, min_length=1)
    csv_delimiter: str = Field(default=",", pattern=r"^[\x20-\x7e\t]$")
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1


class SharePointReadError(ValueError):
    """A SharePoint read failed; the message is safe to expose to callers."""


def request(client: httpx.Client, method: str, url: str, label: str, **kwargs) -> httpx.Response:
    try:
        return client.request(method, url, follow_redirects=False, **kwargs)
    except (httpx.RequestError, httpx.InvalidURL):
        raise SharePointReadError(f"{label} could not be completed.") from None


def check_status(response: httpx.Response, label: str) -> None:
    if not response.is_success:
        raise SharePointReadError(f"{label} failed (HTTP {response.status_code}).")


def acquire_token(client: httpx.Client, tenant_id: str, client_id: str, client_secret: str) -> str:
    try:
        tenant = UUID(tenant_id)
    except ValueError:
        raise SharePointReadError("A valid Microsoft tenant ID is required.") from None

    label = "Microsoft token request"
    token_response = request(
        client,
        "POST",
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        label,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        },
    )
    check_status(token_response, label)
    try:
        token = token_response.json()["access_token"]
    except (ValueError, KeyError, TypeError):
        raise SharePointReadError("Microsoft token response did not contain an access token.") from None
    if not isinstance(token, str) or not token or not token.isascii() or not token.isprintable() or " " in token:
        raise SharePointReadError("Microsoft token response did not contain a valid access token.")

    return token


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
) -> bytes:
    path = "/".join(quote(part, safe="") for part in parts)
    url = f"https://graph.microsoft.com/v1.0/drives/{quote(drive_id, safe='')}/root:/{path}:/content"
    label = "SharePoint file download"
    headers = {"Authorization": f"Bearer {token}"}
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
    raise SharePointReadError(f"{label} could not be completed.")
