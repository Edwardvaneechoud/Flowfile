"""Serialize flow output and upload it to a SharePoint document library."""

import math
import re
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote

import httpx
import polars as pl
from cryptography.fernet import InvalidToken

from flowfile_worker.external_sources.sharepoint_source import _http_client
from flowfile_worker.secrets import decrypt_secret
from shared.sharepoint import (
    SharePointListTarget,
    SharePointReadError,
    SharePointWriteSettings,
    acquire_token,
    check_status,
    graph_listing,
    request,
)

SIMPLE_LIMIT = 4 * 1024 * 1024
CHUNK_SIZE = 10 * 320 * 1024
CONFLICT_BEHAVIOR = {"overwrite": "replace", "error": "fail", "append_suffix": "rename"}


def write_saved_file(frame: pl.LazyFrame, settings: SharePointWriteSettings) -> None:
    try:
        credential = decrypt_secret(settings.client_secret_encrypted)
    except (InvalidToken, ValueError):
        raise SharePointReadError("SharePoint connection credentials are unavailable.") from None
    with _http_client() as client:
        token = acquire_token(client, str(settings.tenant_id), str(settings.client_id), credential.get_secret_value(),
                              auth_method=settings.auth_method)
        if settings.list_target is not None:
            write_list(client, token, frame, settings.list_target)
        else:
            assert settings.drive_id and settings.file_path and settings.format
            write_file(client, token, frame, settings.drive_id, settings.file_path, settings.format,
                       settings.conflict_mode)


def _list_value(value, column):
    if ("text" in column or "choice" in column) and isinstance(value, str):
        return value
    if "boolean" in column and isinstance(value, bool):
        return value
    if ("number" in column or "currency" in column) and (
        type(value) is int or (type(value) is float and math.isfinite(value))
    ):
        return value
    if "dateTime" in column and isinstance(value, date):
        return value.isoformat()
    raise SharePointReadError("A mapped SharePoint list value has an unsupported type.")


def write_list(client: httpx.Client, token: str, frame: pl.LazyFrame, target: SharePointListTarget) -> None:
    path = f"/v1.0/sites/{target.site_id}/lists/{target.list_id}"
    metadata = {
        column.get("name"): column
        for column in graph_listing(client, f"{path}/columns", None, token, "SharePoint list columns")
        if isinstance(column.get("name"), str) and not column.get("readOnly") and not column.get("hidden")
    }
    for name in target.column_mapping.values():
        column = metadata.get(name, {})
        if not {"text", "choice", "boolean", "number", "currency", "dateTime"}.intersection(column):
            raise SharePointReadError("Map only supported, writable SharePoint list columns.")
    try:
        rows = frame.select(list(target.column_mapping)).collect()
    except pl.exceptions.PolarsError:
        raise SharePointReadError("SharePoint list input columns could not be read.") from None

    def fields(row):
        return {name: _list_value(value, metadata[name])
                for (source, name), value in zip(target.column_mapping.items(), row, strict=True)
                if value is not None}

    # Validate all rows before the first append; remote validation can still fail partway through.
    for row in rows.iter_rows():
        if not fields(row):
            raise SharePointReadError("A SharePoint list row must have at least one mapped, non-null value.")
    for created, row in enumerate(rows.iter_rows()):
        try:
            response = request(client, "POST", f"https://graph.microsoft.com{path}/items", "SharePoint list append",
                               headers={"Authorization": f"Bearer {token}"}, json={"fields": fields(row)})
            check_status(response, "SharePoint list append")
        except SharePointReadError as exc:
            raise SharePointReadError(
                f"SharePoint confirmed {created} of {rows.height} new items. {exc} "
                "Check the list before retrying; a retry can create duplicates."
            ) from None


def write_file(
    client: httpx.Client,
    token: str,
    frame: pl.LazyFrame,
    drive_id: str,
    file_path: str,
    format: str,
    conflict_mode: str,
) -> None:
    parts = file_path.split("/")
    if (
        not re.fullmatch(r"[A-Za-z0-9_!.-]{1,512}", drive_id)
        or drive_id in {".", ".."}
        or any(part in {"", ".", ".."} or re.search(r'[\x00-\x1f"*:<>?\\|]', part) for part in parts)
        or format not in {"csv", "xlsx", "parquet"}
        or not parts[-1].lower().endswith(f".{format}")
        or conflict_mode not in CONFLICT_BEHAVIOR
    ):
        raise SharePointReadError("Select a valid SharePoint output path, format and conflict mode.")
    encoded_path = "/".join(quote(part, safe="") for part in parts)
    target = f"https://graph.microsoft.com/v1.0/drives/{quote(drive_id, safe='')}/root:/{encoded_path}:"
    headers = {"Authorization": f"Bearer {token}"}
    with TemporaryDirectory(prefix="flowfile-sharepoint-") as directory:
        output = Path(directory) / f"output.{format}"
        try:
            if format == "csv":
                frame.sink_csv(output)
            elif format == "parquet":
                frame.sink_parquet(output)
            else:
                frame.collect().write_excel(output)
        except (pl.exceptions.PolarsError, OSError, TypeError, ValueError, ImportError):
            raise SharePointReadError("SharePoint output could not be serialized.") from None
        size = output.stat().st_size
        if size <= SIMPLE_LIMIT and (conflict_mode == "overwrite" or size == 0):
            response = request(
                client, "PUT", f"{target}/content", "SharePoint upload",
                headers={**headers, "Content-Type": "application/octet-stream"}, content=output.read_bytes(),
                params={"@microsoft.graph.conflictBehavior": CONFLICT_BEHAVIOR[conflict_mode]},
            )
            check_status(response, "SharePoint upload")
            return
        _upload_session(client, target, headers, output, size, CONFLICT_BEHAVIOR[conflict_mode])


def _upload_session(client, target, headers, output, size, behavior):
    response = request(
        client, "POST", f"{target}/createUploadSession", "SharePoint upload session", headers=headers,
        json={"item": {"@microsoft.graph.conflictBehavior": behavior}},
    )
    check_status(response, "SharePoint upload session")
    try:
        url = httpx.URL(response.json()["uploadUrl"])
        if (
            url.scheme != "https" or url.userinfo or url.port is not None or url.fragment
            or not url.host.endswith((".sharepoint.com", ".up.1drv.com"))
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, httpx.InvalidURL):
        raise SharePointReadError("SharePoint returned an invalid upload session URL.") from None

    with output.open("rb") as source:
        for start in range(0, size, CHUNK_SIZE):
            content = source.read(CHUNK_SIZE)
            stop = start + len(content)
            response = request(
                client, "PUT", str(url), "SharePoint upload chunk", content=content,
                headers={"Content-Length": str(len(content)), "Content-Range": f"bytes {start}-{stop - 1}/{size}"},
            )
            check_status(response, "SharePoint upload chunk")
            if stop == size and response.status_code in {200, 201}:
                return
            if stop < size and response.status_code == 202:
                try:
                    if response.json()["nextExpectedRanges"] == [f"{stop}-"]:
                        continue
                except (KeyError, TypeError, ValueError):
                    pass
            raise SharePointReadError("SharePoint did not confirm the uploaded bytes.")
