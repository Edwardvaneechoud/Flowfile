"""Worker-side file reads from SharePoint Online via Microsoft Graph."""

import csv
import json
import re
from base64 import b64encode
from fnmatch import fnmatchcase
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote

import fastexcel
import httpx
import polars as pl
from cryptography.fernet import InvalidToken

from flowfile_worker.create import FileType, table_creator_factory_method
from flowfile_worker.create.models import InputCsvTable, InputExcelTable, ReceivedTable
from flowfile_worker.secrets import decrypt_secret
from shared.sharepoint import (
    SharePointDeltaState,
    SharePointListSource,
    SharePointReadError,
    SharePointReadSettings,
    acquire_token,
    check_status,
    download_file,
    graph_drive_delta,
    graph_listing,
    is_file_glob,
    request,
)

FILE_DOWNLOAD_LIMIT = 64 * 1024 * 1024
MAX_GLOB_FILES = 32


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=30)


def read_saved_file(settings: SharePointReadSettings, sidecar_path: str | None = None) -> pl.DataFrame:
    try:
        credential = decrypt_secret(settings.client_secret_encrypted)
    except (InvalidToken, ValueError):
        raise SharePointReadError("SharePoint connection credentials are unavailable.") from None

    with _http_client() as client:
        if settings.incremental:
            frame, state = read_incremental_file(client, settings, credential.get_secret_value())
            if sidecar_path:
                Path(sidecar_path).write_text(
                    json.dumps({"delta_state": state.model_dump(mode="json")}), encoding="utf-8"
                )
            return frame
        if settings.list_source is not None:
            if settings.schema_only:
                return pl.DataFrame()
            token = acquire_token(
                client, str(settings.tenant_id), str(settings.client_id), credential.get_secret_value(),
                auth_method=settings.auth_method,
            )
            return _read_list(client, token, settings.list_source)
        return read_file(
            client,
            str(settings.tenant_id),
            str(settings.client_id),
            credential.get_secret_value(),
            settings.drive_id,
            settings.file_path,
            settings.sheet_name,
            csv_delimiter=settings.csv_delimiter,
            sidecar_path=sidecar_path,
            schema_only=settings.schema_only,
            auth_method=settings.auth_method,
            excel_column_preview=settings.excel_column_preview,
        )


def read_incremental_file(client: httpx.Client, settings: SharePointReadSettings, credential: str):
    assert settings.file_path and settings.drive_id
    parts = settings.file_path.strip("/").split("/")
    if settings.drive_id in {".", ".."} or any(
        part in {"", ".", ".."} or "\\" in part or "\0" in part for part in parts
    ):
        raise SharePointReadError("A valid SharePoint drive ID and file path are required.")
    token = acquire_token(client, str(settings.tenant_id), str(settings.client_id), credential,
                          auth_method=settings.auth_method)
    previous = settings.delta_state
    changes, link = graph_drive_delta(client, settings.drive_id, token, previous.link if previous else None)
    path = "/".join(quote(part, safe="") for part in parts)
    base = f"https://graph.microsoft.com/v1.0/drives/{quote(settings.drive_id, safe='')}"
    label = "SharePoint incremental file lookup"
    response = request(client, "GET", f"{base}/root:/{path}:", label,
                       headers={"Authorization": f"Bearer {token}"}, params={"$select": "id,file"})
    item_id = None
    if response.status_code != 404 or previous is None:
        check_status(response, label)
        try:
            item = response.json()
            item_id = item["id"]
            if (not isinstance(item.get("file"), dict) or not isinstance(item_id, str)
                    or not re.fullmatch(r"[A-Za-z0-9_!.-]{1,512}", item_id) or item_id in {".", ".."}):
                raise ValueError
        except (KeyError, ValueError, TypeError):
            raise SharePointReadError("Microsoft Graph returned invalid incremental file metadata.") from None

    if previous is not None and (item_id is None or (item_id == previous.item_id and item_id not in changes)):
        try:
            frame = pl.read_ipc(BytesIO(previous.empty_ipc), n_rows=0)
        except (pl.exceptions.PolarsError, OSError, ValueError):
            raise SharePointReadError(
                "The saved SharePoint delta schema is invalid. Reset the cursor explicitly."
            ) from None
        empty_ipc = previous.empty_ipc
    else:
        content = download_file(client, token, settings.drive_id, parts, item_id=item_id, limit=FILE_DOWNLOAD_LIMIT,
                                limit_message="The SharePoint incremental read exceeds its download size limit.")
        extension = parts[-1].rsplit(".", 1)[-1].lower()
        frame, _ = _parse_file(BytesIO(content), extension, None, settings.csv_delimiter, False)
        empty_ipc = frame.head(0).write_ipc(None).getvalue()
        if len(empty_ipc) > 262144:
            raise SharePointReadError("The SharePoint incremental schema exceeds its size limit.")
    return frame, SharePointDeltaState(link=link, item_id=item_id, empty_ipc=b64encode(empty_ipc))


def _read_list(client: httpx.Client, token: str, source: SharePointListSource) -> pl.DataFrame:
    columns = dict.fromkeys(source.columns)
    params = {"$expand": f"fields($select={','.join(columns)})", "$top": "200"}
    if source.odata_filter:
        params["$filter"] = source.odata_filter
    path = f"/v1.0/sites/{source.site_id}/lists/{source.list_id}/items"
    rows = []
    for item in graph_listing(client, path, None, token, "SharePoint list read", **params):
        fields = item.get("fields")
        if not isinstance(fields, dict):
            raise SharePointReadError("SharePoint returned invalid list fields.")
        rows.append({name: fields.get(name) for name in columns})
    try:
        if not rows:
            return pl.DataFrame(schema=dict.fromkeys(columns, pl.Null))
        return pl.from_dicts(rows, infer_schema_length=None)
    except (pl.exceptions.PolarsError, TypeError, ValueError, OverflowError):
        raise SharePointReadError("SharePoint returned unsupported list field values.") from None


def read_file(
    client: httpx.Client,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    drive_id: str,
    file_path: str,
    sheet_name: str | None = None,
    *,
    sidecar_path: str | None = None,
    schema_only: bool = False,
    csv_delimiter: str = ",",
    auth_method: str = "client_secret",
    excel_column_preview: bool = False,
) -> pl.DataFrame:
    parts = file_path.strip("/").split("/")
    if not drive_id or drive_id in {".", ".."} or any(
        part in {"", ".", ".."} or "\\" in part or "\0" in part for part in parts
    ):
        raise SharePointReadError("A valid SharePoint drive ID and file path are required.")

    _, separator, extension = parts[-1].lower().rpartition(".")
    is_excel = extension in {"xls", "xlsx"}
    readers = {"csv", "parquet", "json"}
    if not separator or (not is_excel and extension not in readers):
        raise SharePointReadError("SharePoint reads support only .csv, .xls, .xlsx, .parquet, and .json files.")
    if sheet_name is not None and not is_excel:
        raise SharePointReadError("Worksheet selection is only supported for .xls and .xlsx files.")

    glob = is_file_glob(file_path)
    if glob and (any(is_file_glob(part) for part in parts[:-1]) or "**" in parts[-1]):
        raise SharePointReadError("SharePoint wildcards are supported only in the filename, without recursion.")
    if schema_only and glob:
        return pl.DataFrame()
    if schema_only and is_excel:
        if extension != "xlsx" or auth_method != "delegated" or not excel_column_preview:
            return pl.DataFrame()
        try:
            token = acquire_token(client, tenant_id, client_id, client_secret,
                                  auth_method=auth_method, excel_column_preview=True)
            return _preview_excel(client, token, drive_id, parts, sheet_name)
        except SharePointReadError:
            # Preview never retries consent or downloads the workbook on failure.
            return pl.DataFrame()
    token = acquire_token(client, tenant_id, client_id, client_secret, auth_method=auth_method)
    if schema_only and extension == "parquet":
        return _preview_parquet(client, token, drive_id, parts)
    if schema_only and extension == "json":
        return _preview_json(client, token, drive_id, parts)
    if schema_only:
        return _preview_csv(client, token, drive_id, parts, csv_delimiter)
    paths = _glob_paths(client, token, drive_id, parts) if glob else [parts]
    remaining = FILE_DOWNLOAD_LIMIT
    combined = None
    worksheets = None
    for path in paths:
        content = download_file(
            client, token, drive_id, path, limit=remaining,
            limit_message=f"The SharePoint read exceeds the download size limit ({FILE_DOWNLOAD_LIMIT:,} bytes).",
        )
        remaining -= len(content)
        frame, names = _parse_file(BytesIO(content), extension, sheet_name, csv_delimiter, bool(sidecar_path))
        if names is not None:
            worksheets = names if worksheets is None else [name for name in worksheets if name in names]
        if combined is None:
            combined = frame
        else:
            # Keep predictable row order without accepting schemas that silently coerce data.
            if combined.schema != frame.schema or combined.columns != frame.columns:
                raise SharePointReadError("Files matching the SharePoint glob must have the same columns and types.")
            combined.vstack(frame, in_place=True)
    if sidecar_path and worksheets is not None:
        Path(sidecar_path).write_text(json.dumps({"worksheet_names": worksheets}), encoding="utf-8")
    assert combined is not None  # Exact paths and successful globs always have at least one file.
    return combined


def _preview_excel(
    client: httpx.Client, token: str, drive_id: str, parts: list[str], sheet_name: str | None,
) -> pl.DataFrame:
    encoded_path = "/".join(quote(part, safe="") for part in parts)
    path = f"/v1.0/drives/{quote(drive_id, safe='')}/root:/{encoded_path}:/workbook/worksheets"
    label = "SharePoint Excel column preview"
    sheets = graph_listing(client, path, "id,name,position", token, label)
    sheet = next((item for item in sheets if (
        item.get("position") == 0 if sheet_name is None else item.get("name") == sheet_name
    )), None)
    ident = sheet.get("id") if sheet else None
    if not isinstance(ident, str) or ident in {"", ".", ".."}:
        return pl.DataFrame()
    url = f"https://graph.microsoft.com{path}/{quote(ident, safe='')}/range(address='A1:IV1')"
    response = request(client, "GET", url, label, headers={"Authorization": f"Bearer {token}"},
                       params={"$select": "text"})
    check_status(response, label)
    try:
        rows = response.json()["text"]
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], list) or len(rows[0]) > 256:
            return pl.DataFrame()
        columns = rows[0]
        if any(not isinstance(col, str) for col in columns):
            return pl.DataFrame()
        while columns and not columns[-1]:
            columns.pop()
        if any(not col.strip() for col in columns) or len(set(columns)) != len(columns):
            return pl.DataFrame()
        return pl.DataFrame(schema=dict.fromkeys(columns, pl.Null))
    except (ValueError, KeyError, TypeError):
        return pl.DataFrame()


def _glob_paths(client: httpx.Client, token: str, drive_id: str, parts: list[str]) -> list[list[str]]:
    folders, pattern = parts[:-1], parts[-1]
    encoded = "/".join(quote(part, safe="") for part in folders)
    folder = f"root:/{encoded}:" if folders else "root"
    path = f"/v1.0/drives/{quote(drive_id, safe='')}/{folder}/children"
    pattern = pattern.lower().replace("[", "[[]")  # Only * and ? are wildcards.
    names = set()
    for item in graph_listing(client, path, "name,file", token, "SharePoint glob listing"):
        if "file" not in item or item["file"] is None:
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name or any(c in name for c in "/\\\0*?"):
            raise SharePointReadError("SharePoint returned invalid file metadata.")
        if fnmatchcase(name.lower(), pattern):
            names.add(name)
            if len(names) > MAX_GLOB_FILES:
                raise SharePointReadError(f"The SharePoint glob matches more than {MAX_GLOB_FILES} files.")
    if not names:
        raise SharePointReadError("No files match the SharePoint glob.")
    return [[*folders, name] for name in sorted(names, key=lambda name: (name.casefold(), name))]


def _parse_file(
    source: BytesIO, extension: str, sheet_name: str | None, csv_delimiter: str, include_names: bool
) -> tuple[pl.DataFrame, list[str] | None]:
    if extension in {"xls", "xlsx"}:
        try:
            names = fastexcel.read_excel(source.getvalue()).sheet_names if include_names or sheet_name else None
            if sheet_name is not None and names is not None and sheet_name not in names:
                raise SharePointReadError(
                    "The selected SharePoint worksheet is missing. "
                    "Enter an existing sheet name or clear Sheet name to use the first sheet, then run again."
                )
            if extension == "xls":
                # The shared reader's openpyxl header handling cannot open binary .xls workbooks.
                frame = pl.read_excel(source, engine="calamine", sheet_name=sheet_name, raise_if_empty=False)
            else:
                frame = _read_with_file_reader(source, extension, "excel", InputExcelTable(sheet_name=sheet_name))
        except SharePointReadError:
            raise
        except Exception:
            # Engine errors can carry workbook data; expose only the stable message.
            raise SharePointReadError("The SharePoint workbook or requested worksheet could not be read.") from None
        return frame, names
    try:
        if extension == "json":
            return pl.read_json(source), None
        settings = InputCsvTable(delimiter=csv_delimiter) if extension == "csv" else None
        return _read_with_file_reader(source, extension, "csv" if extension == "csv" else "parquet", settings), None
    except (pl.exceptions.PolarsError, ValueError):
        raise SharePointReadError(f"The SharePoint file could not be parsed as {extension.upper()}.") from None


def _read_with_file_reader(
    source: BytesIO, extension: str, file_type: FileType, settings: InputCsvTable | InputExcelTable | None
) -> pl.DataFrame:
    with TemporaryDirectory(prefix="flowfile-sharepoint-") as directory:
        path = Path(directory) / f"input.{extension}"
        path.write_bytes(source.getbuffer())
        table = ReceivedTable.create_from_path(str(path), file_type)
        table.abs_file_path = str(path)
        if settings is not None:
            table.table_settings = settings
        result = table_creator_factory_method(file_type)(table)
        # Lazy readers still need the temporary file until collection finishes.
        return result.collect() if isinstance(result, pl.LazyFrame) else result


def _preview_csv(client: httpx.Client, token: str, drive_id: str, parts: list[str], delimiter: str) -> pl.DataFrame:
    try:
        sample = download_file(
            client,
            token,
            drive_id,
            parts,
            limit=64 * 1024,
            partial=True,
            limit_message="SharePoint CSV schema unavailable until run.",
        )
        # Keep only complete records, including when the cap cuts a multiline field.
        stream = StringIO(sample.decode("utf-8-sig"), newline="")
        rows = csv.reader(stream, strict=True, delimiter=delimiter)
        next(rows)
        next(rows)
        complete = stream.tell()
        try:
            for _ in rows:
                complete = stream.tell()
        except csv.Error:
            if stream.tell() != len(stream.getvalue()):
                raise
        return pl.read_csv(BytesIO(stream.getvalue()[:complete].encode("utf-8")), separator=delimiter).clear()
    except (SharePointReadError, UnicodeError, csv.Error, StopIteration, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _preview_parquet(client: httpx.Client, token: str, drive_id: str, parts: list[str]) -> pl.DataFrame:
    try:
        tail = download_file(
            client,
            token,
            drive_id,
            parts,
            limit=256 * 1024,
            tail=True,
            limit_message="SharePoint Parquet schema unavailable until run.",
        )
        if len(tail) < 8 or tail[-4:] != b"PAR1":
            return pl.DataFrame()
        footer_length = int.from_bytes(tail[-8:-4], "little")
        if not 0 < footer_length <= len(tail) - 8:
            return pl.DataFrame()
        # The schema reader needs only the magic bytes and serialized footer, not page offsets.
        schema = pl.read_parquet_schema(BytesIO(b"PAR1" + tail[-(footer_length + 8) :]))
        return pl.DataFrame(schema=schema)
    except (SharePointReadError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _preview_json(client: httpx.Client, token: str, drive_id: str, parts: list[str]) -> pl.DataFrame:
    try:
        sample = download_file(
            client,
            token,
            drive_id,
            parts,
            limit=64 * 1024,
            partial=True,
            trim_partial_line=False,
            limit_message="SharePoint JSON schema unavailable until run.",
        )
        text = sample.decode("utf-8").lstrip(" \t\r\n")
        if not text.startswith("["):
            return pl.DataFrame()
        decoder = json.JSONDecoder()
        pos = 1
        last_complete = 1
        while pos < len(text):
            while pos < len(text) and text[pos] in " \t\r\n":
                pos += 1
            try:
                value, end = decoder.raw_decode(text, pos)
            except json.JSONDecodeError:
                break
            if not isinstance(value, dict):
                return pl.DataFrame()
            pos = end
            while pos < len(text) and text[pos] in " \t\r\n":
                pos += 1
            if pos == len(text) or text[pos] not in ",]":
                break
            last_complete = end
            if text[pos] == "]":
                break
            pos += 1
        if last_complete == 1:
            return pl.DataFrame()
        return pl.read_json(BytesIO((text[:last_complete] + "]").encode("utf-8"))).clear()
    except (SharePointReadError, UnicodeError, RecursionError, ValueError, pl.exceptions.PolarsError):
        return pl.DataFrame()
