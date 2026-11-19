"""Worker-side exact-path file reads from SharePoint Online via Microsoft Graph."""

import csv
import json
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

import fastexcel
import httpx
import polars as pl
from cryptography.fernet import InvalidToken

from flowfile_worker.create import FileType, table_creator_factory_method
from flowfile_worker.create.models import InputCsvTable, InputExcelTable, ReceivedTable
from flowfile_worker.secrets import decrypt_secret
from shared.sharepoint import SharePointReadError, SharePointReadSettings, acquire_token, download_file

FILE_DOWNLOAD_LIMIT = 64 * 1024 * 1024


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=30)


def read_saved_file(settings: SharePointReadSettings, sidecar_path: str | None = None) -> pl.DataFrame:
    try:
        credential = decrypt_secret(settings.client_secret_encrypted)
    except (InvalidToken, ValueError):
        raise SharePointReadError("SharePoint connection credentials are unavailable.") from None

    with _http_client() as client:
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
        )


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
) -> pl.DataFrame:
    parts = file_path.strip("/").split("/")
    if not drive_id or drive_id in {".", ".."} or any(part in {"", ".", ".."} for part in parts):
        raise SharePointReadError("A drive ID and exact file path are required.")

    _, separator, extension = parts[-1].lower().rpartition(".")
    is_excel = extension == "xlsx"
    readers = {"csv", "parquet", "json"}
    if not separator or (not is_excel and extension not in readers):
        raise SharePointReadError("SharePoint reads support only .csv, .xlsx, .parquet, and .json files.")
    if sheet_name is not None and not is_excel:
        raise SharePointReadError("Worksheet selection is only supported for .xlsx files.")

    if schema_only and extension not in {"csv", "parquet", "json"}:
        return pl.DataFrame()
    token = acquire_token(client, tenant_id, client_id, client_secret)
    if schema_only and extension == "parquet":
        return _preview_parquet(client, token, drive_id, parts)
    if schema_only and extension == "json":
        return _preview_json(client, token, drive_id, parts)
    if schema_only:
        return _preview_csv(client, token, drive_id, parts, csv_delimiter)
    source = BytesIO(
        download_file(
            client,
            token,
            drive_id,
            parts,
            limit=FILE_DOWNLOAD_LIMIT,
            limit_message=f"The SharePoint file exceeds the download size limit ({FILE_DOWNLOAD_LIMIT:,} bytes).",
        )
    )
    if is_excel:
        try:
            names = fastexcel.read_excel(source.getvalue()).sheet_names if sidecar_path or sheet_name else None
            if sheet_name is not None and names is not None and sheet_name not in names:
                raise SharePointReadError(
                    "The selected SharePoint worksheet is missing. Refresh File Metadata and choose a worksheet."
                )
            frame = _read_with_file_reader(source, extension, "excel", InputExcelTable(sheet_name=sheet_name))
        except SharePointReadError:
            raise
        except Exception:
            # Engine errors can carry workbook data; expose only the stable message.
            raise SharePointReadError("The SharePoint workbook or requested worksheet could not be read.") from None
        if sidecar_path:
            Path(sidecar_path).write_text(json.dumps({"worksheet_names": names}), encoding="utf-8")
        return frame
    try:
        if extension == "json":
            return pl.read_json(source)
        settings = InputCsvTable(delimiter=csv_delimiter) if extension == "csv" else None
        return _read_with_file_reader(source, extension, "csv" if extension == "csv" else "parquet", settings)
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
