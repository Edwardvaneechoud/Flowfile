"""Manage saved SharePoint Online connections."""

import re
from os import getenv
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID

import httpx
from cryptography.fernet import InvalidToken
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field, SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import Secret, SharePointConnection
from flowfile_core.secret_manager.secret_manager import decrypt_secret, encrypt_secret
from shared.sharepoint import (
    SharePointReadError,
    SharePointReadSettings,
    acquire_token,
    check_status,
    request,
)

router = APIRouter()


def _parse_site_url(site_url: str) -> tuple[str, list[str]]:
    try:
        site = urlsplit(site_url)
        host = site.netloc.lower()
        segments = unquote(site.path).strip("/").split("/") if site.path.strip("/") else []
        if (
            site.scheme != "https"
            or not re.fullmatch(r"[a-z0-9][a-z0-9-]*\.sharepoint\.com", host)
            or site.query
            or site.fragment
            or any(not s or s in {".", ".."} or "\\" in s or not s.isprintable() for s in segments)
        ):
            raise ValueError
    except ValueError:
        raise HTTPException(422, "Enter an HTTPS SharePoint Online site URL.") from None
    return host, segments


class SharePointConnectionInput(BaseModel):
    connection_name: str = Field(min_length=1, max_length=255)
    tenant_id: UUID
    client_id: UUID
    client_secret: SecretStr = Field(min_length=1)
    default_site_url: str | None = Field(default=None, max_length=2048)


class SharePointConnectionMetadata(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    connection_name: str
    tenant_id: str
    client_id: str
    auth_method: str
    default_site_url: str | None


@router.get("")
def list_sharepoint_connections(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointConnectionMetadata]:
    connections = (
        db.query(SharePointConnection)
        .filter_by(user_id=current_user.id)
        .order_by(SharePointConnection.connection_name, SharePointConnection.id)
        .all()
    )
    return [SharePointConnectionMetadata.model_validate(connection) for connection in connections]


@router.post("", status_code=201)
def create_sharepoint_connection(
    connection: SharePointConnectionInput,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> SharePointConnectionMetadata:
    name = connection.connection_name.strip()
    if not name:
        raise HTTPException(422, "A connection name is required.")
    default_site_url = (connection.default_site_url or "").strip() or None
    if default_site_url:
        _parse_site_url(default_site_url)
    secret = Secret(
        name=f"SharePoint: {name}",
        user_id=current_user.id,
        encrypted_value=encrypt_secret(connection.client_secret.get_secret_value(), current_user.id),
    )
    db.add(secret)
    db.flush()
    saved = SharePointConnection(
        connection_name=name,
        tenant_id=str(connection.tenant_id),
        client_id=str(connection.client_id),
        default_site_url=default_site_url,
        auth_method="client_secret",
        user_id=current_user.id,
        credential_secret_id=secret.id,
    )
    db.add(saved)
    db.flush()
    result = SharePointConnectionMetadata.model_validate(saved)
    db.commit()
    return result


class SharePointConnectionTestInput(BaseModel):
    connection_id: int = Field(gt=0)
    drive_id: str = Field(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$")


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=15)


def _resolve_connection(db: Session, user_id: int, connection_id: int) -> tuple[SharePointConnection, str]:
    connection = db.query(SharePointConnection).filter_by(id=connection_id, user_id=user_id).one_or_none()
    if connection is None:
        raise HTTPException(404, "SharePoint connection not found.")
    secret = db.query(Secret).filter_by(id=connection.credential_secret_id, user_id=user_id).one_or_none()
    if connection.auth_method != "client_secret" or secret is None or not secret.encrypted_value:
        raise HTTPException(422, "SharePoint connection credentials are unavailable.")
    return connection, secret.encrypted_value


def build_read_settings(
    db: Session, user_id: int, connection_id: int, drive_id: str, file_path: str, sheet_name: str | None = None
) -> SharePointReadSettings:
    connection, ciphertext = _resolve_connection(db, user_id, connection_id)
    return SharePointReadSettings(
        tenant_id=UUID(connection.tenant_id),
        client_id=UUID(connection.client_id),
        client_secret_encrypted=ciphertext,
        drive_id=drive_id,
        file_path=file_path,
        sheet_name=sheet_name,
    )


@router.post("/test")
def test_sharepoint_connection(
    settings: SharePointConnectionTestInput,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict[str, bool]:
    if settings.drive_id in {".", ".."}:
        raise HTTPException(422, "A valid SharePoint drive ID is required.")
    connection, ciphertext = _resolve_connection(db, current_user.id, settings.connection_id)
    try:
        credential = decrypt_secret(ciphertext, current_user.id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None

    try:
        with _http_client() as client:
            token = acquire_token(client, connection.tenant_id, connection.client_id, credential.get_secret_value())
            label = "SharePoint drive check"
            response = request(
                client,
                "GET",
                f"https://graph.microsoft.com/v1.0/drives/{quote(settings.drive_id, safe='')}?$select=id",
                label,
                headers={"Authorization": f"Bearer {token}"},
            )
            check_status(response, label)
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    return {"success": True}


class SharePointLibrary(BaseModel):
    id: str
    name: str


def _graph_listing(client: httpx.Client, path: str, fields: str, token: str, label: str):
    url = httpx.URL(f"https://graph.microsoft.com{path}", params={"$select": fields})
    for _ in range(10):
        response = request(client, "GET", str(url), label, headers={"Authorization": f"Bearer {token}"})
        check_status(response, label)
        page = response.json()
        if not isinstance(page["value"], list):
            raise ValueError("Expected a list")
        yield from page["value"]
        next_link = page.get("@odata.nextLink")
        if next_link is None:
            return
        url = httpx.URL(next_link)
        if url.scheme != "https" or url.netloc != b"graph.microsoft.com" or url.userinfo or url.path != path:
            raise SharePointReadError("SharePoint returned an invalid listing continuation.")
    raise SharePointReadError("The SharePoint listing exceeds the browsing limit.")


@router.get("/browse_defaults")
def sharepoint_browse_defaults(_user=Depends(get_current_active_user)) -> dict[str, str | None]:
    return {"site_url": getenv("FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL", "").strip() or None}


@router.get("/{connection_id}/libraries")
def list_sharepoint_libraries(
    connection_id: int,
    site_url: str = Query(max_length=2048),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointLibrary]:
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    host, segments = _parse_site_url(site_url)
    try:
        credential = decrypt_secret(ciphertext, current_user.id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None

    try:
        with _http_client() as client:
            token = acquire_token(client, connection.tenant_id, connection.client_id, credential.get_secret_value())
            relative = "/".join(quote(segment, safe="") for segment in segments)
            lookup = f"https://graph.microsoft.com/v1.0/sites/{host}:/{relative}?$select=id"
            response = request(
                client, "GET", lookup, "SharePoint site lookup", headers={"Authorization": f"Bearer {token}"}
            )
            check_status(response, "SharePoint site lookup")
            site_id = response.json()["id"]
            if not isinstance(site_id, str) or not re.fullmatch(r"[A-Za-z0-9.-]+,[A-Za-z0-9-]+,[A-Za-z0-9-]+", site_id):
                raise ValueError
            path = f"/v1.0/sites/{site_id}/drives"
            return [
                SharePointLibrary.model_validate(item)
                for item in _graph_listing(client, path, "id,name", token, "SharePoint library listing")
            ]
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "SharePoint returned invalid site or library metadata.") from None


class SharePointDriveEntry(SharePointLibrary):
    is_folder: bool


@router.get("/{connection_id}/drives/{drive_id}/children")
def browse_sharepoint_drive(
    connection_id: int,
    drive_id: str = Path(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$"),
    folder_id: str | None = Query(default=None, min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointDriveEntry]:
    if drive_id in {".", ".."} or folder_id in {".", ".."}:
        raise HTTPException(422, "A valid SharePoint drive and folder are required.")
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    try:
        credential = decrypt_secret(ciphertext, current_user.id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None

    label = "SharePoint folder listing"
    folder = f"items/{quote(folder_id, safe='')}" if folder_id else "root"
    path = f"/v1.0/drives/{quote(drive_id, safe='')}/{folder}/children"
    try:
        with _http_client() as client:
            token = acquire_token(client, connection.tenant_id, connection.client_id, credential.get_secret_value())
            return [
                SharePointDriveEntry(id=item["id"], name=item["name"], is_folder="folder" in item)
                for item in _graph_listing(client, path, "id,name,folder,file", token, label)
                if "folder" in item or "file" in item
            ]
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "SharePoint returned an invalid folder listing.") from None


class SharePointFileMetadata(BaseModel):
    file_name: str
    file_path: str
    worksheet_names: list[str] | None = None
    version: tuple[str, str] | None = None


@router.get("/{connection_id}/drives/{drive_id}/worksheets")
def list_sharepoint_worksheets(
    connection_id: int,
    drive_id: str = Path(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$"),
    file_path: str = Query(min_length=1, max_length=4096),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> SharePointFileMetadata:
    parts = file_path.strip("/").split("/")
    if (
        drive_id in {".", ".."}
        or any(part in {"", ".", ".."} for part in parts)
        or not parts[-1].lower().endswith(".xlsx")
    ):
        raise HTTPException(422, "A valid SharePoint drive and Excel file path are required.")
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    try:
        credential = decrypt_secret(ciphertext, current_user.id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None
    try:
        with _http_client() as client:
            token = acquire_token(client, connection.tenant_id, connection.client_id, credential.get_secret_value())
            encoded_path = "/".join(quote(part, safe="") for part in parts)
            url = f"https://graph.microsoft.com/v1.0/drives/{quote(drive_id, safe='')}/root:/{encoded_path}"

            def get_item():
                label = "SharePoint file metadata"
                response = request(
                    client,
                    "GET",
                    url,
                    label,
                    headers={"Authorization": f"Bearer {token}"},
                    params={"$select": "id,name,cTag"},
                )
                check_status(response, label)
                try:
                    item = response.json()
                    name, identifier = item["name"], item["id"]
                    if not isinstance(name, str) or not isinstance(identifier, str) or not name or not identifier:
                        raise ValueError
                    tag = item.get("cTag")
                    return name, (identifier, tag) if isinstance(tag, str) and tag else None
                except (ValueError, TypeError, KeyError):
                    raise SharePointReadError("SharePoint returned invalid file metadata.") from None

            name, version = get_item()
            return SharePointFileMetadata(file_name=name, file_path="/".join([*parts[:-1], name]), version=version)
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
