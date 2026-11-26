"""Manage saved SharePoint Online connections."""

import re
from dataclasses import dataclass
from os import getenv
from threading import Lock, Timer
from time import monotonic
from typing import Literal
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

import httpx
from cryptography.fernet import InvalidToken
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field, SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import Secret, SharePointConnection
from flowfile_core.routes._connection_sharing import authorize_connection_mutation
from flowfile_core.schemas.input_schema import NodeSharePointWriter
from flowfile_core.secret_manager.secret_manager import decrypt_secret, encrypt_secret, stage_encrypted_secret
from shared.sharepoint import (
    SharePointListId,
    SharePointListSource,
    SharePointReadError,
    SharePointReadSettings,
    SharePointSiteId,
    SharePointWriteSettings,
    acquire_token,
    check_status,
    delegated_scope,
    graph_listing,
    has_excel_preview_scope,
    is_file_glob,
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


class SharePointTarget(BaseModel):
    connection_name: str = Field(min_length=1, max_length=255)
    tenant_id: UUID
    client_id: UUID
    default_site_url: str | None = Field(default=None, max_length=2048)


class SharePointConnectionInput(SharePointTarget):
    auth_method: Literal["client_secret", "certificate"] = "client_secret"
    client_secret: SecretStr = Field(min_length=1)


class SharePointDeviceTarget(SharePointTarget):
    excel_column_preview: bool = False


class SharePointConnectionMetadata(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    connection_name: str
    tenant_id: str
    client_id: str
    auth_method: str
    excel_column_preview: bool
    is_owner: bool = True
    can_manage: bool = True
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
    shared = sharing.shared_resource_rows(db, current_user.id, "sharepoint_connection")
    return [
        SharePointConnectionMetadata.model_validate(connection)
        for connection in connections
        if connection.auth_method != "delegated" or sharing.browser_sign_in_allowed()
    ] + [
        SharePointConnectionMetadata.model_validate(row).model_copy(
            update={
                "is_owner": False,
                "can_manage": access.access_level == "manage" or getattr(current_user, "is_admin", False),
            }
        )
        for row, access in shared
        if row.auth_method != "delegated"
    ]


@router.post("", status_code=201)
def create_sharepoint_connection(
    connection: SharePointConnectionInput,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> SharePointConnectionMetadata:
    return _save_connection(
        db, current_user.id, connection, connection.auth_method, connection.client_secret.get_secret_value()
    )


def _save_connection(db, user_id, connection, auth_method, credential, *, excel_column_preview=False):
    name = connection.connection_name.strip()
    if not name:
        raise HTTPException(422, "A connection name is required.")
    default_site_url = (connection.default_site_url or "").strip() or None
    if default_site_url:
        _parse_site_url(default_site_url)
    secret = stage_encrypted_secret(db, name=f"SharePoint: {name}", value=credential, user_id=user_id)
    db.flush()
    saved = SharePointConnection(
        connection_name=name,
        tenant_id=str(connection.tenant_id),
        client_id=str(connection.client_id),
        default_site_url=default_site_url,
        auth_method=auth_method,
        excel_column_preview=excel_column_preview,
        user_id=user_id,
        credential_secret_id=secret.id,
    )
    db.add(saved)
    db.flush()
    result = SharePointConnectionMetadata.model_validate(saved)
    db.commit()
    return result


class SharePointMetadataUpdate(BaseModel):
    model_config = {"extra": "forbid"}

    connection_name: str = Field(min_length=1, max_length=255)
    default_site_url: str | None = Field(default=None, max_length=2048)


def _manageable_connection(db: Session, current_user, connection_id: int) -> SharePointConnection:
    connection = db.get(SharePointConnection, connection_id)
    if connection is None or (connection.auth_method == "delegated" and connection.user_id != current_user.id):
        raise HTTPException(404, "SharePoint connection not found.")
    authorize_connection_mutation(db, current_user, "sharepoint_connection", connection)
    return connection


@router.patch("/{connection_id}")
def update_sharepoint_connection(
    connection_id: int,
    metadata: SharePointMetadataUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> SharePointConnectionMetadata:
    connection = _manageable_connection(db, current_user, connection_id)
    name = metadata.connection_name.strip()
    if not name:
        raise HTTPException(422, "A connection name is required.")
    if "default_site_url" in metadata.model_fields_set:
        site = (metadata.default_site_url or "").strip() or None
        if site != connection.default_site_url:
            raise HTTPException(422, "To change the default site, create a new connection and enter credentials again.")
    connection.connection_name = name
    result = SharePointConnectionMetadata.model_validate(connection).model_copy(
        update={"is_owner": connection.user_id == current_user.id},
    )
    db.commit()
    return result


@router.delete("/{connection_id}", status_code=204)
def delete_sharepoint_connection(
    connection_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> None:
    connection = _manageable_connection(db, current_user, connection_id)
    secret_id, owner_id = connection.credential_secret_id, connection.user_id
    # The resource registry removes the connection's grants on ORM delete.
    db.delete(connection)
    db.flush()
    db.query(Secret).filter_by(id=secret_id, user_id=owner_id).delete(synchronize_session=False)
    db.commit()


@dataclass
class _DeviceFlow:
    owner: int
    target: SharePointDeviceTarget
    code: str
    expires: float
    interval: int
    expiry_timer: Timer
    connection_id: int | None = None
    next_poll: float = 0
    refresh_token: str | None = None
    excel_column_preview: bool = False


_device_flows: dict[str, _DeviceFlow] = {}
_device_lock = Lock()


def _discard_device_flow_locked(flow_id: str) -> None:
    flow = _device_flows.pop(flow_id, None)
    if flow is not None:
        flow.expiry_timer.cancel()


def _expire_device_flow(flow_id: str) -> None:
    with _device_lock:
        _discard_device_flow_locked(flow_id)


@router.post("/device/start")
def start_device_sign_in(target: SharePointDeviceTarget, current_user=Depends(get_current_active_user)):
    return _start_device_sign_in(target, current_user.id)


@router.post("/{connection_id}/reconnect")
def reconnect_device_sign_in(
    connection_id: int, excel_column_preview: bool = False,
    current_user=Depends(get_current_active_user), db: Session = Depends(get_db),
):
    if not sharing.browser_sign_in_allowed():
        raise HTTPException(404, "Not Found")
    connection = _owned_delegated_connection(db, current_user.id, connection_id)
    target = SharePointDeviceTarget.model_validate(connection, from_attributes=True)
    target.excel_column_preview = excel_column_preview
    return _start_device_sign_in(target, current_user.id, connection_id)


def _owned_delegated_connection(db: Session, user_id: int, connection_id: int) -> SharePointConnection:
    connection = (
        db.query(SharePointConnection)
        .filter_by(id=connection_id, user_id=user_id, auth_method="delegated")
        .one_or_none()
    )
    if connection is None:
        raise HTTPException(404, "SharePoint connection not found.")
    return connection


def _start_device_sign_in(target: SharePointDeviceTarget, user_id: int, connection_id: int | None = None):
    if not sharing.browser_sign_in_allowed():
        raise HTTPException(404, "Not Found")
    if not target.connection_name.strip():
        raise HTTPException(422, "A connection name is required.")
    if target.default_site_url and target.default_site_url.strip():
        _parse_site_url(target.default_site_url.strip())
    try:
        with _http_client() as client:
            response = request(
                client,
                "POST",
                f"https://login.microsoftonline.com/{target.tenant_id}/oauth2/v2.0/devicecode",
                "Microsoft device sign-in",
                data={"client_id": str(target.client_id), "scope": delegated_scope(target.excel_column_preview)},
            )
        check_status(response, "Microsoft device sign-in")
        data = response.json()
        code, user_code = data["device_code"], data["user_code"]
        interval = max(5, min(60, int(data.get("interval", 5))))
        expires = max(1, min(900, int(data["expires_in"])))
        if not isinstance(code, str) or not code or not isinstance(user_code, str) or not user_code:
            raise ValueError
    except (SharePointReadError, ValueError, TypeError, KeyError):
        raise HTTPException(422, "Could not start Microsoft device sign-in.") from None
    flow_id = uuid4().hex
    now = monotonic()
    timer = Timer(expires, _expire_device_flow, args=(flow_id,))
    timer.daemon = True
    with _device_lock:
        for key, flow in list(_device_flows.items()):
            if flow.expires <= now or flow.owner == user_id:
                _discard_device_flow_locked(key)
        _device_flows[flow_id] = _DeviceFlow(user_id, target, code, now + expires, interval, timer, connection_id)
        timer.start()
    return {
        "flow_id": flow_id,
        "user_code": user_code,
        "verification_uri": "https://microsoft.com/devicelogin",
        "interval": interval,
        "expires_in": expires,
    }


@router.post("/device/{flow_id}")
def poll_device_sign_in(flow_id: str, current_user=Depends(get_current_active_user)):
    if not sharing.browser_sign_in_allowed():
        raise HTTPException(404, "Not Found")
    now = monotonic()
    with _device_lock:
        flow = _device_flows.get(flow_id)
        if flow is None or flow.owner != current_user.id or flow.expires <= now:
            raise HTTPException(404, "Sign-in expired. Start again.")
        if flow.refresh_token is not None:
            return {"status": "ready"}
        if now < flow.next_poll:
            return {"status": "pending", "interval": flow.interval}
        flow.next_poll = float("inf")
    def retry_pending():
        with _device_lock:
            if _device_flows.get(flow_id) is not flow or flow.expires <= monotonic():
                raise HTTPException(404, "Sign-in expired. Start again.")
            flow.next_poll = monotonic() + flow.interval
            return {"status": "pending", "interval": flow.interval}

    target = flow.target
    try:
        with _http_client() as client:
            response = request(
                client,
                "POST",
                f"https://login.microsoftonline.com/{target.tenant_id}/oauth2/v2.0/token",
                "Microsoft device sign-in",
                data={
                    "client_id": str(target.client_id),
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": flow.code,
                },
            )
        if response.status_code == 429 or response.status_code >= 500:
            return retry_pending()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError
        with _device_lock:
            if _device_flows.get(flow_id) is not flow or flow.expires <= monotonic():
                raise ValueError
            if response.status_code == 400 and data.get("error") in {"authorization_pending", "slow_down"}:
                if data["error"] == "slow_down":
                    flow.interval += 5
                flow.next_poll = monotonic() + flow.interval
                return {"status": "pending", "interval": flow.interval}
            refresh_token = data.get("refresh_token")
            if not response.is_success or not isinstance(refresh_token, str) or not refresh_token:
                raise ValueError
            flow.refresh_token = refresh_token
            flow.excel_column_preview = target.excel_column_preview and has_excel_preview_scope(data.get("scope"))
            flow.code = ""
    except SharePointReadError:
        return retry_pending()
    except (ValueError, TypeError):
        with _device_lock:
            _discard_device_flow_locked(flow_id)
        raise HTTPException(422, "Microsoft sign-in did not complete. Start again.") from None
    return {"status": "ready"}


@router.delete("/device/{flow_id}", status_code=204)
def cancel_device_sign_in(flow_id: str, current_user=Depends(get_current_active_user)):
    if not sharing.browser_sign_in_allowed():
        raise HTTPException(404, "Not Found")
    with _device_lock:
        flow = _device_flows.get(flow_id)
        if flow is None or flow.owner != current_user.id:
            raise HTTPException(404, "Sign-in not found.")
        _discard_device_flow_locked(flow_id)


@router.post("/device/{flow_id}/save", response_model=SharePointConnectionMetadata, status_code=201)
def save_device_sign_in(flow_id: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)):
    if not sharing.browser_sign_in_allowed():
        raise HTTPException(404, "Not Found")
    with _device_lock:
        flow = _device_flows.get(flow_id)
        if flow is None or flow.owner != current_user.id or flow.expires <= monotonic() or not flow.refresh_token:
            raise HTTPException(404, "Sign-in not ready or expired. Start again.")
        _discard_device_flow_locked(flow_id)
    if flow.connection_id is None:
        return _save_connection(
            db, flow.owner, flow.target, "delegated", flow.refresh_token, excel_column_preview=flow.excel_column_preview
        )
    connection = _owned_delegated_connection(db, current_user.id, flow.connection_id)
    if (connection.tenant_id, connection.client_id) != (str(flow.target.tenant_id), str(flow.target.client_id)):
        raise HTTPException(409, "Connection settings changed. Start reconnect again.")
    secret = db.query(Secret).filter_by(id=connection.credential_secret_id, user_id=current_user.id).one_or_none()
    if secret is None:
        raise HTTPException(422, "SharePoint connection credentials are unavailable.")
    secret.encrypted_value = encrypt_secret(flow.refresh_token, current_user.id)
    connection.excel_column_preview = flow.excel_column_preview
    result = SharePointConnectionMetadata.model_validate(connection)
    db.commit()
    return result


class SharePointConnectionTestInput(BaseModel):
    connection_id: int = Field(gt=0)
    drive_id: str = Field(min_length=1, max_length=512, pattern=r"^[A-Za-z0-9_!.-]+$")


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=15)


def _resolve_connection(db: Session, user_id: int, connection_id: int) -> tuple[SharePointConnection, str]:
    connection = db.query(SharePointConnection).filter_by(id=connection_id, user_id=user_id).one_or_none()
    if connection is None and connection_id in sharing.granted_resource_ids(db, user_id, "sharepoint_connection"):
        connection = db.get(SharePointConnection, connection_id)
    if connection is None:
        raise HTTPException(404, "SharePoint connection not found.")
    if connection.auth_method == "delegated" and (
        connection.user_id != user_id or not sharing.browser_sign_in_allowed()
    ):
        raise HTTPException(404, "SharePoint connection not found.")
    secret = db.query(Secret).filter_by(id=connection.credential_secret_id, user_id=connection.user_id).one_or_none()
    if (
        connection.auth_method not in {"client_secret", "certificate", "delegated"}
        or secret is None
        or not secret.encrypted_value
    ):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.")
    return connection, secret.encrypted_value


def build_read_settings(
    db: Session, user_id: int, connection_id: int | None, drive_id: str | None, file_path: str | None,
    sheet_name: str | None = None, *, list_source: SharePointListSource | None = None, schema_only: bool = False,
) -> SharePointReadSettings:
    if connection_id is None:
        raise HTTPException(422, "Select a saved SharePoint connection before running this node.")
    connection, ciphertext = _resolve_connection(db, user_id, connection_id)
    return SharePointReadSettings(
        tenant_id=UUID(connection.tenant_id),
        client_id=UUID(connection.client_id),
        auth_method=connection.auth_method,
        client_secret_encrypted=ciphertext,
        drive_id=drive_id,
        file_path=file_path,
        list_source=list_source,
        sheet_name=sheet_name,
        schema_only=schema_only,
        excel_column_preview=schema_only and connection.auth_method == "delegated" and connection.excel_column_preview,
    )


def build_write_settings(db: Session, settings: NodeSharePointWriter) -> SharePointWriteSettings:
    if settings.connection_id is None or settings.user_id is None:
        raise HTTPException(422, "Select a saved SharePoint connection before running this node.")
    connection, ciphertext = _resolve_connection(db, settings.user_id, settings.connection_id)
    if connection.auth_method == "delegated":
        raise HTTPException(422, "SharePoint writing requires an app connection with write access.")
    return SharePointWriteSettings(
        tenant_id=UUID(connection.tenant_id), client_id=UUID(connection.client_id), auth_method=connection.auth_method,
        client_secret_encrypted=ciphertext, drive_id=settings.drive_id, file_path=settings.file_path,
        format=settings.format, conflict_mode=settings.conflict_mode, list_target=settings.list_target,
        sheet_name=settings.sheet_name, write_when_empty=settings.write_when_empty,
        flowfile_flow_id=settings.flow_id, flowfile_node_id=settings.node_id,
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
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None

    try:
        with _http_client() as client:
            token = acquire_token(
                client,
                connection.tenant_id,
                connection.client_id,
                credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
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


@router.get("/browse_defaults")
def sharepoint_browse_defaults(_user=Depends(get_current_active_user)) -> dict[str, str | None]:
    return {"site_url": getenv("FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL", "").strip() or None}


class SharePointSite(BaseModel):
    id: str
    displayName: str
    webUrl: str


@router.get("/{connection_id}/sites")
def search_sharepoint_sites(
    connection_id: int,
    search: str = Query(min_length=1, max_length=255),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointSite]:
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    try:
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None
    try:
        with _http_client() as client:
            token = acquire_token(
                client,
                connection.tenant_id,
                connection.client_id,
                credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
            return [
                SharePointSite.model_validate(site)
                for site in graph_listing(
                    client,
                    "/v1.0/sites",
                    "id,displayName,webUrl",
                    token,
                    "SharePoint site search",
                    search=search,
                )
            ]
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "SharePoint returned invalid site metadata.") from None


@router.get("/{connection_id}/libraries")
def list_sharepoint_libraries(
    connection_id: int,
    site_url: str = Query(max_length=2048),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointLibrary]:
    return _browse_site_resources(db, current_user.id, connection_id, site_url, "drives")[1]


@router.get("/{connection_id}/onedrive")
def discover_onedrive(
    connection_id: int,
    user_id: UUID | None = None,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointLibrary]:
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    delegated = connection.auth_method == "delegated"
    if delegated and user_id is not None:
        raise HTTPException(422, "A delegated connection can only discover its own OneDrive.")
    if not delegated and user_id is None:
        raise HTTPException(422, "Enter the organization's Microsoft Entra user object ID.")
    try:
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None
    path = "/v1.0/me/drives" if delegated else f"/v1.0/users/{user_id}/drives"
    label = "OneDrive for Business discovery"
    try:
        with _http_client() as client:
            token = acquire_token(
                client, connection.tenant_id, connection.client_id, credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
            drives = [SharePointLibrary.model_validate(item) for item in
                      graph_listing(client, path, "id,name,driveType", token, label)
                      if item.get("driveType") == "business"]
        if not drives:
            raise SharePointReadError("No OneDrive for Business drive is available for this user.")
        if any(not re.fullmatch(r"[A-Za-z0-9_!.-]{1,512}", drive.id) or drive.id in {".", ".."} for drive in drives):
            raise ValueError
        return drives
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "Microsoft Graph returned invalid OneDrive metadata.") from None


class SharePointLists(BaseModel):
    site_id: SharePointSiteId
    lists: list[SharePointLibrary]


@router.get("/{connection_id}/lists")
def list_sharepoint_lists(
    connection_id: int,
    site_url: str = Query(max_length=2048),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> SharePointLists:
    site_id, lists = _browse_site_resources(db, current_user.id, connection_id, site_url, "lists")
    return SharePointLists(site_id=site_id, lists=lists)


def _browse_site_resources(db, user_id, connection_id, site_url, resource):
    connection, ciphertext = _resolve_connection(db, user_id, connection_id)
    host, segments = _parse_site_url(site_url)
    try:
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None

    try:
        with _http_client() as client:
            token = acquire_token(
                client,
                connection.tenant_id,
                connection.client_id,
                credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
            relative = "/".join(quote(segment, safe="") for segment in segments)
            lookup = f"https://graph.microsoft.com/v1.0/sites/{host}:/{relative}?$select=id"
            response = request(
                client, "GET", lookup, "SharePoint site lookup", headers={"Authorization": f"Bearer {token}"}
            )
            check_status(response, "SharePoint site lookup")
            site_id = response.json()["id"]
            if not isinstance(site_id, str) or not re.fullmatch(r"[A-Za-z0-9.-]+,[A-Za-z0-9-]+,[A-Za-z0-9-]+", site_id):
                raise ValueError
            path = f"/v1.0/sites/{site_id}/{resource}"
            return site_id, [
                SharePointLibrary.model_validate(item)
                for item in graph_listing(client, path, "id,name", token, "SharePoint site resource listing")
            ]
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "SharePoint returned invalid site resource metadata.") from None


@router.get("/{connection_id}/lists/{site_id}/{list_id}/columns")
def list_sharepoint_columns(
    connection_id: int,
    site_id: SharePointSiteId,
    list_id: SharePointListId,
    writable: bool = False,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[SharePointLibrary]:
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    try:
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None
    try:
        with _http_client() as client:
            token = acquire_token(
                client, connection.tenant_id, connection.client_id, credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
            return [
                SharePointLibrary(id=item["name"], name=item.get("displayName") or item["name"])
                for item in graph_listing(
                    client, f"/v1.0/sites/{site_id}/lists/{list_id}/columns",
                    None if writable else "name,displayName,hidden", token,
                    "SharePoint list column listing",
                )
                if not item.get("hidden") and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", item["name"])
                and (not writable or (not item.get("readOnly") and
                     {"text", "choice", "boolean", "number", "currency", "dateTime"}.intersection(item)))
            ]
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "SharePoint returned invalid column metadata.") from None


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
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None

    label = "SharePoint folder listing"
    folder = f"items/{quote(folder_id, safe='')}" if folder_id else "root"
    path = f"/v1.0/drives/{quote(drive_id, safe='')}/{folder}/children"
    try:
        with _http_client() as client:
            token = acquire_token(
                client,
                connection.tenant_id,
                connection.client_id,
                credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
            return [
                SharePointDriveEntry(id=item["id"], name=item["name"], is_folder="folder" in item)
                for item in graph_listing(client, path, "id,name,folder,file", token, label)
                if "folder" in item or "file" in item
            ]
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, httpx.InvalidURL):
        raise HTTPException(422, "SharePoint returned an invalid folder listing.") from None


class SharePointFileMetadata(BaseModel):
    file_name: str
    file_path: str


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
        or is_file_glob(file_path)
        or any(part in {"", ".", ".."} for part in parts)
        or not parts[-1].lower().endswith((".xls", ".xlsx"))
    ):
        raise HTTPException(422, "A valid SharePoint drive and Excel file path are required.")
    connection, ciphertext = _resolve_connection(db, current_user.id, connection_id)
    try:
        credential = decrypt_secret(ciphertext, connection.user_id)
    except (InvalidToken, ValueError):
        raise HTTPException(422, "SharePoint connection credentials are unavailable.") from None
    try:
        with _http_client() as client:
            token = acquire_token(
                client,
                connection.tenant_id,
                connection.client_id,
                credential.get_secret_value(),
                auth_method=connection.auth_method,
            )
            encoded_path = "/".join(quote(part, safe="") for part in parts)
            url = f"https://graph.microsoft.com/v1.0/drives/{quote(drive_id, safe='')}/root:/{encoded_path}"

            label = "SharePoint file metadata"
            response = request(
                client, "GET", url, label, headers={"Authorization": f"Bearer {token}"}, params={"$select": "name"}
            )
            check_status(response, label)
            try:
                name = response.json()["name"]
                if not isinstance(name, str) or not name:
                    raise ValueError
            except (ValueError, TypeError, KeyError):
                raise SharePointReadError("SharePoint returned invalid file metadata.") from None
            return SharePointFileMetadata(file_name=name, file_path="/".join([*parts[:-1], name]))
    except SharePointReadError as exc:
        raise HTTPException(422, str(exc)) from None
