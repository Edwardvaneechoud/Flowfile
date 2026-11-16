"""Connect this Flowfile to a hub, publish a flow to it and open one from it.

No hub state ever answers 401, which the frontend reads as an expired local session: refused credentials answer 400,
an unreachable hub 502, an unsupported version 426. The password is sent once, on connect, and never stored, logged
or answered. A hub that offers it is also connected through the system browser (``/connection/browser``), where the
person signs in on the hub's own page and this side never sees a password. Connect and disconnect write the backends
table every user shares, so they need an admin.
"""

import os
import re
import socket
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user
from flowfile_core.auth.sharing import browser_sign_in_allowed, sharing_enabled
from flowfile_core.catalog import CatalogService, table_sources
from flowfile_core.catalog.exceptions import FlowExistsError, NamespaceNotFoundError, NotAuthorizedError
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db, get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.catalog_helpers import (
    FlowNameNamespaceCollision,
    FlowPathNamespaceCollision,
    auto_register_flow,
    find_registration_by_name,
    register_flow_in_namespace,
    require_flow_save_permitted,
    resolve_display_name,
    resolve_source_registration_id,
    sync_api_compatibility,
)
from flowfile_core.flowfile.manage.io_flowfile import is_working_copy, working_copy_directory
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.hub_client import browser_sign_in, client, node_addresses, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.routes import compute_backends
from flowfile_core.routes.routes import flow_file_handler, get_flow_or_404
from flowfile_core.schemas import schemas
from shared import server_extension
from shared._version import get_version
from shared.storage_config import registered_flow_path, storage

router = APIRouter()

_LOCAL_PATH_NODE_TYPES = ("read", "output", "list_files")
_FILENAME_STEM_RE = re.compile(r"[^A-Za-z0-9_-]+")


class HubConnection(BaseModel):
    connected: bool
    hub_url: str | None = None
    device_name: str | None = None
    expires_at: datetime | None = None
    org_name: str | None = None
    hub_version: str | None = None
    reachable: bool = False
    error: str | None = None
    account: str | None = None
    joined: bool = False
    server_name: str | None = None
    organisation_storage: str | None = None
    browser_sign_in: bool = False


class _HubAddress(BaseModel):
    hub_url: str

    @field_validator("hub_url")
    @classmethod
    def _http_url(cls, value: str) -> str:
        url = value.strip().rstrip("/")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("The hub URL must start with http:// or https://")
        return url


class HubConnectRequest(_HubAddress):
    username: str
    password: str
    device_name: str | None = None


class HubBrowserConnectRequest(_HubAddress):
    device_name: str | None = None


class HubBrowserSignIn(BaseModel):
    flow_id: str
    authorize_url: str


class HubBrowserSignInStatus(BaseModel):
    """``connection`` is what connect answers, once ``status`` is ``connected``."""

    status: str
    detail: str | None = None
    connection: HubConnection | None = None


def _account(stored: dict | None, current_user) -> str | None:
    if stored is None:
        return None
    if stored.get("kind") == "delegation":
        return current_user.full_name or current_user.username
    if stored.get("kind") == "browser":
        return stored.get("full_name") or session.account(stored)
    return session.account(stored)


def _connection(
    stored: dict | None, info: dict | None = None, error: str | None = None, account: str | None = None
) -> HubConnection:
    expires = stored.get("refresh_expires_at") if stored else None
    return HubConnection(
        connected=stored is not None,
        hub_url=stored["hub_url"] if stored else None,
        device_name=stored.get("device_name") if stored else socket.gethostname(),
        expires_at=datetime.fromtimestamp(expires, timezone.utc) if expires is not None else None,
        org_name=info["org_name"] if info else None,
        hub_version=info["hub_version"] if info else None,
        reachable=info is not None,
        error=error,
        account=account,
        browser_sign_in=browser_sign_in_allowed(),
    )


@router.get("/connection", summary="This user's hub connection")
def get_connection(current_user=Depends(get_current_active_user)) -> HubConnection:
    """The stored connection with the hub's info read live; an unreachable hub sets ``error``."""
    stored = session.load(current_user.id)
    node = server_extension.joined()
    if stored is None and node is None:
        return _connection(None)
    hub_url = stored["hub_url"] if stored else node.url.rstrip("/")
    account = _account(stored, current_user)
    info = None
    try:
        info = client.get_info(hub_url)
        connection = _connection(stored, info, account=account)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        connection = _connection(stored, error=str(e), account=account)
    if node is not None:
        connection.hub_url, connection.joined, connection.server_name = hub_url, True, node.name
        connection.organisation_storage = info.get("organisation_storage") if info else None
    return connection


def _device_name(requested: str | None) -> str:
    return (requested or "").strip() or socket.gethostname()


def _finish_connect(
    db: Session, user_id: int, hub_url: str, info: dict, access_token: str, device_name: str, keep, current_user=None
) -> tuple[HubConnection, list[str] | None]:
    """What both ways of connecting do once signed in: register this machine as a device, keep the session
    (``keep(device name)``), then sync the hub's compute nodes, best effort. A hub error before ``keep`` propagates
    with nothing stored. Answers the connection and the synced backend ids, ``None`` when the nodes were not read."""
    device = client.register_device(hub_url, access_token, device_name, get_version())
    keep(device["name"])
    stored = session.load(user_id)
    connection = _connection(stored, info, account=_account(stored, current_user))
    try:
        synced = node_addresses.sync_nodes(db, user_id, hub_url, access_token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        logger.warning("Connected to the hub at %s, but its compute nodes could not be read: %s", hub_url, e)
        return connection, None
    return connection, synced


@router.post("/connection", summary="Connect to a hub")
def connect(
    request: HubConnectRequest,
    background: BackgroundTasks,
    current_user=Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> HubConnection:
    """Sign in, register this machine as a device and store the session, nothing stored unless every step succeeded;
    then sync the hub's compute nodes, best effort."""

    def keep(device_name: str) -> None:
        session.save(
            current_user.id,
            {
                "hub_url": request.hub_url,
                "refresh_token": tokens["refresh_token"],
                "device_name": device_name,
                "org_name": info["org_name"],
            },
        )

    try:
        info = client.get_info(request.hub_url)
        tokens = client.sign_in(request.hub_url, request.username, request.password)
        connection, synced = _finish_connect(
            db,
            current_user.id,
            request.hub_url,
            info,
            tokens["access_token"],
            _device_name(request.device_name),
            keep,
            current_user,
        )
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e, connecting=True) from e
    if synced is not None:
        background.add_task(compute_backends.check_unchecked, synced)
    return connection


_PASSWORD_ONLY = "password_only"


def _password_only(message: str) -> HTTPException:
    """A browser sign-in this machine or this hub cannot do; the frontend offers the password form instead."""
    return HTTPException(status_code=400, detail={"kind": _PASSWORD_ONLY, "message": message})


@router.post("/connection/browser", summary="Connect to a hub by signing in through the system browser")
def connect_in_browser(
    request: HubBrowserConnectRequest, current_user=Depends(get_current_admin_user)
) -> HubBrowserSignIn:
    """Start a sign-in through the browser: answers the hub's page to open and ``flow_id``, the sign-in's id (no
    flow's) to poll ``GET /connection/browser/{sign_in_id}`` with. Refused on a multi-user server, whose loopback is
    not the person's, and for a hub that does not offer it (``detail.kind`` ``password_only``)."""
    if not browser_sign_in_allowed():
        raise _password_only(
            "Signing in through the browser works only where Flowfile runs on your own computer; "
            "use a username and password here."
        )
    try:
        info = client.get_info(request.hub_url)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e, connecting=True) from e
    if info.get("browser_sign_in") is not True:
        raise _password_only(
            f"The hub at {request.hub_url} does not offer signing in through the browser; "
            "use a username and password instead."
        )
    user_id, hub_url, device_name = current_user.id, request.hub_url, _device_name(request.device_name)

    def complete(tokens: dict) -> dict:
        def keep(registered: str) -> None:
            session.save_browser(user_id, hub_url, tokens, device_name=registered, org_name=info["org_name"])

        try:
            with get_db_context() as db:
                connection, synced = _finish_connect(
                    db, user_id, hub_url, info, tokens["access_token"], device_name, keep
                )
        except (HubUnreachable, HubRefused, HubVersionRefused):
            # Nothing was kept, so the sign-in the hub just granted is ended there too.
            _revoke_quietly(hub_url, tokens["refresh_token"])
            raise
        if synced:
            threading.Thread(target=compute_backends.check_unchecked, args=(synced,), daemon=True).start()
        return connection.model_dump(mode="json")

    try:
        flow_id, url = browser_sign_in.start(
            user_id=user_id, hub_url=hub_url, device_name=device_name, complete=complete
        )
    except browser_sign_in.SignInUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from None
    return HubBrowserSignIn(flow_id=flow_id, authorize_url=url)


@router.get("/connection/browser/{sign_in_id}", summary="A sign-in through the browser, as it stands")
def browser_sign_in_status(sign_in_id: str, current_user=Depends(get_current_admin_user)) -> HubBrowserSignInStatus:
    """``waiting`` until the browser came back, then ``connected`` with the connection or ``failed`` with why;
    ``sign_in_id`` is the ``flow_id`` the start answered."""
    found = browser_sign_in.status(sign_in_id, current_user.id)
    if found is None:
        raise HTTPException(status_code=404, detail="No such sign-in; start again.")
    return HubBrowserSignInStatus(**found)


@router.delete("/connection/browser/{sign_in_id}", summary="Stop waiting for a sign-in through the browser")
def cancel_browser_sign_in(sign_in_id: str, current_user=Depends(get_current_admin_user)) -> dict:
    if not browser_sign_in.cancel(sign_in_id, current_user.id):
        raise HTTPException(status_code=404, detail="No such sign-in.")
    return {"cancelled": True}


def _revoke_quietly(hub_url: str, refresh_token: str) -> None:
    try:
        client.revoke(hub_url, refresh_token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        logger.warning("The hub at %s could not be told to end the sign-in: %s", hub_url, e)


@router.delete("/connection", summary="Disconnect from the hub")
def disconnect(current_user=Depends(get_current_admin_user), db: Session = Depends(get_db)) -> dict:
    """Forget the session, its node tokens and the backends rows it filled. A sign-in through the browser is also
    ended on the hub, best effort; a password connection's hub is not told."""
    stored = session.load(current_user.id)
    rows = db.query(ConfiguredBackend).filter(ConfiguredBackend.account_user_id == current_user.id).all()
    for row in rows:
        node_addresses.drop_row(db, row)
    db.commit()
    session.clear(current_user.id)
    if stored is not None and stored.get("kind") == "browser":
        _revoke_quietly(stored["hub_url"], stored["refresh_token"])
    return {"connected": False}


class LocalPathUse(BaseModel):
    label: str
    path: str


class HubPublishRequest(BaseModel):
    """``as_new`` publishes a flow already on the hub as a new flow under ``name``; ignored for one that is not."""

    flow_id: int
    namespace_id: int | None = None
    message: str | None = None
    overwrite_version: int | None = None
    as_new: bool = False
    name: str | None = None


class HubPublishOut(BaseModel):
    """``flow_id`` is answered only when the open flow moved to a new id (a publish as a new flow)."""

    version: int
    name: str
    flow_id: int | None = None


class HubOpenRequest(BaseModel):
    flow_uuid: str
    version: int | None = None
    replace: bool = False

    @field_validator("flow_uuid")
    @classmethod
    def _uuid(cls, value: str) -> str:
        try:
            return str(uuid.UUID(value))
        except ValueError:
            raise ValueError("flow_uuid is not a valid UUID") from None


class HubOpenOut(BaseModel):
    flow_path: str
    note: str | None = None


def _hub(current_user) -> tuple[str, str]:
    try:
        return session.bearer(current_user.id, "Connect to a hub first (Compute → Hub).", ended=())
    except LookupError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e


def _hub_error(e: Exception, *, connecting: bool = False) -> HTTPException:
    """A hub 4xx keeps its status under ``refused`` (401 as 400); on connect it is a plain 400."""
    if isinstance(e, HubVersionRefused):
        return HTTPException(status_code=426, detail=e.detail)
    if isinstance(e, HubUnreachable):
        return HTTPException(status_code=502, detail=str(e))
    if isinstance(e, HubRefused) and 400 <= e.status < 500:
        if connecting:
            return HTTPException(status_code=400, detail=f"The hub refused the request: {e.detail}")
        status = 400 if e.status == 401 else e.status
        return HTTPException(status_code=status, detail={"kind": "refused", "status": e.status, "detail": e.detail})
    return HTTPException(status_code=502, detail=f"The hub failed to answer: {e}")


def _local_path(node) -> str | None:
    settings = node.setting_input
    if settings is None:
        return None
    if node.node_type == "read":
        received = settings.received_file
        return received.abs_file_path or received.path
    if node.node_type == "output":
        output = settings.output_settings
        if output.abs_file_path:
            return output.abs_file_path
        return os.path.join(output.directory, output.name) if output.directory else None
    if node.node_type == "list_files":
        return settings.path
    return None


def _label(node) -> str:
    settings = node.setting_input
    typed = (getattr(settings, "description", None) or "").strip()
    if typed:
        return typed
    try:
        return settings.get_default_description() or node.node_type
    except Exception:
        return node.node_type


@router.get("/flows/{flow_uuid}/versions", summary="A hub flow's versions")
def hub_flow_versions(flow_uuid: str, current_user=Depends(get_current_active_user)) -> list[dict]:
    hub_url, token = _hub(current_user)
    try:
        return client.list_versions(hub_url, token, flow_uuid)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e


@router.get("/publish_check", summary="Local paths a flow would carry to the hub")
def publish_check(flow_id: int, current_user=Depends(get_current_active_user)) -> list[LocalPathUse]:
    """Nodes whose settings name a local path: a warning for the publisher, never a refusal."""
    flow = get_flow_or_404(flow_id, current_user)
    uses = []
    for node in flow.nodes:
        if node.node_type not in _LOCAL_PATH_NODE_TYPES:
            continue
        path = _local_path(node)
        if path:
            uses.append(LocalPathUse(label=_label(node), path=path))
    return uses


def _registration_identity(flow) -> tuple[str | None, str | None]:
    resolve_source_registration_id(flow)
    reg_id = getattr(flow.flow_settings, "source_registration_id", None)
    if reg_id is None:
        return None, None
    with get_db_context() as db:
        reg = SQLAlchemyCatalogRepository(db).get_flow(reg_id)
        if reg is None:
            return None, None
        return reg.flow_uuid, reg.name


def _version(versions: list, number: int | None) -> dict | None:
    if number is None:
        return None
    return next((v for v in versions if v.get("version") == number), None)


@router.post("/publish", status_code=201, summary="Publish a flow to the hub", response_model_exclude_none=True)
def publish(request: HubPublishRequest, current_user=Depends(get_current_active_user)) -> HubPublishOut:
    """Store the open flow as its hub flow's next version and stamp ``hub_origin``, so the next publish names its base.

    A stale base is 409 ``stale_base``. The flow is looked up before the hub session, so a flow the caller cannot see
    is 404 whether or not a hub is connected. With ``as_new`` a flow already on the hub is published as a new flow
    instead (``_publish_as_new``); a flow on no hub publishes as a first publish, ``as_new`` and ``name`` unread.
    """
    flow = get_flow_or_404(request.flow_id, current_user)
    if request.as_new and request.overwrite_version is not None:
        raise HTTPException(status_code=422, detail="A new flow has no version to publish over.")
    hub_url, token = _hub(current_user)
    flow_path = flow.flow_settings.path or flow.flow_settings.save_location
    if not flow_path:
        raise HTTPException(status_code=422, detail="save the flow first")
    origin = getattr(flow.flow_settings, "hub_origin", None)
    if origin is not None and request.as_new:
        return _publish_as_new(flow, flow_path, request, hub_url, token, current_user)
    if origin is not None and origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
        raise HTTPException(status_code=409, detail={"kind": "other_hub", "hub_url": origin.hub_url})

    reg_uuid, reg_name = _registration_identity(flow)
    flow_uuid = origin.flow_uuid if origin is not None else (reg_uuid or str(uuid.uuid4()))
    if request.overwrite_version is not None:
        base_version = request.overwrite_version
    else:
        base_version = origin.version if origin is not None else None

    document = flow.get_flowfile_data()
    document.flowfile_settings.source_registration_id = None
    document.flowfile_settings.hub_origin = None
    body = {
        "definition": document.model_dump(mode="json"),
        "base_version": base_version,
        "message": request.message,
        "name": reg_name or flow.flow_settings.name or document.flowfile_name,
        "namespace_id": request.namespace_id,
    }
    try:
        published = client.publish(hub_url, token, flow_uuid, body)
    except HubRefused as e:
        if e.status == 409 and isinstance(e.payload, dict) and "current_version" in e.payload:
            try:
                versions = client.list_versions(hub_url, token, flow_uuid)
            except (HubUnreachable, HubRefused, HubVersionRefused):
                versions = []
            raise HTTPException(
                status_code=409,
                detail={
                    "kind": "stale_base",
                    "flow_uuid": flow_uuid,
                    "base": _version(versions, e.payload.get("base_version")),
                    "current": _version(versions, e.payload.get("current_version")),
                },
            ) from e
        raise _hub_error(e) from e
    except (HubUnreachable, HubVersionRefused) as e:
        raise _hub_error(e) from e

    flow.flow_settings.hub_origin = schemas.HubOrigin(
        hub_url=hub_url, flow_uuid=published["flow_uuid"], version=published["version"]
    )
    flow.save_flow(flow_path=flow_path)
    flow.save_note = None
    return HubPublishOut(version=published["version"], name=published["name"])


def _publish_as_new(
    flow, flow_path: str, request: HubPublishRequest, hub_url: str, token: str, current_user
) -> HubPublishOut:
    """Publish what is open as a new hub flow at version 1, then make the open flow that new flow.

    Nothing of the flow it came from is sent or changed: a new uuid, no base, the given name (else the flow's catalog
    name, else its own) and the place picked (none lets the hub use its default schema). Where the open flow will live
    is checked before the hub is asked, so a refusal from either side leaves the open flow, its file and its origin as
    they were.
    """
    document = flow.get_flowfile_data()
    name = (
        (request.name or "").strip()
        or resolve_display_name(flow_path)
        or flow.flow_settings.name
        or document.flowfile_name
    )
    flow_uuid = str(uuid.uuid4())
    target, namespace_id = _new_flow_home(flow_uuid, name, flow_path, current_user)

    document.flowfile_name = name
    document.flowfile_settings.source_registration_id = None
    document.flowfile_settings.hub_origin = None
    body = {
        "definition": document.model_dump(mode="json"),
        "base_version": None,
        "message": request.message,
        "name": name,
        "namespace_id": request.namespace_id,
    }
    try:
        published = client.publish(hub_url, token, flow_uuid, body)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e

    new_flow_id = _become_new_flow(flow, target, namespace_id, published, hub_url, current_user)
    return HubPublishOut(version=published["version"], name=published["name"], flow_id=new_flow_id)


def _new_flow_home(flow_uuid: str, name: str, flow_path: str, current_user) -> tuple[str, int | None]:
    """The file the open flow moves to once it is the new hub flow, and the namespace it is registered in here.

    It is the file ``open_from_hub`` would write for the new flow: a working copy where the open flow is one, else the
    caller's local copy path, registered under the new uuid where this install's catalog takes people's work. So a
    later open of the new flow finds this file, and a later open of the original finds the original's own.
    """
    registers = table_sources.local_catalog_offered()
    if not registers and is_working_copy(flow_path):
        target, namespace_id = _working_copy_path(flow_uuid, current_user), None
    else:
        target, _ = _local_copy_path(flow_uuid, name, current_user)
        namespace_id = _local_flows_namespace_id() if registers else None
    try:
        require_flow_save_permitted(target, namespace_id, current_user)
    except NotAuthorizedError as err:
        raise HTTPException(status_code=403, detail=str(err)) from None
    try:
        target = registered_flow_path(target)
    except ValueError as err:
        raise HTTPException(status_code=422, detail=str(err)) from None
    if namespace_id is not None and find_registration_by_name(name, namespace_id) is not None:
        raise HTTPException(
            status_code=409, detail=f"This install already has a local flow named '{name}'. Pick another name."
        )
    return target, namespace_id


def _become_new_flow(flow, target: str, namespace_id: int | None, published: dict, hub_url: str, current_user) -> int:
    """Move the open flow to ``target`` as a Save As does (new flow id, ``flow_rekeyed`` on the change feed), stamped
    with the new hub flow's origin and carrying its name; the original's file and registration stay as they were. A
    move that fails leaves the open flow on its own origin, so its next publish is still the original's next version."""
    name = published["name"]
    flow_uuid = published["flow_uuid"]
    previous_origin = flow.flow_settings.hub_origin
    flow.flow_settings.hub_origin = schemas.HubOrigin(
        hub_url=hub_url, flow_uuid=flow_uuid, version=published["version"]
    )

    def _register(path: str, _name: str, user_id: int | None) -> None:
        register_flow_in_namespace(path, name, user_id, namespace_id, requesting_user=current_user, flow_uuid=flow_uuid)

    def _adopt_name(graph) -> None:
        resolve_source_registration_id(graph)
        # A save to a new path names the flow after the file; the new flow keeps the name it has on the hub.
        graph.__name__ = name
        graph.flow_settings.name = name

    try:
        new_flow_id = flow_file_handler.save_as_flow(
            flow_id=flow.flow_id,
            new_path=target,
            user_id=current_user.id,
            on_catalog_register=_register if namespace_id is not None else None,
            on_resolve_registration=_adopt_name,
        )
    except (FlowPathNamespaceCollision, FlowNameNamespaceCollision, FlowExistsError) as err:
        flow.flow_settings.hub_origin = previous_origin
        raise HTTPException(status_code=409, detail=str(err)) from err
    except NotAuthorizedError as err:
        flow.flow_settings.hub_origin = previous_origin
        raise HTTPException(status_code=403, detail=str(err)) from None
    except NamespaceNotFoundError:
        flow.flow_settings.hub_origin = previous_origin
        raise HTTPException(status_code=404, detail="Namespace not found") from None
    except Exception:
        flow.flow_settings.hub_origin = previous_origin
        raise
    flow.save_note = None
    sync_api_compatibility(flow)
    return new_flow_id


def _local_copy_path(flow_uuid: str, name: str, current_user) -> tuple[str, str]:
    """The caller's copy path, and who holds the hub's uuid here: ``caller``, ``nobody`` or ``another``, whose file
    the caller never opens or writes."""
    with get_db_context() as db:
        reg = SQLAlchemyCatalogRepository(db).get_flow_by_uuid(flow_uuid)
        holder = "nobody" if reg is None else "another"
        if reg is not None and (reg.owner_id == current_user.id or not sharing_enabled()):
            return reg.flow_path, "caller"
    folder = storage.flows_directory / "hub"
    if sharing_enabled():
        folder = folder / str(current_user.id)
    stem = _FILENAME_STEM_RE.sub("_", name).strip("_-") or "flow"
    return str(folder / f"{stem}_{flow_uuid[:8]}.yaml"), holder


def _working_copy_path(flow_uuid: str, current_user) -> str:
    return str(working_copy_directory() / str(current_user.id) / f"{flow_uuid}.yaml")


def _drop_earlier_copy(flow_uuid: str, name: str, current_user, hub_url: str, token: str, checked_out: dict) -> None:
    """Remove an earlier open's file under ``flows/hub`` unless it holds unpublished edits or is registered."""
    earlier, holder = _local_copy_path(flow_uuid, name, current_user)
    if holder == "caller" or not os.path.exists(earlier):
        return
    local, _ = _read_local_copy(earlier)
    if not _has_unpublished_edits(local, hub_url, token, checked_out):
        Path(earlier).unlink(missing_ok=True)


def _local_flows_namespace_id() -> int | None:
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        namespace = service.ensure_local_flows_namespace()
        return namespace.id if namespace is not None else service.get_default_namespace_id()


def _read_local_copy(flow_path: str) -> tuple[schemas.FlowfileData | None, int | None]:
    try:
        with open(flow_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return None, None
    flow_id = raw.get("flowfile_id") if isinstance(raw, dict) else None
    try:
        return schemas.FlowfileData.model_validate(raw), flow_id
    except ValueError:
        return None, flow_id


def _content(document: schemas.FlowfileData) -> dict:
    content = document.model_dump(mode="json", exclude={"flowfile_id", "flowfile_name", "flowfile_version"})
    content["flowfile_settings"].pop("source_registration_id", None)
    content["flowfile_settings"].pop("hub_origin", None)
    return content


def _has_unpublished_edits(local: schemas.FlowfileData | None, hub_url: str, token: str, checked_out: dict) -> bool:
    """Whether writing over the local copy would lose work; anything that cannot be compared counts as edited, so
    nothing is written over unseen."""
    origin = local.flowfile_settings.hub_origin if local is not None else None
    if origin is None or origin.flow_uuid != checked_out["flow_uuid"]:
        return True
    if origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
        return True
    try:
        if _content(local) == _content(schemas.FlowfileData.model_validate(checked_out["definition"])):
            return False
    except ValueError:
        return True
    base = checked_out
    if origin.version != checked_out["version"] or checked_out.get("note"):
        try:
            base = client.checkout(hub_url, token, origin.flow_uuid, origin.version)
        except (HubUnreachable, HubRefused, HubVersionRefused):
            return True
    try:
        return _content(local) != _content(schemas.FlowfileData.model_validate(base["definition"]))
    except ValueError:
        return True


@router.post("/open", summary="Open a flow from the hub", response_model_exclude_none=True)
def open_from_hub(request: HubOpenRequest, current_user=Depends(get_current_active_user)) -> HubOpenOut:
    """Write the hub's flow as the caller's local copy, carrying ``hub_origin``.

    Where this install offers no catalog of its own (a joined server) the copy is a working copy in the cache, written
    over at every open. A copy with unpublished edits is 409 ``local_copy_exists`` unless ``replace`` is set.
    """
    hub_url, token = _hub(current_user)
    try:
        checked_out = client.for_editing(hub_url, token, request.flow_uuid, request.version)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e

    registers = table_sources.local_catalog_offered()
    # A hub that answers ``draft`` keeps what the person saves; one that does not leaves the file as its only home.
    cached = not registers and "draft" in checked_out
    if not cached:
        flow_path, holder = _local_copy_path(checked_out["flow_uuid"], checked_out["name"], current_user)
    else:
        flow_path, holder = _working_copy_path(checked_out["flow_uuid"], current_user), "nobody"
        _drop_earlier_copy(checked_out["flow_uuid"], checked_out["name"], current_user, hub_url, token, checked_out)
    namespace_id = _local_flows_namespace_id() if holder == "nobody" and registers else None
    try:
        require_flow_save_permitted(flow_path, namespace_id, current_user)
    except NotAuthorizedError as err:
        raise HTTPException(status_code=403, detail=str(err)) from None
    try:
        flow_path = registered_flow_path(flow_path)
    except ValueError as err:
        raise HTTPException(status_code=422, detail=str(err)) from None
    existing_id = None
    if os.path.exists(flow_path):
        local, existing_id = _read_local_copy(flow_path)
        if not cached and not request.replace and _has_unpublished_edits(local, hub_url, token, checked_out):
            raise HTTPException(status_code=409, detail={"kind": "local_copy_exists", "flow_path": flow_path})

    document = schemas.FlowfileData.model_validate(checked_out["definition"])
    # The open editor keeps a reloaded file's id; a new file gets its own, never the publisher's.
    document.flowfile_id = existing_id if isinstance(existing_id, int) else create_unique_id()
    document.flowfile_settings.source_registration_id = None
    document.flowfile_settings.hub_origin = schemas.HubOrigin(
        hub_url=hub_url, flow_uuid=checked_out["flow_uuid"], version=checked_out["version"]
    )
    if cached:
        document.flowfile_name = checked_out["name"]

    if holder == "nobody" and registers:
        try:
            register_flow_in_namespace(
                flow_path,
                checked_out["name"],
                current_user.id,
                namespace_id,
                requesting_user=current_user,
                flow_uuid=checked_out["flow_uuid"],
            )
        except (FlowPathNamespaceCollision, FlowNameNamespaceCollision, FlowExistsError) as err:
            raise HTTPException(status_code=409, detail=str(err)) from err
        except NotAuthorizedError as err:
            raise HTTPException(status_code=403, detail=str(err)) from None
        except NamespaceNotFoundError:
            raise HTTPException(status_code=404, detail="Namespace not found") from None

    Path(flow_path).parent.mkdir(parents=True, exist_ok=True)
    with open(flow_path, "w", encoding="utf-8") as f:
        yaml.dump(document.model_dump(mode="json"), f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    if holder == "another" and registers:
        auto_register_flow(flow_path, checked_out["name"], current_user.id)
    flow_file_handler.note_at_open(flow_path, checked_out.get("note"))
    return HubOpenOut(flow_path=flow_path, note=checked_out.get("note"))
