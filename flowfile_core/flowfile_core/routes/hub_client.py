"""Connect this Flowfile to a hub, see the connection, and disconnect; publish a flow to it and open one from it.

Every route is behind the local sign-in and works on the caller's own connection. Connecting, disconnecting and the
node sync write the backends table every user of this core shares, so they need an administrator, as the backends
routes do (on the desktop, its one user is one). No hub state ever answers 401,
which the frontend reads as an expired local session: refused credentials answer 400, a hub that cannot be reached
502, a hub that does not support this version 426 with the hub's own text, and a URL that is not ``http(s)://`` 422.
The password is sent to the hub once, on connect, and is never stored, logged or answered.

Publish and open keep a flow's hub identity in the document itself (``flowfile_settings.hub_origin``: the hub, the
flow's uuid there and the version last published or checked out), so the next publish names the version it edited
from and the hub can tell a stale copy apart. Refusals of these routes carry a ``kind`` in their detail
(``stale_base``, ``local_copy_exists``, ``refused``, ``other_hub``) so the frontend branches on a word.

The hub's compute nodes fill core's ``compute_backends`` table (``/compute_nodes/sync``, also run by a connect): one
``worker:<name>`` row per enabled node, holding no token, so placement and the per-node picker see them as any other
worker while the registry asks the hub for a short-lived token when one is used. A row an administrator added by
hand under the same id is never touched. Disconnecting removes the caller's hub-filled rows. A server joined with
keys of its own also syncs by itself, with each person's sign-in through the hub (``node_addresses``); this route
stays there as an explicit refresh. A core that is itself a node on the hub, named by ``FLOWFILE_NODE_NAME`` or by the
hub it joined (``hub_node.json``), never writes a row for its own node: its worker is its built-in one.

Each row a sync wrote is checked once as the Handshake button checks any worker (``handshake.verdict_for``): a node
this machine cannot reach, or one that refuses it, gets the verdict the backends listing already carries for every
worker, with the same reason, and the same hiding of the address from a non-administrator. ``/compute_nodes/sync``
checks before it answers, so its answer carries the verdicts; a connect checks after its answer is sent. A row whose
verdict passed is not checked again until the hub moves its node.
"""

import os
import re
import socket
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from flowfile_core.auth import issuer_accounts
from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user
from flowfile_core.auth.sharing import sharing_enabled
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.exceptions import FlowExistsError, NamespaceNotFoundError, NotAuthorizedError
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db, get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.catalog_helpers import (
    FlowNameNamespaceCollision,
    FlowPathNamespaceCollision,
    auto_register_flow,
    register_flow_in_namespace,
    require_flow_save_permitted,
    resolve_source_registration_id,
)
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.hub_client import client, node_addresses, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.routes.compute_backends import ComputeBackendOut, listed_backends
from flowfile_core.routes.routes import get_flow_or_404
from flowfile_core.schemas import schemas
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
    """Who is signed in at the hub through this connection: on a server that signs people in through its hub, the
    person's full name, else their username (their account here mirrors the hub's); on a desktop, the hub username it
    connected with. ``None`` when not connected or not known."""
    joined: bool = False
    """Whether this server joined its hub with keys of its own (``issuer_accounts.joined_with_keys``): it belongs to
    that hub, whose URL ``hub_url`` names even for a person with no hub session here."""
    organisation_storage: str | None = None
    """On a joined server, the organisation's storage root as the hub's ``/hub/info`` gives it; ``None`` elsewhere,
    when the hub has none, or from a hub that does not say."""


class HubConnectRequest(BaseModel):
    hub_url: str
    username: str
    password: str
    device_name: str | None = None

    @field_validator("hub_url")
    @classmethod
    def _http_url(cls, value: str) -> str:
        url = value.strip().rstrip("/")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("The hub URL must start with http:// or https://")
        return url


def _account(stored: dict | None, current_user) -> str | None:
    """Who ``stored`` signs in as at the hub; see ``HubConnection.account``."""
    if stored is None:
        return None
    if stored.get("kind") == "delegation":
        return current_user.full_name or current_user.username
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
    )


def _http_error(e: Exception) -> HTTPException:
    if isinstance(e, HubVersionRefused):
        return HTTPException(status_code=426, detail=e.detail)
    if isinstance(e, HubUnreachable):
        return HTTPException(status_code=502, detail=str(e))
    if isinstance(e, HubRefused) and 400 <= e.status < 500:
        return HTTPException(status_code=400, detail=f"The hub refused the request: {e.detail}")
    return HTTPException(status_code=502, detail=f"The hub failed to answer: {e}")


@router.get("/connection", summary="This user's hub connection")
def get_connection(current_user=Depends(get_current_active_user)) -> HubConnection:
    """The stored connection, with the hub's organisation and version read live from ``/hub/info`` and who it signs
    in as (``account``).

    A hub that does not answer leaves the hub fields empty, ``reachable`` false and ``error`` set; the
    connection itself is still reported. Not connected, ``device_name`` is the name a new connection would use. On a
    server joined with keys of its own the answer is ``joined``, names the hub it joined whether or not the person has
    a hub session, and carries the organisation's storage root its ``/hub/info`` gives.
    """
    stored = session.load(current_user.id)
    node = issuer_accounts.joined_with_keys()
    if stored is None and node is None:
        return _connection(None)
    hub_url = stored["hub_url"] if stored else node.hub_url.rstrip("/")
    account = _account(stored, current_user)
    info = None
    try:
        info = client.get_info(hub_url)
        connection = _connection(stored, info, account=account)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        connection = _connection(stored, error=str(e), account=account)
    if node is not None:
        connection.hub_url, connection.joined = hub_url, True
        connection.organisation_storage = info.get("organisation_storage") if info else None
    return connection


@router.post("/connection", summary="Connect to a hub")
def connect(
    request: HubConnectRequest,
    background: BackgroundTasks,
    current_user=Depends(get_current_admin_user),
    db: Session = Depends(get_db),
) -> HubConnection:
    """Read the hub's info, sign in with the hub account, register this machine as a device, then store the session.

    Nothing is stored unless every step succeeded. Connecting again replaces the earlier connection; the same
    device name under the same hub account is the same device on the hub. Once connected, the hub's compute nodes
    are synced into the backends table as ``/compute_nodes/sync`` does, best effort: a hub whose list could not be
    read leaves one warning in the log, and the connection stands either way. The rows the sync wrote are checked
    after the answer is sent (``_check_nodes``), so it is not held up.
    """
    device_name = (request.device_name or "").strip() or socket.gethostname()
    try:
        info = client.get_info(request.hub_url)
        tokens = client.sign_in(request.hub_url, request.username, request.password)
        device = client.register_device(request.hub_url, tokens["access_token"], device_name, get_version())
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _http_error(e) from e
    session.save(
        current_user.id,
        {
            "hub_url": request.hub_url,
            "refresh_token": tokens["refresh_token"],
            "device_name": device["name"],
            "org_name": info["org_name"],
        },
    )
    stored = session.load(current_user.id)
    connection = _connection(stored, info, account=_account(stored, current_user))
    try:
        synced, _ = node_addresses.sync_nodes(db, current_user.id, request.hub_url, tokens["access_token"])
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        logger.warning("Connected to the hub at %s, but its compute nodes could not be read: %s", request.hub_url, e)
        return connection
    background.add_task(_check_nodes, synced)
    return connection


@router.delete("/connection", summary="Disconnect from the hub")
def disconnect(current_user=Depends(get_current_admin_user), db: Session = Depends(get_db)) -> dict:
    """Forget the hub session on this machine, the node tokens minted through it and the backends rows it filled.

    The hub keeps the device; nothing is sent to it. A row an administrator added by hand stays.
    """
    rows = db.query(ConfiguredBackend).filter(ConfiguredBackend.account_user_id == current_user.id).all()
    for row in rows:
        node_addresses.drop_row(db, row)
    db.commit()
    session.clear(current_user.id)
    return {"connected": False}


class ComputeNodesSync(BaseModel):
    """What a sync left behind: every configured backend as ``GET /compute_backends`` lists it, and the ids of the
    hub's nodes it skipped because a row of that id was added by hand."""

    backends: list[ComputeBackendOut]
    skipped: list[str]


def _check_nodes(backend_ids: list[str]) -> None:
    """Check each of *backend_ids* as the Handshake button does, so the listing carries a verdict for the rows a sync
    wrote: the one kept when it passed, else a fresh check (``handshake.verdict_for``). A refused check keeps its
    verdict; a row the registry cannot make a backend of (no hub session to mint its token) is left unchecked."""
    for backend_id in backend_ids:
        try:
            handshake.verdict_for(registry.backend(backend_id))
        except (LookupError, ComputeError):
            continue


@router.post("/compute_nodes/sync", summary="Fill the backends table from the hub's compute nodes")
def sync_compute_nodes(current_user=Depends(get_current_admin_user), db: Session = Depends(get_db)) -> ComputeNodesSync:
    """Read the hub's compute nodes now and make the backends table match them (``node_addresses.sync_nodes``), then
    check the rows written (``_check_nodes``) so the answer carries their verdicts.

    Explicit, never on a timer: a desktop that wants the hub's newest list asks. 400 without a hub connection,
    502 when the hub does not answer.
    """
    hub_url, token = _hub(current_user)
    try:
        synced, skipped = node_addresses.sync_nodes(db, current_user.id, hub_url, token)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e
    _check_nodes(synced)
    rows = db.query(ConfiguredBackend).order_by(ConfiguredBackend.id).all()
    return ComputeNodesSync(backends=listed_backends(rows, current_user), skipped=skipped)


class LocalPathUse(BaseModel):
    """A node whose settings name a path on this machine, which the hub's other users will not have."""

    label: str
    path: str


class HubPublishRequest(BaseModel):
    flow_id: int
    namespace_id: int | None = None
    message: str | None = None
    overwrite_version: int | None = None


class HubPublishOut(BaseModel):
    version: int
    name: str


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


def _hub(current_user) -> tuple[str, str]:
    """The hub this user is connected to and an access token for it; 400 when there is no connection."""
    stored = session.load(current_user.id)
    if stored is None:
        raise HTTPException(status_code=400, detail="Connect to a hub first (Compute → Hub).")
    try:
        token = session.access_token(current_user.id)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e
    if token is None:
        raise HTTPException(status_code=400, detail="Connect to a hub first (Compute → Hub).")
    return stored["hub_url"], token


def _hub_error(e: Exception) -> HTTPException:
    """A hub refusal as this route's answer: its own status (a hub 401 becomes 400), 502 when the hub did not answer."""
    if isinstance(e, HubVersionRefused):
        return HTTPException(status_code=426, detail=e.detail)
    if isinstance(e, HubUnreachable):
        return HTTPException(status_code=502, detail=str(e))
    if isinstance(e, HubRefused) and 400 <= e.status < 500:
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


@router.get("/flows", summary="The hub's flows")
def hub_flows(namespace_id: int | None = None, current_user=Depends(get_current_active_user)) -> list[dict]:
    """The flows this account may read on the hub, each with its newest version; ``namespace_id`` narrows them."""
    hub_url, token = _hub(current_user)
    try:
        return client.list_flows(hub_url, token, namespace_id)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e


@router.get("/flows/{flow_uuid}/versions", summary="A hub flow's versions")
def hub_flow_versions(flow_uuid: str, current_user=Depends(get_current_active_user)) -> list[dict]:
    """Every published version of the flow, newest first, as the hub lists them."""
    hub_url, token = _hub(current_user)
    try:
        return client.list_versions(hub_url, token, flow_uuid)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e


@router.get("/publish_check", summary="Local paths a flow would carry to the hub")
def publish_check(flow_id: int, current_user=Depends(get_current_active_user)) -> list[LocalPathUse]:
    """The ``read``, ``output`` and ``list_files`` nodes whose settings name a local path.

    A warning for the person publishing, never a refusal: the flow publishes as it is, and whoever opens it from
    the hub will not have these files.
    """
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
    """``(flow_uuid, name)`` of the flow's local catalog registration, ``(None, None)`` when it has none."""
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


@router.post("/publish", status_code=201, summary="Publish a flow to the hub")
def publish(request: HubPublishRequest, current_user=Depends(get_current_active_user)) -> HubPublishOut:
    """Store the open flow as the next version of its hub flow and stamp the document with the version it became.

    The flow publishes under the uuid of its ``hub_origin``, else of its local registration, else a new one, so a
    flow that came from the hub keeps its identity and a local flow carries its own to the hub. The base is
    ``overwrite_version`` when given, else the origin's version, else nothing (a first publish). The document goes
    up without ``source_registration_id`` and ``hub_origin``, which are this machine's. On success the origin is
    stamped and the flow saved to its own path, so it has to have one: an unsaved flow is refused with 422.

    A stale base is 409 ``stale_base`` with the base and current versions as the hub lists them; a document that
    came from another hub is 409 ``other_hub``; any other hub refusal keeps the hub's status under ``refused``.
    The flow is looked up for the caller before the hub session is checked, so a flow the caller cannot see is 404
    whether or not a hub is connected.
    """
    flow = get_flow_or_404(request.flow_id, current_user)
    hub_url, token = _hub(current_user)
    flow_path = flow.flow_settings.path or flow.flow_settings.save_location
    if not flow_path:
        raise HTTPException(status_code=422, detail="save the flow first")
    origin = getattr(flow.flow_settings, "hub_origin", None)
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
    return HubPublishOut(version=published["version"], name=published["name"])


def _local_copy_path(flow_uuid: str, name: str, current_user) -> tuple[str, str]:
    """Where the caller's copy of this hub flow lives, and who holds the hub's uuid in this catalog.

    ``"caller"``: a registration of the caller's holds it, and its file is the copy. ``"nobody"``: the copy is a file
    under ``flows/hub``, to be registered under the uuid. ``"another"``: someone else's registration holds it, which
    the caller never opens or writes; their copy is a file of their own. On a core that several people use, a
    person's files under ``flows/hub`` sit in a folder of their own, so each has a working copy of the same flow.
    """
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


def _local_flows_namespace_id() -> int | None:
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        namespace = service.ensure_local_flows_namespace()
        return namespace.id if namespace is not None else service.get_default_namespace_id()


def _read_local_copy(flow_path: str) -> tuple[schemas.FlowfileData | None, int | None]:
    """The document in the file and the flow id it stores; ``None`` for whichever cannot be read."""
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
    """A flow document without what belongs to one machine: its id, name, app version, registration and origin."""
    content = document.model_dump(mode="json", exclude={"flowfile_id", "flowfile_name", "flowfile_version"})
    content["flowfile_settings"].pop("source_registration_id", None)
    content["flowfile_settings"].pop("hub_origin", None)
    return content


def _has_unpublished_edits(local: schemas.FlowfileData | None, hub_url: str, token: str, checked_out: dict) -> bool:
    """Whether writing over the local copy would lose work: it differs from the hub version it came from.

    A copy that cannot be read, that names no origin on this hub flow, or whose origin version the hub does not
    answer counts as edited, so the caller is asked and nothing is written over unseen.
    """
    origin = local.flowfile_settings.hub_origin if local is not None else None
    if origin is None or origin.flow_uuid != checked_out["flow_uuid"]:
        return True
    if origin.hub_url.rstrip("/") != hub_url.rstrip("/"):
        return True
    base = checked_out
    if origin.version != checked_out["version"]:
        try:
            base = client.checkout(hub_url, token, origin.flow_uuid, origin.version)
        except (HubUnreachable, HubRefused, HubVersionRefused):
            return True
    try:
        return _content(local) != _content(schemas.FlowfileData.model_validate(base["definition"]))
    except ValueError:
        return True


@router.post("/open", summary="Open a flow from the hub")
def open_from_hub(request: HubOpenRequest, current_user=Depends(get_current_active_user)) -> HubOpenOut:
    """Check a version out of the hub and write it as the caller's local copy of that flow.

    The file carries ``hub_origin``, so a later publish from it names this version as its base and its runs are
    reported as the hub flow's. The copy is the file of the caller's registration holding the hub's uuid (a flow
    published from here, or an earlier open), else a file under ``flows/hub`` registered under that uuid. On a core
    several people use, each person has a copy of their own: where another person's registration holds the uuid,
    the caller's file is registered beside it under an id of its own, and the other's is never named or written.

    A copy that exists already is written over when nothing is lost by it: it still equals the hub version it came
    from. One with edits that were not published is 409 ``local_copy_exists`` with its path, unless ``replace`` is
    set; the caller can open that path as any local flow instead.
    """
    hub_url, token = _hub(current_user)
    try:
        checked_out = client.checkout(hub_url, token, request.flow_uuid, request.version)
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        raise _hub_error(e) from e

    flow_path, holder = _local_copy_path(checked_out["flow_uuid"], checked_out["name"], current_user)
    namespace_id = _local_flows_namespace_id() if holder == "nobody" else None
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
        if not request.replace and _has_unpublished_edits(local, hub_url, token, checked_out):
            raise HTTPException(status_code=409, detail={"kind": "local_copy_exists", "flow_path": flow_path})

    document = schemas.FlowfileData.model_validate(checked_out["definition"])
    # The open editor keeps a reloaded file's id; a new file gets its own, never the publisher's.
    document.flowfile_id = existing_id if isinstance(existing_id, int) else create_unique_id()
    document.flowfile_settings.source_registration_id = None
    document.flowfile_settings.hub_origin = schemas.HubOrigin(
        hub_url=hub_url, flow_uuid=checked_out["flow_uuid"], version=checked_out["version"]
    )

    if holder == "nobody":
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
    if holder == "another":
        auto_register_flow(flow_path, checked_out["name"], current_user.id)
    return HubOpenOut(flow_path=flow_path)
