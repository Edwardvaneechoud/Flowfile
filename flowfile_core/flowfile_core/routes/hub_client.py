"""Connect this Flowfile to a hub, see the connection, and disconnect.

Every route is behind the local sign-in and works on the caller's own connection. No hub state ever answers 401,
which the frontend reads as an expired local session: refused credentials answer 400, a hub that cannot be reached
502, a hub that does not support this version 426 with the hub's own text, and a URL that is not ``http(s)://`` 422.
The password is sent to the hub once, on connect, and is never stored, logged or answered.
"""

import socket
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.hub_client import HUB_CONTRACT_VERSION, client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from shared._version import get_version

router = APIRouter()


class HubConnection(BaseModel):
    connected: bool
    hub_url: str | None = None
    device_id: int | None = None
    device_name: str | None = None
    expires_at: datetime | None = None
    client_version: str
    contract_version: str
    org_name: str | None = None
    hub_version: str | None = None
    core_version: str | None = None
    vend_mode: str | None = None
    reachable: bool = False
    error: str | None = None


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


def _hostname() -> str:
    return socket.gethostname()


def _connection(stored: dict | None, info: dict | None = None, error: str | None = None) -> HubConnection:
    expires = stored.get("refresh_expires_at") if stored else None
    return HubConnection(
        connected=stored is not None,
        hub_url=stored["hub_url"] if stored else None,
        device_id=stored.get("device_id") if stored else None,
        device_name=stored.get("device_name") if stored else _hostname(),
        expires_at=datetime.fromtimestamp(expires, timezone.utc) if expires is not None else None,
        client_version=get_version(),
        contract_version=HUB_CONTRACT_VERSION,
        org_name=info["org_name"] if info else None,
        hub_version=info["hub_version"] if info else None,
        core_version=info["core_version"] if info else None,
        vend_mode=info["vend_mode"] if info else None,
        reachable=info is not None,
        error=error,
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
    """The stored connection, with the hub's organisation and versions read live from ``/hub/info``.

    A hub that does not answer leaves the hub fields empty, ``reachable`` false and ``error`` set; the
    connection itself is still reported. Not connected, ``device_name`` is the name a new connection would use.
    """
    stored = session.load(current_user.id)
    if stored is None:
        return _connection(None)
    try:
        info = client.get_info(stored["hub_url"])
    except (HubUnreachable, HubRefused, HubVersionRefused) as e:
        return _connection(stored, error=str(e))
    return _connection(stored, info)


@router.post("/connection", summary="Connect to a hub")
def connect(request: HubConnectRequest, current_user=Depends(get_current_active_user)) -> HubConnection:
    """Read the hub's info, sign in with the hub account, register this machine as a device, then store the session.

    Nothing is stored unless every step succeeded. Connecting again replaces the earlier connection; the same
    device name under the same hub account is the same device on the hub.
    """
    device_name = (request.device_name or "").strip() or _hostname()
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
            "device_id": device["id"],
            "device_name": device["name"],
        },
    )
    return _connection(session.load(current_user.id), info)


@router.delete("/connection", summary="Disconnect from the hub")
def disconnect(current_user=Depends(get_current_active_user)) -> dict:
    """Forget the hub session on this machine. The hub keeps the device; nothing is sent to it."""
    session.clear(current_user.id)
    return {"connected": False}
