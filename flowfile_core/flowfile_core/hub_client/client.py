"""The hub's HTTP contract, as plain functions over ``httpx``.

Each function takes the hub's base URL and an optional ``http`` client, which tests replace with a
``httpx.MockTransport``. Sign-in and refresh use the auth routes the hub mounts from core (form fields, a token pair
back), as does ``/catalog/namespaces``; ``/hub/info``, ``/hub/devices`` and ``/hub/flows`` are the hub's own.
Requests log method, path and status only, never a body, a password or a token.
"""

import threading

import httpx

from flowfile_core.configs import logger

_HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)


class HubUnreachable(Exception):
    """The hub did not answer: a refused connection, a DNS failure or a timeout."""


class HubRefused(Exception):
    """The hub answered with an error, or with something that is not a hub's answer.

    ``detail`` is the hub's text; ``payload`` is its ``detail`` object when the hub sent one (the stale-base
    409 carries ``current_version`` and ``base_version`` that way), else ``None``.
    """

    def __init__(self, status: int, detail: str, payload: dict | None = None):
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.payload = payload


class HubVersionRefused(Exception):
    """The hub does not support this client's version (its 426); ``detail`` is the hub's own text."""

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


_module_http: httpx.Client | None = None
_module_http_lock = threading.Lock()


def _http(http: httpx.Client | None) -> httpx.Client:
    """Injected client for tests, else a lazily-created module singleton."""
    if http is not None:
        return http
    global _module_http
    with _module_http_lock:
        if _module_http is None:
            _module_http = httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=False)
        return _module_http


def _raw_detail(resp: httpx.Response):
    try:
        return resp.json().get("detail")
    except (ValueError, AttributeError):
        return None


def _detail(resp: httpx.Response) -> str:
    detail = _raw_detail(resp)
    if isinstance(detail, str) and detail:
        return detail
    if isinstance(detail, dict) and isinstance(detail.get("message"), str):
        return detail["message"]
    return f"The hub answered {resp.status_code}."


def _send(method: str, url: str, path: str, http: httpx.Client | None, **kwargs):
    try:
        resp = _http(http).request(method, f"{url.rstrip('/')}{path}", **kwargs)
    except httpx.TransportError as e:
        raise HubUnreachable(f"Could not reach the hub at {url}.") from e
    logger.info("hub %s %s -> %s", method, path, resp.status_code)
    if resp.status_code == 426:
        raise HubVersionRefused(_detail(resp))
    if resp.status_code >= 400:
        detail = _raw_detail(resp)
        raise HubRefused(resp.status_code, _detail(resp), detail if isinstance(detail, dict) else None)
    try:
        return resp.json()
    except ValueError:
        return None


def _request(method: str, url: str, path: str, http: httpx.Client | None, **kwargs) -> dict:
    body = _send(method, url, path, http, **kwargs)
    if not isinstance(body, dict):
        raise HubRefused(200, f"{url} did not answer {path} as a Flowfile hub does.")
    return body


def _request_list(method: str, url: str, path: str, http: httpx.Client | None, **kwargs) -> list:
    body = _send(method, url, path, http, **kwargs)
    if not isinstance(body, list):
        raise HubRefused(200, f"{url} did not answer {path} as a Flowfile hub does.")
    return body


def _bearer(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def _require(body: dict, fields: tuple[str, ...], url: str, path: str) -> dict:
    if any(body.get(field) in (None, "") for field in fields):
        raise HubRefused(200, f"{url} did not answer {path} as a Flowfile hub does.")
    return body


def get_info(url: str, *, http: httpx.Client | None = None) -> dict:
    """The hub's ``org_name``, ``hub_version``, ``core_version`` and ``vend_mode``; needs no sign-in."""
    body = _request("GET", url, "/hub/info", http)
    return _require(body, ("org_name", "hub_version", "core_version", "vend_mode"), url, "/hub/info")


def sign_in(url: str, username: str, password: str, *, http: httpx.Client | None = None) -> dict:
    """Exchange a hub account's username and password for ``access_token`` and ``refresh_token``."""
    body = _request("POST", url, "/auth/token", http, data={"username": username, "password": password})
    return _require(body, ("access_token", "refresh_token"), url, "/auth/token")


def refresh(url: str, refresh_token: str, *, http: httpx.Client | None = None) -> dict:
    """A new ``access_token`` and a rotated ``refresh_token``; the old refresh token is not to be used again."""
    body = _request("POST", url, "/auth/refresh", http, data={"refresh_token": refresh_token})
    return _require(body, ("access_token", "refresh_token"), url, "/auth/refresh")


def register_device(
    url: str, access_token: str, name: str, client_version: str, *, http: httpx.Client | None = None
) -> dict:
    """Register this machine under the signed-in account; the same name under the same account is the same device.

    Answers the hub's device: ``id``, ``name``, ``client_version`` and ``last_seen_at``.
    """
    body = _request(
        "POST",
        url,
        "/hub/devices",
        http,
        json={"name": name, "client_version": client_version},
        headers=_bearer(access_token),
    )
    return _require(body, ("id", "name"), url, "/hub/devices")


def list_namespaces(url: str, access_token: str, *, http: httpx.Client | None = None) -> list:
    """The catalog namespaces the signed-in account can see on the hub, as the hub lists them."""
    return _request_list("GET", url, "/catalog/namespaces", http, headers=_bearer(access_token))


def list_flows(
    url: str, access_token: str, namespace_id: int | None = None, *, http: httpx.Client | None = None
) -> list:
    """The flows the account may read, each with its newest hub version; ``namespace_id`` narrows the list."""
    params = {"namespace_id": namespace_id} if namespace_id is not None else None
    return _request_list("GET", url, "/hub/flows", http, params=params, headers=_bearer(access_token))


def list_versions(url: str, access_token: str, flow_uuid: str, *, http: httpx.Client | None = None) -> list:
    """Every published version of the flow, newest first."""
    return _request_list("GET", url, f"/hub/flows/{flow_uuid}/versions", http, headers=_bearer(access_token))


def checkout(
    url: str, access_token: str, flow_uuid: str, version: int | None = None, *, http: httpx.Client | None = None
) -> dict:
    """The stored document of ``version`` (the newest when omitted) with the flow's name and namespace."""
    params = {"version": version} if version is not None else None
    body = _request("GET", url, f"/hub/flows/{flow_uuid}/checkout", http, params=params, headers=_bearer(access_token))
    return _require(body, ("flow_uuid", "version", "name", "definition"), url, "/hub/flows/{flow_uuid}/checkout")


def publish(url: str, access_token: str, flow_uuid: str, body: dict, *, http: httpx.Client | None = None) -> dict:
    """Store ``body["definition"]`` as the flow's next version; the hub's 201 answer with the version it got.

    A stale ``base_version`` is the hub's 409, raised as ``HubRefused`` whose ``payload`` carries
    ``current_version`` and ``base_version``.
    """
    answer = _request("POST", url, f"/hub/flows/{flow_uuid}/publish", http, json=body, headers=_bearer(access_token))
    return _require(answer, ("flow_uuid", "version", "name"), url, "/hub/flows/{flow_uuid}/publish")
