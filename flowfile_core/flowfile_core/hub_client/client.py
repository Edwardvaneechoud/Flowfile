"""The hub's HTTP contract as plain functions over ``httpx``; tests pass ``http`` a ``MockTransport``.

Requests log method, path and status only, never a body, a password or a token.
"""

import threading

import httpx

from flowfile_core.configs import logger

_HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)


class HubUnreachable(Exception):
    """The hub did not answer: a refused connection, a DNS failure or a timeout."""


class HubRefused(Exception):
    """The hub answered with an error, or with something that is not a hub's answer; ``payload`` is its ``detail``
    object when it sent one (the stale-base 409 carries the versions that way)."""

    def __init__(self, status: int, detail: str, payload: dict | None = None):
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.payload = payload


class HubVersionRefused(Exception):
    """The hub does not support this client's version (its 426)."""

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


_module_http: httpx.Client | None = None
_module_http_lock = threading.Lock()


def http_client(http: httpx.Client | None = None) -> httpx.Client:
    """``http`` when given, else the one client this process keeps for its hub."""
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


def answered(status: int) -> str:
    """What a refusal says when the hub gave no text of its own."""
    return f"The hub answered {status}."


def _detail(resp: httpx.Response) -> str:
    detail = _raw_detail(resp)
    if isinstance(detail, str) and detail:
        return detail
    if isinstance(detail, dict) and isinstance(detail.get("message"), str):
        return detail["message"]
    return answered(resp.status_code)


def _answered(method: str, url: str, path: str, http: httpx.Client | None, **kwargs) -> httpx.Response:
    try:
        resp = http_client(http).request(method, f"{url.rstrip('/')}{path}", **kwargs)
    except httpx.RequestError as e:
        raise HubUnreachable(f"Could not reach the hub at {url}.") from e
    logger.info("hub %s %s -> %s", method, path, resp.status_code)
    if resp.status_code == 426:
        raise HubVersionRefused(_detail(resp))
    if resp.status_code >= 400:
        detail = _raw_detail(resp)
        raise HubRefused(resp.status_code, _detail(resp), detail if isinstance(detail, dict) else None)
    return resp


def _json(resp: httpx.Response):
    try:
        return resp.json()
    except ValueError:
        return None


def _send(method: str, url: str, path: str, http: httpx.Client | None, **kwargs):
    return _json(_answered(method, url, path, http, **kwargs))


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
    body = _request("GET", url, "/hub/info", http)
    return _require(body, ("org_name", "hub_version"), url, "/hub/info")


def sign_in(url: str, username: str, password: str, *, http: httpx.Client | None = None) -> dict:
    body = _request("POST", url, "/auth/token", http, data={"username": username, "password": password})
    return _require(body, ("access_token", "refresh_token"), url, "/auth/token")


def refresh(url: str, refresh_token: str, *, http: httpx.Client | None = None) -> dict:
    """A new token pair; the old refresh token is not to be used again."""
    body = _request("POST", url, "/auth/refresh", http, data={"refresh_token": refresh_token})
    return _require(body, ("access_token", "refresh_token"), url, "/auth/refresh")


def register_device(
    url: str, access_token: str, name: str, client_version: str, *, http: httpx.Client | None = None
) -> dict:
    """The same name under the same account is the same device."""
    body = _request(
        "POST",
        url,
        "/hub/devices",
        http,
        json={"name": name, "client_version": client_version},
        headers=_bearer(access_token),
    )
    return _require(body, ("id", "name"), url, "/hub/devices")


def catalog_request(
    url: str,
    access_token: str,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    body=None,
    http: httpx.Client | None = None,
) -> tuple[int, object]:
    """One request to a catalog route the hub mounts from core: its status and answer as sent, ``None`` when empty."""
    kwargs = {"params": params or None, "headers": _bearer(access_token)}
    if body is not None:
        kwargs["json"] = body
    resp = _answered(method, url, path, http, **kwargs)
    return resp.status_code, _json(resp)


def get_table(url: str, access_token: str, table_id: int, *, http: httpx.Client | None = None) -> dict:
    body = _request("GET", url, f"/catalog/tables/{table_id}", http, headers=_bearer(access_token))
    return _require(body, ("id", "name"), url, "/catalog/tables/{table_id}")


def resolve_table(url: str, access_token: str, reference: str, *, http: httpx.Client | None = None) -> dict:
    """The hub's table named ``catalog.schema.table``; an unknown name is its 404."""
    body = _request("GET", url, "/catalog/tables/resolve", http, params={"q": reference}, headers=_bearer(access_token))
    table = _require(body, ("table",), url, "/catalog/tables/resolve")["table"]
    if not isinstance(table, dict):
        raise HubRefused(200, f"{url} did not answer /catalog/tables/resolve as a Flowfile hub does.")
    return _require(table, ("id", "name"), url, "/catalog/tables/resolve")


def register_table_from_data(url: str, access_token: str, body: dict, *, http: httpx.Client | None = None) -> dict:
    """Record a table already written under a namespace's storage root on the hub."""
    answer = _request("POST", url, "/catalog/tables/from-data", http, json=body, headers=_bearer(access_token))
    return _require(answer, ("id", "name"), url, "/catalog/tables/from-data")


def _sealed_to(items, targets) -> bool:
    return isinstance(items, list) and all(
        isinstance(item, dict) and isinstance(item.get("sealed"), dict) and set(targets) <= set(item["sealed"])
        for item in items
    )


def vend(
    url: str,
    access_token: str,
    run_id: str,
    read_namespace_ids: list[int],
    write_namespace_ids: list[int],
    *,
    targets: list[str] | tuple[str, ...] = (),
    exchange: bool = False,
    secrets: list[str] | tuple[str, ...] = (),
    connections: list[tuple[str, str]] | tuple[tuple[str, str], ...] = (),
    http: httpx.Client | None = None,
) -> dict:
    """A credential per storage root of the namespaces, for the run ``run_id``; the hub records the vend.

    With ``targets`` (hub node names) every credential, secret and connection comes back sealed to each target, and
    an answer missing a target is refused. Without targets the body is the one an older hub takes.
    """
    request = {
        "run_id": run_id,
        "read_namespace_ids": list(read_namespace_ids),
        "write_namespace_ids": list(write_namespace_ids),
    }
    if exchange:
        request["exchange"] = True
    if targets:
        request["targets"] = list(targets)
    if secrets:
        request["secrets"] = list(secrets)
    if connections:
        request["connections"] = [{"kind": kind, "name": name} for kind, name in connections]
    body = _request("POST", url, "/hub/vend", http, json=request, headers=_bearer(access_token))
    _require(body, ("vend_id",), url, "/hub/vend")
    credentials, answered, stored = body.get("credentials"), body.get("secrets", []), body.get("connections", [])
    if (
        not isinstance(credentials, list)
        or not all(
            isinstance(credential, dict)
            and isinstance(credential.get("base_uri"), str)
            and isinstance(credential.get("storage_options"), dict)
            and isinstance(credential.get("sealed", {}), dict)
            and (not targets or set(targets) <= set(credential.get("sealed") or {}))
            for credential in credentials
        )
        or (secrets and not (_sealed_to(answered, targets) and set(secrets) <= {s.get("name") for s in answered}))
        or (
            connections
            and not (
                _sealed_to(stored, targets) and set(connections) <= {(c.get("kind"), c.get("name")) for c in stored}
            )
        )
    ):
        raise HubRefused(200, f"{url} did not answer /hub/vend as a Flowfile hub does.")
    return body


def get_exchange_store(url: str, access_token: str, *, http: httpx.Client | None = None) -> dict | None:
    """The organisation's exchange store, ``None`` when the hub sets none (its 404)."""
    try:
        body = _request("GET", url, "/hub/exchange-store", http, headers=_bearer(access_token))
    except HubRefused as e:
        if e.status == 404:
            return None
        raise
    max_rows = body.get("max_rows")
    if (
        not all(isinstance(body.get(field), str) and body[field] for field in ("uri", "connection"))
        or isinstance(max_rows, bool)
        or not (max_rows is None or isinstance(max_rows, int))
    ):
        raise HubRefused(200, f"{url} did not answer /hub/exchange-store as a Flowfile hub does.")
    return body


def list_versions(url: str, access_token: str, flow_uuid: str, *, http: httpx.Client | None = None) -> list:
    return _request_list("GET", url, f"/hub/flows/{flow_uuid}/versions", http, headers=_bearer(access_token))


def checkout(
    url: str, access_token: str, flow_uuid: str, version: int | None = None, *, http: httpx.Client | None = None
) -> dict:
    params = {"version": version} if version is not None else None
    body = _request("GET", url, f"/hub/flows/{flow_uuid}/checkout", http, params=params, headers=_bearer(access_token))
    return _require(body, ("flow_uuid", "version", "name", "definition"), url, "/hub/flows/{flow_uuid}/checkout")


def for_editing(
    url: str, access_token: str, flow_uuid: str, version: int | None = None, *, http: httpx.Client | None = None
) -> dict:
    """The document the hub chooses for the person to work on, with an optional ``note``; a hub without the route
    answers ``checkout``."""
    params = {"version": version} if version is not None else None
    path = f"/hub/flows/{flow_uuid}/edit"
    try:
        body = _request("GET", url, path, http, params=params, headers=_bearer(access_token))
    except HubRefused as e:
        if e.status not in (404, 405) or e.detail not in ("Not Found", "Method Not Allowed"):
            raise
        return checkout(url, access_token, flow_uuid, version, http=http)
    return _require(body, ("flow_uuid", "version", "name", "definition"), url, "/hub/flows/{flow_uuid}/edit")


def save_content(url: str, access_token: str, flow_uuid: str, body: dict, *, http: httpx.Client | None = None) -> dict:
    """Keep the person's working content of the flow on the hub, no version made."""
    return _request("PUT", url, f"/hub/flows/{flow_uuid}/draft", http, json=body, headers=_bearer(access_token))


def publish(url: str, access_token: str, flow_uuid: str, body: dict, *, http: httpx.Client | None = None) -> dict:
    """Store the flow's next version; a stale ``base_version`` is the hub's 409 with the versions in ``payload``."""
    answer = _request("POST", url, f"/hub/flows/{flow_uuid}/publish", http, json=body, headers=_bearer(access_token))
    return _require(answer, ("flow_uuid", "version", "name"), url, "/hub/flows/{flow_uuid}/publish")


def list_compute_nodes(url: str, access_token: str, *, http: httpx.Client | None = None) -> list:
    nodes = _request_list("GET", url, "/hub/compute-nodes", http, headers=_bearer(access_token))
    for node in nodes:
        if not isinstance(node, dict):
            raise HubRefused(200, f"{url} did not answer /hub/compute-nodes as a Flowfile hub does.")
        _require(node, ("id", "name", "worker_url"), url, "/hub/compute-nodes")
        if not isinstance(node.get("enabled"), bool):
            raise HubRefused(200, f"{url} did not answer /hub/compute-nodes as a Flowfile hub does.")
    return nodes


def mint_node_token(url: str, access_token: str, node_id: int, *, http: httpx.Client | None = None) -> dict:
    """A short-lived ``run`` token the hub signs for one of its nodes; the hub shows it once and keeps none."""
    body = _request(
        "POST",
        url,
        "/hub/node-tokens",
        http,
        json={"node_id": node_id, "scope": "run"},
        headers=_bearer(access_token),
    )
    return _require(body, ("token", "node_name", "kid", "expires_at", "issuer"), url, "/hub/node-tokens")
