"""The hub's HTTP contract, as plain functions over ``httpx``.

Each function takes the hub's base URL and an optional ``http`` client, which tests replace with a
``httpx.MockTransport``. Sign-in and refresh use the auth routes the hub mounts from core (form fields, a token pair
back), as do ``/catalog/namespaces`` and ``/catalog/tables``; ``/hub/info``, ``/hub/devices``, ``/hub/flows``,
``/hub/compute-nodes``, ``/hub/node-tokens``, ``/hub/vend`` and ``/hub/exchange-store`` are the hub's own. Requests
log method, path and status only, never a body, a password or a token.
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
    """The hub's ``org_name``, ``hub_version``, ``core_version`` and ``vend_mode``, and ``organisation_storage`` from
    a hub that sends it; needs no sign-in."""
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


def list_namespaces(
    url: str, access_token: str, parent_id: int | None = None, *, http: httpx.Client | None = None
) -> list:
    """The catalog namespaces the signed-in account can see on the hub, as the hub lists them.

    The hub answers its root namespaces without ``parent_id`` and the children of that namespace with one.
    """
    params = {"parent_id": parent_id} if parent_id is not None else None
    return _request_list("GET", url, "/catalog/namespaces", http, params=params, headers=_bearer(access_token))


def list_tables(
    url: str, access_token: str, namespace_id: int | None = None, *, http: httpx.Client | None = None
) -> list:
    """The catalog tables the signed-in account can see on the hub, as the hub lists them.

    Without ``namespace_id`` the hub answers every table the account can see; with one, that namespace's tables.
    """
    params = {"namespace_id": namespace_id} if namespace_id is not None else None
    tables = _request_list("GET", url, "/catalog/tables", http, params=params, headers=_bearer(access_token))
    for table in tables:
        if not isinstance(table, dict):
            raise HubRefused(200, f"{url} did not answer /catalog/tables as a Flowfile hub does.")
        _require(table, ("id", "name"), url, "/catalog/tables")
    return tables


def get_table(url: str, access_token: str, table_id: int, *, http: httpx.Client | None = None) -> dict:
    """The hub's catalog table ``table_id`` as core's ``CatalogTableOut``; an unknown table is the hub's 404."""
    body = _request("GET", url, f"/catalog/tables/{table_id}", http, headers=_bearer(access_token))
    return _require(body, ("id", "name"), url, "/catalog/tables/{table_id}")


def register_table_from_data(url: str, access_token: str, body: dict, *, http: httpx.Client | None = None) -> dict:
    """Record a table whose data is already written under a namespace's storage root on the hub; its 201 answer.

    ``body`` is core's ``CatalogTableFromDataCreate``. A ``table_path`` that is not a table directory under the
    namespace's storage root is the hub's 403, a name already taken in the namespace its 409.
    """
    answer = _request("POST", url, "/catalog/tables/from-data", http, json=body, headers=_bearer(access_token))
    return _require(answer, ("id", "name"), url, "/catalog/tables/from-data")


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
    http: httpx.Client | None = None,
) -> dict:
    """A credential for each storage root the namespaces live under, for the run ``run_id``; the hub records the vend.

    Answers ``vend_id`` and ``credentials``, each with ``base_uri``, ``storage_options``, ``expires_at``, ``mode``,
    ``read_prefixes`` and ``write_prefixes``. A namespace the account may not use (read) or manage (write) is the
    hub's 403, one on the hub's own disk its 422, a hub whose vending is not configured its 501. ``exchange`` asks for
    the organisation's exchange store prefix too. With ``targets``, hub node names, each credential's
    ``storage_options`` is empty and its ``sealed`` maps every target to a token only that node opens; a node the
    caller may not run on, or that is disabled, is the hub's 403, one unknown, removed or without keys its 422. Without
    targets the body is the one an older hub takes. ``secrets``, secret names, needs ``targets``: the answer's
    ``secrets`` holds each name with its ``sealed`` map, and a name the caller cannot reach is the hub's 403.
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
    body = _request("POST", url, "/hub/vend", http, json=request, headers=_bearer(access_token))
    _require(body, ("vend_id",), url, "/hub/vend")
    credentials = body.get("credentials")
    if not isinstance(credentials, list) or not all(
        isinstance(credential, dict)
        and isinstance(credential.get("base_uri"), str)
        and isinstance(credential.get("storage_options"), dict)
        and isinstance(credential.get("sealed", {}), dict)
        and (not targets or set(targets) <= set(credential.get("sealed") or {}))
        for credential in credentials
    ):
        raise HubRefused(200, f"{url} did not answer /hub/vend as a Flowfile hub does.")
    answered = body.get("secrets", [])
    if secrets and not (
        isinstance(answered, list)
        and all(
            isinstance(secret, dict)
            and isinstance(secret.get("sealed"), dict)
            and set(targets) <= set(secret["sealed"])
            for secret in answered
        )
        and set(secrets) <= {secret.get("name") for secret in answered}
    ):
        raise HubRefused(200, f"{url} did not answer /hub/vend as a Flowfile hub does.")
    return body


def get_exchange_store(url: str, access_token: str, *, http: httpx.Client | None = None) -> dict | None:
    """The exchange store the hub sets for its organisation, ``None`` when it sets none (the hub's 404).

    Answers the hub's body: ``uri`` (``s3://`` without a trailing slash), ``connection`` (the name of the stored S3
    connection that opens it), ``max_rows`` (``None`` for no limit), ``set_by`` and ``set_at``.
    """
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


def list_compute_nodes(url: str, access_token: str, *, http: httpx.Client | None = None) -> list:
    """The compute nodes registered on the hub, enabled or not, as the hub lists them.

    Each carries ``id``, ``name``, ``kind``, ``core_url``, ``worker_url``, ``ui_url``, ``enabled`` and ``is_default``.
    """
    nodes = _request_list("GET", url, "/hub/compute-nodes", http, headers=_bearer(access_token))
    for node in nodes:
        if not isinstance(node, dict):
            raise HubRefused(200, f"{url} did not answer /hub/compute-nodes as a Flowfile hub does.")
        _require(node, ("id", "name", "worker_url"), url, "/hub/compute-nodes")
        if not isinstance(node.get("enabled"), bool):
            raise HubRefused(200, f"{url} did not answer /hub/compute-nodes as a Flowfile hub does.")
    return nodes


def mint_node_token(
    url: str, access_token: str, node_id: int, scope: str = "run", *, http: httpx.Client | None = None
) -> dict:
    """A short-lived token the hub signs for one of its nodes, for the signed-in account.

    Answers the hub's body: ``token``, ``node_id``, ``node_name`` (the token's audience), ``scope``, ``kid``,
    ``issued_at``, ``expires_at`` and ``issuer``. The hub shows the token once and keeps none; a node that does
    not exist, is disabled, or a ``dispatch`` scope is the hub's refusal.
    """
    body = _request(
        "POST",
        url,
        "/hub/node-tokens",
        http,
        json={"node_id": node_id, "scope": scope},
        headers=_bearer(access_token),
    )
    return _require(body, ("token", "node_name", "kid", "expires_at", "issuer"), url, "/hub/node-tokens")
