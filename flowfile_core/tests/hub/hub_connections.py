"""A hub that lists a person's stored connections and seals them for a run, as the hub's contract has it, for the
tests of a server joined with keys.

``ConnectionsHub`` is ``hub_secrets.SecretsHub`` (sign-in, the sealing vend, the person's secrets) with core's
connection lists as the hub mounts them, for an access token the hub issued at a sign-in: ``GET
/cloud_connections/cloud_connections``, ``GET /kafka/connections`` and ``GET /db_connection_lib``. A connection is its
kind, name, owner, id and settings as core's model for the kind serialises them, secrets in plain (``add``); ``share``
grants another person use of one, as core's ``/shares`` does on the hub. A list holds the caller's own, then those
shared with them, marked with ``access`` as core's sharing marks them, never a secret. A machine signed in to the hub
asks it nothing else about a connection but the vend: ``connection_calls`` keeps each request to the connection routes
as ``(method, path, bearer)``, and any but a list is the hub's 405.

The vend takes ``connections`` (``kind`` and ``name``) as the hub does: each resolves for the caller (their own, else
one shared with them, the lowest id first) and is answered by the catalog, sealed to every target with its settings and
the names of its secret fields; one the caller reaches none of is the catalog's 403 sentence. ``connection_vends``
keeps each vend that named connections as ``(uid, body)``.
"""

import json
from datetime import datetime, timezone

import httpx
from pydantic import SecretStr

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate, KafkaConnectionOut
from tests.hub.hub_secrets import SecretsHub

LIST_PATHS = {
    "/cloud_connections/cloud_connections": "cloud",
    "/kafka/connections": "kafka",
    "/db_connection_lib": "database",
}
PATHS = ("/cloud_connections/cloud_connection", *LIST_PATHS)
VEND_PATH = "/hub/vend"
SETTINGS = {"cloud": FullCloudStorageConnection, "kafka": KafkaConnectionCreate, "database": FullDatabaseConnection}
OWNER = {"is_owner": True, "access_level": "owner", "shared_by": None}


def serialised(model) -> dict:
    """The model as core serialises it, each ``SecretStr`` as its plain value, as the hub keeps a connection."""
    return {k: v.get_secret_value() if isinstance(v, SecretStr) else v for k, v in model.model_dump().items()}


def _ours(route: str) -> bool:
    return route in PATHS or any(route.startswith(f"{path}/") for path in PATHS)


class ConnectionsHub(SecretsHub):
    def __init__(self, signer):
        super().__init__(signer)
        self.connections: list[dict] = []
        self.connection_calls: list[tuple[str, str, str]] = []
        self.connection_vends: list[tuple[int, dict]] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def _route(self) -> str:
                return self.path.split("?", 1)[0]

            def _body(self) -> bytes:
                length = int(self.headers.get("Content-Length") or 0)
                return self.rfile.read(length) if length else b""

            def do_GET(self):
                self._connections() if _ours(self._route()) else super().do_GET()

            def do_POST(self):
                if _ours(self._route()):
                    self._connections()
                elif self._route() == VEND_PATH:
                    self._vend()
                else:
                    super().do_POST()

            def do_PUT(self):
                self._connections() if _ours(self._route()) else super().do_PUT()

            def do_DELETE(self):
                self._connections() if _ours(self._route()) else super().do_DELETE()

            def _connections(self):
                self._body()
                answer = hub.answer_connections(self.command, self._route(), self.headers.get("Authorization"))
                self._answer(*answer)

            def _vend(self):
                raw = self._body()
                request = httpx.Request("POST", hub.url + self.path, headers=list(self.headers.items()), content=raw)
                _, _, token = (self.headers.get("Authorization") or "").partition(" ")
                with hub._lock:
                    hub.received.append(("POST", VEND_PATH, token))
                self._answer(*hub.vend_with_connections(request, json.loads(raw) if raw else {}))

        self._server.RequestHandlerClass = Handler

    def _caller(self, authorization: str | None) -> int | None:
        scheme, _, token = (authorization or "").partition(" ")
        with self._lock:
            return self.issued.get(token) if scheme == "Bearer" else None

    def add(self, kind: str, owner: int, settings: dict) -> dict:
        """Store ``settings``, a model of ``kind`` as the hub's route takes it, as ``owner``'s connection."""
        now = datetime.now(timezone.utc).isoformat()
        model = SETTINGS[kind].model_validate(settings)
        row = {
            "id": len(self.connections) + 1,
            "kind": kind,
            "name": model.connection_name,
            "owner": owner,
            "settings": serialised(model),
            "secret_fields": tuple(name for name, value in model if isinstance(value, SecretStr)),
            "shared_with": set(),
            "created_at": now,
            "updated_at": now,
        }
        self.connections.append(row)
        return row

    def share(self, kind: str, name: str, owner: int, with_uid: int) -> None:
        """Grant ``with_uid`` use of ``owner``'s connection, as core's ``/shares`` does on the hub."""
        own = next(row for row in self.connections if (row["kind"], row["name"], row["owner"]) == (kind, name, owner))
        own["shared_with"].add(with_uid)

    def resolve(self, kind: str, uid: int, name: str) -> dict | None:
        """The connection ``name`` means for ``uid``: their own, else one shared with them, the lowest id first."""
        named = [row for row in self.connections if (row["kind"], row["name"]) == (kind, name)]
        own = [row for row in named if row["owner"] == uid]
        shared = [row for row in named if uid in row["shared_with"]]
        return (own or shared or [None])[0]

    def _listed(self, row: dict, uid: int) -> dict:
        settings = {**row["settings"], "id": row["id"]}
        if row["owner"] == uid:
            access = OWNER
        else:
            access = {"is_owner": False, "access_level": "use", "shared_by": self.people[row["owner"]]["username"]}
        if row["kind"] == "kafka":
            settings.update(created_at=row["created_at"], updated_at=row["updated_at"])
            return KafkaConnectionOut.model_validate({**settings, "access": access}).model_dump(mode="json")
        interface = FullCloudStorageConnectionInterface if row["kind"] == "cloud" else FullDatabaseConnectionInterface
        return interface.model_validate({**settings, "access": access}).model_dump(mode="json")

    def answer_connections(self, method: str, route: str, authorization: str | None) -> tuple[int, object]:
        with self._lock:
            self.connection_calls.append((method, route, (authorization or "").partition(" ")[2]))
        uid = self._caller(authorization)
        if uid is None:
            return 401, {"detail": "Could not validate credentials"}
        kind = LIST_PATHS.get(route)
        if method != "GET" or kind is None:
            return 405, {"detail": "Method Not Allowed"}
        mine = [row for row in self.connections if row["kind"] == kind and row["owner"] == uid]
        shared = [row for row in self.connections if row["kind"] == kind and uid in row["shared_with"]]
        return 200, [self._listed(row, uid) for row in mine + shared]

    def vend_with_connections(self, request: httpx.Request, body: dict) -> tuple[int, object]:
        """The hub's vend: the catalog's answer, holding for it each connection the body names as the caller reaches
        it."""
        refs = [(ref["kind"], ref["name"]) for ref in body.get("connections") or []]
        uid = self._caller(request.headers.get("Authorization"))
        if refs:
            with self._lock:
                self.connection_vends.append((uid, body))
        reached = {(kind, name): self.resolve(kind, uid, name) if uid is not None else None for kind, name in refs}
        self.catalog.stored_connections = {
            ref: (row["settings"], row["secret_fields"]) for ref, row in reached.items() if row is not None
        }
        response = self.catalog.handle(request)
        return response.status_code, response.json()
