"""A hub that keeps a joined server's stored connections, as the hub's contract has it, for the tests of a server
joined with keys.

``ConnectionsHub`` is ``hub_secrets.SecretsHub`` (sign-in, the sealing vend, the person's secrets) with core's
connection routes as the hub mounts them, for an access token the hub issued at a sign-in: cloud storage
``POST``/``PUT``/``DELETE /cloud_connections/cloud_connection`` (the delete by ``?connection_name=``) and ``GET
/cloud_connections/cloud_connections``; Kafka ``POST``/``GET /kafka/connections`` and ``PUT``/``DELETE
/kafka/connections/{id}``; database ``POST``/``PUT``/``GET``/``DELETE /db_connection_lib``. A connection is its kind,
name, owner, id and settings as core's model for the kind serialises them, secrets in plain; ``share`` grants another
person use of one, as core's ``/shares`` does on the hub. A list holds the caller's own, then those shared with them,
marked with ``access`` as core's sharing marks them, never a secret, and only the owner changes or deletes one.

The vend takes ``connections`` (``kind`` and ``name``) as the hub does: each needs ``targets`` (the hub's 422), resolves
for the caller (their own, else one shared with them, the lowest id first) or is the hub's 403 sentence, and is answered
sealed to every target with ``kind: "connection"`` and its settings. ``connection_calls`` keeps each connection request
as ``(method, path, bearer)``, ``connection_vends`` each vend that named connections as ``(uid, body)``.
"""

import json
from datetime import datetime, timezone
from urllib.parse import parse_qs

import httpx
from pydantic import SecretStr

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate, KafkaConnectionOut
from tests.hub.hub_secrets import SecretsHub

CLOUD_PATH = "/cloud_connections/cloud_connection"
CLOUD_LIST = "/cloud_connections/cloud_connections"
KAFKA_PATH = "/kafka/connections"
DATABASE_PATH = "/db_connection_lib"
VEND_PATH = "/hub/vend"
LABELS = {"cloud": "cloud storage connection", "kafka": "Kafka connection", "database": "database connection"}
SETTINGS = {"cloud": FullCloudStorageConnection, "kafka": KafkaConnectionCreate, "database": FullDatabaseConnection}
NO_CONNECTION = (
    "You have no {} named {} on the hub; add it in the editor of a server joined to the hub, "
    "or ask its owner to share it"
)
UNSEALED = "Connections are vended only sealed to the servers that run the step; name them in targets"
OWNER = {"is_owner": True, "access_level": "owner", "shared_by": None}


def serialised(model) -> dict:
    """The model as core serialises it, each ``SecretStr`` as its plain value, as the hub keeps a connection."""
    return {k: v.get_secret_value() if isinstance(v, SecretStr) else v for k, v in model.model_dump().items()}


def _kind_of(route: str) -> str | None:
    if route in (CLOUD_PATH, CLOUD_LIST):
        return "cloud"
    if route == KAFKA_PATH or route.startswith(f"{KAFKA_PATH}/"):
        return "kafka"
    if route == DATABASE_PATH:
        return "database"
    return None


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
                self._connections() if _kind_of(self._route()) else super().do_GET()

            def do_POST(self):
                if _kind_of(self._route()):
                    self._connections()
                elif self._route() == VEND_PATH:
                    self._vend()
                else:
                    super().do_POST()

            def do_PUT(self):
                self._connections() if _kind_of(self._route()) else super().do_PUT()

            def do_DELETE(self):
                self._connections() if _kind_of(self._route()) else super().do_DELETE()

            def _connections(self):
                raw = self._body()
                query = parse_qs(self.path.split("?", 1)[1]) if "?" in self.path else {}
                answer = hub.answer_connections(
                    self.command,
                    self._route(),
                    {key: values[0] for key, values in query.items()},
                    self.headers.get("Authorization"),
                    json.loads(raw) if raw else None,
                )
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
            "shared_with": set(),
            "created_at": now,
            "updated_at": now,
        }
        self.connections.append(row)
        return row

    def share(self, kind: str, name: str, owner: int, with_uid: int) -> None:
        """Grant ``with_uid`` use of ``owner``'s connection, as core's ``/shares`` does on the hub."""
        self._own(kind, name, owner)["shared_with"].add(with_uid)

    def resolve(self, kind: str, uid: int, name: str) -> dict | None:
        """The connection ``name`` means for ``uid``: their own, else one shared with them, the lowest id first."""
        named = [row for row in self.connections if (row["kind"], row["name"]) == (kind, name)]
        own = [row for row in named if row["owner"] == uid]
        shared = [row for row in named if uid in row["shared_with"]]
        return (own or shared or [None])[0]

    def _own(self, kind: str, name: str | None, uid: int, connection_id: int | None = None) -> dict | None:
        for row in self.connections:
            if row["kind"] == kind and row["owner"] == uid and (row["name"] == name or row["id"] == connection_id):
                return row
        return None

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

    def answer_connections(self, method: str, route: str, query: dict, authorization: str | None, body):
        kind = _kind_of(route)
        with self._lock:
            self.connection_calls.append((method, route, (authorization or "").partition(" ")[2]))
        uid = self._caller(authorization)
        if uid is None:
            return 401, {"detail": "Could not validate credentials"}
        if method == "GET":
            mine = [row for row in self.connections if row["kind"] == kind and row["owner"] == uid]
            shared = [row for row in self.connections if row["kind"] == kind and uid in row["shared_with"]]
            return 200, [self._listed(row, uid) for row in mine + shared]
        connection_id = int(route.rsplit("/", 1)[1]) if route.startswith(f"{KAFKA_PATH}/") else None
        if method == "POST":
            if self._own(kind, body["connection_name"], uid) is not None:
                taken = f"Kafka connection '{body['connection_name']}' already exists"
                return 422, {"detail": taken if kind == "kafka" else "Connection name already exists"}
            row = self.add(kind, uid, body)
            if kind == "kafka":
                return 200, self._listed(row, uid)
            return 200, {"message": f"{'Cloud' if kind == 'cloud' else 'Database'} connection created successfully"}
        name = query.get("connection_name") if method == "DELETE" else (body or {}).get("connection_name")
        row = self._own(kind, name, uid, connection_id)
        if row is None:
            if kind == "kafka":
                return 404, {"detail": f"Kafka connection {connection_id} not found"}
            missing = "Cloud connection connection not found" if method == "DELETE" else "Cloud connection not found"
            return 404, {"detail": missing if kind == "cloud" else "Database connection not found"}
        label = {"cloud": "Cloud", "kafka": "Kafka", "database": "Database"}[kind]
        if method == "DELETE":
            self.connections.remove(row)
            return 200, {"message": f"{label} connection deleted successfully"}
        changed = {key: value for key, value in body.items() if value is not None}
        row["settings"] = serialised(SETTINGS[kind].model_validate({**row["settings"], **changed}))
        row["name"] = row["settings"]["connection_name"]
        if kind == "kafka":
            return 200, self._listed(row, uid)
        return 200, {"message": f"{label} connection updated successfully"}

    def vend_with_connections(self, request: httpx.Request, body: dict) -> tuple[int, object]:
        """The hub's vend: the catalog's answer, with each connection the body names sealed to every target."""
        refs = [(ref["kind"], ref["name"]) for ref in body.get("connections") or []]
        uid = self._caller(request.headers.get("Authorization"))
        if refs:
            with self._lock:
                self.connection_vends.append((uid, body))
            if not body.get("targets"):
                return 422, {"detail": UNSEALED}
            for kind, name in refs:
                if uid is None or self.resolve(kind, uid, name) is None:
                    return 403, {"detail": NO_CONNECTION.format(LABELS[kind], name)}
        response = self.catalog.handle(request)
        answer = response.json()
        if response.status_code != 200:
            return response.status_code, answer
        answer["connections"] = [
            {
                "kind": kind,
                "name": name,
                "sealed": {
                    node: self.signer.sealed_connection(
                        self.catalog.enc_keys[node],
                        node=node,
                        iss=self.issuer,
                        connection_kind=kind,
                        name=name,
                        settings=self.resolve(kind, uid, name)["settings"],
                        run_id=body["run_id"],
                        vend_id=answer["vend_id"],
                    )
                    for node in body["targets"]
                },
            }
            for kind, name in dict.fromkeys(refs)
        ]
        return 200, answer
