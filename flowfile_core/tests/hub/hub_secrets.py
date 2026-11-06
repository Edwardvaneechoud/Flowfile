"""A hub that keeps a joined server's secrets, as the hub's contract has it, for the tests of a server joined with keys.

``SecretsHub`` is ``test_server_sealed_e2e``'s ``SealingHub`` (sign-in, the sealing vend) with the routes a server
joined with keys calls for its people's secrets: core's ``/secrets/secrets`` routes as the hub mounts them, for an
access token the hub issued at a sign-in (list without values, add, ``GET`` by name, ``PUT`` a new value, delete), and
``POST /hub/nodes/secrets/import`` for a node assertion addressed to the issuer and that path, which moves each entry
into its person's secrets or refuses it with the hub's sentence. The hub's secrets are its catalog's
``secret_values``, so the vend seals what the routes stored; ``owners`` says whose each is. ``secret_calls`` keeps each
secrets request as ``(method, path, bearer)``, ``imported`` each import's entries without their values.
"""

import json
from urllib.parse import unquote

from tests.hub.test_server_sealed_e2e import SealingHub

SECRETS_PATH = "/secrets/secrets"
IMPORT_PATH = "/hub/nodes/secrets/import"
NOT_FOUND = "Secret not found"
TAKEN = "Secret with this name already exists"
NOT_ALLOWED = "this person may not work on this server now; an admin of the hub can say why"


def no_person(uid: int) -> str:
    return f"the hub has no person with id {uid}"


def already_there(name: str) -> str:
    return f"a secret named {name} is already on the hub"


class SecretsHub(SealingHub):
    def __init__(self, signer):
        super().__init__(signer)
        self.owners: dict[str, int] = {}
        self.secret_calls: list[tuple[str, str, str]] = []
        self.imported: list[list[dict]] = []
        self._ids: dict[str, int] = {}
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def _ours(self) -> bool:
                route = self.path.split("?", 1)[0]
                return route == SECRETS_PATH or route.startswith(f"{SECRETS_PATH}/") or route == IMPORT_PATH

            def do_GET(self):
                self._secrets() if self._ours() else super().do_GET()

            def do_POST(self):
                self._secrets() if self._ours() else super().do_POST()

            def do_PUT(self):
                self._secrets()

            def do_DELETE(self):
                self._secrets()

            def _secrets(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else None
                route = self.path.split("?", 1)[0]
                self._answer(*hub.answer_secrets(self.command, route, self.headers.get("Authorization"), body))

        self._server.RequestHandlerClass = Handler

    @property
    def values(self) -> dict[str, str]:
        return self.catalog.secret_values

    def _row(self, name: str) -> dict:
        return {
            "name": name,
            "value": "**********",
            "user_id": str(self.owners[name]),
            "id": self._ids[name],
            "access": {"is_owner": True, "access_level": "owner", "shared_by": None},
        }

    def _store(self, uid: int, name: str, value: str) -> None:
        self.values[name] = value
        self.owners[name] = uid
        self._ids.setdefault(name, len(self._ids) + 1)

    def answer_secrets(self, method: str, route: str, authorization: str | None, body) -> tuple[int, object]:
        scheme, _, token = (authorization or "").partition(" ")
        if route == IMPORT_PATH:
            return self._import(token if scheme == "Bearer" else "", body)
        with self._lock:
            self.secret_calls.append((method, route, token))
            uid = self.issued.get(token) if scheme == "Bearer" else None
        if uid is None:
            return 401, {"detail": "Could not validate credentials"}
        own = sorted(name for name, owner in self.owners.items() if owner == uid)
        if route == SECRETS_PATH:
            if method == "GET":
                return 200, [self._row(name) for name in own]
            if body["name"] in own:
                return 400, {"detail": TAKEN}
            self._store(uid, body["name"], body["value"])
            return 200, self._row(body["name"])
        name = unquote(route.removeprefix(f"{SECRETS_PATH}/"))
        if name not in own:
            return 404, {"detail": NOT_FOUND}
        if method == "PUT":
            self.values[name] = body["value"]
        if method == "DELETE":
            del self.values[name], self.owners[name]
            return 204, None
        return 200, self._row(name)

    def _import(self, assertion: str, body) -> tuple[int, object]:
        if not assertion:
            return 401, {"detail": "an import needs a node assertion"}
        refusal, _ = self.check_assertion(assertion, IMPORT_PATH)
        if refusal is not None:
            return 401, {"detail": refusal}
        entries = body["secrets"]
        results = []
        for entry in entries:
            uid, name = entry["hub_user_id"], entry["name"]
            if uid not in self.people:
                reason = no_person(uid)
            elif uid in self.refused_people:
                reason = NOT_ALLOWED
            elif name in self.values:
                reason = already_there(name)
            else:
                reason = None
                self._store(uid, name, entry["value"])
            results.append({"hub_user_id": uid, "name": name, "moved": reason is None, "reason": reason})
        self.imported.append([{"hub_user_id": e["hub_user_id"], "name": e["name"]} for e in entries])
        return 200, {"results": results}
