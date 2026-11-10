"""A hub that lists a person's secrets, as the hub's contract has it, for the tests of a server joined with keys.

``SecretsHub`` is ``test_server_sealed_e2e``'s ``SealingHub`` (sign-in, the sealing vend) with core's ``GET
/secrets/secrets`` as the hub mounts it, for an access token the hub issued at a sign-in: the caller's own secrets,
never a value. The hub's secrets are its catalog's ``secret_values``, so the vend seals what ``keep`` stored;
``owners`` says whose each is. A machine signed in to the hub asks it nothing else about a secret: ``secret_calls``
keeps each request to the secrets routes as ``(method, path, bearer)``, and any but the list is the hub's 405.
"""

from tests.hub.test_server_sealed_e2e import SealingHub

SECRETS_PATH = "/secrets/secrets"


class SecretsHub(SealingHub):
    def __init__(self, signer):
        super().__init__(signer)
        self.owners: dict[str, int] = {}
        self.secret_calls: list[tuple[str, str, str]] = []
        self._ids: dict[str, int] = {}
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def _ours(self) -> bool:
                route = self.path.split("?", 1)[0]
                return route == SECRETS_PATH or route.startswith(f"{SECRETS_PATH}/")

            def do_GET(self):
                self._secrets() if self._ours() else super().do_GET()

            def do_POST(self):
                self._secrets() if self._ours() else super().do_POST()

            def do_PUT(self):
                self._secrets()

            def do_DELETE(self):
                self._secrets()

            def _secrets(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                route = self.path.split("?", 1)[0]
                self._answer(*hub.answer_secrets(self.command, route, self.headers.get("Authorization")))

        self._server.RequestHandlerClass = Handler

    @property
    def values(self) -> dict[str, str]:
        return self.catalog.secret_values

    def keep(self, uid: int, name: str, value: str) -> dict:
        """Hold ``value`` as ``uid``'s secret ``name``, as a person adds one on the hub; answers its list row."""
        self.values[name] = value
        self.owners[name] = uid
        self._ids.setdefault(name, len(self._ids) + 1)
        return self._row(name)

    def _row(self, name: str) -> dict:
        return {
            "name": name,
            "value": "**********",
            "user_id": str(self.owners[name]),
            "id": self._ids[name],
            "access": {"is_owner": True, "access_level": "owner", "shared_by": None},
        }

    def answer_secrets(self, method: str, route: str, authorization: str | None) -> tuple[int, object]:
        scheme, _, token = (authorization or "").partition(" ")
        with self._lock:
            self.secret_calls.append((method, route, token))
            uid = self.issued.get(token) if scheme == "Bearer" else None
        if uid is None:
            return 401, {"detail": "Could not validate credentials"}
        if (method, route) != ("GET", SECRETS_PATH):
            return 405, {"detail": "Method Not Allowed"}
        return 200, [self._row(name) for name in sorted(self.owners) if self.owners[name] == uid]
