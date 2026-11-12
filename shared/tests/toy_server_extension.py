"""A server extension for tests: it records each hook it is called at and answers what the test set.

``use`` puts any object in the extension's place for one test; this module is what a test names in
``FLOWFILE_SERVER_EXTENSION`` when the loading itself is under test, in this process or a child.
"""

from shared import server_extension

calls: list[tuple] = []
answers: dict = {}
caller_issuer = "Toy"
sealer = "toy"


def use(monkeypatch, extension) -> None:
    """Make ``extension`` this process's loaded extension until the test ends; ``None`` for none."""
    monkeypatch.setattr(server_extension, "_module", extension)
    monkeypatch.setattr(server_extension, "_resolved", True)


def _called(hook: str, *args):
    calls.append((hook, *args))
    return answers.get(hook)


def before_import(service):
    _called("before_import", service)


def add_arguments(parser):
    parser.add_argument("--toy")
    _called("add_arguments")


def bind(service, host, port):
    return _called("bind", service, host, port)


def before_serving(service, host, port, app):
    _called("before_serving", service, host, port)


def serving(service, port, app):
    _called("serving", service, port)
    return lambda: calls.append(("stopped", service))


def routers(service):
    from fastapi import APIRouter

    _called("routers", service)
    router = APIRouter()

    @router.get("/hello")
    def hello() -> dict:
        return {"toy": service}

    return [(router, "/toy")]


def identity():
    return _called("identity")


def caller_claims(token):
    claims = _called("caller_claims", token)
    if isinstance(claims, Exception):
        raise claims
    return claims


def can_open_sealed():
    return True


def open_sealed(sealed, required):
    _called("open_sealed", tuple(sorted(sealed)))
    return {"kind": "secret", "name": "toy", "value": "opened by the toy"}
