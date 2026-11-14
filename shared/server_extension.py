"""The one module a server's own configuration names to extend it, loaded on first use.

``FLOWFILE_SERVER_EXTENSION`` names an importable module. Unset, which is every desktop and every plain server, nothing
is imported and every call here answers as a Flowfile on its own does. Only the module the operator named is loaded,
never one that happens to be installed, since a hook decides who signs in and who may call the worker. A named module
that cannot be imported stops the process: a server that ran without it would be another server.

The module defines the hooks it needs and no others (``HOOKS``):

- ``before_import(service)``: before ``service`` (``core`` or ``worker``) reads its storage dir, in a main process.
- ``add_arguments(parser)``: its flags on a command-line parser that refuses unknown ones.
- ``before_serving(service, host, port, app)``: the last step before the server takes requests; it answers the
  socket to serve on when it bound one, else ``None``, and closes that socket itself when it stops the start.
- ``serving(service, port, app)``: starts what runs while serving and answers a callable that stops it, or ``None``.
- ``routers(service)``: ``(router, prefix)`` pairs to mount.
- ``identity()``: this server as a controller knows it, an ``Identity``, or ``None``.
- ``sign_in_method()``: how people sign in here, as ``GET /auth/sign_in_method`` answers it.
- ``password_refusal()``: the sentence that refuses a password sign-in, or ``None``.
- ``refresh_session(db, user, sid)``: the token pair of an account that signs in elsewhere, ``None`` for a local one.
- ``sign_out(user_id)`` and ``account_removed(db, user_id)``.
- ``refresh_delegation(hub_url, refresh_token)``: a new delegation for a session of that kind.
- ``caller_claims(token)``: the claims of a token whose holder may call the worker, ``None`` when it is no such
  token, ``Refused`` when it is one this server does not take.
- ``can_open_sealed()`` and ``open_sealed(sealed, required)``: the claims of this server's entry of a sealed map.

and may set ``caller_issuer`` and ``sealer``, the words a refusal names it by.

A storage dir an extension marked (``mark_storage_dir``) is served only with that extension loaded
(``require_marked``), so a server set up to sign people in elsewhere never comes up taking passwords.
"""

import importlib
import logging
import multiprocessing
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

VARIABLE = "FLOWFILE_SERVER_EXTENSION"
MARKER_FILE = "server_extension"
HOOKS = frozenset(
    {
        "before_import",
        "add_arguments",
        "before_serving",
        "serving",
        "routers",
        "identity",
        "sign_in_method",
        "password_refusal",
        "refresh_session",
        "sign_out",
        "account_removed",
        "refresh_delegation",
        "caller_claims",
        "can_open_sealed",
        "open_sealed",
    }
)

logger = logging.getLogger("flowfile.server_extension")

_lock = threading.Lock()
_module: Any = None
_resolved = False


class Refused(Exception):
    """What an extension refuses; ``reason`` is fit to answer and never names a token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Identity:
    """This server as its controller knows it.

    ``joined`` says it holds keys of its own there. ``recorded_name`` is the name its storage dir keeps, ``None`` for a
    server named by its environment alone.
    """

    name: str | None
    url: str | None
    issuer: str | None
    node_id: int | None = None
    joined: bool = False
    recorded_name: str | None = None


def named() -> str | None:
    """The module this process's configuration names, read on every call."""
    return os.environ.get(VARIABLE, "").strip() or None


def load() -> Any:
    """The named module, imported on the first call; ``None`` when none is named."""
    global _module, _resolved
    if _resolved:
        return _module
    with _lock:
        if not _resolved:
            name = named()
            if name is not None:
                try:
                    _module = importlib.import_module(name)
                except ImportError as exc:
                    raise SystemExit(f"{VARIABLE} names {name}, which cannot be imported here: {exc}") from None
            _resolved = True
    return _module


def reset() -> None:
    """Forget what was loaded, so the next call reads the variable again; for a test that changes it."""
    global _module, _resolved
    with _lock:
        _module, _resolved = None, False


def defines(hook: str) -> bool:
    return getattr(load(), _known(hook), None) is not None


def call(hook: str, *args: Any, default: Any = None, **kwargs: Any) -> Any:
    """What the extension's ``hook`` answers; ``default`` when no extension is named or it does not define the hook."""
    found = getattr(load(), _known(hook), None)
    return default if found is None else found(*args, **kwargs)


def word(name: str, default: str) -> str:
    """The extension's word ``name`` (``caller_issuer``, ``sealer``), else ``default``."""
    return getattr(load(), name, None) or default


def _known(hook: str) -> str:
    if hook not in HOOKS:
        raise ValueError(f"{hook} is no server extension hook")
    return hook


def before_import(service: str) -> None:
    """Run ``before_import`` in a main process; a spawned child loads nothing for it."""
    if named() is not None and multiprocessing.current_process().name == "MainProcess":
        call("before_import", service)


def identity() -> Identity | None:
    return call("identity")


def joined() -> Identity | None:
    """This server's identity when it joined a controller with keys of its own, else ``None``."""
    found = identity()
    return found if found is not None and found.joined else None


def _marker() -> Path:
    from shared.storage_config import storage

    return storage.base_directory / MARKER_FILE


def mark_storage_dir(module_name: str, directory: Path | None = None) -> None:
    """Record that the storage dir (``directory`` when the caller holds it) is served only with ``module_name``
    loaded."""
    target = directory / MARKER_FILE if directory is not None else _marker()
    if target.is_file() and target.read_text(encoding="utf-8").strip() == module_name:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(module_name + "\n", encoding="utf-8")
    os.replace(temporary, target)


def require_marked() -> None:
    """Exit 2 when this storage dir is marked for an extension that is not the one loaded."""
    try:
        wanted = _marker().read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return
    except OSError as exc:
        wanted = f"an extension its marker does not name ({type(exc).__name__})"
    if wanted and wanted != named():
        logger.error(
            "This storage dir is served only with the server extension %s: set %s=%s for this server, as it was "
            "set up with, or serve another storage dir",
            wanted,
            VARIABLE,
            wanted,
        )
        raise SystemExit(2)
