"""Resolve secret *values* on apply from env vars / an untracked ``.env``.

Placeholders in connection files are ``${secret:NAME}``. On a fresh clone the
value behind ``NAME`` is refilled from:

1. ``FLOWFILE_SECRET_<UPPER_SNAKE_NAME>`` environment variable, else
2. the same key in an untracked ``.env`` at the project root.

Interactive wizard and external vault are later phases. Values are never written
to the tree; the resolver only reads them so ``apply`` can re-encrypt under the
local user's key.
"""

from __future__ import annotations

import re
from pathlib import Path

ENV_PREFIX = "FLOWFILE_SECRET_"

_UPPER_RE = re.compile(r"[^A-Z0-9]+")
_PLACEHOLDER_RE = re.compile(r"^\$\{secret:(?P<name>.+)\}$")


def env_var_name(secret_name: str) -> str:
    """``prod_postgres_password`` -> ``FLOWFILE_SECRET_PROD_POSTGRES_PASSWORD``."""
    upper = _UPPER_RE.sub("_", secret_name.strip().upper()).strip("_")
    return f"{ENV_PREFIX}{upper}"


def parse_placeholder(value: str | None) -> str | None:
    """Return the secret name from ``${secret:NAME}`` or ``None`` if not one."""
    if not isinstance(value, str):
        return None
    match = _PLACEHOLDER_RE.match(value.strip())
    return match.group("name") if match else None


def make_placeholder(secret_name: str) -> str:
    return f"${{secret:{secret_name}}}"


def _parse_dotenv(path: Path) -> dict[str, str]:
    """Minimal ``.env`` parser (KEY=VALUE, ``#`` comments, optional quotes).

    Deliberately dependency-free; we only need flat key/value pairs.
    """
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.lower().startswith("export "):
            line = line[len("export ") :].strip()
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key:
            env[key] = value
    return env


class SecretResolver:
    """Resolves secret names to values from the environment and project ``.env``."""

    def __init__(self, project_root: str | Path, environ: dict[str, str] | None = None) -> None:
        import os

        self._environ = environ if environ is not None else dict(os.environ)
        self._dotenv = _parse_dotenv(Path(project_root) / ".env")

    def resolve(self, secret_name: str) -> tuple[str | None, str | None]:
        """Return ``(value, source)``; ``source`` is ``"env"``/``"dotenv"``/``None``."""
        key = env_var_name(secret_name)
        if key in self._environ:
            return self._environ[key], "env"
        if key in self._dotenv:
            return self._dotenv[key], "dotenv"
        return None, None
