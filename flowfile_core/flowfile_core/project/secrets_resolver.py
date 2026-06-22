"""Resolve ${secret:NAME} placeholders from env vars / an untracked .env."""

from __future__ import annotations

import os
import re
from pathlib import Path

_PLACEHOLDER_RE = re.compile(r"^\$\{secret:([^}]+)\}$")
_ENV_PREFIX = "FLOWFILE_SECRET_"


def make_placeholder(name: str) -> str:
    return f"${{secret:{name}}}"


def placeholder_name(value: object) -> str | None:
    """Return the secret NAME if value is a ${secret:NAME} placeholder, else None."""
    if not isinstance(value, str):
        return None
    m = _PLACEHOLDER_RE.match(value.strip())
    return m.group(1) if m else None


def env_key(name: str) -> str:
    return _ENV_PREFIX + re.sub(r"[^A-Za-z0-9]+", "_", name).upper()


def load_dotenv(root: Path) -> dict[str, str]:
    env_file = root / ".env"
    values: dict[str, str] = {}
    if not env_file.exists():
        return values
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        values[key.strip()] = val.strip().strip('"').strip("'")
    return values


def resolve(name: str, dotenv: dict[str, str]) -> str | None:
    """Env var wins over .env; None when neither defines the secret."""
    key = env_key(name)
    if key in os.environ:
        return os.environ[key]
    return dotenv.get(key)
