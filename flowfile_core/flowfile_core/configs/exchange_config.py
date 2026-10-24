"""The exchange store's saved configuration: what the Backends page writes, read under the environment.

``FLOWFILE_EXCHANGE_URI``, ``FLOWFILE_EXCHANGE_CONNECTION`` and ``FLOWFILE_EXCHANGE_MAX_ROWS`` keep winning when set, so
a deployment's compose file pins the store. Without the URI variable, the store is what the page saved to
``exchange_store.yaml`` under core's internal storage, read per call as the variables are, so a save takes effect on
the next run with no restart; the row limit comes from its variable when set, else from the file.
``settings.get_exchange_uri`` and its siblings answer from ``effective_store`` and ``effective_max_rows``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from shared.storage_config import storage

URI_VAR = "FLOWFILE_EXCHANGE_URI"
CONNECTION_VAR = "FLOWFILE_EXCHANGE_CONNECTION"
MAX_ROWS_VAR = "FLOWFILE_EXCHANGE_MAX_ROWS"


@dataclass(frozen=True)
class ExchangeConfig:
    """Where results cross between backends (``uri``), the stored S3 connection that opens it, and the row limit."""

    uri: str | None = None
    connection: str | None = None
    max_rows: int | None = None


def _settings_file() -> Path:
    return storage.base_directory / "exchange_store.yaml"


def env_pins_store() -> bool:
    """Whether ``FLOWFILE_EXCHANGE_URI`` is set, in which case the saved URI and connection do not apply."""
    return bool(os.environ.get(URI_VAR))


def env_pins_max_rows() -> bool:
    return bool(os.environ.get(MAX_ROWS_VAR))


def saved() -> ExchangeConfig:
    """What the page saved; a missing or unreadable file is no store."""
    import yaml

    try:
        data = yaml.safe_load(_settings_file().read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return ExchangeConfig()
    if not isinstance(data, dict):
        return ExchangeConfig()
    uri = data.get("uri")
    connection = data.get("connection")
    max_rows = data.get("max_rows")
    return ExchangeConfig(
        uri=uri if isinstance(uri, str) and uri else None,
        connection=connection if isinstance(connection, str) and connection else None,
        max_rows=max_rows if isinstance(max_rows, int) and not isinstance(max_rows, bool) and max_rows >= 0 else None,
    )


def save(config: ExchangeConfig) -> None:
    """Write ``config`` for the next call to read; atomic, so a reader never sees half a file.

    Raises:
        OSError: The file could not be written.
    """
    import yaml

    target = _settings_file()
    target.parent.mkdir(parents=True, exist_ok=True)
    body = yaml.safe_dump(
        {"uri": config.uri, "connection": config.connection, "max_rows": config.max_rows}, sort_keys=False
    )
    content = (
        "# The exchange store, where results cross between compute backends: managed from Settings → Execution → "
        "Backends.\n"
        "# FLOWFILE_EXCHANGE_URI, when set, overrides the uri and connection here; FLOWFILE_EXCHANGE_MAX_ROWS the row "
        "limit.\n" + body
    )
    tmp = target.with_suffix(".yaml.tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, target)


def effective_store(stored: ExchangeConfig | None = None) -> ExchangeConfig:
    """The store's URI and connection a run uses, with no row limit; never raises, so a bad limit stops no run."""
    if env_pins_store():
        return ExchangeConfig(uri=os.environ.get(URI_VAR) or None, connection=os.environ.get(CONNECTION_VAR) or None)
    stored = saved() if stored is None else stored
    return ExchangeConfig(uri=stored.uri, connection=stored.connection)


def effective_max_rows(stored: ExchangeConfig | None = None) -> int | None:
    """The row limit a crossing is held to: ``FLOWFILE_EXCHANGE_MAX_ROWS`` when set, else the saved one.

    Raises:
        ValueError: ``FLOWFILE_EXCHANGE_MAX_ROWS`` is set to something other than a whole number.
    """
    if env_pins_max_rows():
        value = os.environ[MAX_ROWS_VAR]
        if not value.isdecimal():
            raise ValueError(f"{MAX_ROWS_VAR} must be a whole number of rows, not {value!r}")
        return int(value)
    return (saved() if stored is None else stored).max_rows


def effective() -> ExchangeConfig:
    """The store a run uses: the environment where it pins, the saved file otherwise.

    Raises:
        ValueError: ``FLOWFILE_EXCHANGE_MAX_ROWS`` is set to something other than a whole number.
    """
    stored = saved()
    store = effective_store(stored)
    return ExchangeConfig(uri=store.uri, connection=store.connection, max_rows=effective_max_rows(stored))
