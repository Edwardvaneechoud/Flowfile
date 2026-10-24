"""The exchange store's settings: where results cross between compute backends, managed from the Backends page.

Mounted under ``/system`` in :mod:`flowfile_core.main`, admin-only like the worker pool. ``GET`` answers the store a run
uses now, where it comes from (the environment, the saved file, or nowhere) and the S3 connections it may use; ``POST``
saves a store, or clears it with no URI, and answers the same. ``FLOWFILE_EXCHANGE_URI`` set in the environment keeps
winning: the saved values are kept and reported, and ``env_override`` tells the page so. A save takes effect on the
next run: the settings are read per call (``configs/exchange_config.py``).
"""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.configs import exchange_config
from flowfile_core.configs.exchange_config import ExchangeConfig

router = APIRouter()


class ExchangeStoreInput(BaseModel):
    """Request body for ``POST /system/exchange_store``; no ``uri`` clears the store."""

    uri: str | None = None
    connection: str | None = None
    max_rows: int | None = Field(default=None, ge=0, description="The most rows an input may cross with; none: no cap")


class ExchangeStoreSaved(BaseModel):
    uri: str | None
    connection: str | None
    max_rows: int | None


class ExchangeStoreState(BaseModel):
    """The store a run uses now, where it comes from, what is saved, and the connections a store may use."""

    uri: str | None
    connection: str | None
    max_rows: int | None
    max_rows_error: str | None = Field(
        default=None, description="Why FLOWFILE_EXCHANGE_MAX_ROWS cannot be read; each crossing is refused until fixed."
    )
    source: Literal["env", "saved", "none"]
    env_override: bool = Field(description="FLOWFILE_EXCHANGE_URI is set and overrides the saved URI and connection.")
    saved: ExchangeStoreSaved
    connections: list[str] = Field(description="Names of the S3 connections the store may be opened with.")


def _eligible_connections() -> list[str]:
    """The names of the S3 connections ``exchange_store()`` would open: an administrator's in multi-user mode."""
    from flowfile_core.auth import sharing
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CloudStorageConnection
    from flowfile_core.database.models import User as DbUser

    with get_db_context() as db:
        rows = db.query(CloudStorageConnection.connection_name).filter(CloudStorageConnection.storage_type == "s3")
        if sharing.sharing_enabled():
            rows = rows.join(DbUser).filter(DbUser.is_admin.is_(True))
        return sorted({name for (name,) in rows.all()})


def _state() -> ExchangeStoreState:
    stored = exchange_config.saved()
    effective = exchange_config.effective_store(stored)
    max_rows_error = None
    try:
        max_rows = exchange_config.effective_max_rows(stored)
    except ValueError as e:
        max_rows, max_rows_error = None, str(e)
    if exchange_config.env_pins_store():
        source: Literal["env", "saved", "none"] = "env"
    elif effective.uri:
        source = "saved"
    else:
        source = "none"
    return ExchangeStoreState(
        uri=effective.uri,
        connection=effective.connection,
        max_rows=max_rows,
        max_rows_error=max_rows_error,
        source=source,
        env_override=exchange_config.env_pins_store(),
        saved=ExchangeStoreSaved(uri=stored.uri, connection=stored.connection, max_rows=stored.max_rows),
        connections=_eligible_connections(),
    )


@router.get("/exchange_store", response_model=ExchangeStoreState, tags=["system"])
def get_exchange_store(_current_user: User = Depends(get_current_admin_user)) -> ExchangeStoreState:
    """The exchange store as a run uses it now. Admin-only."""
    return _state()


@router.post("/exchange_store", response_model=ExchangeStoreState, tags=["system"])
def set_exchange_store(
    payload: ExchangeStoreInput, _current_user: User = Depends(get_current_admin_user)
) -> ExchangeStoreState:
    """Save the exchange store, or clear it with no URI, and answer the state as ``GET`` does.

    The URI must be ``s3://`` and name a bucket, and the connection one of the S3 connections the store may use. A set
    ``FLOWFILE_EXCHANGE_URI`` keeps winning after a save; the answer says so.
    """
    uri = (payload.uri or "").strip().rstrip("/")
    if not uri:
        config = ExchangeConfig()
    else:
        parts = urlsplit(uri)
        if parts.scheme != "s3" or not parts.netloc:
            raise HTTPException(
                status_code=422, detail=f"The exchange store must be an s3:// URI naming a bucket, not {uri!r}"
            )
        connection = (payload.connection or "").strip()
        if not connection:
            raise HTTPException(status_code=422, detail="Name the stored S3 connection that opens the exchange store")
        eligible = _eligible_connections()
        if connection not in eligible:
            known = ", ".join(eligible) or "none"
            raise HTTPException(
                status_code=422,
                detail=f"No S3 connection named {connection!r} can open the exchange store; the connections it may "
                f"use: {known}",
            )
        config = ExchangeConfig(uri=uri, connection=connection, max_rows=payload.max_rows)
    try:
        exchange_config.save(config)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Could not save the exchange store: {e}") from e
    return _state()
