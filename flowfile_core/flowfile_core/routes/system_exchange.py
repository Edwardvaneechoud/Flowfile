"""The exchange store's settings: where results cross between compute backends, managed from the Backends page.

Mounted under ``/system`` in :mod:`flowfile_core.main`, admin-only. ``GET`` answers the store a run uses now, its source
(the environment, the saved file, this node's hub, or none) and the S3 connections it may use; ``POST`` saves or clears
it and answers the same. A set ``FLOWFILE_EXCHANGE_URI`` keeps winning over the saved values, and ``env_override`` says
so. On a team node of a hub, both ask the hub for its store with the request's own token: it applies while nothing is
saved here, so clearing the saved store hands the node back to the hub's. A server joined with keys of its own asks
with the admin's hub session, and its hub's store is read-only here (``vended``). The settings are read per call
(``configs/exchange_config.py``), so a save takes effect on the next run.
"""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from flowfile_core.auth.jwt import get_current_admin_user, oauth2_scheme
from flowfile_core.auth.models import User
from flowfile_core.configs import exchange_config
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.hub.exchange_store import joined_server_store, team_node_store

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
    source: Literal["env", "saved", "hub", "none"]
    env_override: bool = Field(description="FLOWFILE_EXCHANGE_URI is set and overrides the saved URI and connection.")
    saved: ExchangeStoreSaved
    hub: ExchangeStoreSaved | None = Field(
        default=None,
        description="The store this node's hub sets, when it is a team node of a hub that sets one; it applies while "
        "nothing is saved here.",
    )
    vended: bool = Field(
        default=False,
        description="The hub's store is opened by what the hub vends each run, sealed to the servers it runs on: on a "
        "server joined to its hub with keys of its own, whose database holds none of the hub's connections.",
    )
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


def _hub_store(token: str | None, user: User) -> ExchangeConfig | None:
    """The store this node's hub sets: a team node's, asked with the request's token, else a joined server's, asked
    with the admin's hub session."""
    return team_node_store(token) or joined_server_store(user.id)


def _state(hub: ExchangeConfig | None = None) -> ExchangeStoreState:
    stored = exchange_config.saved()
    effective = exchange_config.effective_store(stored, hub)
    max_rows_error = None
    try:
        max_rows = exchange_config.effective_max_rows(stored, hub)
    except ValueError as e:
        max_rows, max_rows_error = None, str(e)
    if exchange_config.env_pins_store():
        source: Literal["env", "saved", "hub", "none"] = "env"
    elif stored.uri:
        source = "saved"
    elif hub is not None:
        source = "hub"
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
        hub=None if hub is None else ExchangeStoreSaved(uri=hub.uri, connection=hub.connection, max_rows=hub.max_rows),
        vended=source == "hub" and hub.vended_for is not None,
        connections=_eligible_connections(),
    )


@router.get("/exchange_store", response_model=ExchangeStoreState, tags=["system"])
def get_exchange_store(
    current_user: User = Depends(get_current_admin_user), token: str | None = Depends(oauth2_scheme)
) -> ExchangeStoreState:
    """The exchange store as a run uses it now. Admin-only."""
    return _state(_hub_store(token, current_user))


@router.post("/exchange_store", response_model=ExchangeStoreState, tags=["system"])
def set_exchange_store(
    payload: ExchangeStoreInput,
    current_user: User = Depends(get_current_admin_user),
    token: str | None = Depends(oauth2_scheme),
) -> ExchangeStoreState:
    """Save the exchange store, or clear it with no URI, and answer the state as ``GET`` does.

    The URI must be ``s3://`` and name a bucket, and the connection one of the S3 connections the store may use. A set
    ``FLOWFILE_EXCHANGE_URI`` keeps winning after a save; the answer says so. Clearing it on a team node of a hub that
    sets a store hands the node back to the hub's.
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
    return _state(_hub_store(token, current_user))
