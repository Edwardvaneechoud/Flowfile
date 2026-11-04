"""The local accounts of people from a hub, on a server that joined its hub with keys of its own.

Such a server has people of its own, so a person from the hub is not found by a local id or a username: the table
``issuer_accounts`` maps the hub's issuer and the person's id there to one local account. The account is made the first
time the person comes, named as on the hub unless a local account already holds that name, and its password is a hash
of random bytes nobody keeps, so it signs in through the hub only.
"""

import secrets
from urllib.parse import urlparse

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.password import get_password_hash
from flowfile_core.database import models as db_models
from shared import hub_node


def joined_with_keys() -> hub_node.HubNode | None:
    """``hub_node.joined()``: this storage dir's node when it joined a hub with keys of its own, so people sign in here
    through that hub. Core's name for it, which its routes, placement and vend read."""
    return hub_node.joined()


def hub_host(url: str) -> str:
    return urlparse(url).hostname or url


def issuer_account(db: Session, issuer: str, subject_id: int) -> db_models.User | None:
    """The local account of the issuer's person ``subject_id``; a mapping whose account is gone is dropped."""
    mapping = (
        db.query(db_models.IssuerAccount)
        .filter(db_models.IssuerAccount.issuer == issuer, db_models.IssuerAccount.subject_id == subject_id)
        .first()
    )
    if mapping is None:
        return None
    user = db.query(db_models.User).filter(db_models.User.id == mapping.user_id).first()
    if user is None:
        db.delete(mapping)
        db.flush()
    return user


def _free_username(db: Session, wanted: str, host: str) -> str | None:
    for candidate in (wanted, f"{wanted}@{host}"):
        if candidate == sharing.INTERNAL_SERVICE_USERNAME:
            continue
        if db.query(db_models.User.id).filter(db_models.User.username == candidate).first() is None:
            return candidate
    return None


def new_issuer_account(db: Session, issuer: str, hub_url: str, person: dict) -> db_models.User:
    """A local account for a person from the hub, named as on the hub unless a local account already holds that name.

    ``person`` is the hub's ``uid``, ``username``, ``full_name`` and ``node_admin``. The account and its mapping are
    added to ``db`` and flushed, for the caller to commit; both names taken is a 409.
    """
    host = hub_host(hub_url)
    username = _free_username(db, person["username"], host)
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"This server already has accounts named {person['username']} and {person['username']}@{host}; "
            "an administrator removes one of them before this person can sign in here.",
        )
    user = db_models.User(
        username=username,
        email=None,
        full_name=person["full_name"],
        hashed_password=get_password_hash(secrets.token_urlsafe(32)),
        disabled=False,
        is_admin=person["node_admin"],
        must_change_password=False,
    )
    db.add(user)
    db.flush()
    db.add(db_models.IssuerAccount(issuer=issuer, subject_id=person["uid"], user_id=user.id))
    return user


def subject_id(db: Session, user_id: int) -> int | None:
    """The id on this node's hub of the person whose local account is ``user_id``; ``None`` for an account of the
    server's own, or one mapped to another issuer. The move of a joined server's secrets to the hub names each owner
    by it."""
    issuer = hub_node.resolved().issuer
    if issuer is None:
        return None
    mapping = (
        db.query(db_models.IssuerAccount)
        .filter(db_models.IssuerAccount.issuer == issuer, db_models.IssuerAccount.user_id == user_id)
        .first()
    )
    return None if mapping is None else mapping.subject_id
