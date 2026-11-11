"""Local accounts of people from an issuer, keyed by the issuer and their id there (``issuer_accounts``).

An account's password is a hash of random bytes nobody keeps, so it signs in through the issuer only.
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
    """This storage dir's node when it joined an issuer with keys of its own; ``None`` otherwise."""
    return hub_node.joined()


def joined_name() -> str | None:
    """This server's name as its issuer knows it; ``None`` when it joined with no keys of its own."""
    return hub_node.resolved().name if joined_with_keys() is not None else None


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
    """A local account for ``person``, named as at the issuer unless taken, flushed for the caller to commit."""
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
