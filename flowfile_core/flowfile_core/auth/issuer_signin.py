"""Sign-in through an external issuer: the local account and session of a person whose verified token it signed.

It knows no issuer: its caller says who the issuer is, who the person is there, and how long the issuer's word holds.
"""

import time
from datetime import timedelta

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from flowfile_core.auth import issuer_accounts
from flowfile_core.auth.jwt import create_access_token, create_refresh_token, invalidate_user_cache
from flowfile_core.database import models as db_models


class SignInRefused(Exception):
    """A sign-in this server will not make; the message is one sentence fit to answer."""


def expires_at(seconds: int) -> int:
    """An access token's expiry in milliseconds since the epoch, taken before it is signed so it is never late."""
    return int(time.time() + seconds) * 1000


def refuse_disabled(db: Session, issuer: str, subject_id: int) -> None:
    """``SignInRefused`` when ``subject_id``'s account here is disabled, asked before anything is traded for them."""
    user = issuer_accounts.issuer_account(db, issuer, subject_id)
    if user is not None and user.disabled:
        raise SignInRefused("This account is disabled on this server.")


def admit(db: Session, issuer: str, issuer_url: str, person: dict) -> db_models.User:
    """The local account of ``person``, made at a first sign-in, an admin as ``node_admin`` says at every one."""
    user = issuer_accounts.issuer_account(db, issuer, person["uid"])
    if user is None:
        user = issuer_accounts.new_issuer_account(db, issuer, issuer_url, person)
    user.is_admin = person["node_admin"]
    db.commit()
    invalidate_user_cache(user.username)
    return user


def renew(db: Session, user: db_models.User, person: dict) -> None:
    """Apply a renewal's ``node_admin`` and full name, so someone no longer let manage stops at their next refresh."""
    user.is_admin = person["node_admin"]
    user.full_name = person["full_name"]
    db.commit()
    invalidate_user_cache(user.username)


def issuer_of(db: Session, user_id: int) -> str | None:
    """The issuer the account ``user_id`` signs in through, ``None`` for an account of this server's own."""
    mapping = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.user_id == user_id).first()
    return None if mapping is None else mapping.issuer


def forget(db: Session, user_id: int) -> None:
    """Drop the issuer mapping of ``user_id`` for the caller to commit: a reused rowid would inherit it."""
    db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.user_id == user_id).delete(
        synchronize_session=False
    )


def ended(issuer_name: str) -> HTTPException:
    """The 401 of a session its issuer no longer vouches for."""
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=f"Your sign-in through {issuer_name} has ended; sign in again",
        headers={"WWW-Authenticate": "Bearer"},
    )


def session(user: db_models.User, sid: str | None, expires_in: int) -> dict:
    """This core's token pair for ``user``: the access token lives as long as the issuer's word, the refresh token
    names the sign-in (``sid``) so a renewal asks about that one only."""
    expiry = expires_at(expires_in)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=timedelta(seconds=expires_in))
    refresh_token = create_refresh_token(data={"sub": user.username, "sid": sid})
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_at": expiry,
    }
