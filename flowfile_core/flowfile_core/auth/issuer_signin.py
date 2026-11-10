"""Sign-in through an external issuer: the local account and the local session of a person an issuer vouched for.

An issuer signs people in for this machine: it signs a token, the token is verified against the keys the issuer
publishes (``shared.issuer_tokens``), and the person it names gets a session here on the account keyed by the issuer
and their id there (``issuer_accounts``). This module is that exchange once the token is verified. It knows no issuer:
its caller says who the issuer is on this machine, who the person is there, and how long the issuer's word holds.
"""

import time
from datetime import timedelta

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
    """``SignInRefused`` when the issuer's person ``subject_id`` has an account here that is disabled, asked before
    anything is traded with the issuer for them."""
    user = issuer_accounts.issuer_account(db, issuer, subject_id)
    if user is not None and user.disabled:
        raise SignInRefused("This account is disabled on this server.")


def admit(db: Session, issuer: str, issuer_url: str, person: dict) -> db_models.User:
    """The local account of ``person`` as the issuer described them at this sign-in, committed.

    ``person`` is the issuer's ``uid``, ``username``, ``full_name`` and ``node_admin``. The account is made at a first
    sign-in (``issuer_accounts.new_issuer_account``) and administers this server as ``node_admin`` says at every one.
    """
    user = issuer_accounts.issuer_account(db, issuer, person["uid"])
    if user is None:
        user = issuer_accounts.new_issuer_account(db, issuer, issuer_url, person)
    user.is_admin = person["node_admin"]
    db.commit()
    invalidate_user_cache(user.username)
    return user


def renew(db: Session, user: db_models.User, person: dict) -> None:
    """Set ``user`` to what the issuer said of ``person`` when it renewed their sign-in: whether they administer this
    server, and their full name, so someone the issuer no longer lets manage it stops at their next refresh."""
    user.is_admin = person["node_admin"]
    user.full_name = person["full_name"]
    db.commit()
    invalidate_user_cache(user.username)


def session(user: db_models.User, sid: str | None, expires_in: int) -> dict:
    """This core's own token pair for ``user``, as ``/auth/token`` answers one.

    The access token lives as long as the issuer's word does (``expires_in`` seconds, answered as ``expires_at``), and
    the refresh token names the sign-in (``sid``) so a renewal asks the issuer about that one and no other.
    """
    expiry = expires_at(expires_in)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=timedelta(seconds=expires_in))
    refresh_token = create_refresh_token(data={"sub": user.username, "sid": sid})
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_at": expiry,
    }
