# app_routes/auth.py

import os
import time
from datetime import timedelta

from fastapi import APIRouter, Body, Depends, Form, HTTPException, Request, status
from sqlalchemy.orm import Session

from flowfile_core.auth import issuer_accounts, sharing
from flowfile_core.auth.jwt import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    get_current_active_user,
    get_current_admin_user,
    invalidate_user_cache,
)
from flowfile_core.auth.models import ChangePassword, Token, User, UserCreate, UserUpdate
from flowfile_core.auth.password import PASSWORD_REQUIREMENTS, get_password_hash, validate_password, verify_password
from flowfile_core.configs import logger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.hub_client import delegation
from flowfile_core.hub_client import session as hub_session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable
from shared import hub_node, hub_tokens

router = APIRouter()


@router.post("/token", response_model=Token)
def login_for_access_token(
    request: Request,
    db: Session = Depends(get_db),
    username: str | None = Form(None),
    password: str | None = Form(None),
):
    # In Electron mode, auto-authenticate without requiring form data
    if os.environ.get("FLOWFILE_MODE") == "electron":
        access_token = create_access_token(data={"sub": "local_user"})
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        # In Docker mode, authenticate against database
        _refuse_passwords_once_joined()
        if not username or not password:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = db.query(db_models.User).filter(db_models.User.username == username).first()

        if not user or not verify_password(password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token = create_access_token(data={"sub": user.username})
        refresh_token = create_refresh_token(data={"sub": user.username})
        return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}


def _refuse_passwords_once_joined() -> None:
    """403 while this storage dir's node joined its hub with keys of its own: people sign in here through that hub
    only, so neither a password nor a session that began with one is taken.
    """
    node = issuer_accounts.joined_with_keys()
    if node is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"This server signs people in through {issuer_accounts.hub_host(node.hub_url)}",
        )


@router.post("/refresh", response_model=Token)
def refresh_access_token(
    refresh_token: str = Form(...),
    db: Session = Depends(get_db),
):
    """Exchange a valid refresh token for a new access token and refresh token.

    For a person who signed in through the hub, the sign-in the token was issued for is refreshed at the hub first, so
    this server's session never outlives the hub's: once the hub no longer takes it, or it was replaced here by a later
    sign-in or ended by a sign-out, the answer is a 401 and nothing is issued. The hub's answer then sets whether the
    account administers this server, and its full name, so someone the hub no longer lets manage the server stops
    administering it here at their next refresh. On a server that joined its hub with keys of its own, the session of
    any other account is not renewed: 403, as its password sign-in is.
    """
    claims = decode_refresh_token(refresh_token)
    username, sid = claims["sub"], claims.get("sid")

    user = db.query(db_models.User).filter(db_models.User.username == username).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User no longer exists",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if user.disabled:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is disabled",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expires = None
    expires_at = None
    renewed = {"sub": user.username}
    mapping = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.user_id == user.id).first()
    if mapping is not None:
        refreshed = _refresh_through_hub(user.id, mapping.issuer, sid)
        user.is_admin = refreshed.user["node_admin"]
        user.full_name = refreshed.user["full_name"]
        db.commit()
        invalidate_user_cache(user.username)
        expires = timedelta(seconds=refreshed.expires_in)
        expires_at = _expires_at(refreshed.expires_in)
        renewed["sid"] = sid
    else:
        _refuse_passwords_once_joined()
    new_access_token = create_access_token(data={"sub": user.username}, expires_delta=expires)
    new_refresh_token = create_refresh_token(data=renewed)
    return {
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "token_type": "bearer",
        "expires_at": expires_at,
    }


def _expires_at(seconds: int) -> int:
    """An access token's expiry in milliseconds since the epoch, taken before it is signed so it is never late."""
    return int(time.time() + seconds) * 1000


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(current_user: User = Depends(get_current_active_user)) -> None:
    """Sign out: a sign-in through the hub is revoked at the hub as far as it answers, and forgotten here.

    Answers 204 whether or not the hub took the revocation. Any other hub connection of the account is left as it is.
    """
    stored = hub_session.load(current_user.id)
    if stored is None or stored.get("kind") != "delegation":
        return None
    try:
        delegation.revoke(stored["hub_url"], hub_node.resolved().issuer, stored["refresh_token"])
    except (HubRefused, HubUnreachable, LookupError) as e:
        logger.warning("The hub was not told of a sign-out on this server: %s", e)
    hub_session.clear(current_user.id)
    return None


def _refresh_through_hub(user_id: int, issuer: str, sid: str | None) -> delegation.Delegation:
    """Refresh the sign-in through the hub ``sid`` names at the hub and answer what the hub said; a 401 when the hub no
    longer takes it or it is not the one kept here for ``user_id``, a 502 when the hub does not answer.
    """
    try:
        refreshed = hub_session.refresh_delegation(user_id, sid)
    except HubUnreachable as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e)) from None
    except HubRefused:
        refreshed = None
    if refreshed is None:
        host = issuer_accounts.hub_host(hub_node.resolved().hub_url or issuer)
        raise _refused(f"Your sign-in through {host} has ended; sign in again")
    return refreshed


@router.get("/sign_in_method")
def sign_in_method() -> dict:
    """How people sign in on this server: with a password, or through the hub this server joined.

    Through the hub, ``url`` is the hub's Open page for this node, where a person's browser starts the sign-in.
    """
    node = issuer_accounts.joined_with_keys()
    if node is None:
        return {"method": "password"}
    hub_url = node.hub_url.rstrip("/")
    return {"method": "hub", "url": f"{hub_url}/open/{node.node_id}", "hub_url": hub_url}


def _refused(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail=detail, headers={"WWW-Authenticate": "Bearer"}
    )


def _sentence(text: str) -> str:
    return text.rstrip(".") + "."


@router.post("/hub/signin", response_model=Token)
def sign_in_through_hub(token: str = Body(..., embed=True), db: Session = Depends(get_db)):
    """Sign a person in with the one-time sign-in token the hub's Open page gave their browser.

    The token is verified against the hub's keys as a ``signin`` token for this node, then traded at the hub for a
    delegation signed with this node's own key. The person's local account is the one mapped to the hub's issuer and
    their id there, made at their first sign-in; whether it administers this server follows ``node_admin`` at every
    sign-in. The delegation is kept as the person's hub session, never in the database, and the answer is this core's
    own token pair, as ``/auth/token`` answers, its access token living as long as the hub's (the delegation's
    ``expires_in``, answered as ``expires_at``) and its refresh token naming this sign-in (``sid``). A token or an
    exchange the hub refuses is a 401 with one sentence.
    """
    if issuer_accounts.joined_with_keys() is None:
        raise _refused("This server does not sign people in through a hub.")
    settings = hub_node.resolved()
    try:
        claims = hub_tokens.verify(token, scopes=("signin",))
    except hub_tokens.HubTokenRefused as e:
        raise _refused(_sentence(f"The hub's sign-in token was refused: {e.reason}")) from None
    if claims is None or claims.user_id is None:
        raise _refused("This is not a sign-in token from the hub.")
    user = issuer_accounts.issuer_account(db, settings.issuer, claims.user_id)
    if user is not None and user.disabled:
        raise _refused("This account is disabled on this server.")
    try:
        granted = delegation.exchange(settings.hub_url, settings.issuer, token)
    except HubRefused as e:
        raise _refused(_sentence(f"The hub refused this sign-in: {e.detail}")) from None
    except HubUnreachable as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e)) from None
    except LookupError as e:
        logger.warning("A sign-in through the hub could not be traded: %s", e)
        raise _refused("This server cannot sign for itself at its hub.") from None
    if granted.user["uid"] != claims.user_id:
        raise _refused("The hub answered this sign-in for another person.")
    if user is None:
        user = issuer_accounts.new_issuer_account(db, settings.issuer, settings.hub_url, granted.user)
    user.is_admin = granted.user["node_admin"]
    db.commit()
    invalidate_user_cache(user.username)
    sid = hub_session.save_delegation(user.id, settings.hub_url, granted)
    expires_at = _expires_at(granted.expires_in)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=timedelta(seconds=granted.expires_in))
    refresh_token = create_refresh_token(data={"sub": user.username, "sid": sid})
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_at": expires_at,
    }


@router.get("/users/me", response_model=User)
async def read_users_me(current_user=Depends(get_current_active_user)):
    return current_user


# ============= Admin User Management Endpoints =============


@router.get("/users", response_model=list[User])
def list_users(current_user: User = Depends(get_current_admin_user), db: Session = Depends(get_db)):
    """List all users (admin only)"""
    users = db.query(db_models.User).all()
    return [
        User(
            username=u.username,
            id=u.id,
            email=u.email,
            full_name=u.full_name,
            disabled=u.disabled,
            is_admin=u.is_admin,
            must_change_password=u.must_change_password,
        )
        for u in users
    ]


@router.post("/users", response_model=User)
def create_user(
    user_data: UserCreate, current_user: User = Depends(get_current_admin_user), db: Session = Depends(get_db)
):
    """Create a new user (admin only)"""
    if user_data.username == sharing.INTERNAL_SERVICE_USERNAME:
        # Reserved sentinel: sharing classifies principals as the synthetic kernel
        # identity by this username, so a real user named this way would be locked
        # out of their own resources.
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is reserved")
    existing_user = db.query(db_models.User).filter(db_models.User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already exists")

    if user_data.email:
        existing_email = db.query(db_models.User).filter(db_models.User.email == user_data.email).first()
        if existing_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already exists")

    is_valid, error_message = validate_password(user_data.password)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

    hashed_password = get_password_hash(user_data.password)
    new_user = db_models.User(
        username=user_data.username,
        email=user_data.email or f"{user_data.username}@flowfile.app",
        full_name=user_data.full_name,
        hashed_password=hashed_password,
        is_admin=user_data.is_admin,
        must_change_password=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return User(
        username=new_user.username,
        id=new_user.id,
        email=new_user.email,
        full_name=new_user.full_name,
        disabled=new_user.disabled,
        is_admin=new_user.is_admin,
        must_change_password=new_user.must_change_password,
    )


@router.put("/users/{user_id}", response_model=User)
def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
):
    """Update a user (admin only)"""
    user = db.query(db_models.User).filter(db_models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    # Auth cache is keyed by the pre-edit username.
    username_before_update = user.username

    # Prevent admin from disabling themselves
    if user.id == current_user.id and user_data.disabled:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot disable your own account")

    # Prevent admin from removing their own admin status
    if user.id == current_user.id and user_data.is_admin is False:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot remove your own admin privileges")

    if user_data.email is not None:
        existing_email = (
            db.query(db_models.User)
            .filter(db_models.User.email == user_data.email, db_models.User.id != user_id)
            .first()
        )
        if existing_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already exists")
        user.email = user_data.email

    if user_data.full_name is not None:
        user.full_name = user_data.full_name

    if user_data.disabled is not None:
        user.disabled = user_data.disabled

    if user_data.is_admin is not None:
        user.is_admin = user_data.is_admin

    if user_data.password is not None:
        is_valid, error_message = validate_password(user_data.password)
        if not is_valid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
        user.hashed_password = get_password_hash(user_data.password)
        user.must_change_password = True

    if user_data.must_change_password is not None:
        user.must_change_password = user_data.must_change_password

    db.commit()
    db.refresh(user)
    invalidate_user_cache(username_before_update)

    return User(
        username=user.username,
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        disabled=user.disabled,
        is_admin=user.is_admin,
        must_change_password=user.must_change_password,
    )


@router.delete("/users/{user_id}")
def delete_user(user_id: int, current_user: User = Depends(get_current_admin_user), db: Session = Depends(get_db)):
    """Delete a user (admin only)"""
    user = db.query(db_models.User).filter(db_models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Prevent admin from deleting themselves
    if user.id == current_user.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete your own account")

    # Personal credential rows — everything keyed by user_id in the sharing
    # registry: secrets + all connection types — die with the user, including any
    # sharing grants on them: SQLite reuses rowids, so a stale grant would attach
    # to a future resource created with the same id. Catalog content
    # (owner_id/created_by keyed) is deliberately retained, so its grants stay
    # meaningful. Groups the user ran persist; global admins can administer them.
    for resource_type, spec in sharing.RESOURCE_REGISTRY.items():
        if spec.owner_attr != "user_id":
            continue
        resource_ids = [row[0] for row in db.query(spec.model.id).filter(spec.model.user_id == user_id)]
        if not resource_ids:
            continue
        db.query(db_models.ResourceGrant).filter(
            db_models.ResourceGrant.resource_type == resource_type,
            db_models.ResourceGrant.resource_id.in_(resource_ids),
        ).delete(synchronize_session=False)
        db.query(spec.model).filter(spec.model.id.in_(resource_ids)).delete(synchronize_session=False)
    sharing.delete_memberships_for_user(db, user_id)

    # workspace_projects is not share-registered, so plain row deletion suffices.
    # Removing these prevents a future user receiving the same rowid from inheriting
    # an orphan is_active=True project (rowid reuse; SQLite FK enforcement is off).
    db.query(db_models.WorkspaceProject).filter(db_models.WorkspaceProject.owner_id == user_id).delete(
        synchronize_session=False
    )
    # A hub person's mapping and hub session go too: a reused rowid would otherwise hand both to the next account.
    db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.user_id == user_id).delete(
        synchronize_session=False
    )
    hub_session.clear(user_id)

    # Notification channels hold an encrypted webhook credential and rules keep alerting
    # after the owner is gone — a reused user rowid would inherit both. History rows go
    # with the channels (that is also how history ownership is resolved).
    channel_ids = [
        row[0]
        for row in db.query(db_models.NotificationChannel.id).filter(
            db_models.NotificationChannel.owner_id == user_id
        )
    ]
    if channel_ids:
        db.query(db_models.NotificationOutbox).filter(
            db_models.NotificationOutbox.channel_id.in_(channel_ids)
        ).delete(synchronize_session=False)
    db.query(db_models.NotificationRule).filter(db_models.NotificationRule.owner_id == user_id).delete(
        synchronize_session=False
    )
    db.query(db_models.NotificationChannel).filter(db_models.NotificationChannel.owner_id == user_id).delete(
        synchronize_session=False
    )

    db.delete(user)
    db.commit()
    invalidate_user_cache(user.username)

    return {"message": f"User '{user.username}' deleted successfully"}


# ============= User Self-Service Endpoints =============


@router.post("/users/me/change-password", response_model=User)
def change_own_password(
    password_data: ChangePassword, current_user: User = Depends(get_current_active_user), db: Session = Depends(get_db)
):
    """Change the current user's password"""
    user = db.query(db_models.User).filter(db_models.User.id == current_user.id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    if not verify_password(password_data.current_password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current password is incorrect")

    is_valid, error_message = validate_password(password_data.new_password)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

    user.hashed_password = get_password_hash(password_data.new_password)
    user.must_change_password = False
    db.commit()
    db.refresh(user)
    invalidate_user_cache(user.username)

    return User(
        username=user.username,
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        disabled=user.disabled,
        is_admin=user.is_admin,
        must_change_password=user.must_change_password,
    )


@router.get("/password-requirements")
async def get_password_requirements():
    """Get password requirements for client-side validation"""
    return PASSWORD_REQUIREMENTS
