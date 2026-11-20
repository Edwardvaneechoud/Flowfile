# jwt.py

import logging
import os
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt import PyJWTError as JWTError
from sqlalchemy.orm import Session

from flowfile_core.auth.models import TokenData, User
from flowfile_core.auth.secrets import get_password, set_password
from flowfile_core.configs.settings import ACCESS_TOKEN_EXPIRE_MINUTES
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()

# Constants for JWT
ALGORITHM = "HS256"
REFRESH_TOKEN_EXPIRE_DAYS = 7
KERNEL_TOKEN_TYPE = "kernel"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token", auto_error=False)

# Internal service authentication
# This token is shared between Core and the worker for service-to-service auth
_internal_token: str | None = None


_LOCK_STALE_SECONDS = 5.0
_LOCK_WAIT_SECONDS = 6.0
_LOCK_POLL_SECONDS = 0.05


def _token_lock_path() -> Path:
    from flowfile_core.auth import secrets as secrets_module

    return Path(secrets_module._storage.storage_path) / ".internal_token.lock"


def _read_or_mint_token() -> str:
    """The critical section: adopt a persisted token, else mint and persist one."""
    token = get_password("flowfile", "internal_token")
    if token:
        return token
    token = secrets.token_hex(32)
    set_password("flowfile", "internal_token", token)
    return get_password("flowfile", "internal_token") or token


def _break_stale_token_lock(lock_path: Path) -> None:
    """Drop a lock whose holder died mid-mint."""
    try:
        if time.time() - os.path.getmtime(lock_path) > _LOCK_STALE_SECONDS:
            os.unlink(lock_path)
    except OSError:
        pass


def _load_or_create_persisted_token() -> str:
    """Read the persisted internal token, minting and storing one if absent.

    Serialised across processes by an O_CREAT|O_EXCL lock file beside the store,
    so two processes that both start with an empty store cannot each mint: the
    loser waits for the winner's write and adopts it. A re-read alone does not
    converge — the loser can write after the winner has already re-read, leaving
    the winner on a token the store no longer holds for the rest of its uptime.

    Never raises: any failure in the lock machinery falls back to the unguarded
    read-mint-persist, which is last-writer-wins but still yields a usable token.
    """
    token = get_password("flowfile", "internal_token")
    if token:
        return token

    try:
        lock_path = _token_lock_path()
        deadline = time.monotonic() + _LOCK_WAIT_SECONDS
        while True:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(fd)
                try:
                    return _read_or_mint_token()
                finally:
                    try:
                        os.unlink(lock_path)
                    except OSError:
                        pass

            token = get_password("flowfile", "internal_token")
            if token:
                return token
            _break_stale_token_lock(lock_path)
            if time.monotonic() >= deadline:
                break
            time.sleep(_LOCK_POLL_SECONDS)
    except Exception as exc:
        logger.debug(f"Internal-token lock unavailable, falling back to unguarded mint: {exc}")

    return _read_or_mint_token()


def get_internal_token() -> str:
    """Get the internal service token used for core ↔ worker auth.

    In Docker mode the token MUST be set via the
    ``FLOWFILE_INTERNAL_TOKEN`` environment variable (configured in
    docker-compose.yml, same pattern as ``JWT_SECRET_KEY``).

    In Electron mode it is generated once and persisted through SecureStorage
    (same pattern as ``get_jwt_secret``) so that CLI subprocesses spawned for
    manual and scheduled runs agree with the long-lived server instead of each
    minting a private token. There is no rotation path: delete the
    ``internal_token`` entry from the store and restart to mint a new one.
    """
    global _internal_token
    if _internal_token is None:
        _internal_token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
        if not _internal_token:
            if os.environ.get("FLOWFILE_MODE") == "electron":
                _internal_token = _load_or_create_persisted_token()
                os.environ["FLOWFILE_INTERNAL_TOKEN"] = _internal_token
            else:
                raise ValueError(
                    "FLOWFILE_INTERNAL_TOKEN environment variable must be set in Docker mode. "
                    "Add it to docker-compose.yml alongside JWT_SECRET_KEY."
                )
    return _internal_token


def verify_internal_token(token: str) -> bool:
    """Verify an internal service token."""
    return secrets.compare_digest(token, get_internal_token())


def shared_token_valid(token: str | None) -> bool:
    """True only when ``token`` is the internal token itself; False when core has no token configured."""
    try:
        return bool(token) and verify_internal_token(token)
    except ValueError:
        return False


def internal_token_valid(token: str | None) -> bool:
    """True when ``token`` is what core's own callers sign with in ``X-Internal-Token``: the internal token, or a
    token core signed for a kernel.

    It says the caller is no person's sign-in, not who the caller is or what it may do: a server that replaces
    core's auth dependencies hands such a request to ``get_user_or_internal_service``, which decides. What only the
    internal token opens asks ``shared_token_valid``.
    """
    return shared_token_valid(token) or _kernel_token_claims(token) is not None


def require_internal_token(x_internal_token: str | None = Header(None, alias="X-Internal-Token")) -> None:
    """Admit only a caller that signs with the internal token, as the worker does.

    Fails closed (401) when the header is missing or wrong, and when core has no token configured. A route a kernel
    calls too takes ``require_internal_or_kernel_token``.
    """
    if not shared_token_valid(x_internal_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing internal token")


def get_jwt_secret():
    if os.environ.get("FLOWFILE_MODE") == "electron":
        key = get_password("flowfile", "jwt_secret")
        if not key:
            key = secrets.token_hex(32)
            set_password("flowfile", "jwt_secret", key)
        return key
    else:
        key = os.environ.get("JWT_SECRET_KEY")
        if not key:
            raise Exception("JWT_SECRET_KEY environment variable must be set in Docker mode")
        return key


def get_current_user_sync(token: str, db: Session):
    """Synchronous version of get_current_user for non-FastAPI contexts"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        # Neither a refresh token nor a kernel's token is a sign-in
        if payload.get("type") in ("refresh", KERNEL_TOKEN_TYPE):
            raise credentials_exception
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception from None

    return _resolve_token_user(db, token_data.username, credentials_exception)


USER_CACHE_TTL_SECONDS = 60

_user_cache: dict[str, tuple[float, User]] = {}
_user_cache_lock = threading.Lock()


def invalidate_user_cache(username: str | None = None) -> None:
    """Drop cached lookups for *username* (all when ``None``).

    Call after any user-row mutation. Keyed by username, so a rename must pass the old name.
    """
    with _user_cache_lock:
        if username is None:
            _user_cache.clear()
        else:
            _user_cache.pop(username, None)


def _resolve_token_user(db: Session, username: str, credentials_exception: HTTPException) -> User:
    """Token subject → ``User``.

    Cached per process for ``USER_CACHE_TTL_SECONDS`` so a page-load fan-out needs no DB
    connections for auth; misses are not cached.
    """
    if os.environ.get("FLOWFILE_MODE") == "electron":
        if username == "local_user":
            return User(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
        raise credentials_exception

    now = time.monotonic()
    with _user_cache_lock:
        cached = _user_cache.get(username)
    if cached is not None and cached[0] > now:
        user = cached[1]
    else:
        row = db.query(db_models.User).filter(db_models.User.username == username).first()
        if row is None:
            raise credentials_exception
        user = User(
            username=row.username,
            id=row.id,
            email=row.email,
            full_name=row.full_name,
            disabled=row.disabled,
            is_admin=row.is_admin,
            must_change_password=row.must_change_password,
        )
        with _user_cache_lock:
            _user_cache[username] = (now + USER_CACHE_TTL_SECONDS, user)
    if user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return user.model_copy()


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """Sync on purpose: a pool wait must land on the threadpool, not the event loop."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        # Neither a refresh token nor a kernel's token is a sign-in
        if payload.get("type") in ("refresh", KERNEL_TOKEN_TYPE):
            raise credentials_exception
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception from None

    return _resolve_token_user(db, token_data.username, credentials_exception)


def get_current_active_user(current_user=Depends(get_current_user)):
    if hasattr(current_user, "disabled") and current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, get_jwt_secret(), algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, get_jwt_secret(), algorithm=ALGORITHM)


def decode_refresh_token(token: str) -> dict:
    """Decode a refresh token and return its claims: ``sub`` is the username, ``sid`` the sign-in through a hub it
    renews, when it was issued for one.

    Raises HTTPException(401) if the token is invalid, expired, or not a refresh token.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid refresh token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        if payload.get("type") != "refresh":
            raise credentials_exception
        if payload.get("sub") is None:
            raise credentials_exception
        return payload
    except JWTError:
        raise credentials_exception from None


async def get_current_admin_user(current_user: User = Depends(get_current_user)):
    """Dependency that requires the current user to be an admin"""
    if not current_user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required")
    return current_user


def active_db_user(db: Session, user_id: int) -> db_models.User | None:
    """The person's ``users`` row, or ``None`` when it is gone or disabled (a NULL ``disabled`` counts as active)."""
    user = db.get(db_models.User, user_id)
    return user if user is not None and not user.disabled else None


@dataclass(frozen=True)
class KernelCaller:
    """A kernel a request comes from, as its token names it: the kernel's id and its owner, never an admin."""

    kernel_id: str
    user: User


def legacy_kernel_auth_allowed() -> bool:
    """Whether a kernel may still sign its calls with the internal token, as a kernel image from before a token per
    kernel does: on a desktop (``electron``, ``package``), not on a multi-user server (``docker``, ``server``). Read
    live, like the sharing gates."""
    return os.environ.get("FLOWFILE_MODE", "electron") not in ("docker", "server")


def create_kernel_token(owner_id: int, kernel_id: str, token_id: str) -> str:
    """The token a kernel signs its calls to core with: its owner, its id and ``token_id``, the id of its current run.

    Signed with core's JWT key. It has no expiry: it ends when the kernel stops or starts again, which is when
    ``token_id`` stops being the kernel's. It names no ``sub`` and its type is not an access token's, so it is no
    sign-in.
    """
    claims = {"type": KERNEL_TOKEN_TYPE, "scope": "kernel", "uid": owner_id, "kernel_id": kernel_id, "jti": token_id}
    return jwt.encode(claims, get_jwt_secret(), algorithm=ALGORITHM)


def _kernel_token_claims(token: str | None) -> dict | None:
    """The claims of ``token`` when core signed it as a kernel's token, else ``None``.

    Anything that is not shaped like a JWT, the internal token among them, is answered before the key is read.
    """
    if not token or not isinstance(token, str) or token.count(".") != 2:
        return None
    try:
        claims = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
    except JWTError:
        return None
    return claims if claims.get("type") == KERNEL_TOKEN_TYPE and claims.get("scope") == "kernel" else None


def kernel_caller(token: str | None, db: Session) -> KernelCaller | None:
    """The kernel ``token`` is the token of, with its owner; ``None`` when ``token`` is no kernel token at all.

    The one check of a kernel's token, for every route and for a server that replaces core's auth dependencies.
    A kernel token that is no longer good is refused here (401) and never answered as anyone else: its kernel is
    gone or stopped, it started again since, it changed owner, or its owner is disabled or gone. The owner is
    answered without the admin flag.
    """
    claims = _kernel_token_claims(token)
    if claims is None:
        return None
    refused = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="This kernel's token is no longer valid",
        headers={"WWW-Authenticate": "Bearer"},
    )
    kernel = db.query(db_models.Kernel).filter(db_models.Kernel.id == claims.get("kernel_id")).first()
    if kernel is None or not kernel.token_id or kernel.user_id != claims.get("uid"):
        raise refused
    if not secrets.compare_digest(str(kernel.token_id), str(claims.get("jti"))):
        raise refused
    owner = active_db_user(db, kernel.user_id)
    if owner is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="The kernel's owner is disabled or no longer exists",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user = User(
        username=owner.username,
        id=owner.id,
        email=owner.email,
        full_name=owner.full_name,
        disabled=False,
        is_admin=False,
        must_change_password=False,
    )
    return KernelCaller(kernel_id=kernel.id, user=user)


def calling_kernel(
    token: str = Depends(oauth2_scheme),
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_kernel_id: str | None = Header(None, alias="X-Kernel-Id"),
    db: Session = Depends(get_db),
) -> KernelCaller | None:
    """The kernel a request comes from, by the kernel token it carries; ``None`` for every other caller.

    The token sits in ``X-Internal-Token``, where a kernel has always put what core hands it, so a server that
    replaces core's auth dependencies passes it on as it passes the internal token; as the bearer it is taken too.
    ``X-Kernel-Id`` decides nothing: a request that names another kernel than its token's
    is refused (401), like a token that is no longer good (``kernel_caller``).
    """
    for candidate in (token, x_internal_token):
        caller = kernel_caller(candidate, db)
        if caller is None:
            continue
        if isinstance(x_kernel_id, str) and x_kernel_id and x_kernel_id != caller.kernel_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="This kernel's token is for another kernel",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return caller
    return None


def calling_kernel_id(
    x_kernel_id: str | None = Header(None, alias="X-Kernel-Id"),
    kernel: KernelCaller | None = Depends(calling_kernel),
) -> str | None:
    """The id of the kernel a request is made for: its kernel token's, else the one ``X-Kernel-Id`` names, which
    proves nothing by itself. For a route that has authenticated its caller and checks them against that kernel."""
    return kernel.kernel_id if kernel is not None else x_kernel_id


def require_internal_or_kernel_token(
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    kernel: KernelCaller | None = Depends(calling_kernel),
) -> None:
    """Admit the worker, which signs with the internal token, and a kernel, which signs with its own.

    Fails closed (401) for anything else, and when core has no internal token configured.
    """
    if kernel is None and not shared_token_valid(x_internal_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing internal token")


def kernel_owner_id(
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_kernel_id: str | None = Header(None, alias="X-Kernel-Id"),
    db: Session = Depends(get_db),
    token: str = Depends(oauth2_scheme),
) -> int | None:
    """The id of the person whose kernel a request comes from, for what only a person may read through one.

    ``None`` for every other caller: no kernel token, one that is no longer good, or, where a kernel may still sign
    with the internal token (``legacy_kernel_auth_allowed``), no kernel id, a token that does not verify, a kernel
    core does not know, or one whose owner is disabled. It refuses nothing itself; the route decides.
    """
    try:
        caller = calling_kernel(token, x_internal_token, x_kernel_id, db)
    except HTTPException:
        return None
    if caller is not None:
        return caller.user.id
    if not x_kernel_id or not legacy_kernel_auth_allowed() or not shared_token_valid(x_internal_token):
        return None
    kernel = db.query(db_models.Kernel).filter(db_models.Kernel.id == x_kernel_id).first()
    owner = active_db_user(db, kernel.user_id) if kernel is not None else None
    return owner.id if owner is not None else None


def get_user_or_internal_service(
    token: str = Depends(oauth2_scheme),
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
    x_kernel_id: str | None = Header(None, alias="X-Kernel-Id"),
    db: Session = Depends(get_db),
) -> User:
    """Auth dependency that accepts a person's JWT or a kernel's call.

    Used for endpoints that need to be accessible from both:
    - External clients (using JWT authentication)
    - Kernels, which sign with their own token (``calling_kernel``)

    A kernel is answered as its owner, without the admin flag, and a kernel token that is no longer good is refused
    (401) whatever else the request carries. The internal token, which a kernel image from before a token per
    kernel signs with, is taken only where ``legacy_kernel_auth_allowed``: with ``X-Kernel-Id`` as the kernel's
    owner (401 when the owner is disabled or gone), without it as the synthetic ``_internal_service`` user. On a
    multi-user server it is refused (401).
    """
    caller = calling_kernel(token, x_internal_token, x_kernel_id, db)
    if caller is not None:
        return caller.user

    if shared_token_valid(x_internal_token):
        if not legacy_kernel_auth_allowed():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="This server takes a kernel's own token: update the kernel image",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if x_kernel_id:
            kernel_row = db.query(db_models.Kernel).filter(db_models.Kernel.id == x_kernel_id).first()
            if kernel_row is not None:
                owner = active_db_user(db, kernel_row.user_id)
                if owner is not None:
                    return User(
                        username=owner.username,
                        id=owner.id,
                        email=owner.email,
                        full_name=owner.full_name,
                        disabled=owner.disabled,
                        is_admin=owner.is_admin,
                        must_change_password=owner.must_change_password,
                    )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="The kernel's owner is disabled or no longer exists",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        try:
            # FLOWFILE_INTERNAL_SERVICE_USER_ID names the synthetic user's id; 1 unless set.
            service_user_id = int(os.environ.get("FLOWFILE_INTERNAL_SERVICE_USER_ID", "1"))
        except ValueError:
            service_user_id = None
        if service_user_id is not None:
            return User(
                username="_internal_service",
                id=service_user_id,
                disabled=False,
                is_admin=False,
                must_change_password=False,
            )

    return get_current_user(token=token, db=db)
