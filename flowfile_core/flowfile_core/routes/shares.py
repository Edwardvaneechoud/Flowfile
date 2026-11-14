"""Generic resource-sharing endpoints, driven by auth.sharing.RESOURCE_REGISTRY.

Authorization rules (see plan in docs):
- Every operation requires ``can_manage`` on the underlying resource. Missing
  resources and unmanageable resources return an IDENTICAL 404 so the API is
  not an id-enumeration oracle.
- ``manage``-level grants are minted only by the resource owner or a global
  admin. A manage-grantee may re-share at ``use`` level only, and only to
  groups they are a member of.
- Secrets never accept ``manage`` grants (422) — managing a credential is
  equivalent to owning it.
- A request that names a row of a remote catalog (a negative id) is that
  catalog's to answer: its grants and the groups it shares with are its own.
"""

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.routes.catalog import _RemoteRoute, relay_remote
from flowfile_core.routes.user_groups import _groups_out, require_sharing_enabled
from flowfile_core.schemas.sharing_schema import ResourceType, ShareCreate, ShareOut, ShareUpdate, UserGroupOut


async def _remote_shares(request: Request, current_user: User = Depends(get_current_active_user)) -> None:
    """Hand a sharing request that names a row of a remote catalog to that catalog, as the signed-in person: the row
    is shared with that catalog's groups, whatever this install's own sharing is."""
    await relay_remote(request, current_user.id, "/shares")


router = APIRouter(
    dependencies=[Depends(get_current_active_user), Depends(_remote_shares), Depends(require_sharing_enabled)],
    route_class=_RemoteRoute,
)


def _uniform_not_found() -> HTTPException:
    # Same response for "does not exist" and "exists but you cannot manage it".
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found")


def _require_manageable(db: Session, user: User, resource_type: str, resource_id: int) -> None:
    if not sharing.can_manage(db, user, resource_type, resource_id):
        raise _uniform_not_found()


def _is_resource_owner(db: Session, user: User, resource_type: str, resource_id: int) -> bool:
    spec = sharing.RESOURCE_REGISTRY[resource_type]
    row = db.query(spec.model).filter(spec.model.id == resource_id).first()
    return row is not None and getattr(row, spec.owner_attr) == user.id


def _share_out(db: Session, grant: db_models.ResourceGrant) -> ShareOut:
    group = db.get(db_models.UserGroup, grant.group_id)
    granted_by_user = db.get(db_models.User, grant.granted_by) if grant.granted_by else None
    return ShareOut(
        id=grant.id,
        resource_type=grant.resource_type,
        resource_id=grant.resource_id,
        group_id=grant.group_id,
        group_name=group.name if group else f"#{grant.group_id}",
        permission=grant.permission,
        granted_by=grant.granted_by,
        granted_by_username=granted_by_user.username if granted_by_user else None,
        created_at=grant.created_at,
    )


@router.get("", response_model=list[ShareOut])
def list_shares(
    resource_type: ResourceType,
    resource_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    _require_manageable(db, current_user, resource_type, resource_id)
    grants = (
        db.query(db_models.ResourceGrant)
        .filter(
            db_models.ResourceGrant.resource_type == resource_type,
            db_models.ResourceGrant.resource_id == resource_id,
        )
        .order_by(db_models.ResourceGrant.id)
        .all()
    )
    return [_share_out(db, grant) for grant in grants]


@router.get("/groups", response_model=list[UserGroupOut])
def list_share_groups(
    resource_type: ResourceType,
    resource_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """The groups the caller may share the resource with: every group for an admin, else their own."""
    _require_manageable(db, current_user, resource_type, resource_id)
    groups = db.query(db_models.UserGroup).order_by(db_models.UserGroup.name)
    if not current_user.is_admin:
        groups = groups.filter(db_models.UserGroup.id.in_(sharing.user_group_ids(db, current_user.id)))
    return _groups_out(db, groups.all(), current_user.id)


@router.post("", response_model=ShareOut)
def create_share(
    share: ShareCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    if share.permission == sharing.PERMISSION_MANAGE and share.resource_type in sharing.MANAGE_DISALLOWED_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Secrets can only be shared at 'use' level",
        )
    _require_manageable(db, current_user, share.resource_type, share.resource_id)
    if sharing.is_personal_connection(db, share.resource_type, share.resource_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=sharing.PERSONAL_CONNECTION_MESSAGE
        )

    is_owner = _is_resource_owner(db, current_user, share.resource_type, share.resource_id)
    if not (is_owner or current_user.is_admin):
        # Manage-grantees: use-level re-shares only, to groups they belong to.
        if share.permission == sharing.PERMISSION_MANAGE:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the resource owner or an admin can grant manage access",
            )
        if share.group_id not in sharing.user_group_ids(db, current_user.id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only share to groups you are a member of",
            )

    if db.get(db_models.UserGroup, share.group_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group not found")

    existing = (
        db.query(db_models.ResourceGrant)
        .filter(
            db_models.ResourceGrant.resource_type == share.resource_type,
            db_models.ResourceGrant.resource_id == share.resource_id,
            db_models.ResourceGrant.group_id == share.group_id,
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Resource is already shared with this group",
        )

    grant = db_models.ResourceGrant(
        resource_type=share.resource_type,
        resource_id=share.resource_id,
        group_id=share.group_id,
        permission=share.permission,
        granted_by=current_user.id,
    )
    db.add(grant)
    db.commit()
    db.refresh(grant)
    return _share_out(db, grant)


@router.patch("/{grant_id}", response_model=ShareOut)
def update_share(
    grant_id: int,
    share: ShareUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    grant = db.get(db_models.ResourceGrant, grant_id)
    if grant is None:
        raise _uniform_not_found()
    _require_manageable(db, current_user, grant.resource_type, grant.resource_id)
    if share.permission == sharing.PERMISSION_MANAGE:
        if grant.resource_type in sharing.MANAGE_DISALLOWED_TYPES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Secrets can only be shared at 'use' level",
            )
        if not (_is_resource_owner(db, current_user, grant.resource_type, grant.resource_id) or current_user.is_admin):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the resource owner or an admin can grant manage access",
            )
    grant.permission = share.permission
    db.commit()
    db.refresh(grant)
    return _share_out(db, grant)


@router.delete("/{grant_id}", status_code=204)
def delete_share(
    grant_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    grant = db.get(db_models.ResourceGrant, grant_id)
    if grant is None:
        raise _uniform_not_found()
    _require_manageable(db, current_user, grant.resource_type, grant.resource_id)
    db.delete(grant)
    db.commit()
