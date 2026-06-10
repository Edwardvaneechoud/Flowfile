"""Group-based resource sharing authorization layer.

Single source of truth for "who can use/manage what" across secrets, database/
cloud/GA/Kafka connections, and catalog namespaces/tables/flows. Sharing is an
authorization-only feature: ciphertext stays keyed to the owning user
(``$ffsec$1$<owner_id>$``), so granting access never re-encrypts anything and
the worker needs no changes.

Deliberately import-light (database models + sqlalchemy only — no catalog or
route imports) so any layer can use it. Everything degenerates to owner-only
behavior when ``sharing_enabled()`` is False (electron/desktop mode).
"""

import os
from dataclasses import dataclass

from sqlalchemy import or_
from sqlalchemy.orm import Session

from flowfile_core.database import models as db_models

PERMISSION_USE = "use"
PERMISSION_MANAGE = "manage"
PERMISSIONS = (PERMISSION_USE, PERMISSION_MANAGE)

ROLE_OWNER = "owner"
ROLE_MANAGER = "manager"
ROLE_MEMBER = "member"
GROUP_ROLES = (ROLE_OWNER, ROLE_MANAGER, ROLE_MEMBER)

# Username of the synthetic principal minted by auth.jwt.get_user_or_internal_service
# when an internal-token request carries no kernel id. Its ``id`` defaults to 1 — a
# real user — so synthetic detection MUST key on the username sentinel, never the id.
INTERNAL_SERVICE_USERNAME = "_internal_service"


@dataclass(frozen=True)
class ResourceSpec:
    model: type
    owner_attr: str  # "user_id" or "owner_id" — naming is split across models
    label: str


RESOURCE_REGISTRY: dict[str, ResourceSpec] = {
    "secret": ResourceSpec(db_models.Secret, "user_id", "secret"),
    "database_connection": ResourceSpec(db_models.DatabaseConnection, "user_id", "database connection"),
    "cloud_connection": ResourceSpec(db_models.CloudStorageConnection, "user_id", "cloud storage connection"),
    "ga_connection": ResourceSpec(db_models.GoogleAnalyticsConnection, "user_id", "Google Analytics connection"),
    "kafka_connection": ResourceSpec(db_models.KafkaConnection, "user_id", "Kafka connection"),
    "catalog_namespace": ResourceSpec(db_models.CatalogNamespace, "owner_id", "namespace"),
    "catalog_table": ResourceSpec(db_models.CatalogTable, "owner_id", "table"),
    "flow": ResourceSpec(db_models.FlowRegistration, "owner_id", "flow"),
}

# Secrets are use-only when shared: a manage grant would imply edit/re-share rights
# on a credential, which collapses into "give me the plaintext".
MANAGE_DISALLOWED_TYPES = frozenset({"secret"})

_NAMESPACE_SCOPED_TYPES = frozenset({"catalog_table", "flow"})


def sharing_enabled() -> bool:
    # Read per call: configs.settings caches FLOWFILE_MODE at import time, which would
    # make docker-mode behavior untestable in-process (cf. tests/test_file_manager.py).
    return os.environ.get("FLOWFILE_MODE", "electron") != "electron"


def is_synthetic_principal(user) -> bool:
    return getattr(user, "username", None) == INTERNAL_SERVICE_USERNAME


def user_group_ids(db: Session, user_id: int) -> list[int]:
    """Ids of every group the user belongs to (any role)."""
    if not sharing_enabled():
        return []
    rows = db.query(db_models.UserGroupMembership.group_id).filter(db_models.UserGroupMembership.user_id == user_id)
    return [r[0] for r in rows]


def manageable_group_ids(db: Session, user_id: int) -> list[int]:
    """Ids of groups the user administers (role owner or manager)."""
    if not sharing_enabled():
        return []
    rows = db.query(db_models.UserGroupMembership.group_id).filter(
        db_models.UserGroupMembership.user_id == user_id,
        db_models.UserGroupMembership.role.in_((ROLE_OWNER, ROLE_MANAGER)),
    )
    return [r[0] for r in rows]


def group_role(db: Session, group_id: int, user_id: int) -> str | None:
    row = (
        db.query(db_models.UserGroupMembership.role)
        .filter(
            db_models.UserGroupMembership.group_id == group_id,
            db_models.UserGroupMembership.user_id == user_id,
        )
        .first()
    )
    return row[0] if row else None


def expand_namespace_grants(db: Session, user_id: int, permission: str = PERMISSION_USE) -> set[int]:
    """Namespace ids granted to the user's groups, plus their direct children.

    Namespaces are hard-capped at two levels (catalog -> schema), so one
    parent_id expansion covers the whole subtree.
    """
    if not sharing_enabled():
        return set()
    granted = _direct_granted_ids(db, user_id, "catalog_namespace", permission)
    if not granted:
        return set()
    children = db.query(db_models.CatalogNamespace.id).filter(db_models.CatalogNamespace.parent_id.in_(granted))
    return granted | {r[0] for r in children}


def _direct_granted_ids(db: Session, user_id: int, resource_type: str, permission: str) -> set[int]:
    gids = user_group_ids(db, user_id)
    if not gids:
        return set()
    q = db.query(db_models.ResourceGrant.resource_id).filter(
        db_models.ResourceGrant.resource_type == resource_type,
        db_models.ResourceGrant.group_id.in_(gids),
    )
    if permission == PERMISSION_MANAGE:
        q = q.filter(db_models.ResourceGrant.permission == PERMISSION_MANAGE)
    return {r[0] for r in q}


def granted_resource_ids(db: Session, user_id: int, resource_type: str, permission: str = PERMISSION_USE) -> set[int]:
    """Resource ids of ``resource_type`` the user can reach via group grants.

    ``use`` matches both grant levels; ``manage`` only manage-level grants.
    Tables and flows union direct grants with namespace-inherited ones.
    """
    if not sharing_enabled():
        return set()
    ids = _direct_granted_ids(db, user_id, resource_type, permission)
    if resource_type in _NAMESPACE_SCOPED_TYPES:
        ns_ids = expand_namespace_grants(db, user_id, permission)
        if ns_ids:
            spec = RESOURCE_REGISTRY[resource_type]
            rows = db.query(spec.model.id).filter(spec.model.namespace_id.in_(ns_ids))
            ids |= {r[0] for r in rows}
    return ids


def granted_access_details(db: Session, user_id: int, resource_type: str) -> dict[int, tuple[str, int | None]]:
    """resource_id -> (highest permission, granted_by user id) for list annotation.

    Namespace-inherited table/flow access is reported with ``granted_by=None``
    (the grant lives on the namespace, not the item).
    """
    if not sharing_enabled():
        return {}
    gids = user_group_ids(db, user_id)
    details: dict[int, tuple[str, int | None]] = {}
    if gids:
        rows = db.query(
            db_models.ResourceGrant.resource_id,
            db_models.ResourceGrant.permission,
            db_models.ResourceGrant.granted_by,
        ).filter(
            db_models.ResourceGrant.resource_type == resource_type,
            db_models.ResourceGrant.group_id.in_(gids),
        )
        for resource_id, permission, granted_by in rows:
            existing = details.get(resource_id)
            if existing is None or (permission == PERMISSION_MANAGE and existing[0] != PERMISSION_MANAGE):
                details[resource_id] = (permission, granted_by)
    if resource_type in _NAMESPACE_SCOPED_TYPES:
        for permission in (PERMISSION_USE, PERMISSION_MANAGE):
            ns_ids = expand_namespace_grants(db, user_id, permission)
            if not ns_ids:
                continue
            spec = RESOURCE_REGISTRY[resource_type]
            rows = db.query(spec.model.id).filter(spec.model.namespace_id.in_(ns_ids))
            for (resource_id,) in rows:
                existing = details.get(resource_id)
                if existing is None or (permission == PERMISSION_MANAGE and existing[0] != PERMISSION_MANAGE):
                    details[resource_id] = (permission, None)
    return details


_UNRESOLVED = object()


def _resolve_owner_id(db: Session, resource_type: str, resource_id: int):
    spec = RESOURCE_REGISTRY[resource_type]
    row = db.query(spec.model).filter(spec.model.id == resource_id).first()
    if row is None:
        return _UNRESOLVED
    return getattr(row, spec.owner_attr)


def _has_access(db: Session, user, resource_type: str, resource_id: int, owner_id, permission: str) -> bool:
    if owner_id is None:
        owner_id = _resolve_owner_id(db, resource_type, resource_id)
        if owner_id is _UNRESOLVED:
            return False
    if owner_id == getattr(user, "id", None) and not is_synthetic_principal(user):
        return True
    if getattr(user, "is_admin", False):
        return True
    if not sharing_enabled() or is_synthetic_principal(user):
        return False
    return resource_id in granted_resource_ids(db, user.id, resource_type, permission)


def can_use(db: Session, user, resource_type: str, resource_id: int, owner_id: int | None = None) -> bool:
    return _has_access(db, user, resource_type, resource_id, owner_id, PERMISSION_USE)


def can_manage(db: Session, user, resource_type: str, resource_id: int, owner_id: int | None = None) -> bool:
    return _has_access(db, user, resource_type, resource_id, owner_id, PERMISSION_MANAGE)


def accessible_filter(db: Session, user_id: int, resource_type: str, permission: str = PERMISSION_USE):
    """SQLAlchemy criterion matching rows the user owns or was granted. For list queries."""
    spec = RESOURCE_REGISTRY[resource_type]
    owner_col = getattr(spec.model, spec.owner_attr)
    granted = granted_resource_ids(db, user_id, resource_type, permission)
    if granted:
        return or_(owner_col == user_id, spec.model.id.in_(granted))
    return owner_col == user_id


def delete_grants_for_resource(db: Session, resource_type: str, resource_id: int) -> None:
    """Must be called from every resource-delete path (no FK cascades; rowids get reused)."""
    db.query(db_models.ResourceGrant).filter(
        db_models.ResourceGrant.resource_type == resource_type,
        db_models.ResourceGrant.resource_id == resource_id,
    ).delete(synchronize_session=False)


def delete_grants_for_group(db: Session, group_id: int) -> None:
    db.query(db_models.ResourceGrant).filter(db_models.ResourceGrant.group_id == group_id).delete(
        synchronize_session=False
    )


def delete_memberships_for_group(db: Session, group_id: int) -> None:
    db.query(db_models.UserGroupMembership).filter(db_models.UserGroupMembership.group_id == group_id).delete(
        synchronize_session=False
    )


def delete_memberships_for_user(db: Session, user_id: int) -> None:
    db.query(db_models.UserGroupMembership).filter(db_models.UserGroupMembership.user_id == user_id).delete(
        synchronize_session=False
    )
