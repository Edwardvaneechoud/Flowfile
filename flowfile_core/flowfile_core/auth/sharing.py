"""Group-based resource sharing authorization layer.

Single source of truth for "who can use/manage what" across secrets, database/
cloud/GA/Kafka connections, and catalog namespaces/tables/flows. Sharing is an
authorization-only feature: ciphertext stays keyed to the owning user
(``$ffsec$1$<owner_id>$``), so granting access never re-encrypts anything and
the worker needs no changes.

Deliberately import-light (database models, sqlalchemy and the plain pydantic
sharing schema — no catalog or route imports) so any layer can use it.
Everything degenerates to owner-only behavior when ``sharing_enabled()`` is
False (electron/desktop mode).
"""

import os
from dataclasses import dataclass

from sqlalchemy import event, or_
from sqlalchemy.orm import Session

from flowfile_core.database import models as db_models
from flowfile_core.schemas.sharing_schema import AccessInfo

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
    # Catalog content. Visualizations/dashboards use the NULLABLE created_by column,
    # so a NULL-owner row is reachable only by an admin or an explicit grant.
    "visualization": ResourceSpec(db_models.CatalogVisualization, "created_by", "visualization"),
    "dashboard": ResourceSpec(db_models.CatalogDashboard, "created_by", "dashboard"),
    "global_artifact": ResourceSpec(db_models.GlobalArtifact, "owner_id", "model"),
}

# Secrets are use-only when shared: a manage grant would imply edit/re-share rights
# on a credential, which collapses into "give me the plaintext".
MANAGE_DISALLOWED_TYPES = frozenset({"secret"})

# Types with a namespace_id: a namespace grant cascades to all of them.
_NAMESPACE_SCOPED_TYPES = frozenset({"catalog_table", "flow", "visualization", "dashboard", "global_artifact"})


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


def _granted_namespace_permissions(db: Session, user_id: int, group_ids: list[int] | None = None) -> dict[int, str]:
    """namespace_id -> highest granted permission, expanded to direct children.

    Namespaces are hard-capped at two levels (catalog -> schema), so one
    parent_id expansion covers the whole subtree. Computing both permission
    levels in one walk lets callers avoid a second expansion.

    ``group_ids`` may be passed pre-computed (per-request memo) to avoid
    re-running the membership query on every catalog list/tree call.
    """
    gids = user_group_ids(db, user_id) if group_ids is None else group_ids
    if not gids:
        return {}
    rows = db.query(db_models.ResourceGrant.resource_id, db_models.ResourceGrant.permission).filter(
        db_models.ResourceGrant.resource_type == "catalog_namespace",
        db_models.ResourceGrant.group_id.in_(gids),
    )
    perms: dict[int, str] = {}
    for ns_id, permission in rows:
        if permission == PERMISSION_MANAGE or ns_id not in perms:
            perms[ns_id] = permission
    if not perms:
        return {}
    children = db.query(db_models.CatalogNamespace.id, db_models.CatalogNamespace.parent_id).filter(
        db_models.CatalogNamespace.parent_id.in_(perms.keys())
    )
    for child_id, parent_id in children:
        parent_perm = perms[parent_id]
        if child_id not in perms or parent_perm == PERMISSION_MANAGE:
            perms[child_id] = parent_perm
    return perms


def expand_namespace_grants(
    db: Session,
    user_id: int,
    permission: str = PERMISSION_USE,
    group_ids: list[int] | None = None,
    ns_perms: dict[int, str] | None = None,
) -> set[int]:
    """Namespace ids granted to the user's groups, plus their direct children.

    ``ns_perms`` (the full ``_granted_namespace_permissions`` map) may be passed
    pre-computed to skip recomputation.
    """
    if not sharing_enabled():
        return set()
    perms = _granted_namespace_permissions(db, user_id, group_ids=group_ids) if ns_perms is None else ns_perms
    if permission == PERMISSION_MANAGE:
        return {ns_id for ns_id, p in perms.items() if p == PERMISSION_MANAGE}
    return set(perms)


def _direct_granted_ids(
    db: Session, user_id: int, resource_type: str, permission: str, group_ids: list[int] | None = None
) -> set[int]:
    gids = user_group_ids(db, user_id) if group_ids is None else group_ids
    if not gids:
        return set()
    q = db.query(db_models.ResourceGrant.resource_id).filter(
        db_models.ResourceGrant.resource_type == resource_type,
        db_models.ResourceGrant.group_id.in_(gids),
    )
    if permission == PERMISSION_MANAGE:
        q = q.filter(db_models.ResourceGrant.permission == PERMISSION_MANAGE)
    return {r[0] for r in q}


def granted_resource_ids(
    db: Session,
    user_id: int,
    resource_type: str,
    permission: str = PERMISSION_USE,
    group_ids: list[int] | None = None,
    ns_perms: dict[int, str] | None = None,
) -> set[int]:
    """Resource ids of ``resource_type`` the user can reach via group grants.

    ``use`` matches both grant levels; ``manage`` only manage-level grants.
    Tables and flows union direct grants with namespace-inherited ones.

    ``group_ids`` / ``ns_perms`` may be passed pre-computed (per-request memo)
    so list/tree endpoints that hit many resource types share one membership and
    one namespace-grant query instead of re-running them per type.
    """
    if not sharing_enabled():
        return set()
    ids = _direct_granted_ids(db, user_id, resource_type, permission, group_ids=group_ids)
    if resource_type in _NAMESPACE_SCOPED_TYPES:
        ns_ids = expand_namespace_grants(db, user_id, permission, group_ids=group_ids, ns_perms=ns_perms)
        if ns_ids:
            spec = RESOURCE_REGISTRY[resource_type]
            rows = db.query(spec.model.id).filter(spec.model.namespace_id.in_(ns_ids))
            ids |= {r[0] for r in rows}
    return ids


def granted_access_details(
    db: Session,
    user_id: int,
    resource_type: str,
    group_ids: list[int] | None = None,
    ns_perms: dict[int, str] | None = None,
) -> dict[int, tuple[str, int | None]]:
    """resource_id -> (highest permission, granted_by user id) for list annotation.

    Namespace-inherited table/flow access is reported with ``granted_by=None``
    (the grant lives on the namespace, not the item).

    ``group_ids`` / ``ns_perms`` may be passed pre-computed (per-request memo).
    """
    if not sharing_enabled():
        return {}
    gids = user_group_ids(db, user_id) if group_ids is None else group_ids
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
        ns_perms_map = _granted_namespace_permissions(db, user_id, group_ids=gids) if ns_perms is None else ns_perms
        if ns_perms_map:
            spec = RESOURCE_REGISTRY[resource_type]
            rows = db.query(spec.model.id, spec.model.namespace_id).filter(
                spec.model.namespace_id.in_(ns_perms_map.keys())
            )
            for resource_id, ns_id in rows:
                permission = ns_perms_map[ns_id]
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


def user_id_can_use(db: Session, user_id: int | None, resource_type: str, resource_id: int) -> bool:
    """``can_use`` for a bare user id (resolves the ``User`` row).

    Used by deep resolution paths (virtual-table SQL context) that only carry a
    user id. Unrestricted when sharing is off or the user id is ``None``
    (internal/unscoped resolution). A non-None id that doesn't resolve to a
    ``User`` row is denied — a stale id must never widen access.
    """
    if not sharing_enabled() or user_id is None:
        return True
    user = db.get(db_models.User, user_id)
    if user is None:
        return False
    return can_use(db, user, resource_type, resource_id)


def shared_resource_rows(db: Session, user_id: int, resource_type: str) -> list[tuple[object, AccessInfo]]:
    """(row, AccessInfo) pairs for rows reachable via group grants, excluding own rows.

    The single implementation behind every "list shared X" endpoint: resolves the
    grant details, loads the rows, and annotates each with the granter's username.
    """
    details = granted_access_details(db, user_id, resource_type)
    if not details:
        return []
    spec = RESOURCE_REGISTRY[resource_type]
    owner_col = getattr(spec.model, spec.owner_attr)
    rows = (
        db.query(spec.model)
        .filter(spec.model.id.in_(details.keys()), owner_col != user_id)
        .order_by(spec.model.id.asc())
        .all()
    )
    granter_ids = {details[row.id][1] for row in rows if details[row.id][1] is not None}
    usernames = {}
    if granter_ids:
        usernames = dict(
            db.query(db_models.User.id, db_models.User.username).filter(db_models.User.id.in_(granter_ids))
        )
    out = []
    for row in rows:
        permission, granted_by = details[row.id]
        out.append((row, AccessInfo(is_owner=False, access_level=permission, shared_by=usernames.get(granted_by))))
    return out


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


def _register_grant_cleanup_backstop() -> None:
    """ORM-delete backstop: deleting a registered resource row via ``session.delete``
    also deletes its grants in the same flush, so a forgotten
    ``delete_grants_for_resource`` call can't leave a grant behind to re-attach to a
    reused rowid. Bulk ``query.delete()`` paths bypass ORM events and must still
    call ``delete_grants_for_resource`` explicitly."""
    grants = db_models.ResourceGrant.__table__

    for resource_type, spec in RESOURCE_REGISTRY.items():

        def _on_delete(mapper, connection, target, _resource_type=resource_type):
            connection.execute(
                grants.delete()
                .where(grants.c.resource_type == _resource_type)
                .where(grants.c.resource_id == target.id)
            )

        event.listen(spec.model, "after_delete", _on_delete)


_register_grant_cleanup_backstop()
