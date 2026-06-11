"""Per-request catalog authorization resolver.

Wraps ``auth.sharing`` for the catalog's resource types (namespaces, tables,
flows). Built in ``routes/catalog.py`` from the request user and attached to the
``CatalogService``. When absent (internal callers, electron mode, tests) the
service runs unrestricted — exactly today's behavior. When present and
``restricted`` is True (multi-user, non-admin, non-kernel), the catalog becomes
private-by-default: a user sees only resources they own or were granted, plus
public namespaces as tree containers.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.catalog.exceptions import NotAuthorizedError
from flowfile_core.database.models import CatalogNamespace


class AccessResolver:
    def __init__(self, db: Session, user) -> None:
        self.db = db
        self.user = user
        self.user_id = getattr(user, "id", None)
        # Admins and the kernel internal-service principal bypass filtering;
        # electron mode is unrestricted because sharing_enabled() is False.
        self.restricted = (
            sharing.sharing_enabled()
            and not getattr(user, "is_admin", False)
            and not sharing.is_synthetic_principal(user)
        )
        # The resolver lives for one request; id-set lookups repeat across resource
        # types in tree/list endpoints, so memoize them for the request's lifetime.
        self._accessible_cache: dict[str, set[int]] = {}
        self._public_cache: set[int] | None = None

    # ---- per-resource checks ----

    def can_use(self, resource_type: str, resource_id: int, owner_id: int | None = None) -> bool:
        if not self.restricted:
            return True
        return sharing.can_use(self.db, self.user, resource_type, resource_id, owner_id)

    def can_manage(self, resource_type: str, resource_id: int, owner_id: int | None = None) -> bool:
        if not self.restricted:
            return True
        return sharing.can_manage(self.db, self.user, resource_type, resource_id, owner_id)

    def require_use(self, resource_type: str, resource_id: int, owner_id: int | None = None) -> None:
        if not self.can_use(resource_type, resource_id, owner_id):
            raise NotAuthorizedError(user_id=self.user_id or -1, action=f"access this {resource_type}")

    def require_manage(self, resource_type: str, resource_id: int, owner_id: int | None = None) -> None:
        if not self.can_manage(resource_type, resource_id, owner_id):
            raise NotAuthorizedError(user_id=self.user_id or -1, action=f"modify this {resource_type}")

    # ---- id sets for list filtering ----

    def accessible_ids(self, resource_type: str) -> set[int]:
        """Own ∪ granted ids for a resource type (only meaningful when restricted)."""
        cached = self._accessible_cache.get(resource_type)
        if cached is None:
            spec = sharing.RESOURCE_REGISTRY[resource_type]
            owner_col = getattr(spec.model, spec.owner_attr)
            own = {r[0] for r in self.db.query(spec.model.id).filter(owner_col == self.user_id)}
            cached = own | sharing.granted_resource_ids(self.db, self.user_id, resource_type)
            self._accessible_cache[resource_type] = cached
        return cached

    def public_namespace_ids(self) -> set[int]:
        if self._public_cache is None:
            rows = self.db.query(CatalogNamespace.id).filter(CatalogNamespace.is_public.is_(True))
            self._public_cache = {r[0] for r in rows}
        return self._public_cache

    def visible_namespace_ids(self) -> set[int]:
        """Namespaces a user may see as tree containers: owned ∪ granted(+children) ∪ public."""
        return self.accessible_ids("catalog_namespace") | self.public_namespace_ids()

    def writable_namespace_ids(self) -> set[int]:
        """Namespaces a user may create/move items into: owned ∪ manage-granted(+children) ∪ public.

        A use-level grant makes a namespace visible (read) but never a write target.
        """
        own = {r[0] for r in self.db.query(CatalogNamespace.id).filter(CatalogNamespace.owner_id == self.user_id)}
        manage = sharing.expand_namespace_grants(self.db, self.user_id, sharing.PERMISSION_MANAGE)
        return own | manage | self.public_namespace_ids()
