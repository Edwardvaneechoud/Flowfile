"""Namespace CRUD, tree assembly, default-namespace seeding."""

from __future__ import annotations

import logging

from flowfile_core.catalog.exceptions import (
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NestingLimitError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.validators import reject_dot_in_name
from flowfile_core.database.models import CatalogNamespace

logger = logging.getLogger(__name__)


class NamespaceService:
    """CRUD + lookup for namespaces. No peer-service dependencies."""

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo

    def resolve_namespace_name(self, namespace_id: int | None) -> str | None:
        if namespace_id is None:
            return None
        namespace = self.repo.get_namespace(namespace_id)
        return namespace.name if namespace is not None else None

    def create_namespace(
        self,
        name: str,
        owner_id: int,
        parent_id: int | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Create a catalog (level 0) or schema (level 1) namespace."""
        reject_dot_in_name(name, "Namespace")
        level = 0
        if parent_id is not None:
            parent = self.repo.get_namespace(parent_id)
            if parent is None:
                raise NamespaceNotFoundError(namespace_id=parent_id)
            if parent.level >= 1:
                raise NestingLimitError(parent_id=parent_id, parent_level=parent.level)
            level = parent.level + 1

        existing = self.repo.get_namespace_by_name(name, parent_id)
        if existing is not None:
            raise NamespaceExistsError(name=name, parent_id=parent_id)

        namespace = CatalogNamespace(
            name=name,
            parent_id=parent_id,
            level=level,
            description=description,
            owner_id=owner_id,
        )
        return self.repo.create_namespace(namespace)

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Update a namespace's name and/or description."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        if name is not None:
            namespace.name = name
        if description is not None:
            namespace.description = description
        return self.repo.update_namespace(namespace)

    def delete_namespace(self, namespace_id: int) -> None:
        """Delete a namespace if it has no children, flows or tables."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        children = self.repo.count_children(namespace_id)
        flows = self.repo.count_flows_in_namespace(namespace_id)
        tables = self.repo.count_tables_in_namespace(namespace_id)
        if children > 0 or flows > 0 or tables > 0:
            raise NamespaceNotEmptyError(
                namespace_id=namespace_id, children=children, flows=flows, tables=tables
            )
        self.repo.delete_namespace(namespace_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        """Retrieve a single namespace by ID."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        return namespace

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        """List namespaces, optionally filtered by parent."""
        return self.repo.list_namespaces(parent_id)

    def get_default_namespace_id(self) -> int | None:
        """Return the ID of the default 'default' schema under 'General'."""
        general = self.repo.get_namespace_by_name("General", parent_id=None)
        if general is None:
            return None
        default_schema = self.repo.get_namespace_by_name("default", parent_id=general.id)
        if default_schema is None:
            return None
        return default_schema.id
