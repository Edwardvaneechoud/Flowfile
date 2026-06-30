"""Namespace CRUD, tree assembly, default-namespace seeding."""

from __future__ import annotations

import logging
from collections.abc import Callable

from flowfile_core.catalog.exceptions import (
    InvalidNamespaceStorageError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NamespaceStorageLockedError,
    NestingLimitError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import artifact_to_out
from flowfile_core.catalog.storage_backend import _is_cloud_uri
from flowfile_core.catalog.validators import reject_dot_in_name
from flowfile_core.configs import settings
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, GlobalArtifact
from flowfile_core.schemas.catalog_schema import (
    CatalogTableOut,
    FlowRegistrationOut,
    GlobalArtifactOut,
    NamespaceTree,
    NotebookSummaryOut,
    VisualizationOut,
)

logger = logging.getLogger(__name__)

# Sentinel distinguishing "storage field omitted" from "explicitly set to None" on update.
STORAGE_UNSET: object = object()

# Object-storage URI scheme -> the cloud connection's provider (``storage_type``) it must pair with.
_STORAGE_SCHEME_PROVIDER = {
    "s3": "s3",
    "s3a": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "az": "adls",
    "abfs": "adls",
    "abfss": "adls",
    "adl": "adls",
}


def _validate_namespace_storage(owner_id: int, storage_uri: str | None, storage_connection_name: str | None) -> None:
    """Validate per-catalog storage: a cloud URI plus a connection that resolves for the catalog owner."""
    if not storage_uri:
        if storage_connection_name:
            raise InvalidNamespaceStorageError("storage_connection_name requires storage_uri to be set.")
        return
    if not _is_cloud_uri(storage_uri):
        raise InvalidNamespaceStorageError(
            f"storage_uri must be an object-storage URI (s3://, gs://, abfss://, ...): {storage_uri!r}"
        )
    if not storage_connection_name:
        raise InvalidNamespaceStorageError("storage_uri requires a storage_connection_name.")
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection_schema

    with get_db_context() as db:
        conn = get_cloud_connection_schema(db, storage_connection_name, owner_id)
    if conn is None:
        raise InvalidNamespaceStorageError(
            f"Cloud connection {storage_connection_name!r} was not found or is not accessible for the catalog owner."
        )
    provider = getattr(conn, "storage_type", None)
    expected_provider = _STORAGE_SCHEME_PROVIDER.get(storage_uri.split("://", 1)[0].lower())
    if expected_provider is not None and provider != expected_provider:
        raise InvalidNamespaceStorageError(
            f"storage_uri {storage_uri!r} does not match connection {storage_connection_name!r} "
            f"(provider {provider!r})."
        )


def _project_sync_namespace(owner_id: int) -> None:
    """Mirror a namespace create/update/delete into the active project's namespaces.yaml (no-op
    when none active)."""
    from flowfile_core.project import project_sync

    project_sync.namespace_changed(owner_id)


def bulk_enrich_artifacts(artifacts: list[GlobalArtifact]) -> list[GlobalArtifactOut]:
    """Serialize artifacts, flagging ones whose backing blob is missing on disk
    (filesystem backend only — exists() is one cheap stat; an S3 probe per item
    would be a network call on every load)."""
    from flowfile_core.artifacts import get_storage_backend
    from shared.artifact_storage import SharedFilesystemStorage

    storage = get_storage_backend()
    fs_storage = storage if isinstance(storage, SharedFilesystemStorage) else None
    items: list[GlobalArtifactOut] = []
    for a in artifacts:
        blob_exists = True
        if fs_storage is not None and a.storage_key:
            blob_exists = fs_storage.exists(a.storage_key)
        items.append(artifact_to_out(a, blob_exists=blob_exists))
    return items


class NamespaceService:
    """CRUD + lookup for namespaces. No peer-service dependencies."""

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo

    def resolve_namespace_name(self, namespace_id: int | None) -> str | None:
        if namespace_id is None:
            return None
        namespace = self.repo.get_namespace(namespace_id)
        return namespace.name if namespace is not None else None

    def resolve_namespace_id_by_path(self, catalog_name: str | None, schema_name: str | None) -> int | None:
        """Resolve a portable (catalog, schema) name pair to an existing namespace id (resolve-only,
        never creates). Returns the schema id (level 1), the catalog id when no schema, or None."""
        if not catalog_name:
            return None
        catalog = self.repo.get_namespace_by_name(catalog_name, None)
        if catalog is None:
            return None
        if not schema_name:
            return catalog.id
        schema = self.repo.get_namespace_by_name(schema_name, catalog.id)
        return schema.id if schema is not None else None

    def _env_default_storage(self, owner_id: int) -> tuple[str | None, str | None]:
        """Resolve the optional creation-time storage default from the environment, best-effort.

        Returns ``(None, None)`` (⇒ local) when unset or when the configured connection is not usable
        for *owner_id*, so a misconfigured env default never blocks catalog creation."""
        env_uri = settings.get_catalog_storage_uri()
        env_conn = settings.get_catalog_storage_connection()
        if not env_uri:
            return None, None
        try:
            _validate_namespace_storage(owner_id, env_uri, env_conn)
        except InvalidNamespaceStorageError:
            logger.warning(
                "FLOWFILE_CATALOG_STORAGE_* default not applied: connection %r is not usable for user %s; "
                "the new catalog uses local storage.",
                env_conn,
                owner_id,
            )
            return None, None
        return env_uri, env_conn

    def create_namespace(
        self,
        name: str,
        owner_id: int,
        parent_id: int | None = None,
        description: str | None = None,
        storage_uri: str | None = None,
        storage_connection_name: str | None = None,
    ) -> CatalogNamespace:
        """Create a catalog (level 0) or schema (level 1) namespace.

        ``storage_uri`` / ``storage_connection_name`` configure per-catalog object storage and are
        only valid on a level-0 catalog; schemas inherit their root catalog's storage.
        """
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

        explicit_storage = storage_uri is not None or storage_connection_name is not None
        if explicit_storage:
            if level != 0:
                raise InvalidNamespaceStorageError(
                    "Per-catalog storage can only be set on a catalog (level-0 namespace)."
                )
            _validate_namespace_storage(owner_id, storage_uri, storage_connection_name)
        elif level == 0:
            # Headless/docker convenience: snapshot the env default onto the new catalog (one-time copy,
            # never a live override). Best-effort — an unusable env connection falls back to local rather
            # than blocking catalog creation.
            storage_uri, storage_connection_name = self._env_default_storage(owner_id)

        namespace = CatalogNamespace(
            name=name,
            parent_id=parent_id,
            level=level,
            description=description,
            owner_id=owner_id,
            storage_uri=storage_uri,
            storage_connection_name=storage_connection_name,
        )
        created = self.repo.create_namespace(namespace)
        _project_sync_namespace(owner_id)
        return created

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
        storage_uri: str | None | object = STORAGE_UNSET,
        storage_connection_name: str | None | object = STORAGE_UNSET,
    ) -> CatalogNamespace:
        """Update a namespace's name/description, and (catalog-level only) its per-catalog storage.

        Storage is immutable once the catalog — or any schema beneath it — holds a physical table.
        """
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        if name is not None:
            namespace.name = name
        if description is not None:
            namespace.description = description

        if storage_uri is not STORAGE_UNSET or storage_connection_name is not STORAGE_UNSET:
            if namespace.level != 0:
                raise InvalidNamespaceStorageError(
                    "Per-catalog storage can only be set on a catalog (level-0 namespace)."
                )
            new_uri = namespace.storage_uri if storage_uri is STORAGE_UNSET else storage_uri
            new_conn = (
                namespace.storage_connection_name
                if storage_connection_name is STORAGE_UNSET
                else storage_connection_name
            )
            if not new_uri:
                # A connection without a URI is meaningless: clearing the URI clears both in lockstep.
                new_conn = None
            if (new_uri, new_conn) != (namespace.storage_uri, namespace.storage_connection_name):
                existing_tables = self.repo.count_physical_tables_under_namespace(namespace_id)
                if existing_tables > 0:
                    raise NamespaceStorageLockedError(namespace_id=namespace_id, tables=existing_tables)
                _validate_namespace_storage(namespace.owner_id, new_uri, new_conn)
                namespace.storage_uri = new_uri
                namespace.storage_connection_name = new_conn

        updated = self.repo.update_namespace(namespace)
        _project_sync_namespace(updated.owner_id)
        return updated

    def delete_namespace(self, namespace_id: int) -> None:
        """Delete a namespace if it has no children, flows, tables or notebooks."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        children = self.repo.count_children(namespace_id)
        flows = self.repo.count_flows_in_namespace(namespace_id)
        tables = self.repo.count_tables_in_namespace(namespace_id)
        notebooks = self.repo.count_notebooks_in_namespace(namespace_id)
        if children > 0 or flows > 0 or tables > 0 or notebooks > 0:
            raise NamespaceNotEmptyError(
                namespace_id=namespace_id, children=children, flows=flows, tables=tables, notebooks=notebooks
            )
        owner_id = namespace.owner_id
        self.repo.delete_namespace(namespace_id)
        _project_sync_namespace(owner_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        """Retrieve a single namespace by ID."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        return namespace

    def get_root_namespace(self, namespace_id: int) -> CatalogNamespace | None:
        """Return the level-0 catalog at the top of *namespace_id*'s parent chain."""
        return self.repo.get_root_namespace(namespace_id)

    def get_effective_storage(self, namespace_id: int | None) -> tuple[str, str | None, int] | None:
        """Resolve (storage_uri, storage_connection_name, owner_id) inherited from the root catalog,
        or None when the root sets no storage (⇒ local filesystem)."""
        if namespace_id is None:
            return None
        root = self.repo.get_root_namespace(namespace_id)
        if root is None or not root.storage_uri:
            return None
        return (root.storage_uri, root.storage_connection_name, root.owner_id)

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        """List namespaces, optionally filtered by parent."""
        return self.repo.list_namespaces(parent_id)

    def get_namespace_tree(
        self,
        user_id: int,
        *,
        list_visualizations: Callable[[int | None], list[VisualizationOut]],
        list_notebooks: Callable[[int | None], list[NotebookSummaryOut]],
        bulk_enrich_tables: Callable[[list, int], list[CatalogTableOut]],
        bulk_enrich_flows: Callable[[list[FlowRegistration], int], list[FlowRegistrationOut]],
    ) -> list[NamespaceTree]:
        """Build the full catalog tree with flows nested under schemas.

        Cross-cutting enrichment is injected via callbacks so this service
        stays a leaf — see ``CatalogService.get_namespace_tree`` for wiring.
        """
        catalogs = self.repo.list_root_namespaces()

        all_flows: list[FlowRegistration] = []
        namespace_flow_map: dict[int, list[FlowRegistration]] = {}
        namespace_artifact_map: dict[int, list[GlobalArtifactOut]] = {}
        namespace_table_map: dict[int, list[CatalogTableOut]] = {}

        def _artifacts_out(ns_id: int) -> list[GlobalArtifactOut]:
            return bulk_enrich_artifacts(self.repo.list_artifacts_for_namespace(ns_id))

        # Visualizations are surfaced as a peer of flows / tables / artifacts in
        # whatever namespace they were saved into (their own ``namespace_id``,
        # not the parent table's). Resolve once and bucket by namespace.
        viz_by_namespace: dict[int, list[VisualizationOut]] = {}
        for v in list_visualizations(user_id):
            if v.namespace_id is None:
                continue
            viz_by_namespace.setdefault(v.namespace_id, []).append(v)

        nb_by_namespace: dict[int, list[NotebookSummaryOut]] = {}
        for nb in list_notebooks(user_id):
            if nb.namespace_id is None:
                continue
            nb_by_namespace.setdefault(nb.namespace_id, []).append(nb)

        for cat in catalogs:
            cat_flows = self.repo.list_flows(namespace_id=cat.id)
            namespace_flow_map[cat.id] = cat_flows
            all_flows.extend(cat_flows)
            namespace_artifact_map[cat.id] = _artifacts_out(cat.id)
            namespace_table_map[cat.id] = bulk_enrich_tables(self.repo.list_tables_for_namespace(cat.id), user_id)

            for schema in self.repo.list_child_namespaces(cat.id):
                schema_flows = self.repo.list_flows(namespace_id=schema.id)
                namespace_flow_map[schema.id] = schema_flows
                all_flows.extend(schema_flows)
                namespace_artifact_map[schema.id] = _artifacts_out(schema.id)
                namespace_table_map[schema.id] = bulk_enrich_tables(
                    self.repo.list_tables_for_namespace(schema.id), user_id
                )

        enriched = bulk_enrich_flows(all_flows, user_id)
        enriched_map = {e.id: e for e in enriched}

        result: list[NamespaceTree] = []
        for cat in catalogs:
            schemas = self.repo.list_child_namespaces(cat.id)
            children: list[NamespaceTree] = []
            for schema in schemas:
                schema_flows = namespace_flow_map.get(schema.id, [])
                flow_outs = [enriched_map[f.id] for f in schema_flows if f.id in enriched_map]
                children.append(
                    NamespaceTree(
                        id=schema.id,
                        name=schema.name,
                        parent_id=schema.parent_id,
                        level=schema.level,
                        description=schema.description,
                        owner_id=schema.owner_id,
                        created_at=schema.created_at,
                        updated_at=schema.updated_at,
                        storage_uri=schema.storage_uri,
                        storage_connection_name=schema.storage_connection_name,
                        children=[],
                        flows=flow_outs,
                        artifacts=namespace_artifact_map.get(schema.id, []),
                        tables=namespace_table_map.get(schema.id, []),
                        visualizations=viz_by_namespace.get(schema.id, []),
                        notebooks=nb_by_namespace.get(schema.id, []),
                    )
                )
            cat_flows = namespace_flow_map.get(cat.id, [])
            root_flow_outs = [enriched_map[f.id] for f in cat_flows if f.id in enriched_map]
            result.append(
                NamespaceTree(
                    id=cat.id,
                    name=cat.name,
                    parent_id=cat.parent_id,
                    level=cat.level,
                    description=cat.description,
                    owner_id=cat.owner_id,
                    created_at=cat.created_at,
                    updated_at=cat.updated_at,
                    storage_uri=cat.storage_uri,
                    storage_connection_name=cat.storage_connection_name,
                    children=children,
                    flows=root_flow_outs,
                    artifacts=namespace_artifact_map.get(cat.id, []),
                    tables=namespace_table_map.get(cat.id, []),
                    visualizations=viz_by_namespace.get(cat.id, []),
                    notebooks=nb_by_namespace.get(cat.id, []),
                )
            )
        return result

    def get_default_namespace_id(self) -> int | None:
        """Return the ID of the default 'default' schema under 'General'."""
        general = self.repo.get_namespace_by_name("General", parent_id=None)
        if general is None:
            return None
        default_schema = self.repo.get_namespace_by_name("default", parent_id=general.id)
        if default_schema is None:
            return None
        return default_schema.id
