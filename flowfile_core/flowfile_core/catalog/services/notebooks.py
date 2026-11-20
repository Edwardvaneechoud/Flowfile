"""Catalog notebooks: CRUD over saved mixed-cell (Python/SQL/Markdown) notebooks.

Notebooks are the catalog's exploration console. The DB row holds metadata only;
the cells live on disk as a deterministic YAML file keyed by ``notebook_uuid``
(``notebook_store``), so notebooks diff cleanly and fold into project
git-tracking. Execution is NOT owned by this service — SQL cells hit the
existing ``/catalog/sql/execute`` (worker) and Python cells hit the existing
kernel ``execute_cell`` endpoint; this service only persists the notebook
document and its metadata.
"""

from __future__ import annotations

import logging

from sqlalchemy.exc import IntegrityError

from flowfile_core.catalog.exceptions import NamespaceNotFoundError, NotebookExistsError, NotebookNotFoundError
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.database.models import CatalogNotebook
from flowfile_core.schemas.catalog_schema import (
    NotebookCreate,
    NotebookOut,
    NotebookSummaryOut,
    NotebookUpdate,
)

logger = logging.getLogger(__name__)


class NotebookService:
    """Owns catalog notebooks: metadata rows + on-disk cell files."""

    def __init__(self, repo: CatalogRepository, namespaces: NamespaceService) -> None:
        self.repo = repo
        self._namespaces = namespaces

    def _notebook_to_out(self, nb: CatalogNotebook) -> NotebookOut:
        return NotebookOut(
            id=nb.id,
            name=nb.name,
            description=nb.description,
            namespace_id=nb.namespace_id,
            cells=notebook_store.read_notebook_cells(nb.owner_id, nb.notebook_uuid),
            default_kernel_id=nb.default_kernel_id,
            owner_id=nb.owner_id,
            created_at=nb.created_at,
            updated_at=nb.updated_at,
            namespace_name=self._namespaces.resolve_namespace_name(nb.namespace_id),
        )

    def _notebook_to_summary(self, nb: CatalogNotebook) -> NotebookSummaryOut:
        return NotebookSummaryOut(
            id=nb.id,
            name=nb.name,
            description=nb.description,
            namespace_id=nb.namespace_id,
            default_kernel_id=nb.default_kernel_id,
            owner_id=nb.owner_id,
            created_at=nb.created_at,
            updated_at=nb.updated_at,
            namespace_name=self._namespaces.resolve_namespace_name(nb.namespace_id),
        )

    # ---- CRUD ----------------------------------------------------------- #

    def list_notebooks(self, user_id: int | None = None) -> list[NotebookSummaryOut]:
        return [self._notebook_to_summary(nb) for nb in self.repo.list_notebooks()]

    def get_notebook(self, notebook_id: int, user_id: int | None = None) -> NotebookOut:
        nb = self.repo.get_notebook(notebook_id)
        if nb is None:
            raise NotebookNotFoundError(notebook_id=notebook_id)
        return self._notebook_to_out(nb)

    def create_notebook(self, payload: NotebookCreate, user_id: int) -> NotebookOut:
        # Unfiled notebooks default to General/default so they surface in the
        # catalog tree instead of being reachable only via the notebook list.
        namespace_id = payload.namespace_id
        if namespace_id is None:
            namespace_id = self._namespaces.get_default_namespace_id()
        if namespace_id is not None and self.repo.get_namespace(namespace_id) is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        nb = CatalogNotebook(
            name=payload.name,
            description=payload.description,
            namespace_id=namespace_id,
            default_kernel_id=payload.default_kernel_id,
            owner_id=user_id,
        )
        # Commit the row first so name-uniqueness is enforced before any file is
        # written (a rejected create must leave no orphan file).
        try:
            created = self.repo.create_notebook(nb)
        except IntegrityError as exc:
            raise NotebookExistsError(payload.name, payload.namespace_id) from exc
        try:
            self._write_file(created, payload.cells)
        except Exception:
            self.repo.delete_notebook(created.id)
            raise
        return self._notebook_to_out(created)

    def update_notebook(self, notebook_id: int, payload: NotebookUpdate, user_id: int) -> NotebookOut:
        nb = self.repo.get_notebook(notebook_id)
        if nb is None:
            raise NotebookNotFoundError(notebook_id=notebook_id)
        provided = payload.model_fields_set
        if "name" in provided:
            if payload.name is None:
                raise ValueError("name cannot be cleared")
            nb.name = payload.name
        if "description" in provided:
            nb.description = payload.description
        if "namespace_id" in provided:
            if payload.namespace_id is not None and self.repo.get_namespace(payload.namespace_id) is None:
                raise NamespaceNotFoundError(namespace_id=payload.namespace_id)
            nb.namespace_id = payload.namespace_id
        if "cells" in provided and payload.cells is None:
            raise ValueError("cells cannot be cleared")
        if "default_kernel_id" in provided:
            nb.default_kernel_id = payload.default_kernel_id
        try:
            updated = self.repo.update_notebook(nb)
        except IntegrityError as exc:
            raise NotebookExistsError(nb.name, nb.namespace_id) from exc
        # Rewrite the whole file so embedded metadata stays in sync; reuse the
        # on-disk cells when the payload didn't carry new ones.
        cells = (
            payload.cells
            if "cells" in provided
            else notebook_store.read_notebook_cells(updated.owner_id, updated.notebook_uuid)
        )
        self._write_file(updated, cells)
        return self._notebook_to_out(updated)

    def delete_notebook(self, notebook_id: int, user_id: int) -> None:
        nb = self.repo.get_notebook(notebook_id)
        if nb is None:
            raise NotebookNotFoundError(notebook_id=notebook_id)
        owner_id, notebook_uuid = nb.owner_id, nb.notebook_uuid
        self.repo.delete_notebook(notebook_id)
        notebook_store.delete_notebook_file(owner_id, notebook_uuid)

    def _write_file(self, nb: CatalogNotebook, cells: list) -> None:
        notebook_store.write_notebook_file(
            nb.owner_id,
            nb.notebook_uuid,
            name=nb.name,
            description=nb.description,
            namespace_name=self._namespaces.resolve_namespace_name(nb.namespace_id),
            default_kernel_id=nb.default_kernel_id,
            cells=cells,
        )
