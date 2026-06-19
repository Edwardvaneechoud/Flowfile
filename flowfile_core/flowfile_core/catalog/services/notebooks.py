"""Catalog notebooks: CRUD over saved mixed-cell (Python/SQL/Markdown) notebooks.

Notebooks are the catalog's exploration console. Cells are stored as a JSON
document in ``CatalogNotebook.cells_json`` and round-tripped through
``NotebookCellModel`` here. Execution is NOT owned by this service — SQL cells
hit the existing ``/catalog/sql/execute`` (worker) and Python cells hit the
existing kernel ``execute_cell`` endpoint; this service only persists the
notebook document and its metadata.
"""

from __future__ import annotations

import json
import logging

from sqlalchemy.exc import IntegrityError

from flowfile_core.catalog.exceptions import NamespaceNotFoundError, NotebookExistsError, NotebookNotFoundError
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.database.models import CatalogNotebook
from flowfile_core.schemas.catalog_schema import (
    NotebookCellModel,
    NotebookCreate,
    NotebookOut,
    NotebookSummaryOut,
    NotebookUpdate,
)

logger = logging.getLogger(__name__)


class NotebookService:
    """Owns catalog notebooks: CRUD + cell serialization."""

    def __init__(self, repo: CatalogRepository, namespaces: NamespaceService) -> None:
        self.repo = repo
        self._namespaces = namespaces

    # ---- serialization -------------------------------------------------- #

    @staticmethod
    def _parse_cells(cells_json: str | None) -> list[NotebookCellModel]:
        try:
            raw = json.loads(cells_json) if cells_json else []
        except (TypeError, ValueError):
            raw = []
        if not isinstance(raw, list):
            return []
        cells: list[NotebookCellModel] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                cells.append(NotebookCellModel.model_validate(item))
            except (TypeError, ValueError):
                continue
        return cells

    @staticmethod
    def _dump_cells(cells: list[NotebookCellModel]) -> str:
        return json.dumps([c.model_dump() for c in cells])

    def _notebook_to_out(self, nb: CatalogNotebook) -> NotebookOut:
        return NotebookOut(
            id=nb.id,
            name=nb.name,
            description=nb.description,
            namespace_id=nb.namespace_id,
            cells=self._parse_cells(nb.cells_json),
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
        if payload.namespace_id is not None and self.repo.get_namespace(payload.namespace_id) is None:
            raise NamespaceNotFoundError(namespace_id=payload.namespace_id)
        nb = CatalogNotebook(
            name=payload.name,
            description=payload.description,
            namespace_id=payload.namespace_id,
            cells_json=self._dump_cells(payload.cells),
            default_kernel_id=payload.default_kernel_id,
            owner_id=user_id,
        )
        try:
            created = self.repo.create_notebook(nb)
        except IntegrityError as exc:
            raise NotebookExistsError(payload.name, payload.namespace_id) from exc
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
        if "cells" in provided:
            if payload.cells is None:
                raise ValueError("cells cannot be cleared")
            nb.cells_json = self._dump_cells(payload.cells)
        if "default_kernel_id" in provided:
            nb.default_kernel_id = payload.default_kernel_id
        try:
            updated = self.repo.update_notebook(nb)
        except IntegrityError as exc:
            raise NotebookExistsError(nb.name, nb.namespace_id) from exc
        return self._notebook_to_out(updated)

    def delete_notebook(self, notebook_id: int, user_id: int) -> None:
        nb = self.repo.get_notebook(notebook_id)
        if nb is None:
            raise NotebookNotFoundError(notebook_id=notebook_id)
        self.repo.delete_notebook(notebook_id)
