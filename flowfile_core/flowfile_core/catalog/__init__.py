"""Flow Catalog service layer.

Public interface:

* ``CatalogService`` — business-logic orchestrator
* ``CatalogRepository`` — data-access protocol (for type-hints / mocking)
* ``SQLAlchemyCatalogRepository`` — concrete SQLAlchemy implementation
* Domain exceptions (``CatalogError`` hierarchy)
"""

from .exceptions import (
    AmbiguousTableError,
    CatalogError,
    DashboardNotFoundError,
    FavoriteNotFoundError,
    FlowAlreadyRunningError,
    FlowExistsError,
    FlowHasArtifactsError,
    FlowNotFoundError,
    FollowNotFoundError,
    InvalidNamespaceStorageError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NamespaceStorageLockedError,
    NestingLimitError,
    NoSnapshotError,
    NotAuthorizedError,
    NotebookExistsError,
    NotebookNotFoundError,
    RunNotFoundError,
    ScheduleConflictError,
    ScheduleNotFoundError,
    TableExistsError,
    TableFavoriteNotFoundError,
    TableNotFoundError,
    VisualizationComputeError,
    VisualizationExistsError,
    VisualizationNotFoundError,
)
from .repository import CatalogRepository, SQLAlchemyCatalogRepository
from .service import CatalogService

__all__ = [
    "CatalogService",
    "CatalogRepository",
    "SQLAlchemyCatalogRepository",
    "CatalogError",
    "NamespaceNotFoundError",
    "NamespaceExistsError",
    "NestingLimitError",
    "NamespaceNotEmptyError",
    "NamespaceStorageLockedError",
    "InvalidNamespaceStorageError",
    "FlowHasArtifactsError",
    "FlowNotFoundError",
    "FlowExistsError",
    "RunNotFoundError",
    "NotAuthorizedError",
    "FavoriteNotFoundError",
    "FollowNotFoundError",
    "NoSnapshotError",
    "ScheduleNotFoundError",
    "ScheduleConflictError",
    "FlowAlreadyRunningError",
    "TableNotFoundError",
    "TableExistsError",
    "TableFavoriteNotFoundError",
    "AmbiguousTableError",
    "VisualizationNotFoundError",
    "VisualizationExistsError",
    "VisualizationComputeError",
    "DashboardNotFoundError",
    "NotebookNotFoundError",
    "NotebookExistsError",
]
