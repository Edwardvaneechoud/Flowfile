"""Flow Catalog service layer.

Public interface:

* ``CatalogService`` — business-logic orchestrator
* ``CatalogRepository`` — data-access protocol (for type-hints / mocking)
* ``SQLAlchemyCatalogRepository`` — concrete SQLAlchemy implementation
* Domain exceptions (``CatalogError`` hierarchy)
"""

from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from .service import CatalogService


def __getattr__(name: str):
    # ``.service`` transitively builds the whole catalog schema/serializer stack
    # (~150ms of pydantic model construction). flow_graph — and through it
    # `import flowfile_frame` — imports this package eagerly, so resolve
    # CatalogService on first use instead.
    if name == "CatalogService":
        from .service import CatalogService

        return CatalogService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
