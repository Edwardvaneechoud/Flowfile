"""Flow Catalog service layer.

Public interface:

* ``CatalogService`` — business-logic orchestrator
* ``CatalogRepository`` — data-access protocol (for type-hints / mocking)
* ``SQLAlchemyCatalogRepository`` — concrete SQLAlchemy implementation
* Domain exceptions (``CatalogError`` hierarchy)
"""

from .exceptions import (
    CatalogError,
    FavoriteNotFoundError,
    FlowExistsError,
    FlowHasArtifactsError,
    FlowNotFoundError,
    FollowNotFoundError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NestingLimitError,
    NoSnapshotError,
    NotAuthorizedError,
    RunNotFoundError,
    TableExistsError,
    TableNotFoundError,
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
    "FlowHasArtifactsError",
    "FlowNotFoundError",
    "FlowExistsError",
    "RunNotFoundError",
    "NotAuthorizedError",
    "FavoriteNotFoundError",
    "FollowNotFoundError",
    "NoSnapshotError",
    "TableNotFoundError",
    "TableExistsError",
]
