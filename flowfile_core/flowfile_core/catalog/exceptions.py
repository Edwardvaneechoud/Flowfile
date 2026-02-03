"""Domain-specific exceptions for the Flow Catalog system.

These exceptions represent business-rule violations and are raised by the
service layer.  Route handlers catch them and translate to appropriate
HTTP responses.
"""


class CatalogError(Exception):
    """Base exception for all catalog domain errors."""


class NamespaceNotFoundError(CatalogError):
    """Raised when a namespace lookup fails."""

    def __init__(self, namespace_id: int | None = None, name: str | None = None):
        self.namespace_id = namespace_id
        self.name = name
        detail = "Namespace not found"
        if namespace_id is not None:
            detail = f"Namespace with id={namespace_id} not found"
        elif name is not None:
            detail = f"Namespace '{name}' not found"
        super().__init__(detail)


class NamespaceExistsError(CatalogError):
    """Raised when attempting to create a duplicate namespace."""

    def __init__(self, name: str, parent_id: int | None = None):
        self.name = name
        self.parent_id = parent_id
        super().__init__(
            f"Namespace '{name}' already exists"
            + (f" under parent_id={parent_id}" if parent_id is not None else " at root level")
        )


class NestingLimitError(CatalogError):
    """Raised when attempting to nest namespaces deeper than catalog -> schema."""

    def __init__(self, parent_id: int, parent_level: int):
        self.parent_id = parent_id
        self.parent_level = parent_level
        super().__init__("Cannot nest deeper than catalog -> schema")


class NamespaceNotEmptyError(CatalogError):
    """Raised when trying to delete a namespace that still has children or flows."""

    def __init__(self, namespace_id: int, children: int = 0, flows: int = 0):
        self.namespace_id = namespace_id
        self.children = children
        self.flows = flows
        super().__init__("Cannot delete namespace with children or flows")


class FlowNotFoundError(CatalogError):
    """Raised when a flow registration lookup fails."""

    def __init__(self, registration_id: int | None = None, name: str | None = None):
        self.registration_id = registration_id
        self.name = name
        detail = "Flow not found"
        if registration_id is not None:
            detail = f"Flow with id={registration_id} not found"
        elif name is not None:
            detail = f"Flow '{name}' not found"
        super().__init__(detail)


class FlowExistsError(CatalogError):
    """Raised when attempting to create a duplicate flow registration."""

    def __init__(self, name: str, namespace_id: int | None = None):
        self.name = name
        self.namespace_id = namespace_id
        super().__init__(f"Flow '{name}' already exists in namespace_id={namespace_id}")


class RunNotFoundError(CatalogError):
    """Raised when a flow run lookup fails."""

    def __init__(self, run_id: int):
        self.run_id = run_id
        super().__init__(f"Run with id={run_id} not found")


class NotAuthorizedError(CatalogError):
    """Raised when a user attempts an action they are not permitted to perform."""

    def __init__(self, user_id: int, action: str = "perform this action"):
        self.user_id = user_id
        self.action = action
        super().__init__(f"User {user_id} is not authorized to {action}")


class FavoriteNotFoundError(CatalogError):
    """Raised when a favorite record is not found."""

    def __init__(self, user_id: int, registration_id: int):
        self.user_id = user_id
        self.registration_id = registration_id
        super().__init__(f"Favorite not found for user={user_id}, flow={registration_id}")


class FollowNotFoundError(CatalogError):
    """Raised when a follow record is not found."""

    def __init__(self, user_id: int, registration_id: int):
        self.user_id = user_id
        self.registration_id = registration_id
        super().__init__(f"Follow not found for user={user_id}, flow={registration_id}")


class NoSnapshotError(CatalogError):
    """Raised when a run has no flow snapshot available."""

    def __init__(self, run_id: int):
        self.run_id = run_id
        super().__init__(f"No flow snapshot available for run id={run_id}")
