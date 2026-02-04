"""Domain-specific exceptions for the Global Artifacts system.

These exceptions represent business-rule violations and are raised by the
service layer. Route handlers catch them and translate to appropriate
HTTP responses.
"""


class ArtifactError(Exception):
    """Base exception for all artifact domain errors."""


class ArtifactNotFoundError(ArtifactError):
    """Raised when an artifact lookup fails."""

    def __init__(
        self,
        artifact_id: int | None = None,
        name: str | None = None,
        version: int | None = None,
    ):
        self.artifact_id = artifact_id
        self.name = name
        self.version = version

        detail = "Artifact not found"
        if artifact_id is not None:
            detail = f"Artifact with id={artifact_id} not found"
        elif name is not None:
            if version is not None:
                detail = f"Artifact '{name}' version {version} not found"
            else:
                detail = f"Artifact '{name}' not found"
        super().__init__(detail)


class ArtifactStateError(ArtifactError):
    """Raised when an artifact is not in the expected state for an operation.

    Examples:
    - Trying to finalize an artifact that is already 'active' (not 'pending')
    - Trying to download a 'pending' or 'failed' artifact
    """

    def __init__(self, artifact_id: int, actual_status: str, expected_status: str = "pending"):
        self.artifact_id = artifact_id
        self.status = actual_status  # Keep for backwards compatibility
        self.actual_status = actual_status
        self.expected_status = expected_status
        super().__init__(
            f"Artifact {artifact_id} is in '{actual_status}' state, expected '{expected_status}'"
        )


# Backwards compatibility alias - TODO: Remove after deprecation period
ArtifactNotActiveError = ArtifactStateError


class ArtifactUploadError(ArtifactError):
    """Raised when artifact upload fails."""

    def __init__(self, artifact_id: int, reason: str):
        self.artifact_id = artifact_id
        self.reason = reason
        super().__init__(f"Upload failed for artifact {artifact_id}: {reason}")


class ArtifactIntegrityError(ArtifactError):
    """Raised when SHA-256 verification fails."""

    def __init__(self, expected: str, actual: str):
        self.expected = expected
        self.actual = actual
        super().__init__(f"SHA-256 mismatch: expected {expected}, got {actual}")


class StorageError(ArtifactError):
    """Raised when storage backend operations fail."""

    def __init__(self, operation: str, reason: str):
        self.operation = operation
        self.reason = reason
        super().__init__(f"Storage {operation} failed: {reason}")


class NamespaceNotFoundError(ArtifactError):
    """Raised when a namespace lookup fails for artifact operations."""

    def __init__(self, namespace_id: int):
        self.namespace_id = namespace_id
        super().__init__(f"Namespace with id={namespace_id} not found")
