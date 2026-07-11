"""Secret resolution seam for custom nodes.

The SDK never talks to a database or decrypts anything itself; the hosting
process (core, worker, dry-run child) injects a resolver that knows how.
"""

from typing import Protocol, runtime_checkable

from pydantic import SecretStr


class SecretResolutionError(Exception):
    """Raised when a secret cannot be resolved in the current execution context."""


@runtime_checkable
class SecretResolver(Protocol):
    def resolve(self, secret_name: str) -> SecretStr:
        """Return the decrypted value for ``secret_name``; raise SecretResolutionError if unavailable."""
        ...
