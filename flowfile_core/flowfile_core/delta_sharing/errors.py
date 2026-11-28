"""The protocol's errors: an HTTP status and the ``{errorCode, message}`` body Delta Sharing clients read."""

from __future__ import annotations


class SharingError(Exception):
    """A refusal answered to a recipient as ``{"errorCode": code, "message": message}`` with *status*."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def not_found(what: str) -> SharingError:
    """404 for anything the recipient may not read, so no share, schema or table is confirmed to exist."""
    return SharingError(404, "RESOURCE_DOES_NOT_EXIST", f"{what} does not exist or you have no access to it.")


def unauthenticated() -> SharingError:
    return SharingError(401, "UNAUTHENTICATED", "The bearer token is missing, unknown, disabled or expired.")


def invalid(message: str) -> SharingError:
    return SharingError(400, "INVALID_PARAMETER_VALUE", message)
