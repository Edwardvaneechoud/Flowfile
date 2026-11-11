"""Secrets and stored connections a provider elsewhere keeps for a person, beside this machine's own.

A provider's client registers itself here (``register``). The list routes show its rows after this machine's, read-only
(``secret_rows``, ``connection_rows``); a step that names a connection or secret this machine has none of takes the
provider's by name (``connection``, ``keeps_secret``), without its secrets. A provider's secret reaches a step only in
the form the run was granted for the server that runs it, which core hands on and never opens: the provider says what
that form is (``source()``). The dependency points from the provider's client to this module, never the other way.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any, Protocol


class CredentialSource(Protocol):
    """What a provider of credentials answers; a core has one."""

    def secret_rows(self, user_id: int) -> list[dict]:
        """The secrets it lists for ``user_id``, names and no value; none when it cannot be asked."""

    def connection_rows(self, user_id: int, kind: str) -> list[dict]:
        """The ``cloud``, ``database`` or ``kafka`` connections it lists for ``user_id``, no secret."""

    def carried(self, kind: str, name: str, field: str, granted: Mapping[str, str]) -> str:
        """What a step carries in a ciphertext's place for the secret ``field`` of the ``kind`` connection ``name``,
        from what the run was ``granted`` for it."""

    def storage_provider(self, name: str, granted: Mapping[str, str]) -> Any:
        """A Polars ``credential_provider`` for the cloud storage connection ``name``, opened where the plan runs."""

    def unopened(self, value: object) -> str | None:
        """Why core does not open ``value``, when it is a form this provider granted; ``None`` for any other."""

    def refusal(self, name: str | None, *, secret: bool = False, why: str | None = None) -> str:
        """Why a step that uses its connection or secret ``name`` cannot run where it was placed, in one sentence."""


_source: CredentialSource | None = None


def register(source: CredentialSource) -> None:
    """Make ``source`` this core's provider of credentials."""
    global _source
    _source = source


def source() -> CredentialSource:
    """The registered provider.

    Raises:
        LookupError: No provider registered in this process.
    """
    if _source is None:
        raise LookupError("This Flowfile has no provider of credentials other than itself")
    return _source


def secret_rows(user_id: int, taken: Collection[str] = ()) -> list[dict]:
    """The provider's secrets for ``user_id`` whose name is not ``taken`` by one of this machine's."""
    rows = _source.secret_rows(user_id) if _source is not None else []
    return [row for row in rows if row.get("name") not in taken]


def connection_rows(user_id: int, kind: str, taken: Collection[str] = ()) -> list[dict]:
    """The provider's ``kind`` connections for ``user_id`` whose name is not ``taken`` by one of this machine's."""
    rows = _source.connection_rows(user_id, kind) if _source is not None else []
    return [row for row in rows if row.get("connection_name") not in taken]


def connection(user_id: int, kind: str, name: str | None) -> dict | None:
    """The provider's row of ``user_id``'s ``kind`` connection ``name``, no secret; ``None`` when it lists none."""
    if not name:
        return None
    return next((row for row in connection_rows(user_id, kind) if row.get("connection_name") == name), None)


def keeps_secret(user_id: int, name: str | None) -> bool:
    """Whether the provider lists a secret ``name`` for ``user_id``."""
    return bool(name) and any(row.get("name") == name for row in secret_rows(user_id))


def unopened(value: object) -> str | None:
    """Why core does not open ``value``: a form a provider granted to the server that runs a step. ``None`` for a
    value of this machine's own."""
    return (_source.unopened(value) or None) if _source is not None else None
