"""Read and write one Iceberg table with the storage keys its catalog hands out.

An Iceberg REST catalog can answer a table load with short-lived keys for that table's files. These helpers pick
those keys out of a loaded table, carry them with the table's metadata location, and give them to Polars, so a
process can read the table without any storage key of its own and without talking to the catalog.

Only S3-compatible storage is handled. The keys are not renewed when they expire: load the table again for new ones.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import polars as pl

if TYPE_CHECKING:
    from pyiceberg.table import Table

_STORAGE_PREFIXES = ("s3.",)
_STORAGE_KEYS = ("client.region",)
_CATALOG_PREFIXES = ("s3.signer",)


@dataclass(frozen=True)
class IcebergTableAccess:
    """What a reader needs to open one table without its catalog: where its metadata is and the keys to its files.

    The keys are left out of ``repr`` so the object can be logged without leaking them.
    """

    metadata_location: str
    storage_properties: dict[str, str] = field(repr=False)


def storage_properties(table: Table) -> dict[str, str]:
    """The storage settings and keys the catalog gave for this table, and nothing about the catalog itself.

    ``table.io.properties`` is the client's own catalog configuration merged with what the catalog answered, so it
    also holds the catalog's address, its token or client secret and an auth manager object. Only ``s3.*`` entries
    (less ``s3.signer.*``, which name the catalog) and ``client.region`` are kept, as strings. This cannot tell keys
    the catalog answered from ``s3.*`` settings the catalog client was given: load the catalog without any.
    """
    return {
        key: str(value)
        for key, value in table.io.properties.items()
        if (key.startswith(_STORAGE_PREFIXES) and not key.startswith(_CATALOG_PREFIXES)) or key in _STORAGE_KEYS
    }


def _handed_out_keys(table: Table) -> dict[str, str]:
    """The table's storage properties, refused when they hold no key Polars can use on their own."""
    properties = storage_properties(table)
    if properties.get("s3.remote-signing-enabled", "false").lower() == "true":
        raise ValueError(
            "The catalog signs each storage request for this table itself (remote signing), which Polars cannot use."
        )
    if not properties.get("s3.access-key-id"):
        raise ValueError("The catalog handed out no storage keys for this table.")
    return properties


def table_access(table: Table) -> IcebergTableAccess:
    """Everything another process needs to read this table, taken from a table loaded from its catalog."""
    return IcebergTableAccess(table.metadata_location, _handed_out_keys(table))


def scan_table(
    access: IcebergTableAccess, *, reader_override: Literal["native", "pyiceberg"] | None = None
) -> pl.LazyFrame:
    """A lazy scan of the table, opened from its metadata file with the handed-over keys and no catalog.

    Polars translates the ``s3.*`` names to its object store's options itself, and uses the same properties to read
    the metadata file. Build the scan in the process that runs it: a serialized Iceberg scan carries its keys.
    """
    return pl.scan_iceberg(
        access.metadata_location, storage_options=access.storage_properties, reader_override=reader_override
    )


def write_table(table: Table, frame: pl.LazyFrame, mode: Literal["append", "overwrite"]) -> None:
    """Append ``frame`` to the table or replace its rows, writing the files with the keys the catalog gave.

    Polars writes the data files and the commit goes through the table's catalog, so ``table`` must be loaded from
    a catalog: a table opened from its metadata file alone can be read but not written.
    """
    frame.sink_iceberg(table, mode=mode, storage_options=_handed_out_keys(table))
