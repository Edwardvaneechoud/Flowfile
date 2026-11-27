"""A separate process that reads or writes one Iceberg table and prints one JSON line about what it saw.

Run as ``python -m shared.tests.iceberg.table_child`` with a JSON document on stdin. It is given a catalog's address
(``"catalog"``), a table's metadata location and storage keys (``"access"``), a table ref (``"ref"``) or a serialized
plan (``"plan"``), and nothing else: it never imports the module that holds the test store's root keys, so those are
not even in its memory. A sign-in comes as ``"decrypts"``, a ``{ciphertext: plain}`` map this process decrypts with, as
the worker decrypts a stored secret with its master key.
"""

import base64
import io
import json
import os
import sys
import warnings

import polars as pl

from shared.iceberg_utils import (
    IcebergTableAccess,
    IcebergTableRef,
    register_run_log,
    scan_iceberg_table,
    scan_table,
    storage_properties,
    table_access,
    table_schema,
    write_iceberg_table,
    write_table,
)

READERS = {"default": None, "pyiceberg": "pyiceberg", "native": "native"}


def rows_frame(rows: int, start: int = 0) -> pl.LazyFrame:
    """Deterministic rows ``start .. start + rows - 1``; the tests compute what to expect from the same formula."""
    return (
        pl.LazyFrame()
        .select(pl.int_range(start, start + rows, dtype=pl.Int64).alias("id"))
        .with_columns(
            grp=pl.col("id") % 10,
            val=pl.col("id").cast(pl.Float64) * 0.5,
            name=pl.format("name-{}", pl.col("id")),
        )
    )


def expected(rows: int, start: int = 0) -> tuple[int, int]:
    """Row count and id sum of ``rows_frame(rows, start)``."""
    return rows, sum(range(start, start + rows))


def _load(payload: dict):
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("child", **payload["catalog"])
    return catalog.load_table(payload["identifier"])


def _count(frame: pl.LazyFrame) -> tuple[int, int]:
    result = frame.select(pl.len().alias("rows"), pl.col("id").sum().alias("id_sum")).collect()
    return int(result["rows"][0]), int(result["id_sum"][0] or 0)


def read(payload: dict) -> dict:
    if "access" in payload:
        access = IcebergTableAccess(**payload["access"])
    else:
        access = table_access(_load(payload))
    answer: dict = {"readers": {}}
    caught: list[str] = []
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        for name, override in READERS.items():
            try:
                rows, id_sum = _count(scan_table(access, reader_override=override))
            except Exception as exc:  # noqa: BLE001
                if name == "default":
                    raise
                answer["readers"][name] = f"error: {type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
                continue
            if name == "default":
                answer.update(rows=rows, id_sum=id_sum)
            elif (rows, id_sum) != (answer["rows"], answer["id_sum"]):
                raise SystemExit(f"reader {name} read {rows} rows, id sum {id_sum}; default read {answer['rows']}")
            answer["readers"][name] = "ok"
        scan = scan_table(access)
        answer["len_pushdown"] = int(scan.select(pl.len()).collect().item())
        bound = int(payload.get("bound", 0))
        answer["filtered_rows"] = int(scan.filter(pl.col("id") > bound).select(pl.len()).collect().item())
        caught = sorted({f"{w.category.__name__}: {str(w.message).splitlines()[0][:200]}" for w in seen})
    answer["warnings"] = caught
    answer["property_names"] = sorted(access.storage_properties)
    answer["aws_env"] = sorted(name for name in os.environ if name.startswith("AWS_"))
    return answer


def write(payload: dict) -> dict:
    table = _load(payload)
    write_table(table, rows_frame(int(payload["rows"]), int(payload.get("start", 0))), payload["mode"])
    table.refresh()
    return {
        "snapshot_id": table.current_snapshot().snapshot_id,
        "property_names": sorted(storage_properties(table)),
        "aws_env": sorted(name for name in os.environ if name.startswith("AWS_")),
    }


def _aws_env() -> list[str]:
    return sorted(name for name in os.environ if name.startswith("AWS_"))


def _noting() -> list[str]:
    notes: list[str] = []
    register_run_log(lambda flow_id, node_id, message: notes.append(message))
    return notes


def collect_plan(payload: dict) -> dict:
    """Read the table through a plan another process built and serialized; the plan is all this process is given."""
    notes = _noting()
    plan = pl.LazyFrame.deserialize(io.BytesIO(base64.b64decode(payload["plan"])))
    with warnings.catch_warnings(record=True) as seen:
        rows, id_sum = _count(plan)
        answer = {"rows": rows, "id_sum": id_sum}
        if "bound" in payload:
            filtered = plan.filter(pl.col("id") > int(payload["bound"])).select("id", "grp")
            answer["filtered_rows"] = filtered.select(pl.len()).collect().item()
    answer["warnings"] = sorted({f"{w.category.__name__}: {str(w.message).splitlines()[0][:200]}" for w in seen})
    return {**answer, "notes": notes, "aws_env": _aws_env()}


def collect_count(payload: dict) -> dict:
    """The one value of a plan another process built that counts rows, such as ``scan.with_row_index().select(pl.len())``."""
    plan = pl.LazyFrame.deserialize(io.BytesIO(base64.b64decode(payload["plan"])))
    return {"count": int(plan.collect().item()), "aws_env": _aws_env()}


def schema_ref(payload: dict) -> dict:
    schema = table_schema(IcebergTableRef(**payload["ref"]))
    return {"schema": {name: str(dtype) for name, dtype in schema.items()}, "aws_env": _aws_env()}


def write_ref(payload: dict) -> dict:
    """Write rows through a ref, then read them back through a fresh scan of the same ref."""
    notes = _noting()
    ref = IcebergTableRef(**payload["ref"])
    write_iceberg_table(ref, rows_frame(int(payload["rows"]), int(payload.get("start", 0))), payload["mode"])
    rows, id_sum = _count(scan_iceberg_table(ref))
    return {"rows": rows, "id_sum": id_sum, "notes": notes, "aws_env": _aws_env()}


OPS = {
    "read": read,
    "write": write,
    "collect_plan": collect_plan,
    "collect_count": collect_count,
    "schema_ref": schema_ref,
    "write_ref": write_ref,
}


def main() -> None:
    payload = json.loads(sys.stdin.read())
    if "decrypts" in payload:
        from shared.cloud_credential_provider import register_secret_decryptor

        register_secret_decryptor(payload.pop("decrypts").__getitem__)
    print(json.dumps(OPS[payload["op"]](payload)))


if __name__ == "__main__":
    main()
