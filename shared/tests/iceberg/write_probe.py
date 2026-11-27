"""Append generated rows to one Iceberg table in a fresh process and report how long it took and the memory it used.

Run as ``python -m shared.tests.iceberg.write_probe`` with ``{"catalog", "identifier", "path", "rows"}`` on stdin.
``path`` is ``"sink"`` (Polars streams the rows into the table's files, one generated batch at a time) or
``"in_memory"`` (the rows are collected into one frame first, then written). It prints one JSON line.
"""

import json
import sys
import time

import polars as pl

BATCH_ROWS = 1_000_000


def probe_batch(start: int, rows: int) -> pl.DataFrame:
    """Rows ``start .. start + rows - 1``: two integers, a float and strings of about 20 and 40 characters."""
    ids = pl.int_range(start, start + rows, dtype=pl.Int64, eager=True).alias("id")
    padded = ids.cast(pl.String)
    return pl.DataFrame(
        {
            "id": ids,
            "grp": ids % 100,
            "val": ids.cast(pl.Float64) * 0.25,
            "name": "customer-" + padded.str.zfill(11),
            "note": "order note for row " + padded.str.zfill(21),
        }
    )


def generated_rows(rows: int) -> pl.LazyFrame:
    """A lazy source that makes its rows in batches, so it never holds more than one batch."""

    def source(with_columns, predicate, n_rows, _batch_size):
        remaining = rows if n_rows is None else min(rows, n_rows)
        for start in range(0, remaining, BATCH_ROWS):
            batch = probe_batch(start, min(BATCH_ROWS, remaining - start))
            if predicate is not None:
                batch = batch.filter(predicate)
            yield batch.select(with_columns) if with_columns else batch

    return pl.io.plugins.register_io_source(source, schema=probe_batch(0, 1).schema)


def _peak_rss_bytes() -> int:
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def main() -> None:
    import psutil
    from pyiceberg.catalog import load_catalog

    from shared.iceberg_utils import write_table

    payload = json.loads(sys.stdin.read())
    table = load_catalog("probe", **payload["catalog"]).load_table(payload["identifier"])
    baseline = psutil.Process().memory_info().rss
    began = time.monotonic()
    frame_bytes = None
    if payload["path"] == "sink":
        write_table(table, generated_rows(int(payload["rows"])), "append")
    else:
        frame = generated_rows(int(payload["rows"])).collect()
        frame_bytes = frame.estimated_size()
        frame.write_iceberg(table, mode="append")
    seconds = time.monotonic() - began
    print(
        json.dumps(
            {
                "path": payload["path"],
                "rows": int(payload["rows"]),
                "seconds": round(seconds, 2),
                "baseline_rss": baseline,
                "peak_rss": _peak_rss_bytes(),
                "frame_bytes": frame_bytes,
            }
        )
    )


if __name__ == "__main__":
    main()
