"""1-row kernel dry-run for code-bearing nodes.

Code-bearing tool calls (``flowfile.graph.add_polars_code`` /
``add_python_script`` / ``add_sql_query``) cannot have their output
schema predicted by inspecting settings. The strategy is to dry-run
the proposed code on a single row of upstream data via the existing
``kernel_runtime`` Docker sandbox, then read the output schema from
the materialised parquet.

This module is the single seam tests stub:
:func:`_run_kernel_for_dry_run` is the only function that touches
the real kernel client, so a test fixture can replace it without
poking the rest of the executor logic.

The :class:`DryRunCache` LRU keys on ``(code_hash, sample_hash)``.
Identical ``(code, sample-row)`` pairs hit cache on subsequent calls
— important for an agent that tries the same proposal twice while
iterating.

**Cost.** Cold kernel: ~30s container start. Warm: 100–300ms
HTTP+execute on a 1-row sample. Acceptable for an agent step that's
already on a multi-second budget. The cache amortises repeats.

**Sample-row sourcing.** Schema-driven: prefer
``upstream.data_frame.head(1)`` when the upstream has been
materialised; fall back to a 1-row null Polars LazyFrame built from
the upstream's resolved ``predicted_schema`` (which the upstream-tier
handler always populates for tiers 0–2; tier-3 upstreams cause the
executor to short-circuit before reaching this module).
"""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from typing import Any

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

DEFAULT_CACHE_CAPACITY: int = 64


class DryRunCache:
    """Per-session LRU keyed on ``(code_hash, sample_hash)``.

    Capacity is intentionally small (64) — this isn't a process-wide cache,
    it's a planning-conversation memo. The agent rarely repeats the *same*
    code on the *same* sample more than a handful of times in a session.
    """

    def __init__(self, capacity: int = DEFAULT_CACHE_CAPACITY) -> None:
        self._capacity = capacity
        self._store: OrderedDict[tuple[str, str], list[FlowfileColumn]] = OrderedDict()

    def get(self, code_hash: str, sample_hash: str) -> list[FlowfileColumn] | None:
        key = (code_hash, sample_hash)
        if key not in self._store:
            return None
        self._store.move_to_end(key)
        return list(self._store[key])

    def put(self, code_hash: str, sample_hash: str, schema: list[FlowfileColumn]) -> None:
        key = (code_hash, sample_hash)
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = list(schema)
        while len(self._store) > self._capacity:
            self._store.popitem(last=False)

    def __len__(self) -> int:
        return len(self._store)

    def clear(self) -> None:
        self._store.clear()


def _hash_code(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()[:16]


def _canonicalise_sample(sample: Any) -> bytes:
    """Stable byte-encoding of a 1-row sample for hashing.

    Accepts a Polars DataFrame / LazyFrame / dict / list-of-dicts. The encoded
    form is column-name-sorted JSON so logically equivalent samples produce
    identical hashes.
    """
    import polars as pl  # local import — keeps module-level lazy

    if isinstance(sample, pl.LazyFrame):
        sample = sample.collect()
    if isinstance(sample, pl.DataFrame):
        rows = sample.to_dicts()
    elif isinstance(sample, dict):
        rows = [sample]
    elif isinstance(sample, list):
        rows = sample
    else:
        rows = [{"__repr__": repr(sample)}]
    sortable = [dict(sorted(r.items())) for r in rows]
    return json.dumps(sortable, sort_keys=True, default=str).encode("utf-8")


def _hash_sample(sample: Any) -> str:
    return hashlib.sha256(_canonicalise_sample(sample)).hexdigest()[:16]


def _build_null_row_from_schema(schema: list[FlowfileColumn]):
    """Build a 1-row Polars LazyFrame of nulls matching the column names + dtypes.

    Used when the upstream node hasn't been materialised. The dry-run kernel
    sees null values everywhere, but the *column names* and *types* are real,
    so the kernel-emitted parquet schema is correct as long as the proposed
    code doesn't branch on values.
    """
    import polars as pl

    from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type

    pl_schema: dict[str, pl.DataType] = {}
    for col in schema:
        dtype = cast_str_to_polars_type(col.data_type)
        pl_schema[col.column_name] = dtype if dtype is not None else pl.String

    data = {name: [None] for name in pl_schema}
    return pl.DataFrame(data, schema=pl_schema).lazy()


def _resolve_sample_row(
    flow,
    upstream_id: int,
    upstream_schemas: dict[int, list[FlowfileColumn]],
) -> Any | None:
    """Return a 1-row sample for the upstream — real ``head(1)`` if materialised,
    otherwise a null-row LazyFrame built from the resolved schema.

    Returns ``None`` if neither path is available (which should not happen if
    the executor honours the upstream-tier handler before calling here)."""
    upstream_node = flow.get_node(upstream_id) if hasattr(flow, "get_node") else None
    if upstream_node is not None:
        data_frame = getattr(upstream_node, "data_frame", None)
        has_run = getattr(upstream_node, "has_run", False)
        if has_run and data_frame is not None:
            try:
                return data_frame.head(1)
            except Exception:
                pass
    schema = upstream_schemas.get(upstream_id)
    if not schema:
        return None
    return _build_null_row_from_schema(schema)


def _run_kernel_for_dry_run(
    flow,
    node_id: int,
    code: str,
    output_names: list[str],
    sample,
) -> list[FlowfileColumn]:
    """Single test seam — invokes the kernel client to dry-run ``code`` on
    ``sample`` and returns the output schema.

    Production wraps :meth:`FlowGraph._execute_on_kernel`. Tests
    monkey-patch this function directly to avoid Docker. Raises if the kernel
    call fails — the caller (executor) catches and surfaces as a refusal.
    """
    # Local imports — keep the kernel client off the module's import path.
    # The kernel client pulls in httpx + the Docker manager.
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

    sample_engine = sample if isinstance(sample, FlowDataEngine) else FlowDataEngine(sample)
    kernel_id = getattr(flow, "_default_kernel_id", None) or "default"

    result = flow._execute_on_kernel(  # noqa: SLF001 — official seam
        node_id=node_id,
        kernel_id=kernel_id,
        code=code,
        output_names=output_names,
        flow_data_engine=(sample_engine,),
    )
    if result is None:
        raise RuntimeError("kernel dry-run returned no output for node " f"{node_id}")
    return list(result.schema)


def dry_run_code(
    *,
    flow,
    node_id: int,
    upstream_node_ids: list[int],
    code: str,
    output_names: list[str],
    cache: DryRunCache,
    upstream_schemas: dict[int, list[FlowfileColumn]],
) -> list[FlowfileColumn]:
    """Dry-run ``code`` on a 1-row sample of the upstream, return predicted schema.

    Raises ``RuntimeError`` if no upstream sample can be resolved (caller should
    have screened via the upstream-tier handler before calling) or if the kernel
    dry-run fails.
    """
    if not upstream_node_ids:
        raise RuntimeError("dry_run_code requires at least one upstream node id")

    primary_upstream = upstream_node_ids[0]
    sample = _resolve_sample_row(flow, primary_upstream, upstream_schemas)
    if sample is None:
        raise RuntimeError(
            f"unable to source a sample row for upstream node {primary_upstream}; "
            "upstream is unrun and has no resolved schema"
        )

    code_hash = _hash_code(code)
    sample_hash = _hash_sample(sample)
    cached = cache.get(code_hash, sample_hash)
    if cached is not None:
        return cached

    schema = _run_kernel_for_dry_run(flow, node_id, code, output_names, sample)
    cache.put(code_hash, sample_hash, schema)
    return schema


__all__ = [
    "DryRunCache",
    "DEFAULT_CACHE_CAPACITY",
    "dry_run_code",
    "_run_kernel_for_dry_run",
    "_resolve_sample_row",
    "_build_null_row_from_schema",
    "_hash_code",
    "_hash_sample",
]
