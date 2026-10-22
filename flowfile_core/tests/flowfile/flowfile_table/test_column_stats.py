"""Graph-free tests for the on-demand column statistics module.

The engine's FlowfileColumn is the single source of truth: compute writes the
exact counts onto it (drop_nulls before n_unique, sentinel-free wire values)
and returns its ordinary FileColumn representation. Percentages/badges are
frontend derivations and are tested there. Every aggregate runs on the backend
it is given, core's own and the worker's (``context_backend``), and never on
another one when it fails.
"""

import io
from unittest.mock import patch

import polars as pl
import pytest
from polars.exceptions import ColumnNotFoundError

from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, TaskFailed
from flowfile_core.flowfile.execution.protocol import JobHandle, ResultRef, SubmitResult
from flowfile_core.flowfile.flow_data_engine import column_stats as column_stats_module
from flowfile_core.flowfile.flow_data_engine.column_stats import (
    ColumnStatsUnavailable,
    build_stats_exprs,
    compute_column_stats,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    CLOUD_PLACEHOLDER_RECORD_COUNT,
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import MAX_STAT_VALUE_LENGTH
from flowfile_core.schemas.output_model import FileColumn
from shared.compute_protocol import PROTOCOL_VERSION


def lazy_engine(data: dict) -> FlowDataEngine:
    return FlowDataEngine(pl.LazyFrame(data))


def total_rows(stats: FileColumn) -> int:
    return stats.number_of_empty_values + stats.number_of_filled_values


def test_all_null_column(context_backend):
    stats = compute_column_stats(lazy_engine({"a": [None, None, None]}), "a", context_backend)
    assert stats.number_of_empty_values == 3
    assert stats.number_of_filled_values == 0
    assert stats.number_of_unique_values == 0
    assert not stats.is_unique
    assert stats.min_value is None
    assert stats.max_value is None


def test_constant_column_with_nulls(context_backend):
    stats = compute_column_stats(lazy_engine({"a": [7, 7, None]}), "a", context_backend)
    assert stats.number_of_unique_values == 1
    assert stats.number_of_empty_values == 1
    assert stats.number_of_filled_values == 2
    assert not stats.is_unique
    assert stats.min_value == "7"
    assert stats.max_value == "7"


def test_unique_column(context_backend):
    stats = compute_column_stats(lazy_engine({"a": [1, 2, 3]}), "a", context_backend)
    assert stats.is_unique
    assert stats.number_of_unique_values == 3
    assert stats.number_of_empty_values == 0
    assert stats.min_value == "1"
    assert stats.max_value == "3"


def test_nulls_not_counted_as_distinct(context_backend):
    """[1, 1, None] has ONE distinct value — without drop_nulls Polars says 2."""
    stats = compute_column_stats(lazy_engine({"a": [1, 1, None]}), "a", context_backend)
    assert stats.number_of_unique_values == 1
    assert not stats.is_unique


def test_unique_among_filled_values(context_backend):
    """FlowfileColumn semantics: unique means all present values are distinct."""
    stats = compute_column_stats(lazy_engine({"a": [1, 2, None]}), "a", context_backend)
    assert stats.number_of_unique_values == 2
    assert stats.is_unique


def test_empty_frame(context_backend):
    stats = compute_column_stats(lazy_engine({"a": []}), "a", context_backend)
    assert total_rows(stats) == 0
    assert not stats.is_unique


def test_nested_dtypes_have_no_min_max(context_backend):
    engine = lazy_engine({"l": [[1], [2]], "s": [{"x": 1}, {"x": 2}]})
    for column in ("l", "s"):
        stats = compute_column_stats(engine, column, context_backend)
        assert stats.min_value is None
        assert stats.max_value is None
        assert total_rows(stats) == 2


def test_missing_column_raises(context_backend):
    with pytest.raises(ColumnNotFoundError):
        compute_column_stats(lazy_engine({"a": [1]}), "missing", context_backend)


def test_categorical_min_max_are_lexical_strings(context_backend):
    engine = FlowDataEngine(pl.LazyFrame({"c": pl.Series(["b", "a", "a"], dtype=pl.Categorical)}))
    stats = compute_column_stats(engine, "c", context_backend)
    assert stats.min_value == "a"
    assert stats.max_value == "b"
    assert stats.number_of_unique_values == 2


def test_min_max_values_are_truncated(context_backend):
    stats = compute_column_stats(lazy_engine({"a": ["x" * 1000]}), "a", context_backend)
    assert len(stats.max_value) == MAX_STAT_VALUE_LENGTH


def test_compute_memoizes_row_count_on_engine(context_backend):
    engine = lazy_engine({"a": [1, 2, 3]})
    assert engine.known_record_count() is None
    compute_column_stats(engine, "a", context_backend)
    assert engine.number_of_records == 3
    assert engine.known_record_count() == 3


def test_schema_repr_unknown_stats_are_none():
    """Never-computed stats ship as None on the wire, not -1/'' sentinels."""
    column_repr = lazy_engine({"a": [1]}).schema[0].get_column_repr()
    assert column_repr["number_of_empty_values"] is None
    assert column_repr["number_of_filled_values"] is None
    assert column_repr["number_of_unique_values"] is None
    assert column_repr["min_value"] is None
    assert column_repr["max_value"] is None
    FileColumn.model_validate(column_repr)


def test_stats_land_on_engine_schema_via_copy_on_write(context_backend):
    """The engine's schema is the single truth and carries the stats afterwards,
    but the pre-compute column object is never mutated: schema lists are shared
    by reference across engines (samples, splits, shallow copies)."""
    engine = lazy_engine({"a": [1, 2, 3]})
    original = engine.schema[0]
    assert original.is_unique is False, "sentinel-derived before compute"

    stats = compute_column_stats(engine, "a", context_backend)

    assert stats.is_unique is True
    assert original.stats_applied is False, "possibly-shared object stays untouched"
    enriched = engine.schema[0]
    assert enriched.is_unique is True
    assert enriched.number_of_unique_values == 3
    assert enriched.get_column_repr()["number_of_unique_values"] == 3


def test_shared_schema_list_is_not_contaminated_across_engines(context_backend):
    """Sibling engines built with the same schema list (random_split handles,
    get_sample) must not see each other's computed stats."""
    parent = lazy_engine({"a": [1, 2, 3, 4]})
    child = FlowDataEngine(pl.LazyFrame({"a": [1, 1]}), schema=parent.schema)

    compute_column_stats(child, "a", context_backend)

    parent_repr = parent.schema[0].get_column_repr()
    assert parent_repr["number_of_filled_values"] is None
    assert parent_repr["number_of_unique_values"] is None
    assert child.schema[0].number_of_filled_values == 2
    assert child.schema[0].number_of_unique_values == 1


def test_repeat_compute_answers_from_schema_without_recollecting(context_backend):
    engine = lazy_engine({"a": [1, 2, 2]})
    first = compute_column_stats(engine, "a", context_backend)
    with patch.object(column_stats_module, "run_stats_query") as mock_run:
        second = compute_column_stats(engine, "a", context_backend)
    mock_run.assert_not_called()
    assert second == first


def test_degraded_count_only_result_is_not_memoized(context_backend):
    """A count-only retry leaves unique unknown; the next click retries in full."""
    engine = lazy_engine({"a": [1, 2, 2]})
    with patch.object(
        column_stats_module, "_execute_stats_query", return_value={"total_rows": 3, "null_count": 0}
    ):
        degraded = compute_column_stats(engine, "a", context_backend)
    assert degraded.number_of_unique_values is None

    full = compute_column_stats(engine, "a", context_backend)
    assert full.number_of_unique_values == 2


def test_empty_string_min_is_a_real_value(context_backend):
    """'' is a legal min of a String column — it must not ship as unknown."""
    stats = compute_column_stats(lazy_engine({"s": ["", "b", None]}), "s", context_backend)
    assert stats.min_value == ""
    assert stats.max_value == "b"


def test_average_value_for_numeric_column(context_backend):
    stats = compute_column_stats(lazy_engine({"a": [1, 2, None]}), "a", context_backend)
    assert stats.average_value == "1.5"


def test_average_value_only_for_numeric_dtypes(context_backend):
    stats = compute_column_stats(lazy_engine({"s": ["a", "b"]}), "s", context_backend)
    assert stats.average_value is None


def test_apply_statistics_resets_fields_missing_from_the_batch():
    """A count-only application must not keep unique/min/max/avg from an earlier
    (or, pre copy-on-write, a sibling's) full computation."""
    column = lazy_engine({"a": [1, 2]}).schema[0]
    column.apply_statistics(total_rows=2, null_count=0, n_unique=2, min_value=1, max_value=2, average_value=1.5)
    column.apply_statistics(total_rows=2, null_count=0)

    column_repr = column.get_column_repr()
    assert column_repr["number_of_unique_values"] is None
    assert column_repr["min_value"] is None
    assert column_repr["max_value"] is None
    assert column_repr["average_value"] is None


def test_cloud_placeholder_count_is_not_a_known_count(context_backend):
    """The cloud readers stamp a fictive count so __len__ never queries the
    provider — it must not surface as a real total, and a stats pass heals it."""
    engine = FlowDataEngine(pl.LazyFrame({"a": [1, 2]}), number_of_records=CLOUD_PLACEHOLDER_RECORD_COUNT)
    assert engine.known_record_count() is None

    compute_column_stats(engine, "a", context_backend)
    assert engine.number_of_records == 2


def test_build_stats_exprs_counts_only_for_opaque_dtype():
    exprs = build_stats_exprs("a", pl.Object())
    assert len(exprs) == 2, "only len + null_count are valid for every dtype"


def test_known_record_count_is_free_only():
    lazy = lazy_engine({"a": [1, 2, 3]})
    with patch.object(FlowDataEngine, "get_number_of_records") as mock_count:
        assert lazy.known_record_count() is None, "unknown lazy count stays unknown"

        lazy.number_of_records = 5
        assert lazy.known_record_count() == 5

        eager = FlowDataEngine(pl.DataFrame({"a": [1, 2, 3]}))
        eager.number_of_records = -1
        assert eager.known_record_count() == 3, "eager height is free"
    mock_count.assert_not_called()


def test_binary_min_max_use_the_preview_hex_encoding(context_backend):
    """Bounds and preview cells must agree on how a WKB blob looks on the wire."""
    short, long = bytes.fromhex("0102"), bytes.fromhex("0101000000EE5A423EE8991340F1F44A5986304A40")
    stats = compute_column_stats(FlowDataEngine(pl.DataFrame({"g": [long, short]})), "g", context_backend)
    # Bytes order lexicographically: 0x0101… sorts before 0x0102.
    assert stats.min_value == "0x0101000000EE5A423EE8991340F1F44A\u2026 (21 bytes)"
    assert stats.max_value == "0x0102"


class ScriptedBackend:
    """Answers each read with its next scripted outcome, a row or an error, and computes nothing itself."""

    id = "scripted"
    protocol_version = PROTOCOL_VERSION

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.plans: list[pl.LazyFrame] = []
        self.released: list[str] = []

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        self.plans.append(plan)
        ref = ResultRef(self.id, result_id, [], None, "deferred", result_id.encode())
        return SubmitResult(ref=ref, sample=None, handle=JobHandle(self.id, result_id))

    def sample(self, ref, n):
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        buffer = io.BytesIO()
        pl.DataFrame([outcome]).write_ipc_stream(buffer)
        return buffer.getvalue()

    def release(self, ref):
        self.released.append(ref.result_id)


def count_collects(monkeypatch) -> list[int]:
    """One entry per ``LazyFrame.collect`` in this process from here on."""
    collects: list[int] = []
    real_collect = pl.LazyFrame.collect

    def counting_collect(self, *args, **kwargs):
        collects.append(1)
        return real_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", counting_collect)
    return collects


def test_a_failed_full_query_retries_counts_only_on_the_same_backend(monkeypatch):
    backend = ScriptedBackend(TaskFailed("dtype error on the worker"), {"total_rows": 3, "null_count": 1})
    collects = count_collects(monkeypatch)

    stats = compute_column_stats(lazy_engine({"a": [1, None, 3]}), "a", backend)

    assert [plan.collect_schema().names() for plan in backend.plans] == [
        ["total_rows", "null_count", "unique_count", "min_value", "max_value", "average_value"],
        ["total_rows", "null_count"],
    ]
    assert len(backend.released) == 2
    assert collects == [], "the retry stays on the backend; nothing runs in core"
    assert (stats.number_of_filled_values, stats.number_of_empty_values) == (2, 1)
    assert stats.number_of_unique_values is None, "a degraded result carries counts only"


def test_a_backend_that_cannot_compute_the_counts_leaves_the_stats_unavailable(monkeypatch):
    refused = ComputeUnavailable("Could not reach the worker: connection refused")
    backend = ScriptedBackend(refused, refused)
    collects = count_collects(monkeypatch)

    with pytest.raises(ColumnStatsUnavailable, match="'scripted' backend.*connection refused") as raised:
        compute_column_stats(lazy_engine({"a": [1, 2, 3]}), "a", backend)

    assert raised.value.__cause__ is refused
    assert len(backend.plans) == 2, "the full query, then the count-only retry"
    assert collects == [], "a failed backend is never replaced by a collect in core"


def test_a_query_the_embedded_worker_cannot_run_retries_counts_only_there(monkeypatch):
    monkeypatch.setattr(column_stats_module, "build_stats_exprs", lambda name, dtype: [pl.col(name).str.len_chars()])

    stats = compute_column_stats(lazy_engine({"a": [1, 2, None]}), "a", EmbeddedWorkerBackend())

    assert (stats.number_of_filled_values, stats.number_of_empty_values) == (2, 1)
    assert stats.number_of_unique_values is None
