"""Tests for the SCD2 Delta primitive in ``shared.delta_utils``.

Everything runs against real Delta tables in tmp dirs — the merge recipe is only meaningful
against the deltalake version actually installed, so nothing here is mocked. The golden
surrogate-key vectors freeze ``SCD2_SK_ALGORITHM = "sha256-v1"``: they are computed by hand from
the documented payload encoding, so a change in Polars' or hashlib's behaviour breaks them loudly
instead of silently minting different keys for rows that already exist in users' tables.
"""

import concurrent.futures
import hashlib
import threading
from datetime import date, datetime, timezone

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from shared import delta_utils
from shared.delta_utils import (
    SCD2_SK_ALGORITHM,
    Scd2Result,
    scd2_into_delta,
    scd2_surrogate_keys,
)

VF1 = "2026-01-01T00:00:00+00:00"
VF2 = "2026-02-01T00:00:00+00:00"
VF3 = "2026-03-01T00:00:00+00:00"
VF4 = "2026-04-01T00:00:00+00:00"
VF2_MID = "2026-02-15T00:00:00+00:00"  # strictly between VF2 and VF3


def _scd2(df, path, *, keys=("cust_id",), valid_from=VF1, compare=None, snapshot=False, **kwargs) -> Scd2Result:
    return scd2_into_delta(
        df,
        str(path),
        business_keys=list(keys),
        valid_from_iso=valid_from,
        compare_columns=compare,
        full_snapshot=snapshot,
        **kwargs,
    )


def _read(path) -> pl.DataFrame:
    return pl.scan_delta(str(path)).collect()


def _version(path) -> int:
    return DeltaTable(str(path)).version()


def _base_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cust_id": [1, 2, 3],
            "city": ["AMS", "BER", "PAR"],
            "tier": ["gold", "silver", None],
        }
    )


def _overlapping_versions(path, key: str = "cust_id") -> list:
    """Every (key, span, span) whose half-open validity intervals overlap — must always be empty.

    The invariant the whole SCD2 recipe exists to hold: one business key is active at most once at
    any instant. Asserted directly rather than through ``active_at``, so a violation is reported
    where it was written instead of where it is later observed.
    """
    spans_by_key: dict = {}
    for row in _read(path).sort(key, "valid_from").iter_rows(named=True):
        spans_by_key.setdefault(row[key], []).append((row["valid_from"], row["valid_to"]))
    overlaps = []
    for k, spans in spans_by_key.items():
        for i, (a_from, a_to) in enumerate(spans):
            for b_from, b_to in spans[i + 1 :]:
                if (a_to is None or b_from < a_to) and (b_to is None or a_from < b_to):
                    overlaps.append((k, (a_from, a_to), (b_from, b_to)))
    return overlaps


# ---- Surrogate key: frozen golden vectors ---------------------------------- #


def _payload_digest(*canonical: str, valid_from: str = VF1) -> str:
    """Recompute the documented payload by hand: length-prefixed, \\x1f-joined, sha256[:32]."""
    parts = [f"{len(c.encode('utf-8'))}:{c}" for c in canonical]
    parts.append(f"{len(valid_from.encode('utf-8'))}:{valid_from}")
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]


class TestSurrogateKeyGoldenVectors:
    def test_algorithm_id_is_frozen(self):
        assert SCD2_SK_ALGORITHM == "sha256-v1"

    def test_string_key(self):
        keys = scd2_surrogate_keys(pl.DataFrame({"k": ["abc"]}), ["k"], VF1)
        assert keys.to_list() == ["7af8df3833a3b211138b2072758e1744"]
        assert keys.to_list()[0] == _payload_digest("abc")

    def test_integer_key(self):
        keys = scd2_surrogate_keys(pl.DataFrame({"k": [42]}), ["k"], VF1)
        assert keys.to_list() == ["c108601794b0318ee0e7a877d45862db"]
        assert keys.to_list()[0] == _payload_digest("42")

    def test_datetime_key(self):
        naive = pl.DataFrame({"k": [datetime(2026, 3, 4, 5, 6, 7, 890123)]})
        keys = scd2_surrogate_keys(naive, ["k"], VF1)
        assert keys.to_list() == ["892c7025b21e5c7546eb4538342be65b"]
        assert keys.to_list()[0] == _payload_digest("2026-03-04T05:06:07.890123")

    def test_multi_column_key(self):
        df = pl.DataFrame({"a": ["x"], "b": [7], "c": [False]})
        keys = scd2_surrogate_keys(df, ["a", "b", "c"], VF1)
        assert keys.to_list() == ["3c426b1aa1dae0ae1b1e5f94b61073de"]
        assert keys.to_list()[0] == _payload_digest("x", "7", "false")

    def test_boolean_and_date_keys(self):
        assert scd2_surrogate_keys(pl.DataFrame({"k": [True]}), ["k"], VF1).to_list()[0] == _payload_digest("true")
        assert scd2_surrogate_keys(pl.DataFrame({"k": [False]}), ["k"], VF1).to_list()[0] == _payload_digest("false")
        dated = pl.DataFrame({"k": [date(2026, 3, 4)]})
        assert scd2_surrogate_keys(dated, ["k"], VF1).to_list()[0] == _payload_digest("2026-03-04")

    def test_integer_width_does_not_change_the_key(self):
        wide = pl.DataFrame({"k": pl.Series([42], dtype=pl.Int64)})
        narrow = pl.DataFrame({"k": pl.Series([42], dtype=pl.Int32)})
        assert scd2_surrogate_keys(wide, ["k"], VF1).to_list() == scd2_surrogate_keys(narrow, ["k"], VF1).to_list()

    def test_timezone_and_time_unit_are_normalized(self):
        instant = datetime(2026, 3, 4, 5, 6, 7, 890123, tzinfo=timezone.utc)
        utc = pl.Series([instant]).dt.cast_time_unit("us").dt.replace_time_zone("UTC")
        amsterdam = utc.dt.convert_time_zone("Europe/Amsterdam")
        expected = _payload_digest("2026-03-04T05:06:07.890123")
        assert scd2_surrogate_keys(pl.DataFrame({"k": utc}), ["k"], VF1).to_list() == [expected]
        assert scd2_surrogate_keys(pl.DataFrame({"k": amsterdam}), ["k"], VF1).to_list() == [expected]

        millis = pl.Series([datetime(2026, 3, 4, 5, 6, 7, 890000)]).dt.cast_time_unit("ms")
        assert scd2_surrogate_keys(pl.DataFrame({"k": millis}), ["k"], VF1).to_list() == [
            _payload_digest("2026-03-04T05:06:07.890000")
        ]

    def test_valid_from_participates_in_the_key(self):
        df = pl.DataFrame({"k": ["abc"]})
        assert scd2_surrogate_keys(df, ["k"], VF1).to_list() != scd2_surrogate_keys(df, ["k"], VF2).to_list()

    def test_length_prefixing_resists_separator_injection(self):
        collide_a = pl.DataFrame({"a": ["x"], "b": ["1:y"]})
        collide_b = pl.DataFrame({"a": ["x\x1f1:y"], "b": [""]})
        assert (
            scd2_surrogate_keys(collide_a, ["a", "b"], VF1).to_list()
            != scd2_surrogate_keys(collide_b, ["a", "b"], VF1).to_list()
        )

    def test_empty_frame_yields_an_empty_string_series(self):
        keys = scd2_surrogate_keys(pl.DataFrame({"k": pl.Series([], dtype=pl.String)}), ["k"], VF1)
        assert keys.len() == 0
        assert keys.dtype == pl.String

    @pytest.mark.parametrize(
        "df",
        [
            pl.DataFrame({"k": [1.5]}),
            pl.DataFrame({"k": pl.Series([1.5], dtype=pl.Float32)}),
            pl.DataFrame({"k": [[1, 2]]}),
            pl.DataFrame({"k": [{"a": 1}]}),
        ],
    )
    def test_float_and_nested_keys_are_rejected(self, df):
        with pytest.raises(ValueError, match="unsupported dtype"):
            scd2_surrogate_keys(df, ["k"], VF1)


# ---- Initial load ---------------------------------------------------------- #


class TestInitialLoad:
    def test_creates_table_with_system_columns(self, tmp_path):
        path = tmp_path / "dim"
        result = _scd2(_base_frame(), path)

        assert result.created is True
        assert result.skipped is False
        assert (result.rows_inserted, result.rows_total, result.rows_current) == (3, 3, 3)

        table = _read(path)
        assert table.columns == ["cust_id", "city", "tier", "sk", "valid_from", "valid_to", "is_current"]
        assert table["valid_to"].null_count() == 3
        assert table["is_current"].to_list() == [True, True, True]
        assert table["sk"].n_unique() == 3
        assert all(len(sk) == 32 for sk in table["sk"].to_list())
        assert _version(path) == 0

    def test_timestamps_round_trip_as_microsecond_utc(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, valid_from="2026-01-01T12:34:56.789012+00:00")

        schema = pl.scan_delta(str(path)).collect_schema()
        assert schema["valid_from"] == pl.Datetime("us", "UTC")
        assert schema["valid_to"] == pl.Datetime("us", "UTC")
        assert _read(path)["valid_from"].to_list() == [datetime(2026, 1, 1, 12, 34, 56, 789012, tzinfo=timezone.utc)] * 3

    def test_zero_row_batch_still_creates_the_table(self, tmp_path):
        path = tmp_path / "dim"
        result = _scd2(_base_frame().clear(), path)

        assert (result.created, result.skipped, result.rows_total) == (True, False, 0)
        assert DeltaTable(str(path)).version() == 0
        assert "is_current" in pl.scan_delta(str(path)).collect_schema()

    def test_new_tables_default_to_is_current_partitioning(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)

        assert DeltaTable(str(path)).metadata().partition_columns == ["is_current"]

    def test_default_partitioning_honors_a_renamed_is_current_column(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, is_current_column="current_flag")

        assert DeltaTable(str(path)).metadata().partition_columns == ["current_flag"]

    def test_explicit_partition_by_nests_above_is_current(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, partition_by=["city"])

        assert DeltaTable(str(path)).metadata().partition_columns == ["city", "is_current"]

    def test_partition_on_current_false_creates_an_unpartitioned_table(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, partition_on_current=False)

        assert DeltaTable(str(path)).metadata().partition_columns == []

    def test_partition_on_current_false_keeps_explicit_columns_only(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, partition_by=["city"], partition_on_current=False)

        assert DeltaTable(str(path)).metadata().partition_columns == ["city"]

    def test_partition_by_is_current_is_accepted(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, partition_by=["is_current"])

        assert DeltaTable(str(path)).metadata().partition_columns == ["is_current"]
        _scd2(
            pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}),
            path,
            valid_from=VF2,
        )
        table = _read(path).sort("cust_id", "valid_from")
        assert table.height == 4
        assert table.filter(pl.col("is_current"))["city"].sort().to_list() == ["BER", "PAR", "ROT"]

    def test_custom_column_names(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(
            _base_frame(),
            path,
            surrogate_key_column="row_key",
            valid_from_column="from_ts",
            valid_to_column="to_ts",
            is_current_column="active",
        )
        assert _read(path).columns[-4:] == ["row_key", "from_ts", "to_ts", "active"]


# ---- Change detection ------------------------------------------------------ #


class TestChangeDetection:
    def test_changed_new_and_unchanged(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)

        batch = pl.DataFrame(
            {
                "cust_id": [1, 2, 4],
                "city": ["ROT", "BER", "LON"],
                "tier": ["gold", "silver", "bronze"],
            }
        )
        result = _scd2(batch, path, valid_from=VF2)

        assert (result.rows_inserted, result.rows_closed) == (2, 1)
        assert (result.rows_total, result.rows_current) == (5, 4)

        table = _read(path).sort("cust_id", "valid_from")
        closed = table.filter(~pl.col("is_current"))
        assert closed["cust_id"].to_list() == [1]
        assert closed["valid_to"].to_list() == [datetime(2026, 2, 1, tzinfo=timezone.utc)]
        current = table.filter(pl.col("is_current"))
        assert current["cust_id"].to_list() == [1, 2, 3, 4]
        assert current.filter(pl.col("cust_id") == 1)["city"].to_list() == ["ROT"]

    def test_half_open_validity_has_no_gap(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)

        versions = _read(path).filter(pl.col("cust_id") == 1).sort("valid_from")
        assert versions["valid_to"].to_list()[0] == versions["valid_from"].to_list()[1]

    def test_null_transitions_are_detected_both_ways(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)

        # key 3: NULL -> 'bronze' opens a version; key 2 keeps its value; key 1 value -> NULL opens one.
        batch = pl.DataFrame(
            {
                "cust_id": [1, 2, 3],
                "city": ["AMS", "BER", "PAR"],
                "tier": [None, "silver", "bronze"],
            }
        )
        result = _scd2(batch, path, valid_from=VF2)
        assert (result.rows_inserted, result.rows_closed) == (2, 2)

        current = _read(path).filter(pl.col("is_current")).sort("cust_id")
        assert current["tier"].to_list() == [None, "silver", "bronze"]

    def test_null_equals_null_is_not_a_change(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        result = _scd2(_base_frame(), path, valid_from=VF2)
        assert result.skipped is True

    def test_untracked_column_change_is_invisible(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, compare=["city"])

        batch = _base_frame().with_columns(pl.lit("platinum").alias("tier"))
        assert _scd2(batch, path, valid_from=VF2, compare=["city"]).skipped is True
        assert _read(path)["tier"].to_list() == ["gold", "silver", None]

    def test_business_key_named_as_compare_column_is_ignored(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, compare=["cust_id", "city"])
        assert _scd2(_base_frame(), path, valid_from=VF2, compare=["cust_id", "city"]).skipped is True


# ---- full_snapshot --------------------------------------------------------- #


class TestFullSnapshot:
    def test_absent_keys_are_end_dated(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)

        snapshot = pl.DataFrame({"cust_id": [1, 3], "city": ["ROT", "PAR"], "tier": ["gold", None]})
        result = _scd2(snapshot, path, valid_from=VF3, snapshot=True)

        assert (result.rows_inserted, result.rows_closed) == (0, 1)
        table = _read(path).sort("cust_id", "valid_from")
        assert table.filter(pl.col("is_current"))["cust_id"].to_list() == [1, 3]
        gone = table.filter(pl.col("cust_id") == 2)
        assert gone["valid_to"].to_list() == [datetime(2026, 3, 1, tzinfo=timezone.utc)]

    def test_already_closed_rows_are_not_restamped(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)

        snapshot = pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]})
        _scd2(snapshot, path, valid_from=VF3, snapshot=True)

        historic = _read(path).filter((pl.col("cust_id") == 1) & (pl.col("city") == "AMS"))
        assert historic["valid_to"].to_list() == [datetime(2026, 2, 1, tzinfo=timezone.utc)]

    def test_empty_batch_is_skipped_even_with_full_snapshot(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        before = _version(path)

        result = _scd2(_base_frame().clear(), path, valid_from=VF2, snapshot=True)

        assert result.skipped is True
        assert _version(path) == before
        assert _read(path).filter(pl.col("is_current")).height == 3

    def test_unchanged_snapshot_with_no_absentees_is_skipped(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        before = _version(path)
        assert _scd2(_base_frame(), path, valid_from=VF2, snapshot=True).skipped is True
        assert _version(path) == before


# ---- Idempotency and commit accounting ------------------------------------- #


class TestCommitAccounting:
    def test_rerunning_an_identical_batch_is_a_zero_commit_skip(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        before = _version(path)

        result = _scd2(_base_frame(), path, valid_from=VF2)

        assert result == Scd2Result(skipped=True)
        assert _version(path) == before
        assert _read(path).height == 3

    def test_exactly_one_delta_version_per_effective_write(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        assert _version(path) == 0

        _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)
        assert _version(path) == 1

        assert _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF3).skipped
        assert _version(path) == 1

        _scd2(
            pl.DataFrame({"cust_id": [1, 4], "city": ["ROT", "LON"], "tier": ["gold", "bronze"]}),
            path,
            valid_from=VF3,
            snapshot=True,
        )
        assert _version(path) == 2

        history_ops = [entry["operation"] for entry in DeltaTable(str(path)).history()]
        assert history_ops.count("MERGE") == 2

    def test_discriminator_never_leaks_into_the_target(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)

        assert not any(c.startswith("__scd2") for c in _read(path).columns)


# ---- Pre-flight guards ----------------------------------------------------- #


class TestGuards:
    def test_duplicate_business_keys(self, tmp_path):
        df = pl.DataFrame({"cust_id": [1, 1], "city": ["AMS", "ROT"], "tier": ["gold", "gold"]})
        with pytest.raises(ValueError, match="is not unique"):
            _scd2(df, tmp_path / "dim")

    def test_null_business_keys(self, tmp_path):
        df = pl.DataFrame({"cust_id": [1, None], "city": ["AMS", "ROT"], "tier": ["gold", "gold"]})
        with pytest.raises(ValueError, match="contain NULL values"):
            _scd2(df, tmp_path / "dim")

    def test_missing_business_key_column(self, tmp_path):
        with pytest.raises(ValueError, match="not present in the input data"):
            _scd2(_base_frame(), tmp_path / "dim", keys=("nope",))

    def test_no_business_keys(self, tmp_path):
        with pytest.raises(ValueError, match="at least one business-key column"):
            _scd2(_base_frame(), tmp_path / "dim", keys=())

    def test_system_column_collision(self, tmp_path):
        df = _base_frame().with_columns(pl.lit(1).alias("valid_from"))
        with pytest.raises(ValueError, match="collide with input columns"):
            _scd2(df, tmp_path / "dim")

    @pytest.mark.parametrize("name", ["__scd2_op", "__scd2_hit", "__scd2_class", "__scd2_t0", "__scd2_anything"])
    def test_reserved_internal_column_name(self, tmp_path, name):
        # __scd2_t{i} is the alias the classify join renames the target's tracked columns to: an
        # input column of that name would be compared in their place, silently.
        df = _base_frame().with_columns(pl.lit("x").alias(name))
        with pytest.raises(ValueError, match="reserved SCD2 internal column name"):
            _scd2(df, tmp_path / "dim")

    def test_nanosecond_keys_colliding_after_canonicalisation_are_rejected(self, tmp_path):
        # Both the surrogate key and the Delta write truncate ns to us, so these two rows are one
        # business key by the time they land — reject before the table holds two "current" rows.
        ns = pl.Series("ts", [1_700_000_000_000_000_001, 1_700_000_000_000_000_999]).cast(pl.Datetime("ns"))
        df = pl.DataFrame({"ts": ns, "v": ["a", "b"]})
        with pytest.raises(ValueError, match="not unique"):
            _scd2(df, tmp_path / "dim", keys=("ts",))

    def test_system_column_names_must_be_distinct(self, tmp_path):
        with pytest.raises(ValueError, match="must be distinct"):
            _scd2(_base_frame(), tmp_path / "dim", valid_to_column="valid_from")

    def test_unknown_compare_column(self, tmp_path):
        with pytest.raises(ValueError, match="compare_columns not present"):
            _scd2(_base_frame(), tmp_path / "dim", compare=["nope"])

    def test_existing_non_scd2_table_is_refused(self, tmp_path):
        path = tmp_path / "plain"
        _base_frame().write_delta(str(path))

        with pytest.raises(ValueError, match="is not an SCD2 table"):
            _scd2(_base_frame(), path)

    def test_dtype_drift_is_rejected_with_an_actionable_message(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)

        drifted = pl.DataFrame({"cust_id": [1], "city": [99], "tier": ["gold"]})
        with pytest.raises(ValueError, match="incompatible column type"):
            _scd2(drifted, path, valid_from=VF2)

    def test_wider_incoming_type_is_rejected(self, tmp_path):
        # Int64 does not fit an Int32 column: the MERGE would narrow it back on insert, so the
        # difference is re-detected on every run. Same class as the Float64-into-Int64 bug.
        path = tmp_path / "dim"
        narrow = pl.DataFrame(
            {"cust_id": pl.Series([1, 2], dtype=pl.Int32), "v": pl.Series([10, 20], dtype=pl.Int32)}
        )
        _scd2(narrow, path)

        wide = pl.DataFrame({"cust_id": pl.Series([1, 2], dtype=pl.Int64), "v": pl.Series([10, 99], dtype=pl.Int64)})
        with pytest.raises(ValueError, match="incompatible column type"):
            _scd2(wide, path, valid_from=VF2)

    def test_narrower_incoming_type_is_allowed(self, tmp_path):
        path = tmp_path / "dim"
        wide = pl.DataFrame({"cust_id": pl.Series([1, 2], dtype=pl.Int64), "v": pl.Series([10, 20], dtype=pl.Int64)})
        _scd2(wide, path)

        narrow = pl.DataFrame(
            {"cust_id": pl.Series([1, 2], dtype=pl.Int32), "v": pl.Series([10, 99], dtype=pl.Int32)}
        )
        result = _scd2(narrow, path, valid_from=VF2)
        assert (result.rows_inserted, result.rows_closed) == (1, 1)

    def test_float_into_an_integer_column_is_rejected_before_any_write(self, tmp_path):
        # Regression: the pair used to pass the compatibility probe, change detection ran in
        # Float64 while the insert narrowed back to Int64, so every run closed the current row and
        # re-inserted the same truncated value — an unbounded close-1/insert-1 loop.
        path = tmp_path / "dim"
        _scd2(pl.DataFrame({"cust_id": [1], "amt": [10]}), path)
        before = _version(path)

        drifted = pl.DataFrame({"cust_id": [1], "amt": [10.5]})
        with pytest.raises(ValueError, match="incompatible column type"):
            _scd2(drifted, path, valid_from=VF2)
        assert _version(path) == before
        assert _read(path).height == 1

    def test_datetime_time_unit_drift_is_rejected(self, tmp_path):
        path = tmp_path / "dim"
        base = pl.DataFrame({"cust_id": [1], "seen": pl.Series([datetime(2026, 1, 1)], dtype=pl.Datetime("us"))})
        _scd2(base, path)

        drifted = pl.DataFrame({"cust_id": [1], "seen": pl.Series([datetime(2026, 1, 2)], dtype=pl.Datetime("ns"))})
        with pytest.raises(ValueError, match="incompatible column type"):
            _scd2(drifted, path, valid_from=VF2)

    def test_a_valid_batch_still_converges_to_skipped(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(pl.DataFrame({"cust_id": [1], "amt": [10]}), path)

        changed = pl.DataFrame({"cust_id": [1], "amt": [11]})
        assert _scd2(changed, path, valid_from=VF2).rows_inserted == 1
        for vf in (VF3, VF4):
            assert _scd2(changed, path, valid_from=vf).skipped
        assert _read(path).height == 2

    def test_partition_by_on_existing_table_warns_and_is_ignored(self, tmp_path, caplog):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)

        with caplog.at_level("WARNING", logger="shared.delta_utils"):
            _scd2(
                pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}),
                path,
                valid_from=VF2,
                partition_by=["is_current"],
            )
        assert "partitioning is immutable" in caplog.text
        assert DeltaTable(str(path)).metadata().partition_columns == ["is_current"]

    def test_partition_by_on_a_missing_column_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="partition_by columns not present"):
            _scd2(_base_frame(), tmp_path / "dim", partition_by=["region"])


# ---- Schema evolution ------------------------------------------------------ #


class TestSchemaEvolution:
    def test_new_tracked_column_reversion_and_historic_nulls(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)

        widened = _base_frame().with_columns(pl.Series("score", [10, 20, 30]))
        result = _scd2(widened, path, valid_from=VF2)

        assert (result.rows_inserted, result.rows_closed) == (3, 3)
        table = _read(path).sort("cust_id", "valid_from")
        assert "score" in table.columns
        assert table.filter(~pl.col("is_current"))["score"].null_count() == 3
        assert table.filter(pl.col("is_current")).sort("cust_id")["score"].to_list() == [10, 20, 30]

    def test_pinning_compare_columns_also_pins_the_schema(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, compare=["city", "tier"])

        widened = _base_frame().with_columns(pl.Series("score", [10, 20, 30]))
        assert _scd2(widened, path, valid_from=VF2, compare=["city", "tier"]).skipped is True
        assert "score" not in _read(path).columns

    def test_an_untracked_column_rides_along_and_widens_the_target(self, tmp_path):
        """An effective write carries every input column, not just the tracked ones."""
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, compare=["city"])

        batch = pl.DataFrame(
            {
                "cust_id": [1, 2, 3],
                "city": ["ROT", "BER", "PAR"],
                "tier": ["gold", "silver", None],
                "notes": ["fresh", "fresh", "fresh"],
            }
        )
        result = _scd2(batch, path, valid_from=VF2, compare=["city"])

        assert (result.rows_inserted, result.rows_closed) == (1, 1)
        table = _read(path)
        assert "notes" in table.columns
        # Only the row this write inserted carries it; history is back-filled with NULL.
        assert table.filter(pl.col("valid_from") == datetime(2026, 2, 1, tzinfo=timezone.utc))["notes"].to_list() == [
            "fresh"
        ]
        assert table.filter(pl.col("valid_from") == datetime(2026, 1, 1, tzinfo=timezone.utc))["notes"].null_count() == 3


# ---- Resurrection ---------------------------------------------------------- #


class TestResurrection:
    def test_key_returning_after_close_out_gets_a_fresh_open_interval(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(pl.DataFrame({"cust_id": [1], "city": ["AMS"], "tier": ["gold"]}), path)
        _scd2(
            pl.DataFrame({"cust_id": [2], "city": ["BER"], "tier": ["silver"]}),
            path,
            valid_from=VF2,
            snapshot=True,
        )
        assert _read(path).filter((pl.col("cust_id") == 1) & pl.col("is_current")).height == 0

        result = _scd2(pl.DataFrame({"cust_id": [1], "city": ["AMS"], "tier": ["gold"]}), path, valid_from=VF4)
        assert (result.rows_inserted, result.rows_closed) == (1, 0)

        versions = _read(path).filter(pl.col("cust_id") == 1).sort("valid_from")
        assert versions.height == 2
        assert versions["valid_to"].to_list() == [datetime(2026, 2, 1, tzinfo=timezone.utc), None]
        assert versions["valid_from"].to_list()[1] == datetime(2026, 4, 1, tzinfo=timezone.utc)
        # A gap, not a continuous timeline: the key genuinely did not exist between Feb and Apr.
        assert versions["sk"].n_unique() == 2

    def test_a_key_cannot_be_resurrected_inside_its_own_closed_interval(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        # Key 2 leaves the snapshot and nothing else changes, so this write closes it at VF3 and
        # inserts nothing — VF3 exists only as a valid_to.
        _scd2(
            pl.DataFrame({"cust_id": [1, 3], "city": ["AMS", "PAR"], "tier": ["gold", None]}),
            path,
            valid_from=VF3,
            snapshot=True,
        )
        before = _version(path)

        # VF2 sits inside key 2's closed [VF1, VF3): reopening it there would make the key active
        # twice over [VF2, VF3), which is what an active_at read would then return.
        with pytest.raises(ValueError, match="not later than the table's newest version"):
            _scd2(pl.DataFrame({"cust_id": [2], "city": ["BER"], "tier": ["silver"]}), path, valid_from=VF2)

        assert _version(path) == before
        assert _overlapping_versions(path) == []


# ---- Identifier quoting ---------------------------------------------------- #


class TestIdentifierQuoting:
    def test_business_key_containing_a_double_quote_round_trips(self, tmp_path):
        path = tmp_path / "dim"
        key = 'we"ird'
        first = pl.DataFrame({key: ["a", "b"], "v": ["x", "y"]})
        _scd2(first, path, keys=(key,))

        second = pl.DataFrame({key: ["a", "b"], "v": ["x", "CHANGED"]})
        result = _scd2(second, path, keys=(key,), valid_from=VF2)

        assert (result.rows_inserted, result.rows_closed) == (1, 1)
        rows = _read(path).filter(pl.col(key) == "b").sort("valid_from")
        assert rows["v"].to_list() == ["y", "CHANGED"]
        assert rows["is_current"].to_list() == [False, True]

    def test_generated_column_name_containing_a_double_quote_round_trips(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path, valid_to_column='valid_to"x')

        result = _scd2(
            pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}),
            path,
            valid_from=VF2,
            valid_to_column='valid_to"x',
        )
        assert (result.rows_inserted, result.rows_closed) == (1, 1)
        closed = _read(path).filter((pl.col("cust_id") == 1) & ~pl.col("is_current"))
        assert closed['valid_to"x'].to_list() == [datetime(2026, 2, 1, tzinfo=timezone.utc)]


# ---- Clock monotonicity ---------------------------------------------------- #


class TestClockMonotonicity:
    def test_stale_instant_is_refused_instead_of_writing_an_inverted_interval(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(pl.DataFrame({"cust_id": [1], "v": ["a"]}), path, valid_from=VF2)
        before = _version(path)

        with pytest.raises(ValueError, match="not later than the table's newest version"):
            _scd2(pl.DataFrame({"cust_id": [1], "v": ["b"]}), path, valid_from=VF1)

        assert _version(path) == before
        rows = _read(path)
        assert rows.height == 1
        assert rows["valid_to"].to_list() == [None]

    def test_an_unchanged_rerun_at_the_same_instant_still_skips(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        assert _scd2(_base_frame(), path).skipped

    def test_a_close_only_snapshot_still_advances_the_watermark(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)

        closing = _scd2(
            pl.DataFrame({"cust_id": [1, 3], "city": ["ROT", "PAR"], "tier": ["gold", None]}),
            path,
            valid_from=VF3,
            snapshot=True,
        )
        # The premise: VF3 is recorded only in valid_to, so the current slice still tops out at VF2.
        assert (closing.rows_inserted, closing.rows_closed) == (0, 1)
        before = _version(path)

        with pytest.raises(ValueError, match="not later than the table's newest version"):
            _scd2(pl.DataFrame({"cust_id": [2], "city": ["BER"], "tier": ["silver"]}), path, valid_from=VF2_MID)

        assert _version(path) == before

    def test_a_later_instant_after_a_close_only_snapshot_still_writes(self, tmp_path):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        _scd2(
            pl.DataFrame({"cust_id": [1, 3], "city": ["AMS", "PAR"], "tier": ["gold", None]}),
            path,
            valid_from=VF3,
            snapshot=True,
        )

        result = _scd2(pl.DataFrame({"cust_id": [2], "city": ["BER"], "tier": ["silver"]}), path, valid_from=VF4)

        assert (result.rows_inserted, result.rows_closed) == (1, 0)
        assert _overlapping_versions(path) == []


# ---- Commit conflicts ------------------------------------------------------ #


class TestCommitRetry:
    """The retry shell around ``_scd2_once``.

    Real contention (below) proves the outcome is safe but cannot deterministically drive
    attempt-count or re-raise behaviour, so those are exercised at the ``_scd2_once`` seam.
    """

    def test_a_conflict_is_retried_against_a_re_read_table(self, tmp_path, monkeypatch):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        real = delta_utils._scd2_once
        attempts = []

        def conflict_once(*args, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                raise CommitFailedError("simulated concurrent writer")
            return real(*args, **kwargs)

        monkeypatch.setattr(delta_utils, "_scd2_once", conflict_once)
        result = _scd2(pl.DataFrame({"cust_id": [1], "city": ["ROT"], "tier": ["gold"]}), path, valid_from=VF2)

        assert len(attempts) == 2
        assert (result.rows_inserted, result.rows_closed) == (1, 1)

    def test_a_non_conflict_error_is_not_retried(self, tmp_path, monkeypatch):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        attempts = []

        def boom(*args, **kwargs):
            attempts.append(1)
            raise ValueError("not a commit conflict")

        monkeypatch.setattr(delta_utils, "_scd2_once", boom)
        with pytest.raises(ValueError, match="not a commit conflict"):
            _scd2(_base_frame(), path, valid_from=VF2)

        assert len(attempts) == 1

    def test_exhausting_the_attempts_reraises_the_conflict(self, tmp_path, monkeypatch):
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        attempts = []

        def always_conflict(*args, **kwargs):
            attempts.append(1)
            raise CommitFailedError("simulated concurrent writer")

        monkeypatch.setattr(delta_utils, "_scd2_once", always_conflict)
        with pytest.raises(CommitFailedError):
            _scd2(_base_frame(), path, valid_from=VF2, max_commit_attempts=3)

        assert len(attempts) == 3

    def test_racing_writers_never_leave_overlapping_history(self, tmp_path):
        """Losing the race to a later instant is a refusal, not corruption — and never a torn table."""
        path = tmp_path / "dim"
        _scd2(_base_frame(), path)
        start = threading.Barrier(2)

        def write(cust_id: int, city: str, valid_from: str):
            start.wait(timeout=30)
            return _scd2(
                pl.DataFrame({"cust_id": [cust_id], "city": [city], "tier": ["gold"]}),
                path,
                valid_from=valid_from,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(write, 1, "ROT", VF2), pool.submit(write, 2, "HAM", VF3)]
            landed = []
            for future in futures:
                try:
                    landed.append(future.result(timeout=60))
                except ValueError as exc:
                    # The clock guard's deliberate refusal when the winner stamped a later instant.
                    assert "not later than the table's newest version" in str(exc)

        assert landed, "both writers were refused; at least one must commit"
        assert _overlapping_versions(path) == []
