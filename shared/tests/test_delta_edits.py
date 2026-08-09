"""Tests for the in-place edit primitives in ``shared.delta_utils``.

Everything runs against real Delta tables in tmp dirs — the merge behaviour is only
meaningful against the deltalake version actually installed, so nothing is mocked.
The payloads mirror what the catalog edit grid ships: row-major JSON cell values.
"""

import polars as pl
import pytest
from deltalake import DeltaTable

from shared import delta_utils
from shared.delta_utils import (
    DeltaEditError,
    DeltaEditStaleError,
    add_delta_row_id_column,
    apply_delta_edits,
)


@pytest.fixture()
def delta_table(tmp_path):
    path = str(tmp_path / "tbl")
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "text": ["alpha", "beta", "gamma"],
            "score": [1.5, 2.5, 3.5],
        }
    ).with_columns(pl.lit("2024-01-02 10:11:12").str.to_datetime().alias("created"))
    df.write_delta(path)
    return path


class TestApplyDeltaEdits:
    def test_cell_edit_preserves_unsent_columns(self, delta_table):
        result = apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "text"],
            upsert_rows=[[2, "beta-fixed"]],
            expected_version=0,
        )
        assert result["rows_upserted"] == 1
        out = pl.read_delta(delta_table).sort("id")
        assert out["text"].to_list() == ["alpha", "beta-fixed", "gamma"]
        # Columns absent from the payload keep their exact original values.
        assert out["score"].to_list() == [1.5, 2.5, 3.5]
        assert out["created"].null_count() == 0

    def test_insert_and_delete_rows(self, delta_table):
        result = apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "text"],
            upsert_rows=[[4, "delta"]],
            delete_keys=[[1]],
        )
        assert result["rows_upserted"] == 1
        assert result["rows_deleted"] == 1
        out = pl.read_delta(delta_table).sort("id")
        assert out["id"].to_list() == [2, 3, 4]
        assert out.filter(pl.col("id") == 4)["score"].to_list() == [None]

    def test_new_label_column_with_values(self, delta_table):
        result = apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "label"],
            upsert_rows=[[1, "spam"], [3, "ham"]],
            new_columns=[{"name": "label", "dtype": "String"}],
        )
        out = pl.read_delta(delta_table).sort("id")
        assert out["label"].to_list() == ["spam", None, "ham"]
        assert result["column_count"] == 5
        assert {c["name"] for c in result["schema"]} >= {"id", "text", "score", "created", "label"}

    def test_new_column_survives_unrelated_upserts(self, delta_table):
        """A declared new column must be created even when the same save's upserts don't reference it."""
        result = apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "text"],
            upsert_rows=[[1, "edited"]],
            new_columns=[{"name": "label", "dtype": "String"}],
        )
        out = pl.read_delta(delta_table).sort("id")
        assert out["label"].null_count() == 3
        assert out["text"][0] == "edited"
        assert any(c["name"] == "label" for c in result["schema"])

    def test_key_column_named_len_works(self, tmp_path):
        path = str(tmp_path / "len_key")
        pl.DataFrame({"len": [1, 2], "v": ["a", "b"]}).write_delta(path)
        apply_delta_edits(path, key_columns=["len"], upsert_columns=["len", "v"], upsert_rows=[[2, "b2"]])
        assert pl.read_delta(path).sort("len")["v"].to_list() == ["a", "b2"]

    def test_rejected_payload_leaves_no_partial_commits(self, delta_table):
        """Validation failures must raise before any commit, including schema evolution."""
        before = DeltaTable(delta_table).version()
        with pytest.raises(DeltaEditError):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "nope"],
                upsert_rows=[[1, "x"]],
                new_columns=[{"name": "label", "dtype": "String"}],
            )
        assert DeltaTable(delta_table).version() == before
        assert "label" not in pl.read_delta(delta_table).columns

    def test_new_column_without_rows_evolves_schema(self, delta_table):
        result = apply_delta_edits(
            delta_table,
            key_columns=["id"],
            new_columns=[{"name": "note", "dtype": "Float64"}],
        )
        out = pl.read_delta(delta_table)
        assert out["note"].dtype == pl.Float64
        assert out["note"].null_count() == 3
        assert result["row_count"] == 3

    def test_new_column_listed_in_upsert_columns_but_carrying_no_rows(self, delta_table):
        """An empty upsert_rows builds no source, so the declared column never reaches Delta."""
        result = apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "label"],
            upsert_rows=[],
            new_columns=[{"name": "label", "dtype": "String"}],
            expected_version=0,
        )
        out = pl.read_delta(delta_table)
        assert "label" in out.columns
        assert out["label"].null_count() == 3
        assert any(c["name"] == "label" for c in result["schema"])
        assert result["new_version"] > 0

    def test_commit_landing_during_validation_is_rejected(self, delta_table, monkeypatch):
        """expected_version must be re-checked at commit time, not only at entry."""
        original = delta_utils._edit_rows_to_frame

        def commit_then_build(*args, **kwargs):
            monkeypatch.setattr(delta_utils, "_edit_rows_to_frame", original)
            racer = pl.DataFrame({"id": [9], "text": ["racer"], "score": [9.9]}).with_columns(
                pl.lit("2024-01-02 10:11:12").str.to_datetime().alias("created")
            )
            racer.write_delta(delta_table, mode="append")
            return original(*args, **kwargs)

        monkeypatch.setattr(delta_utils, "_edit_rows_to_frame", commit_then_build)

        with pytest.raises(DeltaEditStaleError):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "text"],
                upsert_rows=[[2, "beta-fixed"]],
                expected_version=0,
            )

        out = pl.read_delta(delta_table).sort("id")
        assert out["text"].to_list() == ["alpha", "beta", "gamma", "racer"]

    def test_commit_landing_during_validation_ignored_without_expected_version(self, delta_table, monkeypatch):
        """Callers who opt out of optimistic concurrency keep last-write-wins."""
        original = delta_utils._edit_rows_to_frame

        def commit_then_build(*args, **kwargs):
            monkeypatch.setattr(delta_utils, "_edit_rows_to_frame", original)
            racer = pl.DataFrame({"id": [9], "text": ["racer"], "score": [9.9]}).with_columns(
                pl.lit("2024-01-02 10:11:12").str.to_datetime().alias("created")
            )
            racer.write_delta(delta_table, mode="append")
            return original(*args, **kwargs)

        monkeypatch.setattr(delta_utils, "_edit_rows_to_frame", commit_then_build)

        apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "text"],
            upsert_rows=[[2, "beta-fixed"]],
        )

        out = pl.read_delta(delta_table).sort("id")
        assert out["text"].to_list() == ["alpha", "beta-fixed", "gamma", "racer"]

    def test_string_values_cast_to_temporal_and_numeric(self, delta_table):
        apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "created", "score"],
            upsert_rows=[[1, "2025-05-05 01:02:03", "9.75"]],
        )
        row = pl.read_delta(delta_table).filter(pl.col("id") == 1)
        assert row["created"][0].year == 2025
        assert row["score"][0] == 9.75

    def test_stale_version_raises_before_writing(self, delta_table):
        with pytest.raises(DeltaEditStaleError) as exc:
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "text"],
                upsert_rows=[[1, "x"]],
                expected_version=7,
            )
        assert exc.value.expected == 7
        assert exc.value.current == 0
        assert pl.read_delta(delta_table).sort("id")["text"][0] == "alpha"

    def test_duplicate_target_keys_rejected(self, tmp_path):
        path = str(tmp_path / "dupes")
        pl.DataFrame({"k": [1, 1, 2], "v": ["a", "b", "c"]}).write_delta(path)
        with pytest.raises(DeltaEditError, match="do not uniquely identify"):
            apply_delta_edits(path, key_columns=["k"], upsert_columns=["k", "v"], upsert_rows=[[2, "z"]])

    def test_duplicate_payload_keys_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="duplicate key values"):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "text"],
                upsert_rows=[[1, "x"], [1, "y"]],
            )

    def test_null_payload_keys_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="null key values"):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "text"],
                upsert_rows=[[None, "x"]],
            )

    def test_unknown_column_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="Unknown columns"):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "nope"],
                upsert_rows=[[1, "x"]],
            )

    def test_missing_key_column_in_payload_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="must include the key columns"):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["text"],
                upsert_rows=[["x"]],
            )

    def test_existing_new_column_name_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="already exists"):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                new_columns=[{"name": "text", "dtype": "String"}],
            )

    def test_commit_metadata_lands_in_history(self, delta_table):
        apply_delta_edits(
            delta_table,
            key_columns=["id"],
            upsert_columns=["id", "text"],
            upsert_rows=[[1, "edited"]],
            commit_metadata={"flowfile_user": "ada", "flowfile_operation": "catalog_table_edit"},
        )
        history = DeltaTable(delta_table).history(1)
        # delta-rs surfaces custom commit metadata on the commitInfo entry; the exact
        # key layout varies by version, so assert on content rather than shape.
        assert "ada" in str(history[0])

    def test_uncastable_value_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="could not be converted"):
            apply_delta_edits(
                delta_table,
                key_columns=["id"],
                upsert_columns=["id", "score"],
                upsert_rows=[[1, "not-a-number"]],
            )


class TestAddDeltaRowIdColumn:
    def test_adds_sequential_int64(self, delta_table):
        result = add_delta_row_id_column(delta_table, "record_id", expected_version=0)
        out = pl.read_delta(delta_table)
        assert out["record_id"].dtype == pl.Int64
        assert sorted(out["record_id"].to_list()) == [1, 2, 3]
        assert result["row_count"] == 3
        assert any(c["name"] == "record_id" for c in result["schema"])

    def test_existing_column_rejected(self, delta_table):
        with pytest.raises(DeltaEditError, match="already exists"):
            add_delta_row_id_column(delta_table, "text")

    def test_stale_version_rejected(self, delta_table):
        with pytest.raises(DeltaEditStaleError):
            add_delta_row_id_column(delta_table, "record_id", expected_version=3)
