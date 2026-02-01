"""Tests for flowfile/flow_data_engine/sample_data module."""

import polars as pl

from flowfile_core.flowfile.flow_data_engine.sample_data import (
    create_fake_data,
    create_fake_data_raw,
)


class TestCreateFakeData:
    """Test create_fake_data function."""

    def test_returns_dataframe(self):
        df = create_fake_data(n_records=10)
        assert isinstance(df, pl.DataFrame)

    def test_correct_record_count(self):
        df = create_fake_data(n_records=50)
        assert len(df) == 50

    def test_expected_columns(self):
        df = create_fake_data(n_records=10)
        expected_cols = {"ID", "Name", "Address", "City", "Email", "Phone",
                         "DOB", "Work", "Zipcode", "Country", "sales_data"}
        assert set(df.columns) == expected_cols

    def test_optimized_mode_large(self):
        """Test that optimized mode works for larger datasets."""
        df = create_fake_data(n_records=20_000, optimized=True)
        assert len(df) == 20_000

    def test_non_optimized_mode(self):
        df = create_fake_data(n_records=50, optimized=False)
        assert len(df) == 50

    def test_small_dataset(self):
        df = create_fake_data(n_records=5)
        assert len(df) == 5
        assert len(df.columns) > 0


class TestCreateFakeDataRaw:
    """Test create_fake_data_raw generator function."""

    def test_returns_generator(self):
        gen = create_fake_data_raw(n_records=5)
        assert hasattr(gen, "__next__")

    def test_correct_count(self):
        records = list(create_fake_data_raw(n_records=10))
        assert len(records) == 10

    def test_record_structure(self):
        records = list(create_fake_data_raw(n_records=1))
        record = records[0]
        assert "ID" in record
        assert "Name" in record
        assert "Email" in record
        assert "City" in record

    def test_col_selection(self):
        records = list(create_fake_data_raw(n_records=5, col_selection=["ID", "Name"]))
        for record in records:
            assert "ID" in record
            assert "Name" in record
            # Other fields should not be present
            assert "City" not in record
            assert "Phone" not in record

    def test_col_selection_with_email(self):
        """Test that Email is generated correctly even with col_selection."""
        records = list(create_fake_data_raw(n_records=5, col_selection=["Name", "Email"]))
        for record in records:
            assert "Name" in record
            assert "Email" in record
            assert "@" in record["Email"]

    def test_empty_generator(self):
        records = list(create_fake_data_raw(n_records=0))
        assert len(records) == 0
