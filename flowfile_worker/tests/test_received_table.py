import os
import tempfile

import polars as pl
import pytest

from flowfile_worker.create.funcs import create_from_path_csv
from flowfile_worker.create.models import InputCsvTable, ReceivedTable

URL = "https://raw.githubusercontent.com/edwardvaneechoud/Flowfile/main/flowfile_wasm/public/demo/regions.csv"


@pytest.mark.worker
def test_url_path_passes_through_unchanged():
    rt = ReceivedTable(name="regions.csv", path=URL, file_type="csv", table_settings=InputCsvTable())
    # A URL must not be resolved against the local filesystem (would become /app/https:/...).
    assert rt.abs_file_path == URL


@pytest.mark.worker
def test_infer_schema_field_defaults_true():
    assert InputCsvTable().infer_schema is True


@pytest.mark.worker
def test_no_inference_reads_all_columns_as_text():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, "typed.csv")
        with open(path, "w") as fh:
            fh.write("id,amount\n")
            for i in range(20):
                fh.write(f"{i},{i * 10}\n")
        rt = ReceivedTable(
            name="typed.csv", path=path, file_type="csv",
            table_settings=InputCsvTable(infer_schema=False),
        )
        schema = create_from_path_csv(rt).collect().schema
        assert schema["id"] == pl.String
        assert schema["amount"] == pl.String


@pytest.mark.worker
def test_local_path_is_resolved_to_absolute():
    rt = ReceivedTable(name="regions.csv", path="data/regions.csv", file_type="csv", table_settings=InputCsvTable())
    assert rt.abs_file_path.endswith("regions.csv")
    assert "https:/" not in rt.abs_file_path
