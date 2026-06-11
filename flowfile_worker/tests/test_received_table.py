import pytest

from flowfile_worker.create.models import InputCsvTable, ReceivedTable

URL = "https://raw.githubusercontent.com/edwardvaneechoud/Flowfile/main/flowfile_wasm/public/demo/regions.csv"


@pytest.mark.worker
def test_url_path_passes_through_unchanged():
    rt = ReceivedTable(name="regions.csv", path=URL, file_type="csv", table_settings=InputCsvTable())
    # A URL must not be resolved against the local filesystem (would become /app/https:/...).
    assert rt.abs_file_path == URL


@pytest.mark.worker
def test_local_path_is_resolved_to_absolute():
    rt = ReceivedTable(name="regions.csv", path="data/regions.csv", file_type="csv", table_settings=InputCsvTable())
    assert rt.abs_file_path.endswith("regions.csv")
    assert "https:/" not in rt.abs_file_path
