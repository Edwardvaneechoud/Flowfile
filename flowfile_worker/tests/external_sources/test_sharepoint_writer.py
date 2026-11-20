import io
import json

import httpx
import polars as pl
import pytest

from flowfile_worker.external_sources import sharepoint_writer
from shared.sharepoint import SharePointReadError


def test_list_writer_maps_scalar_rows_and_reports_partial_writes():
    from shared.sharepoint import SharePointListTarget

    target = SharePointListTarget(site_id="example.sharepoint.com,site,web", list_id="list",
                                  column_mapping={"title": "Title", "active": "Active"})
    posts = []

    def graph(req):
        if req.method == "GET":
            assert req.url.path.endswith("/lists/list/columns")
            return httpx.Response(200, json={"value": [
                {"name": "Title", "text": {}}, {"name": "Active", "boolean": {}},
            ]})
        assert req.method == "POST"
        assert req.url.path == "/v1.0/sites/example.sharepoint.com,site,web/lists/list/items"
        posts.append(json.loads(req.content))
        return httpx.Response(201 if len(posts) == 1 else 429)

    frame = pl.DataFrame({"title": ["A", "B", "C"], "active": [True, None, False]}).lazy()
    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        with pytest.raises(SharePointReadError, match=r"1 of 3.*HTTP 429"):
            sharepoint_writer.write_list(client, "fixture", frame, target)
    assert posts == [{"fields": {"Title": "A", "Active": True}}, {"fields": {"Title": "B"}}]


@pytest.mark.parametrize("format", ["csv", "xlsx", "parquet"])
def test_overwrite_serializes_actual_rows(format):
    source = pl.DataFrame({"sku": ["A"], "quantity": [2]})

    def graph(request):
        assert request.method == "PUT"
        assert request.url.path == f"/v1.0/drives/drive/root:/Sales Team/report.{format}:/content"
        assert request.headers["authorization"] == "Bearer fixture"
        reader = {"csv": pl.read_csv, "xlsx": pl.read_excel, "parquet": pl.read_parquet}[format]
        assert reader(io.BytesIO(request.read())).to_dicts() == source.to_dicts()
        return httpx.Response(201, json={"id": "item"})

    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        sharepoint_writer.write_file(client, "fixture", source.lazy(), "drive", f"Sales Team/report.{format}", format, "overwrite")


@pytest.mark.parametrize("mode,behavior", [("error", "fail"), ("append_suffix", "rename"), ("overwrite", "replace")])
def test_upload_session_conflict_and_chunk_contract(monkeypatch, caplog, mode, behavior):
    import json

    caplog.set_level("INFO", logger="httpx")
    monkeypatch.setattr(sharepoint_writer, "CHUNK_SIZE", 320 * 1024)
    monkeypatch.setattr(sharepoint_writer, "SIMPLE_LIMIT", 1)
    data = pl.DataFrame({"body": ["x" * (320 * 1024 + 5)]})
    received = bytearray()

    def graph(request):
        if request.method == "POST":
            assert request.url.path == "/v1.0/drives/drive/root:/report.csv:/createUploadSession"
            assert request.headers["authorization"] == "Bearer fixture"
            assert json.loads(request.content)["item"]["@microsoft.graph.conflictBehavior"] == behavior
            return httpx.Response(200, json={"uploadUrl": "https://upload.sharepoint.com/session?opaque=fixture"})
        assert request.method == "PUT"
        assert request.url.host == "upload.sharepoint.com"
        assert "authorization" not in request.headers
        size = len(request.content)
        end = len(received) + size
        assert request.headers["content-range"] == f"bytes {len(received)}-{end - 1}/{len(data.write_csv().encode())}"
        received.extend(request.content)
        if size == sharepoint_writer.CHUNK_SIZE:
            return httpx.Response(202, json={"nextExpectedRanges": [f"{end}-"]})
        return httpx.Response(201, json={"id": "item"})

    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        sharepoint_writer.write_file(client, "fixture", data.lazy(), "drive", "report.csv", "csv", mode)
    assert bytes(received) == data.write_csv().encode()
    assert "opaque=fixture" not in caplog.text, "SharePoint upload capability appeared in HTTP logs"


@pytest.mark.parametrize("status", [403, 409, 429])
def test_upload_session_failure_keeps_graph_details_private(status):
    methods = []

    def graph(request):
        methods.append(request.method)
        return httpx.Response(status, json={"error": {"message": "private destination"}})

    data = pl.LazyFrame({"quantity": [2]})
    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        with pytest.raises(SharePointReadError) as failure:
            sharepoint_writer.write_file(client, "fixture", data, "drive", "report.csv", "csv", "error")
    assert str(failure.value) == f"SharePoint upload session failed (HTTP {status})."
    assert methods == ["POST"]
