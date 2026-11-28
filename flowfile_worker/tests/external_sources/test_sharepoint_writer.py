import io
import json

import httpx
import polars as pl
import pytest
from openpyxl import Workbook, load_workbook

from flowfile_worker.external_sources import sharepoint_writer
from shared.sharepoint import SharePointReadError, SharePointWriteTarget


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
        sharepoint_writer.write_file(
            client, "fixture", source.lazy(), "drive", f"Sales Team/report.{format}", format, "overwrite"
        )


@pytest.mark.parametrize("format,mode", [("csv", "overwrite"), ("parquet", "error"), ("xlsx", "update")])
def test_empty_file_output_skips_all_graph_requests(format, mode):
    def graph(request):
        pytest.fail(f"Unexpected request: {request.method} {request.url}")

    frame = pl.LazyFrame(schema={"quantity": pl.Int64})
    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        sharepoint_writer.write_file(client, "fixture", frame, "drive", f"report.{format}", format, mode)


@pytest.mark.parametrize("mode,exists", [("error", False), ("update", False), ("update", True), ("overwrite", True)])
def test_write_empty_excel_follows_selected_mode(mode, exists):
    workbook = Workbook()
    workbook.active.title = "Keep"
    workbook["Keep"]["A1"] = "=1+1"
    workbook.create_sheet("Report").append(["stale"])
    original = io.BytesIO()
    workbook.save(original)
    uploaded = []

    def graph(request):
        if request.method == "GET":
            assert mode == "update"
            if request.url.path.endswith("/content"):
                assert request.url.path.endswith("/items/item/content")
                return httpx.Response(200, content=original.getvalue())
            assert request.url.path.endswith("/root:/report.xlsx")
            return httpx.Response(200, json={"id": "item", "eTag": '"v1"'}) if exists else httpx.Response(404)
        if request.method == "POST":
            if mode == "update" and exists:
                assert request.url.path.endswith("/items/item/createUploadSession")
                assert request.headers["If-Match"] == '"v1"'
            else:
                assert request.url.path.endswith("/root:/report.xlsx:/createUploadSession")
                assert json.loads(request.content)["item"]["@microsoft.graph.conflictBehavior"] == "fail"
            return httpx.Response(200, json={"uploadUrl": "https://upload.sharepoint.com/session"})
        assert request.method == "PUT"
        uploaded.append(load_workbook(io.BytesIO(request.content)))
        return httpx.Response(201)

    frame = pl.LazyFrame(schema={"quantity": pl.Int64})
    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        sharepoint_writer.write_file(client, "fixture", frame, "drive", "report.xlsx", "xlsx", mode,
                                     sheet_name="Report", write_when_empty=True)

    assert len(uploaded) == 1
    [result] = uploaded
    assert list(result["Report"].values) == [("quantity",)]
    assert result.sheetnames == (["Keep", "Report"] if mode == "update" and exists else ["Report"])
    if "Keep" in result:
        assert result["Keep"]["A1"].value == "=1+1"


@pytest.mark.parametrize("case", ["plain", "table", "missing_sheet", "missing_file", "mismatch", "empty", "conflict"])
def test_append_rows_preserves_existing_excel_data(case):
    from openpyxl.worksheet.table import Table

    workbook = Workbook()
    keep = workbook.worksheets[0]
    keep.title = "Keep"
    keep["A1"] = "=1+1"
    if case != "missing_sheet":
        report = workbook.create_sheet("Report")
        report.append(["sku", "quantity"])
        report.append(["A", 2])
        if case == "table":
            report.add_table(Table(displayName="Sales", ref="A1:B2"))
    original = io.BytesIO()
    workbook.save(original)
    uploads = []

    def graph(request):
        assert case != "empty", "Should not contact Graph"
        if request.method == "GET":
            if request.url.path.endswith("/content"):
                return httpx.Response(200, content=original.getvalue())
            if case == "missing_file":
                return httpx.Response(404)
            return httpx.Response(200, json={"id": "item", "eTag": '"v1"'})
        assert case != "mismatch", "Should not upload"
        if request.method == "POST":
            if case == "missing_file":
                assert json.loads(request.content)["item"]["@microsoft.graph.conflictBehavior"] == "fail"
            else:
                assert request.url.path.endswith("/items/item/createUploadSession")
                assert request.headers["If-Match"] == '"v1"'
                if case == "conflict":
                    return httpx.Response(412)
            return httpx.Response(200, json={"uploadUrl": "https://upload.sharepoint.com/session"})
        assert request.method == "PUT"
        uploads.append(load_workbook(io.BytesIO(request.content)))
        return httpx.Response(201)

    settings = SharePointWriteTarget(drive_id="drive", file_path="report.xlsx", format="xlsx",
                                     conflict_mode="append", sheet_name="Report", write_when_empty=True)
    frame = pl.DataFrame({"sku": ["B"], "quantity": [3]}).lazy()
    if case == "mismatch":
        frame = frame.select("quantity", "sku")
    elif case == "empty":
        frame = frame.limit(0)
    with httpx.Client(transport=httpx.MockTransport(graph)) as client:
        def write():
            sharepoint_writer.write_file(client, "fixture", frame, **settings.model_dump(exclude={"list_target"}))

        if case == "mismatch":
            with pytest.raises(SharePointReadError, match="headers.*same order"):
                write()
            return
        if case == "conflict":
            with pytest.raises(SharePointReadError):
                write()
            assert not uploads
            return
        write()

    if case == "empty":
        assert not uploads
        return
    [result] = uploads
    expected = [("sku", "quantity"), ("B", 3)]
    if case in {"plain", "table"}:
        expected.insert(1, ("A", 2))
    assert list(result["Report"].values) == expected
    if case != "missing_file":
        assert result["Keep"]["A1"].value == "=1+1"
    if case == "table":
        assert result["Report"].tables["Sales"].ref == "A1:B3"
        assert result["Report"].tables["Sales"].autoFilter.ref == "A1:B3"


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
