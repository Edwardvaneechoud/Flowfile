from urllib.parse import parse_qs

import httpx
import pytest

TENANT = "a01b7819-6a0a-47c9-a6ff-e1563529ac33"


def test_graph_delta_pages_validate_scope_and_use_last_item():
    from shared.sharepoint import SharePointReadError, graph_drive_delta

    root = "https://graph.microsoft.com/v1.0/drives/drive/root/delta"

    def handle(req):
        assert req.headers["Authorization"] == "Bearer fixture"
        if req.url.params.get("token") == "expired":
            return httpx.Response(410, headers={"Location": root + "?token=replacement"})
        if req.url.params.get("token") == "invalid":
            return httpx.Response(200, json={"value": [], "@odata.nextLink": root.replace("/drive/", "/other/")})
        if req.url.path == "/v1.0/drives/drive/delta(token=page)":
            return httpx.Response(200, json={"value": [{"id": "id1", "deleted": {}}],
                                             "@odata.deltaLink": root + "?token=done"})
        assert req.url.params.get("token") == "start"
        return httpx.Response(200, json={"value": [{"id": "id1", "name": "a.csv"}],
                                         "@odata.nextLink": root.replace("/root/delta", "/delta(token=page)")})

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        entries, cursor = graph_drive_delta(client, "drive", "fixture", root + "?token=start")
        assert entries == {"id1": {"id": "id1", "deleted": {}}}
        assert cursor == root + "?token=done"
        with pytest.raises(SharePointReadError, match="expired"):
            graph_drive_delta(client, "drive", "fixture", root + "?token=expired")
        with pytest.raises(SharePointReadError, match="invalid continuation"):
            graph_drive_delta(client, "drive", "fixture", root + "?token=invalid")


@pytest.mark.parametrize("sheet", [None, "Q1 / 'Sales'"])
@pytest.mark.parametrize("case", ["disabled", "success", "consent_missing", "denied", "missing_sheet"])
def test_excel_column_preview(sheet, case, caplog):
    from flowfile_worker.external_sources.sharepoint_source import read_file

    caplog.set_level("INFO", logger="httpx")
    seen = []

    def handle(req):
        seen.append(req)
        if req.method == "POST":
            scopes = parse_qs(req.content.decode())["scope"][0].split()
            assert "https://graph.microsoft.com/Files.ReadWrite" in scopes
            scope = "Files.Read.All" if case == "consent_missing" else "Files.ReadWrite"
            return httpx.Response(200, json={"access_token": "test-token", "scope": scope})
        assert req.method == "GET" and req.url.host == "graph.microsoft.com"
        assert req.headers["Authorization"] == "Bearer test-token"
        if req.url.path.endswith("/workbook/worksheets"):
            return httpx.Response(200, json={"value": [
                {"id": "other-id", "name": "Other", "position": 1},
                {"id": "private-worksheet-id", "name": "Absent" if case == "missing_sheet" else "Q1 / 'Sales'",
                 "position": 1 if case == "missing_sheet" else 0},
            ]})
        assert req.url.path.endswith("/worksheets/private-worksheet-id/range(address='A1:IV1')")
        assert req.url.params["$select"] == "text"
        if case == "denied":
            return httpx.Response(403, json={"error": "private-provider-detail"})
        return httpx.Response(200, json={"text": [["sku", "quantity", ""]]})

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        frame = read_file(client, TENANT, "test-client", "private-refresh", "drive", "sales.xlsx", sheet,
                          schema_only=True, auth_method="delegated", excel_column_preview=case != "disabled")

    assert frame.height == 0
    assert frame.columns == (["sku", "quantity"] if case == "success" else [])
    assert len(seen) == {"disabled": 0, "success": 3, "consent_missing": 1, "denied": 3, "missing_sheet": 2}[case]
    assert "private-" not in caplog.text


@pytest.mark.parametrize("body", [b"sku\nA\n", b"sku\nAB\n"])
def test_enforces_download_size_without_content_length(monkeypatch, body):
    from flowfile_worker.external_sources import sharepoint_source
    from shared.sharepoint import SharePointReadError

    monkeypatch.setattr(sharepoint_source, "FILE_DOWNLOAD_LIMIT", 6)

    class Download(httpx.SyncByteStream):
        def __iter__(self):
            yield body[:4]
            yield body[4:]

    def handle(request):
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "test-token"})
        return httpx.Response(200, stream=Download())

    def read():
        with httpx.Client(transport=httpx.MockTransport(handle)) as client:
            return sharepoint_source.read_file(client, TENANT, "test-client", "test-secret", "drive", "sales.csv")

    if len(body) > 6:
        with pytest.raises(SharePointReadError, match="exceeds the download size limit"):
            read()
    else:
        assert read().to_dicts() == [{"sku": "A"}]


@pytest.mark.parametrize(
    ("path", "case", "error"),
    [
        ("reports/*/sales.csv", "invalid", "only in the filename"),
        ("reports/**.csv", "invalid", "without recursion"),
        ("reports/sales-?.csv", "empty", "No files match"),
        ("reports/sales-?.csv", "too-many", "more than 2 files"),
        ("reports/sales-?.csv", "wrong-host", "invalid listing continuation"),
        ("reports/sales-?.csv", "different-columns", "same columns and types"),
        ("reports/sales-?.csv", "too-large", "exceeds the download size limit"),
    ],
)
def test_glob_rejects_invalid_or_unbounded_reads(monkeypatch, path, case, error):
    from flowfile_worker.external_sources import sharepoint_source
    from shared.sharepoint import SharePointReadError

    monkeypatch.setattr(sharepoint_source, "MAX_GLOB_FILES", 2)
    monkeypatch.setattr(sharepoint_source, "FILE_DOWNLOAD_LIMIT", 14)

    def handle(request):
        assert case != "invalid", "Invalid patterns must fail before contacting Microsoft"
        if request.method == "POST":
            return httpx.Response(200, json={"access_token": "test-token"})
        assert request.url.host == "graph.microsoft.com"
        if request.url.path.endswith("/children"):
            count = {"empty": 0, "too-many": 3}.get(case, 2)
            page: dict[str, object] = {"value": [{"name": f"sales-{i}.csv", "file": {}} for i in range(count)]}
            if case == "wrong-host":
                page["@odata.nextLink"] = f"https://example.invalid{request.url.path}?$skiptoken=next"
            return httpx.Response(200, json=page)
        assert case in {"different-columns", "too-large"}, "Failed resolution must not download a file"
        body = b"sku\nA\n"
        if request.url.path.endswith("sales-1.csv:/content"):
            body = b"qty\n2\n" if case == "different-columns" else b"sku\nABCDE\n"
        return httpx.Response(200, content=body)

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(SharePointReadError, match=error):
            sharepoint_source.read_file(client, TENANT, "test-client", "test-secret", "drive", path)


@pytest.mark.parametrize("delimiter", [",", ";"])
def test_reads_csv_through_graph_download_redirect(delimiter, caplog):
    caplog.set_level("INFO", logger="httpx")
    from flowfile_worker.external_sources.sharepoint_source import read_file

    def handle(request):
        if request.url.host == "login.microsoftonline.com":
            assert request.url.path == f"/{TENANT}/oauth2/v2.0/token"
            assert parse_qs(request.content.decode()) == {
                "grant_type": ["client_credentials"],
                "client_id": ["test-client"],
                "client_secret": ["test-secret"],
                "scope": ["https://graph.microsoft.com/.default"],
            }
            return httpx.Response(200, json={"access_token": "test-token"})
        if request.url.host == "graph.microsoft.com":
            assert request.headers["authorization"] == "Bearer test-token"
            assert request.url.raw_path == b"/v1.0/drives/drive/root:/reports/sales%20Q1.csv:/content"
            return httpx.Response(302, headers={"Location": "https://files.example.test/download?signature=opaque"})
        assert request.url.host == "files.example.test"
        assert "authorization" not in request.headers
        return httpx.Response(200, content=f"sku{delimiter}quantity\nA{delimiter}2\nB{delimiter}5\n".encode())

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        options = {} if delimiter == "," else {"csv_delimiter": delimiter}
        frame = read_file(client, TENANT, "test-client", "test-secret", "drive", "reports/sales Q1.csv", **options)
    assert frame.to_dicts() == [{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}]
    assert "signature=opaque" not in caplog.text


@pytest.mark.parametrize("sheet_name", [None, "Sales", "Removed"])
def test_reads_excel_through_graph(sheet_name):
    extension = "XLSX"
    from io import BytesIO

    from openpyxl import Workbook

    from flowfile_worker.external_sources.sharepoint_source import read_file

    book = Workbook()
    book.active.append(["sku", "quantity"])
    book.active.append(["A", 2])
    sales = book.create_sheet("Sales")
    sales.append(["sku", "quantity"])
    sales.append(["B", 5])
    output = BytesIO()
    book.save(output)
    content = output.getvalue()

    def handle(request):
        if request.method == "POST":
            return httpx.Response(200, json={"access_token": "test-token"})
        assert request.url.path == f"/v1.0/drives/drive/root:/reports/sales.{extension}:/content"
        return httpx.Response(200, content=content)

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        if sheet_name == "Removed":
            from shared.sharepoint import SharePointReadError

            with pytest.raises(SharePointReadError, match="Enter an existing sheet name or clear Sheet name"):
                read_file(
                    client, TENANT, "test-client", "test-secret", "drive", f"reports/sales.{extension}", sheet_name
                )
            return
        frame = read_file(
            client, TENANT, "test-client", "test-secret", "drive", f"reports/sales.{extension}", sheet_name
        )
    assert frame.to_dicts() == ([{"sku": "B", "quantity": 5}] if sheet_name else [{"sku": "A", "quantity": 2}])


def test_token_failure_does_not_echo_upstream_details():
    from flowfile_worker.external_sources.sharepoint_source import SharePointReadError, read_file

    def handle(request):
        assert request.url.host == "login.microsoftonline.com"
        return httpx.Response(401, json={"error_description": "Rejected test-secret for test-client"})

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(SharePointReadError, match=r"^Microsoft token request failed \(HTTP 401\)\.$"):
            read_file(client, TENANT, "test-client", "test-secret", "drive", "data.csv")
