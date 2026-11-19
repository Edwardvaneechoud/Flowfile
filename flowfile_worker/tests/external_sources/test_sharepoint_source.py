from urllib.parse import parse_qs

import httpx
import pytest

TENANT = "a01b7819-6a0a-47c9-a6ff-e1563529ac33"


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


@pytest.mark.parametrize("delimiter", [",", ";"])
def test_reads_csv_through_graph_download_redirect(delimiter):
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

            with pytest.raises(SharePointReadError, match="selected SharePoint worksheet is missing"):
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
