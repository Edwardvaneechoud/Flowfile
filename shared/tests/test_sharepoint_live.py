"""Opt-in, read-only smoke test against a dedicated SharePoint Online test site."""

import os
from urllib.parse import quote
from uuid import UUID

import httpx
import polars as pl
import pytest

from flowfile_worker.external_sources.sharepoint_source import read_file
from shared.sharepoint import acquire_token, graph_listing

pytestmark = pytest.mark.sharepoint

_PREFIX = "FLOWFILE_TEST_SHAREPOINT_"


@pytest.fixture
def live_target():
    if os.environ.get(f"{_PREFIX}ENABLED") != "1":
        pytest.skip(f"Set {_PREFIX}ENABLED=1 to opt in to a dedicated non-production test site")

    required = ("TENANT_ID", "CLIENT_ID", "CLIENT_SECRET", "SITE_ID", "DRIVE_ID")
    values = [os.environ.get(f"{_PREFIX}{name}", "") for name in required]
    missing = [f"{_PREFIX}{name}" for name, value in zip(required, values, strict=True) if not value.strip()]
    if missing:
        pytest.skip(f"SharePoint live test configuration is incomplete: {', '.join(missing)}")

    tenant_id, client_id, _, site_id, _ = values
    try:
        UUID(tenant_id)
        UUID(client_id)
        host, site, web = site_id.split(",")
        if not host.endswith(".sharepoint.com"):
            raise ValueError
        UUID(site)
        UUID(web)
    except ValueError:
        pytest.fail("Expected Entra UUIDs and a Graph site ID (hostname,site UUID,web UUID)", pytrace=False)
    return values


def test_read_known_csv_from_dedicated_sharepoint_library(live_target):
    tenant_id, client_id, credential, site_id, drive_id = live_target
    with httpx.Client(timeout=30) as client:
        token = acquire_token(client, tenant_id, client_id, credential)
        drives = graph_listing(
            client, f"/v1.0/sites/{quote(site_id, safe=',')}/drives", "id", token, "SharePoint test library listing"
        )
        if not any(drive.get("id") == drive_id for drive in drives):
            pytest.fail("Configured test drive is not a library on the configured site", pytrace=False)
        data = read_file(client, tenant_id, client_id, credential, drive_id, "flowfile-live-input.csv")

    assert data.schema == {"sku": pl.String, "quantity": pl.Int64}
    assert data.to_dicts() == [{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}]
