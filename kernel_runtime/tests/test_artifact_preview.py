"""Tests for artifact preview publishing (publish_artifact preview=True) and retrieval."""

from fastapi.testclient import TestClient

from kernel_runtime import flowfile_client

_MPL_CHART = (
    "import matplotlib\n"
    'matplotlib.use("Agg")\n'
    "import matplotlib.pyplot as plt\n"
    "fig, ax = plt.subplots()\n"
    "ax.plot([1, 2, 3])\n"
    'flowfile_ctx.publish_artifact("chart", fig, preview={preview})\n'
)


def _execute(client: TestClient, code: str, node_id: int = 1, flow_id: int = 1):
    return client.post(
        "/execute",
        json={
            "node_id": node_id,
            "code": code,
            "flow_id": flow_id,
            "input_paths": {},
            "output_dir": "",
        },
    )


class TestArtifactPreviewViaExecute:
    def test_matplotlib_preview_published_and_fetchable(self, client: TestClient):
        resp = _execute(client, _MPL_CHART.format(preview=True))
        data = resp.json()
        assert data["success"] is True
        published = data["artifacts_published"]
        assert len(published) == 1
        assert published[0]["name"] == "chart"
        assert published[0]["has_preview"] is True
        assert published[0]["preview_mime"] == "image/png"

        preview = client.get("/artifact_preview", params={"flow_id": 1, "name": "chart"})
        assert preview.status_code == 200
        blob = preview.json()
        assert blob is not None
        assert blob["mime_type"] == "image/png"
        assert blob["data"]
        assert blob["title"] == "chart"

    def test_default_preview_false(self, client: TestClient):
        resp = _execute(client, _MPL_CHART.format(preview=False))
        published = resp.json()["artifacts_published"]
        assert published[0]["has_preview"] is False
        assert published[0]["preview_mime"] is None

        preview = client.get("/artifact_preview", params={"flow_id": 1, "name": "chart"})
        assert preview.status_code == 200
        assert preview.json() is None

    def test_non_renderable_object_stores_no_preview(self, client: TestClient):
        resp = _execute(client, 'flowfile_ctx.publish_artifact("cfg", {"a": 1}, preview=True)')
        data = resp.json()
        assert data["success"] is True
        published = data["artifacts_published"]
        assert published[0]["name"] == "cfg"
        assert published[0]["has_preview"] is False
        assert published[0]["preview_mime"] is None

        preview = client.get("/artifact_preview", params={"flow_id": 1, "name": "cfg"})
        assert preview.json() is None

    def test_html_string_preview(self, client: TestClient):
        resp = _execute(client, 'flowfile_ctx.publish_artifact("report", "<b>hi</b>", preview=True)')
        published = resp.json()["artifacts_published"]
        assert published[0]["has_preview"] is True
        assert published[0]["preview_mime"] == "text/html"

        blob = client.get("/artifact_preview", params={"flow_id": 1, "name": "report"}).json()
        assert blob["mime_type"] == "text/html"
        assert blob["data"] == "<b>hi</b>"

    def test_reexecute_without_preview_clears_stored_preview(self, client: TestClient):
        first = _execute(client, _MPL_CHART.format(preview=True))
        assert first.json()["artifacts_published"][0]["has_preview"] is True
        assert client.get("/artifact_preview", params={"flow_id": 1, "name": "chart"}).json() is not None

        second = _execute(client, _MPL_CHART.format(preview=False))
        assert second.json()["artifacts_published"][0]["has_preview"] is False
        assert client.get("/artifact_preview", params={"flow_id": 1, "name": "chart"}).json() is None


class TestRenderDisplayPayload:
    def test_matplotlib_figure_returns_png(self):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        ax.plot([1, 2, 3])
        payload = flowfile_client._render_display_payload(fig)
        plt.close(fig)
        assert payload is not None
        assert payload["mime_type"] == "image/png"
        assert payload["data"]

    def test_plain_string_returns_none(self):
        assert flowfile_client._render_display_payload("just plain text") is None

    def test_dict_returns_none(self):
        assert flowfile_client._render_display_payload({"a": 1}) is None
