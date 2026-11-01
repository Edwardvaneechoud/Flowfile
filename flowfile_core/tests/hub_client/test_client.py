"""The hub client functions, against a fake hub through the ``http=`` MockTransport seam; no network."""

import httpx
import pytest

from flowfile_core.hub_client import client
from tests.hub_client.conftest import HUB_URL, INFO, FakeHub


def _mk(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_get_info_reads_the_hub_without_signing_in():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=INFO)

    assert client.get_info(HUB_URL + "/", http=_mk(handler)) == INFO
    assert str(seen[0].url) == f"{HUB_URL}/hub/info"
    assert "Authorization" not in seen[0].headers


def test_sign_in_posts_the_form_and_answers_the_token_pair():
    hub = FakeHub()
    tokens = client.sign_in(HUB_URL, "ana", "s3cret", http=hub.http())
    assert tokens["access_token"] and tokens["refresh_token"]
    assert hub.calls == [("POST", "/auth/token")]


def test_sign_in_with_a_wrong_password_is_refused_with_the_hubs_words():
    with pytest.raises(client.HubRefused) as exc:
        client.sign_in(HUB_URL, "ana", "wrong", http=FakeHub().http())
    assert exc.value.status == 401
    assert exc.value.detail == "Incorrect username or password"


def test_refresh_rotates_the_refresh_token():
    hub = FakeHub()
    first = client.sign_in(HUB_URL, "ana", "s3cret", http=hub.http())
    second = client.refresh(HUB_URL, first["refresh_token"], http=hub.http())
    assert second["refresh_token"] != first["refresh_token"]
    with pytest.raises(client.HubRefused):
        client.refresh(HUB_URL, first["refresh_token"], http=hub.http())


def test_register_device_sends_the_bearer_name_and_client_version():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(
            200, json={"id": 7, "name": "laptop", "client_version": "0.24.0", "last_seen_at": "2026-11-01T12:00:00Z"}
        )

    device = client.register_device(HUB_URL, "tok", "laptop", "0.24.0", http=_mk(handler))
    assert device["id"] == 7
    assert seen[0].headers["Authorization"] == "Bearer tok"
    assert httpx.Response(200, content=seen[0].content).json() == {"name": "laptop", "client_version": "0.24.0"}


def test_a_426_is_a_version_refusal_carrying_the_hubs_text():
    text = "client 0.24.0 is not supported by this hub (core 0.30.0)"
    handler = lambda request: httpx.Response(426, json={"detail": text})  # noqa: E731
    with pytest.raises(client.HubVersionRefused) as exc:
        client.register_device(HUB_URL, "tok", "laptop", "0.24.0", http=_mk(handler))
    assert exc.value.detail == text


def test_a_hub_that_does_not_answer_is_unreachable():
    def handler(request):
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(client.HubUnreachable):
        client.get_info(HUB_URL, http=_mk(handler))


def test_an_answer_that_is_not_a_hubs_is_refused():
    handler = lambda request: httpx.Response(200, text="<html>a web page</html>")  # noqa: E731
    with pytest.raises(client.HubRefused):
        client.get_info(HUB_URL, http=_mk(handler))


def test_an_error_without_a_detail_still_says_what_the_hub_answered():
    handler = lambda request: httpx.Response(500, text="Internal Server Error")  # noqa: E731
    with pytest.raises(client.HubRefused) as exc:
        client.get_info(HUB_URL, http=_mk(handler))
    assert exc.value.status == 500
    assert exc.value.detail == "The hub answered 500."
