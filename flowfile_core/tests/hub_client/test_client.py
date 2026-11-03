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


STORE = {
    "uri": "s3://org-bucket/exchange",
    "connection": "org exchange",
    "max_rows": 50000,
    "set_by": 1,
    "set_at": "2026-11-03T09:00:00Z",
}


def test_get_exchange_store_sends_the_bearer_and_answers_the_hubs_store():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=STORE)

    assert client.get_exchange_store(HUB_URL, "tok", http=_mk(handler)) == STORE
    assert str(seen[0].url) == f"{HUB_URL}/hub/exchange-store"
    assert seen[0].headers["Authorization"] == "Bearer tok"


def test_a_hub_that_sets_no_exchange_store_answers_none():
    hub = FakeHub()

    assert client.get_exchange_store(HUB_URL, "tok", http=hub.http()) is None
    assert hub.calls == [("GET", "/hub/exchange-store")]


def test_an_exchange_store_without_a_limit_is_the_hubs_store():
    answer = {**STORE, "max_rows": None}
    handler = lambda request: httpx.Response(200, json=answer)  # noqa: E731

    assert client.get_exchange_store(HUB_URL, "tok", http=_mk(handler)) == answer


@pytest.mark.parametrize(
    "body",
    [
        {key: value for key, value in STORE.items() if key != "connection"},
        {**STORE, "uri": 7},
        {**STORE, "connection": ""},
        {**STORE, "max_rows": "50000"},
        {**STORE, "max_rows": True},
    ],
    ids=["no-connection", "uri-not-a-string", "blank-connection", "max-rows-a-string", "max-rows-a-bool"],
)
def test_an_exchange_store_answer_that_is_not_a_hubs_is_refused(body):
    handler = lambda request: httpx.Response(200, json=body)  # noqa: E731

    with pytest.raises(client.HubRefused) as exc:
        client.get_exchange_store(HUB_URL, "tok", http=_mk(handler))
    assert exc.value.status == 200
    assert str(exc.value) == f"{HUB_URL} did not answer /hub/exchange-store as a Flowfile hub does."


def test_the_hubs_other_refusals_of_the_exchange_store_are_raised():
    handler = lambda request: httpx.Response(401, json={"detail": "Could not validate credentials"})  # noqa: E731

    with pytest.raises(client.HubRefused) as exc:
        client.get_exchange_store(HUB_URL, "tok", http=_mk(handler))
    assert (exc.value.status, exc.value.detail) == (401, "Could not validate credentials")
