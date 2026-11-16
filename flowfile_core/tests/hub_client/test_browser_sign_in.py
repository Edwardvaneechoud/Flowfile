"""Connecting to a hub by signing in through the system browser (authorization code with PKCE and a loopback redirect),
against the fake hub served over real HTTP; an ``httpx.get`` of the redirect plays the browser coming back."""

import json
import time
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import browser_sign_in, client, node_tokens, session
from flowfile_core.routes import compute_backends as compute_backends_routes
from flowfile_core.routes import hub_client as hub_client_routes
from tests.hub_client.conftest import HUB_URL, FakeHub, compute_node

USER_ID = 43


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    """No node token, backends row or pending sign-in outlives a test, and the sync's checks are stood down."""
    monkeypatch.setattr(node_tokens, "_tokens", {})
    monkeypatch.setattr(compute_backends_routes, "check_unchecked", lambda backend_ids: None)
    monkeypatch.setattr(client, "_module_http", None)
    yield
    browser_sign_in.forget_all()
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    handshake.reset_handshake()


@pytest.fixture
def api(store) -> TestClient:
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


@pytest.fixture
def served(serve_fake):
    fake = FakeHub()
    fake.nodes = [compute_node(1, "team")]
    return fake, serve_fake(fake)


def _user_id(api: TestClient) -> int:
    return api.get("/auth/users/me").json()["id"]


def _stored(api: TestClient) -> dict | None:
    raw = core_secrets.get_password(session.SERVICE, f"session:{_user_id(api)}")
    return json.loads(raw) if raw else None


def _start(api: TestClient, url: str) -> dict:
    response = api.post("/hub_client/connection/browser", json={"hub_url": url, "device_name": "ana-laptop"})
    assert response.status_code == 200, response.text
    return response.json()


def _status(api: TestClient, flow_id: str) -> dict:
    response = api.get(f"/hub_client/connection/browser/{flow_id}")
    assert response.status_code == 200, response.text
    return response.json()


def _redirect_uri(authorize_url: str) -> str:
    return parse_qs(urlsplit(authorize_url).query)["redirect_uri"][0]


def _with_query(redirect: str, **changes) -> str:
    base, _, query = redirect.partition("?")
    fields = {k: v[0] for k, v in parse_qs(query).items()} | changes
    return f"{base}?{urlencode(fields)}"


def test_the_whole_sign_in_connects_registers_the_device_and_keeps_an_opaque_session(api, served):
    fake, url = served
    started = _start(api, url)

    assert started["authorize_url"].startswith(f"{url}/hub/authorize?")
    query = parse_qs(urlsplit(started["authorize_url"]).query)
    assert query["device_name"] == ["ana-laptop"]
    assert _status(api, started["flow_id"]) == {"status": "waiting", "detail": None, "connection": None}

    page = httpx.get(fake.authorize(started["authorize_url"]), timeout=10)

    assert page.status_code == 200
    assert browser_sign_in.SIGNED_IN in page.text
    status = _status(api, started["flow_id"])
    assert status["status"] == "connected"
    connection = status["connection"]
    assert connection["connected"] is True
    assert (connection["account"], connection["device_name"]) == ("Ana Lima", "ana-laptop")
    assert connection["org_name"] == fake.info["org_name"]
    assert connection["expires_at"] is not None

    stored = _stored(api)
    assert stored["kind"] == "browser"
    assert stored["account"] == "ana"
    assert stored["refresh_token"] in fake.browser_refresh
    assert stored["device_name"] == "ana-laptop"
    assert fake.devices == {("ana", "ana-laptop"): 1}
    assert [c for c in fake.calls if c[1] != "/hub/info"] == [
        ("POST", "/hub/token"),
        ("POST", "/hub/devices"),
        ("GET", "/hub/compute-nodes"),
    ]
    assert "worker:team" in {row.id for row in _rows()}

    now = api.get("/hub_client/connection").json()
    assert (now["connected"], now["account"], now["hub_url"]) == (True, "Ana Lima", url)

    with pytest.raises(httpx.TransportError):
        httpx.get(fake.authorize(started["authorize_url"]), timeout=2)


def _rows() -> list[ConfiguredBackend]:
    with get_db_context() as db:
        return db.query(ConfiguredBackend).all()


def test_a_redirect_with_another_state_is_refused_and_the_sign_in_keeps_waiting(api, served):
    fake, url = served
    started = _start(api, url)
    redirect = fake.authorize(started["authorize_url"])

    forged = httpx.get(_with_query(redirect, state="not-the-state"), timeout=10)

    assert forged.status_code == 400
    assert _status(api, started["flow_id"])["status"] == "waiting"
    assert ("POST", "/hub/token") not in fake.calls
    assert httpx.get(redirect, timeout=10).status_code == 200
    assert _status(api, started["flow_id"])["status"] == "connected"


def test_any_other_path_on_the_listener_is_a_404_that_leaves_the_sign_in_alone(api, served):
    fake, url = served
    started = _start(api, url)
    listener = _redirect_uri(started["authorize_url"]).removesuffix("/callback")

    assert httpx.get(f"{listener}/favicon.ico", timeout=10).status_code == 404
    assert httpx.get(f"{listener}/callback", timeout=10).status_code == 404
    assert _status(api, started["flow_id"])["status"] == "waiting"


def test_a_code_the_hub_refuses_for_its_verifier_fails_the_sign_in_with_the_hubs_words(api, served):
    fake, url = served
    started = _start(api, url)
    redirect = fake.authorize(started["authorize_url"])
    code = parse_qs(urlsplit(redirect).query)["code"][0]
    fake.codes[code]["challenge"] = "a" * 43

    page = httpx.get(redirect, timeout=10)

    assert page.status_code == 200
    status = _status(api, started["flow_id"])
    assert status["status"] == "failed"
    assert status["detail"] == "The hub refused the sign-in: The code verifier does not match the sign-in."
    assert status["detail"] in page.text
    assert _stored(api) is None
    assert ("POST", "/hub/devices") not in fake.calls


def test_saying_no_on_the_hub_fails_the_sign_in_and_keeps_nothing(api, served):
    fake, url = served
    started = _start(api, url)

    page = httpx.get(fake.authorize(started["authorize_url"], deny=True), timeout=10)

    assert browser_sign_in.DECLINED in page.text
    assert _status(api, started["flow_id"]) == {
        "status": "failed",
        "detail": browser_sign_in.DECLINED,
        "connection": None,
    }
    assert _stored(api) is None
    assert ("POST", "/hub/token") not in fake.calls


def test_a_device_the_hub_refuses_fails_the_sign_in_and_ends_the_grant_on_the_hub(api, served):
    fake, url = served
    fake.version_refusal = "client 0.24.0 is not supported by this hub"
    started = _start(api, url)

    httpx.get(fake.authorize(started["authorize_url"]), timeout=10)

    status = _status(api, started["flow_id"])
    assert (status["status"], status["detail"]) == ("failed", fake.version_refusal)
    assert _stored(api) is None
    assert len(fake.revoked) == 1 and fake.browser_refresh == {}


def test_a_sign_in_nobody_finishes_ends_by_itself_and_closes_its_port(api, served, monkeypatch):
    fake, url = served
    monkeypatch.setattr(browser_sign_in, "TTL_SECONDS", 0.5)
    started = _start(api, url)
    redirect = fake.authorize(started["authorize_url"])
    time.sleep(1.0)

    assert _status(api, started["flow_id"]) == {
        "status": "failed",
        "detail": browser_sign_in.EXPIRED,
        "connection": None,
    }
    with pytest.raises(httpx.TransportError):
        httpx.get(redirect, timeout=2)
    assert _stored(api) is None


def test_cancelling_ends_the_sign_in_and_closes_its_port(api, served):
    fake, url = served
    started = _start(api, url)

    assert api.delete(f"/hub_client/connection/browser/{started['flow_id']}").status_code == 200

    assert _status(api, started["flow_id"])["detail"] == browser_sign_in.CANCELLED
    with pytest.raises(httpx.TransportError):
        httpx.get(fake.authorize(started["authorize_url"]), timeout=2)


def test_an_unknown_sign_in_is_a_404(api):
    assert api.get("/hub_client/connection/browser/nope").status_code == 404
    assert api.delete("/hub_client/connection/browser/nope").status_code == 404


def test_an_older_hub_without_browser_sign_in_is_refused_with_a_sentence_and_no_listener(api, served):
    fake, url = served
    del fake.info["browser_sign_in"]

    response = api.post("/hub_client/connection/browser", json={"hub_url": url})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["kind"] == "password_only"
    assert detail["message"] == (
        f"The hub at {url} does not offer signing in through the browser; use a username and password instead."
    )
    assert browser_sign_in._flows == {}


def test_a_machine_that_does_not_allow_browser_sign_in_refuses_before_asking_the_hub(api, served, monkeypatch):
    fake, url = served
    monkeypatch.setattr(hub_client_routes, "browser_sign_in_allowed", lambda: False)

    response = api.post("/hub_client/connection/browser", json={"hub_url": url})

    assert response.status_code == 400
    assert response.json()["detail"]["kind"] == "password_only"
    assert fake.calls == []
    assert api.get("/hub_client/connection").json()["browser_sign_in"] is False


def test_an_unreachable_hub_answers_502(api, served):
    fake, url = served
    fake.down = True

    assert api.post("/hub_client/connection/browser", json={"hub_url": url}).status_code == 502


def test_only_an_administrator_starts_a_sign_in(store, served, non_admin_client):
    fake, url = served

    assert non_admin_client.post("/hub_client/connection/browser", json={"hub_url": url}).status_code == 403
    assert fake.calls == []


def test_disconnecting_a_browser_session_ends_it_on_the_hub(api, served):
    fake, url = served
    started = _start(api, url)
    httpx.get(fake.authorize(started["authorize_url"]), timeout=10)
    refresh = _stored(api)["refresh_token"]

    assert api.delete("/hub_client/connection").status_code == 200

    assert fake.revoked == [refresh]
    assert refresh not in fake.browser_refresh
    assert _stored(api) is None
    assert _rows() == []


def test_disconnecting_when_the_hub_cannot_be_told_still_forgets_the_session(api, served):
    fake, url = served
    started = _start(api, url)
    httpx.get(fake.authorize(started["authorize_url"]), timeout=10)
    fake.down = True

    assert api.delete("/hub_client/connection").status_code == 200
    assert _stored(api) is None


def _keep_browser_session(hub: FakeHub) -> dict:
    granted = hub._browser_grant("ana")
    session.save_browser(USER_ID, HUB_URL, granted, device_name="laptop", org_name="Acme Data")
    return granted


def _raw() -> dict:
    return json.loads(core_secrets.get_password(session.SERVICE, f"session:{USER_ID}"))


def test_a_browser_session_is_used_from_the_cache_and_its_account_is_the_hubs_username(store, hub):
    granted = _keep_browser_session(hub)

    assert session.access_token(USER_ID) == granted["access_token"]
    assert hub.calls == []
    assert session.account(_raw()) == "ana"
    assert _raw()["refresh_expires_at"] == pytest.approx(time.time() + hub.browser_refresh_ttl, abs=5)


def test_refreshing_a_browser_session_rotates_its_refresh_token(store, hub):
    first = _keep_browser_session(hub)
    session._access_tokens.clear()

    token = session.access_token(USER_ID)

    assert token and token != first["access_token"]
    assert hub.calls == [("POST", "/hub/token")]
    rotated = _raw()["refresh_token"]
    assert rotated != first["refresh_token"]
    assert rotated in hub.browser_refresh and first["refresh_token"] not in hub.browser_refresh
    assert session.access_token(USER_ID) == token


def test_a_browser_session_ended_on_the_hub_is_cleared_so_the_panel_asks_to_connect_again(store, hub):
    _keep_browser_session(hub)
    session._access_tokens.clear()
    hub.browser_refresh.clear()

    with pytest.raises(client.HubRefused) as refused:
        session.access_token(USER_ID)

    assert refused.value.payload["error"] == "invalid_grant"
    assert refused.value.detail == "This sign-in has ended; sign in again."
    assert session.load(USER_ID) is None
    with pytest.raises(LookupError):
        session.bearer(USER_ID, "Connect to a hub first.")


def test_a_cached_access_token_the_hub_refuses_clears_a_session_ended_on_the_hub_at_that_call(store, hub):
    _keep_browser_session(hub)
    hub.sign_out_everywhere("ana")

    with pytest.raises(client.HubRefused) as refused:
        client.list_compute_nodes(*session.bearer(USER_ID, "Connect to a hub first."))

    assert refused.value.status == 401
    assert hub.calls == [("GET", "/hub/compute-nodes"), ("POST", "/hub/token")]
    assert session.load(USER_ID) is None
    with pytest.raises(LookupError):
        session.bearer(USER_ID, "Connect to a hub first.")


def test_a_refused_access_token_of_a_live_browser_session_is_replaced_and_the_session_kept(store, hub):
    first = _keep_browser_session(hub)
    session._access_tokens[USER_ID] = ("not-a-token-the-hub-issued", time.time() + 900)

    with pytest.raises(client.HubRefused):
        client.list_compute_nodes(HUB_URL, "not-a-token-the-hub-issued")

    assert session.load(USER_ID)["refresh_token"] != first["refresh_token"]
    assert client.list_compute_nodes(*session.bearer(USER_ID, "Connect to a hub first.")) == []


def test_a_refresh_the_hub_cannot_answer_keeps_the_browser_session(store, hub):
    _keep_browser_session(hub)
    session._access_tokens.clear()
    hub.down = True

    with pytest.raises(client.HubUnreachable):
        session.access_token(USER_ID)
    assert session.load(USER_ID) is not None


def test_authorize_url_names_the_desktop_client_the_redirect_and_an_s256_challenge():
    url = client.authorize_url(
        HUB_URL + "/",
        redirect_uri="http://127.0.0.1:5000/callback",
        code_challenge="c" * 43,
        state="st",
        device_name="ana laptop",
    )
    parts = urlsplit(url)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == f"{HUB_URL}/hub/authorize"
    assert {k: v[0] for k, v in parse_qs(parts.query).items()} == {
        "response_type": "code",
        "client_id": "flowfile-desktop",
        "redirect_uri": "http://127.0.0.1:5000/callback",
        "code_challenge": "c" * 43,
        "code_challenge_method": "S256",
        "state": "st",
        "device_name": "ana laptop",
    }


def test_exchange_code_posts_the_form_and_a_refusal_carries_the_oauth_error():
    seen = []

    def handler(request):
        seen.append({k: v[0] for k, v in parse_qs(request.content.decode()).items()})
        return httpx.Response(400, json={"error": "invalid_grant", "error_description": "Code already used."})

    with pytest.raises(client.HubRefused) as refused:
        client.exchange_code(HUB_URL, "the-code", "the-verifier", "http://127.0.0.1:5000/callback", http=_mk(handler))

    assert seen == [
        {
            "grant_type": "authorization_code",
            "code": "the-code",
            "code_verifier": "the-verifier",
            "client_id": "flowfile-desktop",
            "redirect_uri": "http://127.0.0.1:5000/callback",
        }
    ]
    assert (refused.value.status, refused.value.detail) == (400, "Code already used.")
    assert refused.value.payload == {"error": "invalid_grant", "error_description": "Code already used."}


@pytest.mark.parametrize(
    "answer",
    [
        {"access_token": "a", "refresh_token": "r", "expires_in": 900, "refresh_expires_in": 60},
        {
            "access_token": "a",
            "refresh_token": "r",
            "expires_in": "900",
            "refresh_expires_in": 60,
            "user": {"username": "ana"},
        },
        {
            "access_token": "a",
            "refresh_token": "r",
            "expires_in": 900,
            "refresh_expires_in": 60,
            "user": {"username": ""},
        },
        {"refresh_token": "r", "expires_in": 900, "refresh_expires_in": 60, "user": {"username": "ana"}},
    ],
    ids=["no user", "lifetime not a number", "empty username", "no access token"],
)
def test_a_token_answer_that_is_not_a_hubs_is_refused(answer):
    handler = lambda request: httpx.Response(200, json=answer)  # noqa: E731

    with pytest.raises(client.HubRefused, match="did not answer /hub/token"):
        client.refresh_browser(HUB_URL, "r", http=_mk(handler))


def test_revoke_posts_the_token_and_the_client():
    seen = []

    def handler(request):
        seen.append((request.url.path, {k: v[0] for k, v in parse_qs(request.content.decode()).items()}))
        return httpx.Response(200, json={})

    client.revoke(HUB_URL, "the-refresh", http=_mk(handler))

    assert seen == [("/hub/token/revoke", {"token": "the-refresh", "client_id": "flowfile-desktop"})]


def _mk(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))
