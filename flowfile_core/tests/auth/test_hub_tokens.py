"""Core's own API takes no hub token as a user: it signs its own sessions, and a hub's ``run`` token is for the worker.

The core, its storage dir and the fake hub are the sign-in tests' own (``tests/test_hub_sign_in.py``): docker mode,
the storage dir joined to ``FakeJoinHub`` with its own keys, the hub's tokens signed by the fake's signer. That is the
server on which a sign-in through the hub makes a person's account, so it is where a ``run`` token must make none.
"""

from tests import test_hub_sign_in
from tests.test_hub_sign_in import NODE, accounts_named, bearer, client, mapping

node_file = test_hub_sign_in.node_file
hub = test_hub_sign_in.hub
username = test_hub_sign_in.username


def test_a_run_token_is_no_users_token_on_core_and_makes_no_account(hub, username):
    token = hub.signer.token(sub=username, uid=7, aud=NODE)

    for path in ("/auth/users/me", "/auth/users"):
        response = client.get(path, headers=bearer(token))
        assert (response.status_code, response.json()["detail"]) == (401, "Could not validate credentials")
    assert mapping(hub, 7) is None and accounts_named(username) == 0
    assert hub.token_requests == []
