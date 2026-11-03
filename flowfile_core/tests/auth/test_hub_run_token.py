"""On a server that joined its hub with keys of its own, a hub's ``run`` token is the person it names by the hub's id.

That person is the account mapped to the hub's issuer and their id there, made at their first token as at a first
sign-in, never a local account with the same id or name, and never made an administrator by a ``run`` token. The core,
its storage dir and the fake hub are the sign-in tests' own (``tests/test_hub_sign_in.py``): docker mode, the storage
dir joined to ``FakeJoinHub`` with its own keys, the hub's tokens signed by the fake's signer. A node that did not join
with keys reads the token's ``uid`` as this core's own id, as ``test_hub_tokens.py`` tests.
"""

import uuid

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from shared import hub_node_key
from tests import test_hub_sign_in
from tests.test_hub_sign_in import NODE, accounts_named, bearer, client, mapping, me, sign_in

node_file = test_hub_sign_in.node_file
hub = test_hub_sign_in.hub
local_user = test_hub_sign_in.local_user
username = test_hub_sign_in.username


def run_token(hub, *, uid: int | None, sub: str, **claims) -> str:
    return hub.signer.token(sub=sub, uid=uid, aud=NODE, **claims)


def as_bearer(token: str):
    return client.get("/auth/users/me", headers=bearer(token))


def test_a_run_token_is_the_person_the_hub_names_never_the_local_account_holding_their_id(hub, local_user):
    alice = local_user(f"alice-{uuid.uuid4().hex[:8]}")
    bob = f"bob-{uuid.uuid4().hex[:8]}"

    person = me(run_token(hub, uid=alice, sub=bob))

    assert person["username"] == bob and person["id"] != alice
    assert mapping(hub, alice) == person["id"]
    assert me(run_token(hub, uid=alice, sub=bob))["id"] == person["id"]


def test_a_run_token_never_takes_over_a_local_account_of_the_same_name(hub, local_user, username):
    local_id = local_user(username)

    person = me(run_token(hub, uid=987654, sub=username))

    assert person["username"] == f"{username}@127.0.0.1" and person["id"] != local_id
    assert mapping(hub, 987654) == person["id"]


def test_the_first_run_token_of_a_person_makes_their_account_which_the_next_one_reuses_never_an_administrator(
    hub, username
):
    first = me(run_token(hub, uid=7, sub=username, node_admin=True, name="Ana Lima"))
    second = me(run_token(hub, uid=7, sub=username))

    assert first["id"] == second["id"] == mapping(hub, 7)
    assert (first["username"], first["full_name"]) == (username, None)
    assert (first["is_admin"], first["must_change_password"], first["disabled"]) == (False, False, False)
    assert accounts_named(username) == 1
    assert hub.token_requests == []


def test_a_person_who_came_by_a_run_token_signs_in_as_that_account_and_only_the_sign_in_sets_administration(
    hub, username
):
    first = me(run_token(hub, uid=7, sub=username))
    admin = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username, node_admin=True)).json()["access_token"])
    after = me(run_token(hub, uid=7, sub=username))
    demoted = me(sign_in(hub.sign_in_token(NODE, uid=7, username=username, node_admin=False)).json()["access_token"])
    still = me(run_token(hub, uid=7, sub=username, node_admin=True))

    assert first["id"] == admin["id"] == after["id"] == demoted["id"] == still["id"] == mapping(hub, 7)
    assert [first["is_admin"], admin["is_admin"], after["is_admin"]] == [False, True, True]
    assert [demoted["is_admin"], still["is_admin"]] == [False, False]
    assert accounts_named(username) == 1


def test_a_run_token_without_the_persons_id_on_the_hub_is_refused(hub, local_user, username):
    local_user(username)

    response = as_bearer(run_token(hub, uid=None, sub=username))

    assert (response.status_code, response.json()["detail"]) == (401, "Hub token refused: missing claim uid")
    assert accounts_named(username) == 1


def test_a_dispatch_token_is_still_no_users_token(hub):
    response = as_bearer(hub.signer.token(scope="dispatch", sub="hub", uid=None, aud=NODE))

    assert (response.status_code, response.json()["detail"]) == (401, "dispatch tokens are not user tokens")


def test_a_disabled_mapped_account_answers_as_inactive(hub, username):
    user_id = me(run_token(hub, uid=7, sub=username))["id"]
    with get_db_context() as db:
        db.query(db_models.User).filter(db_models.User.id == user_id).update({"disabled": True})
        db.commit()

    response = as_bearer(run_token(hub, uid=7, sub=username))

    assert (response.status_code, response.json()["detail"]) == (400, "Inactive user")
    assert accounts_named(username) == 1


def test_a_person_whose_both_names_are_taken_is_refused_as_at_a_sign_in(hub, local_user, username):
    local_user(username)
    local_user(f"{username}@127.0.0.1")

    response = as_bearer(run_token(hub, uid=7, sub=username))

    assert response.status_code == 409
    assert response.json()["detail"] == (
        f"This server already has accounts named {username} and {username}@127.0.0.1; "
        "an administrator removes one of them before this person can sign in here."
    )
    assert mapping(hub, 7) is None


def test_a_node_that_has_no_keys_of_its_own_reads_the_hubs_id_as_its_own_as_before(
    hub, node_file, local_user, username
):
    local_id = local_user(username)
    node_file.with_name(hub_node_key.FILE_NAME).unlink()

    by_id = me(run_token(hub, uid=local_id, sub="someone-else"))
    by_name = me(run_token(hub, uid=None, sub=username))

    assert by_id["id"] == by_name["id"] == local_id
    assert mapping(hub, local_id) is None
