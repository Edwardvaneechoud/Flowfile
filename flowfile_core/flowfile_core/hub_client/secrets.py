"""A person's secrets on the hub, for a server that joined it with keys of its own: there every secret lives on the hub.

The Secrets page's routes forward to the hub's own ``/secrets`` routes with the person's delegated access token
(``listed``, ``found``, ``create``, ``change``, ``remove``), so nothing of them is written to this server's database
and sharing stays on the hub's pages. A lookup by name asks the hub's vend for the secret sealed to this server itself
(``sealed_here``), which this server's core and worker open with its own key, as a server of the hub opens what is
sealed to it. At start, the named secrets still in this server's database move to their owners on the hub
(``move_to_hub``); what cannot move stays, and ``kept`` says why, by name. A secret a stored connection or setting
holds stays here with it. The order a name resolves in, and who may move what, are the hub's.

Requests log method, path and status only, never a body, a value, an assertion or a token.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass

from sqlalchemy import select

from flowfile_core.auth import issuer_accounts, sharing
from flowfile_core.configs import logger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from shared import hub_node, hub_node_key, hub_sealed

SECRETS_PATH = "/secrets/secrets"
IMPORT_PATH = "/hub/nodes/secrets/import"
NOT_SIGNED_IN = "Your secrets are on the hub, and your sign-in through it has ended; sign in again"
HELD_HERE = "a connection or setting on this server holds it, so it stays here with it"
NO_OWNER = "its owner has no account on the hub"
NO_SUCH = "You have no secret named {name} on the hub"
"""How the hub's 403 to a vend begins for a name the person reaches no secret of."""
NOT_YET = "it has not moved to the hub yet; this server moves it when it starts and reaches the hub"
_reasons: dict[int, str] = {}


@dataclass(frozen=True)
class KeptSecret:
    """A secret of this server's database that did not move to the hub, and the one sentence saying why."""

    name: str
    reason: str


def _signed_in(user_id: int) -> tuple[str, str]:
    """The hub's URL and an access token from ``user_id``'s sign-in through it; ``LookupError`` when there is none."""
    stored = session.load(user_id)
    if stored is None:
        raise LookupError(NOT_SIGNED_IN)
    try:
        token = session.access_token(user_id)
    except HubRefused as e:
        if e.status in (400, 401):
            raise LookupError(NOT_SIGNED_IN) from e
        raise
    if token is None:
        raise LookupError(NOT_SIGNED_IN)
    return stored["hub_url"], token


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _path(name: str) -> str:
    from urllib.parse import quote

    return f"{SECRETS_PATH}/{quote(name, safe='')}"


def listed(user_id: int) -> list[dict]:
    """The person's secrets as the hub lists them: names and metadata, never a value."""
    hub_url, token = _signed_in(user_id)
    return client._request_list("GET", hub_url, SECRETS_PATH, None, headers=_bearer(token))


def found(user_id: int, name: str) -> dict:
    """The person's secret ``name`` as the hub answers it; ``HubRefused`` 404 when the hub has none they reach."""
    hub_url, token = _signed_in(user_id)
    return client._request("GET", hub_url, _path(name), None, headers=_bearer(token))


def create(user_id: int, name: str, value: str) -> dict:
    """Add the secret ``name`` to the person's own secrets on the hub; the hub's 400 when they hold one of that name."""
    hub_url, token = _signed_in(user_id)
    body = {"name": name, "value": value}
    return client._request("POST", hub_url, SECRETS_PATH, None, json=body, headers=_bearer(token))


def change(user_id: int, name: str, value: str) -> dict:
    """Give the person's own secret ``name`` on the hub a new value, keeping it and its shares; the hub's 404 else."""
    hub_url, token = _signed_in(user_id)
    return client._request("PUT", hub_url, _path(name), None, json={"value": value}, headers=_bearer(token))


def remove(user_id: int, name: str) -> None:
    """Delete the person's own secret ``name`` on the hub; the hub's 404 when they own none of that name."""
    hub_url, token = _signed_in(user_id)
    client._send("DELETE", hub_url, _path(name), None, headers=_bearer(token))


def lookup_run_id() -> str:
    """The run id a lookup outside a run's vend asks with; the hub's ledger records it as given."""
    return f"core-lookup-{uuid.uuid4().hex}"


def sealed_here(user_id: int, name: str, run_id: str | None = None) -> str | None:
    """The person's secret ``name`` as the hub resolves it for them, sealed to this server: a ``sealed_secret`` string
    that this server's core and worker open with its own key. ``None`` when the hub says they reach no secret of that
    name.

    Raises:
        LookupError: A sentence: the sign-in through the hub ended, the hub refused for another reason, or it did not
            answer.
    """
    from flowfile_core.hub_client import vend

    here = hub_node.resolved().name
    hub_url, token = _signed_in(user_id)
    try:
        answer = client.vend(hub_url, token, run_id or lookup_run_id(), [], [], targets=[here], secrets=[name])
    except HubRefused as e:
        if e.status == 403 and e.detail.startswith(NO_SUCH.format(name=name)):
            return None
        if e.status == 401:
            raise LookupError(NOT_SIGNED_IN) from e
        raise LookupError(vend.refusal_sentence(e.detail, e.status)) from e
    except HubVersionRefused as e:
        raise LookupError(vend.refusal_sentence(e.detail, 426)) from e
    except HubUnreachable as e:
        raise LookupError(str(e)) from e
    sealed = next(secret["sealed"] for secret in answer["secrets"] if secret.get("name") == name)
    return hub_sealed.sealed_secret(name, sealed)


def _held(db) -> set[int]:
    """The ids of the secrets a row of another table points at: a stored connection's or a credential's own."""
    held: set[int] = set()
    for table in db_models.Base.metadata.tables.values():
        for column in table.columns:
            if table.name != "secrets" and any(fk.column.table.name == "secrets" for fk in column.foreign_keys):
                held.update(value for (value,) in db.execute(select(column)) if value is not None)
    return held


def _held_names() -> set[str]:
    """The names this server's own settings keep secrets under, read by name here and nowhere else."""
    from flowfile_core.configs.app_settings import _OAUTH_KEYS
    from flowfile_core.routes.community_github import GITHUB_PUBLISH_LOGIN_KEY, GITHUB_PUBLISH_TOKEN_KEY

    return {*_OAUTH_KEYS, GITHUB_PUBLISH_TOKEN_KEY, GITHUB_PUBLISH_LOGIN_KEY}


def _is_held(row: db_models.Secret, held: set[int]) -> bool:
    return row.id in held or row.name in _held_names()


def kept(user_id: int) -> list[KeptSecret]:
    """The person's secrets that are still in this server's database, each with the sentence saying why."""
    with get_db_context() as db:
        rows = db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).order_by(db_models.Secret.id)
        held = _held(db)
        return [
            KeptSecret(row.name, HELD_HERE if _is_held(row, held) else _reasons.get(row.id, NOT_YET)) for row in rows
        ]


def import_secrets(entries: list[dict]) -> list[dict]:
    """Post ``entries`` (``hub_user_id``, ``name``, ``value``) to the hub's import with this node's assertion, and
    answer its result for each, in order (``moved`` and the hub's ``reason``).

    Raises:
        LookupError: This node cannot sign for itself (it left the hub, or has no keys).
        HubRefused, HubUnreachable, HubVersionRefused: The hub refused the call or did not answer.
    """
    settings = hub_node.resolved()
    if not settings.hub_url or not settings.name:
        raise LookupError("this server has not joined a hub")
    try:
        keys = hub_node_key.load()
    except (OSError, ValueError) as e:
        raise LookupError(f"this server's keys could not be read ({type(e).__name__})") from None
    if keys is None:
        raise LookupError("this server has no keys of its own")
    assertion = keys.assertion(settings.name, settings.issuer.rstrip("/") + IMPORT_PATH)
    answer = client._request(
        "POST", settings.hub_url, IMPORT_PATH, None, json={"secrets": entries}, headers=_bearer(assertion)
    )
    results = answer.get("results")
    if not (
        isinstance(results, list)
        and len(results) == len(entries)
        and all(isinstance(r, dict) and isinstance(r.get("moved"), bool) for r in results)
    ):
        raise HubRefused(200, f"{settings.hub_url} did not answer {IMPORT_PATH} as a Flowfile hub does.")
    return results


def move_to_hub() -> None:
    """Move the named secrets in this server's database to their owners on the hub, when it joined with keys.

    Each secret whose owner the hub knows (``issuer_accounts.subject_id``) is sent with its value; one the hub took
    leaves this database, with its shares here. One the hub refused stays, with the hub's sentence, as does one whose
    owner has no account on the hub or that a connection or setting holds; ``kept`` lists them for their owner and the
    log names each, never a value. A hub that cannot be asked leaves every secret here, said once.
    """
    if issuer_accounts.joined_with_keys() is None:
        return
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    _reasons.clear()
    with get_db_context() as db:
        held = _held(db)
        sending: list[tuple[db_models.Secret, dict]] = []
        for row in db.query(db_models.Secret).order_by(db_models.Secret.id):
            if _is_held(row, held):
                continue
            hub_user_id = issuer_accounts.subject_id(db, row.user_id)
            if hub_user_id is None:
                _reasons[row.id] = NO_OWNER
                logger.warning("Secret %s stays on this server: %s", row.name, NO_OWNER)
                continue
            try:
                value = decrypt_secret(row.encrypted_value, row.user_id).get_secret_value()
            except Exception as e:
                _reasons[row.id] = f"it does not decrypt on this server ({type(e).__name__})"
                logger.warning("Secret %s stays on this server: %s", row.name, _reasons[row.id])
                continue
            sending.append((row, {"hub_user_id": hub_user_id, "name": row.name, "value": value}))
        if not sending:
            return
        try:
            results = import_secrets([entry for _, entry in sending])
        except (LookupError, HubRefused, HubUnreachable, HubVersionRefused) as e:
            reason = f"the hub did not take it: {e}"
            for row, _ in sending:
                _reasons[row.id] = reason
            names = ", ".join(row.name for row, _ in sending)
            logger.warning("Secrets %s stay on this server: %s", names, reason)
            return
        for (row, entry), result in zip(sending, results, strict=True):
            if result["moved"]:
                logger.info("Moved secret %s of hub person %s to the hub", row.name, entry["hub_user_id"])
                _reasons.pop(row.id, None)
                sharing.delete_grants_for_resource(db, "secret", row.id)
                db.delete(row)
            else:
                reason = str(result.get("reason") or "the hub did not take it")
                _reasons[row.id] = reason
                logger.warning("Secret %s stays on this server: %s", row.name, reason)
        db.commit()


def start_move() -> threading.Thread | None:
    """Run ``move_to_hub`` from a daemon thread, so a slow hub never holds up the start; ``None`` when not joined."""
    if issuer_accounts.joined_with_keys() is None:
        return None

    def run() -> None:
        try:
            move_to_hub()
        except Exception:
            logger.exception("Moving this server's secrets to the hub failed")

    thread = threading.Thread(target=run, name="hub-secrets-move", daemon=True)
    thread.start()
    return thread
