"""The stored connections a person reaches on their hub, for any machine signed in to it: a desktop's Connect or a
joined server's delegation, both ``session.load``.

The hub's connections are never copied here. Each connection page lists them after the machine's own, read-only and
marked ``from_hub`` (``listed``, ``hub_rows``); a step that names one gets its list row, no secret (``found``), and the
run's vend seals its secrets to the hub server that runs the step (``flow_graph.execution``). Core never opens them.
That is so for cloud storage connections; on a server joined with keys the database and Kafka pages still forward to
the hub (``create``, ``change``, ``remove``) and a step there opens those by name (``opened_here``).

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import quote

from pydantic import BaseModel, SecretStr

from flowfile_core.configs import logger
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.hub_client.secrets import _bearer, _signed_in, lookup_run_id
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate
from shared import hub_node, hub_sealed

Kind = Literal["cloud", "kafka", "database"]

CLOUD_PATH = "/cloud_connections/cloud_connection"
CLOUD_LIST = "/cloud_connections/cloud_connections"
KAFKA_PATH = "/kafka/connections"
DATABASE_PATH = "/db_connection_lib"
LIST_PATHS = {"cloud": CLOUD_LIST, "kafka": KAFKA_PATH, "database": DATABASE_PATH}
WRITE_PATHS = {"cloud": CLOUD_PATH, "kafka": KAFKA_PATH, "database": DATABASE_PATH}
LABELS = {"cloud": "cloud storage connection", "kafka": "Kafka connection", "database": "database connection"}
NOT_SIGNED_IN = "Your connections are on the hub, and your sign-in through it has ended; sign in again"
NO_SUCH = "You have no {label} named {name} on the hub"
"""How the hub's 403 to a vend begins for a name the person reaches no connection of."""


def _signed_in_for_connections(user_id: int) -> tuple[str, str]:
    try:
        return _signed_in(user_id)
    except LookupError:
        raise LookupError(NOT_SIGNED_IN) from None


def plain(model: BaseModel) -> dict[str, Any]:
    """The fields the person set on ``model``, each ``SecretStr`` as its plain value, as the hub's route takes them."""
    return {
        key: value.get_secret_value() if isinstance(value, SecretStr) else value
        for key, value in model.model_dump(exclude_unset=True).items()
    }


def listed(user_id: int, kind: Kind) -> list[dict]:
    """The person's connections of ``kind`` as the hub lists them: their own and those shared with them, no secret,
    each marked ``from_hub``."""
    hub_url, token = _signed_in_for_connections(user_id)
    rows = client._request_list("GET", hub_url, LIST_PATHS[kind], None, headers=_bearer(token))
    return [{**row, "from_hub": True} for row in rows]


def hub_rows(user_id: int, kind: Kind) -> list[dict]:
    """``listed``, or none when the person has no sign-in to a hub here or the hub does not answer."""
    try:
        return listed(user_id, kind) if session.load(user_id) is not None else []
    except Exception as e:
        logger.warning(f"The hub's {LABELS[kind]}s are not listed: {e}")
        return []


def found(user_id: int, kind: Kind, name: str | None) -> dict | None:
    """The hub's list row of the person's ``kind`` connection ``name``, no secret; ``None`` when the hub lists none."""
    if not name:
        return None
    return next((row for row in hub_rows(user_id, kind) if row.get("connection_name") == name), None)


def create(user_id: int, kind: Kind, connection: BaseModel) -> dict:
    """Add ``connection`` to the person's own connections of ``kind`` on the hub; the hub's 422 for a name taken."""
    hub_url, token = _signed_in_for_connections(user_id)
    return client._request("POST", hub_url, WRITE_PATHS[kind], None, json=plain(connection), headers=_bearer(token))


def change(user_id: int, kind: Kind, connection: BaseModel, connection_id: int | None = None) -> dict:
    """Change the person's connection of ``kind`` on the hub: by name for cloud and database, by the hub's id for
    Kafka; the hub's 404 when they reach none."""
    hub_url, token = _signed_in_for_connections(user_id)
    path = WRITE_PATHS[kind] if connection_id is None else f"{KAFKA_PATH}/{connection_id}"
    return client._request("PUT", hub_url, path, None, json=plain(connection), headers=_bearer(token))


def remove(user_id: int, kind: Kind, name: str | None = None, connection_id: int | None = None) -> dict:
    """Delete the person's connection of ``kind`` on the hub: by name for cloud and database, by the hub's id for
    Kafka; the hub's 404 when they reach none."""
    hub_url, token = _signed_in_for_connections(user_id)
    if connection_id is not None:
        path, params = f"{KAFKA_PATH}/{quote(str(connection_id), safe='')}", None
    else:
        path, params = WRITE_PATHS[kind], {"connection_name": name}
    return client._request("DELETE", hub_url, path, None, params=params, headers=_bearer(token))


def opened_here(user_id: int, kind: Kind, name: str) -> dict[str, Any] | None:
    """The whole settings of the person's ``kind`` connection ``name``, secrets in plain, as the hub resolves the name
    for them and seals it to this server, opened with this server's key. ``None`` when the hub says they reach none.

    Raises:
        LookupError: A sentence: the sign-in through the hub ended, the hub refused for another reason, or it did not
            answer.
    """
    from flowfile_core.hub_client import vend

    here = hub_node.resolved().name
    hub_url, token = _signed_in_for_connections(user_id)
    try:
        answer = client.vend(hub_url, token, lookup_run_id(), [], [], targets=[here], connections=[(kind, name)])
    except HubRefused as e:
        if e.status == 403 and e.detail.startswith(NO_SUCH.format(label=LABELS[kind], name=name)):
            return None
        if e.status == 401:
            raise LookupError(NOT_SIGNED_IN) from e
        raise LookupError(vend.refusal_sentence(e.detail, e.status)) from e
    except HubVersionRefused as e:
        raise LookupError(vend.refusal_sentence(e.detail, 426)) from e
    except HubUnreachable as e:
        raise LookupError(str(e)) from e
    sealed = next(c["sealed"] for c in answer["connections"] if (c.get("kind"), c.get("name")) == (kind, name))
    return hub_sealed.open_connection(sealed, kind, name)


def database_here(user_id: int, name: str) -> FullDatabaseConnection | None:
    """The person's database connection ``name`` from the hub, its password encrypted for ``user_id`` here as a
    connection stored here carries it."""
    from flowfile_core.secret_manager.secret_manager import encrypt_secret

    settings = opened_here(user_id, "database", name)
    if settings is None:
        return None
    connection = FullDatabaseConnection.model_validate(settings)
    return connection.model_copy(
        update={"password": SecretStr(encrypt_secret(connection.password.get_secret_value(), user_id))}
    )


def kafka_here(user_id: int, name: str) -> KafkaConnectionCreate | None:
    """The person's Kafka connection ``name`` from the hub, its secrets in plain."""
    settings = opened_here(user_id, "kafka", name)
    return None if settings is None else KafkaConnectionCreate.model_validate(settings)
