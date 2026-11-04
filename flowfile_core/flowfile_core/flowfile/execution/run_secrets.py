"""The secrets a step names, and the sealed form of each that a hub vended for the step running now.

A server joined to its hub with keys of its own has a master key of its own, and so has every other server of the hub:
a ``$ffsec$`` ciphertext of this server's opens nowhere else. A run that places a step naming secrets on another server
of the hub asks the hub for those names in its vend (``names_for`` gives them), and while that step runs
``use_sealed`` holds each name's sealed form (``shared.hub_sealed.sealed_secret``). ``encrypted_secret`` and
``in_place_of`` then hand that on where the step carried the ciphertext, and the server that runs the step opens it
with its own key. Outside such a step, which is every step of a server that never joined a hub, both answer exactly
what the step carried before.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import TYPE_CHECKING

from flowfile_core.schemas import input_schema

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

_sealed: ContextVar[Mapping[str, str]] = ContextVar("sealed_secrets", default=MappingProxyType({}))


@contextmanager
def use_sealed(values: Mapping[str, str]) -> Iterator[None]:
    """Hold ``values``, secret name to its sealed form, for the step that runs inside the block."""
    token = _sealed.set(MappingProxyType(dict(values)))
    try:
        yield
    finally:
        _sealed.reset(token)


def active() -> bool:
    """Whether the step running now carries any secret the hub sealed for it."""
    return bool(_sealed.get())


def sealed(name: str | None) -> str | None:
    """The sealed form of the secret ``name`` for the step running now; ``None`` when the hub sealed it none."""
    return _sealed.get().get(name) if name else None


def in_place_of(name: str | None, encrypted: str | None) -> str | None:
    """What the running step carries for the secret ``name`` it holds as ``encrypted``: its sealed form, if any."""
    return sealed(name) or encrypted


def encrypted_secret(user_id: int, name: str) -> str | None:
    """The secret ``name`` as the running step carries it: its sealed form, else the user's ``$ffsec$`` ciphertext."""
    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

    return sealed(name) or get_encrypted_secret(current_user_id=user_id, secret_name=name)


def database_secret_name(settings: input_schema.DatabaseSettings | input_schema.DatabaseWriteSettings, user_id: int):
    """The name of the secret a database reader's or writer's connection signs in with; ``None`` when it has none.

    An inline connection names its ``password_ref``; a stored one keeps its password as a secret of its own, whose name
    is the one asked for. A file-based database, or a stored connection this user cannot reach, names none.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Secret
    from flowfile_core.flowfile.database_connection_manager.db_connections import get_database_connection
    from shared.db_dialects import get_dialect_or_generic

    connection = settings.database_connection
    if settings.connection_mode == "inline":
        if connection is None or get_dialect_or_generic(connection.database_type).file_based:
            return None
        return connection.password_ref or None
    with get_db_context() as db:
        row = get_database_connection(db, settings.database_connection_name, user_id)
        secret = db.get(Secret, row.password_id) if row is not None and row.password_id else None
        return secret.name if secret is not None else None


def kafka_secret_names(settings: input_schema.KafkaSourceSettings, user_id: int) -> dict[str, str]:
    """The names of the secrets a Kafka source's stored connection holds, by the ``KafkaReadSettings`` field of each."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Secret
    from flowfile_core.kafka.connection_manager import get_kafka_connection, get_kafka_connection_by_name

    with get_db_context() as db:
        row = get_kafka_connection(db, settings.kafka_connection_id, user_id)
        if row is None and settings.kafka_connection_name:
            row = get_kafka_connection_by_name(db, settings.kafka_connection_name, user_id)
        if row is None:
            return {}
        names = {}
        for field, secret_id in (("sasl_password", row.sasl_password_id), ("ssl_key_pem", row.ssl_key_id)):
            secret = db.get(Secret, secret_id) if secret_id else None
            if secret is not None:
                names[field] = secret.name
        return names


def _custom_node_secret_names(node: FlowNode) -> list[str]:
    from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
    from shared.node_designer import SecretSelector

    node_class = CUSTOM_NODE_STORE.get(node.node_type)
    if node_class is None:
        return []
    schema = node_class.from_settings(node.setting_input.settings).settings_schema
    if schema is None:
        return []
    return [c.value for c in schema.get_all_components().values() if isinstance(c, SecretSelector) and c.value]


def names_for(node: FlowNode) -> dict[str, bool]:
    """The secret names ``node``'s settings use, each with whether it exists on this server for the node's user.

    A database reader's or writer's password, a Kafka source's stored secrets, a REST source's auth secret and a custom
    node's selected secrets; any other node names none. A stored connection's secrets exist here by definition.
    """
    from flowfile_core.secret_manager.secret_manager import stored_encrypted_secret

    settings = node.setting_input
    user_id = getattr(settings, "user_id", None)
    if user_id is None:
        return {}
    stored: list[str] = []
    named: list[str] = []
    if isinstance(settings, input_schema.NodeDatabaseReader | input_schema.NodeDatabaseWriter):
        database = (
            settings.database_settings
            if isinstance(settings, input_schema.NodeDatabaseReader)
            else settings.database_write_settings
        )
        name = database_secret_name(database, user_id)
        if name is not None:
            (named if database.connection_mode == "inline" else stored).append(name)
    elif isinstance(settings, input_schema.NodeKafkaSource):
        stored.extend(kafka_secret_names(settings.kafka_settings, user_id).values())
    elif isinstance(settings, input_schema.NodeRestApiReader):
        auth = settings.rest_api_settings.auth
        if auth.auth_type != "none" and auth.secret_name:
            named.append(auth.secret_name)
    elif isinstance(settings, input_schema.UserDefinedNode):
        named.extend(_custom_node_secret_names(node))
    names = dict.fromkeys(stored, True)
    for name in named:
        names.setdefault(name, stored_encrypted_secret(current_user_id=user_id, secret_name=name) is not None)
    return names
