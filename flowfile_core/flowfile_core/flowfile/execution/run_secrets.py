"""The secrets a step names, and the sealed form of each that a hub vended for the step running now.

A server joined to its hub with keys of its own has a master key of its own, and so has every other server of the hub:
a ``$ffsec$`` ciphertext of this server's opens nowhere else. A run that places a step naming secrets on another server
of the hub asks the hub for those names in its vend (``names_for`` gives them), and while that step runs
``use_sealed`` holds each name's sealed form (``shared.hub_sealed.sealed_secret``). ``encrypted_secret`` and
``in_place_of`` then hand that on where the step carried the ciphertext, and the server that runs the step opens it
with its own key. Outside such a step, which is every step of a server that never joined a hub, both answer exactly
what the step carried before.

On a server joined with keys every stored cloud storage, Kafka and database connection lives on the hub too. Such a
step names the connection (``connections_for``), the run's vend asks for it, and ``use_sealed`` holds its sealed map:
``connection_secret`` gives one secret field of it in a ciphertext's place, ``sealed_connection`` the map a cloud
read's provider carries. A connection this server's database held before it joined names no secret here.
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
_connections: ContextVar[Mapping[tuple[str, str], Mapping[str, str]]] = ContextVar(
    "sealed_connections", default=MappingProxyType({})
)


@contextmanager
def use_sealed(
    values: Mapping[str, str], connections: Mapping[tuple[str, str], Mapping[str, str]] | None = None
) -> Iterator[None]:
    """Hold ``values``, secret name to its sealed form, and ``connections``, each ``(kind, name)`` of a stored
    connection to its sealed map, for the step that runs inside the block."""
    token = _sealed.set(MappingProxyType(dict(values)))
    held = _connections.set(MappingProxyType(dict(connections or {})))
    try:
        yield
    finally:
        _connections.reset(held)
        _sealed.reset(token)


def active() -> bool:
    """Whether the step running now carries any secret or connection the hub sealed for it."""
    return bool(_sealed.get()) or bool(_connections.get())


def sealed_connection(kind: str, name: str | None) -> Mapping[str, str] | None:
    """The sealed map of the ``kind`` connection ``name`` for the step running now; ``None`` when the hub sealed it
    none."""
    return _connections.get().get((kind, name)) if name else None


def connection_secret(kind: str, name: str | None, field: str) -> str | None:
    """The secret ``field`` of the ``kind`` connection ``name`` as the running step carries it in a ciphertext's place;
    ``None`` when the hub sealed the step no such connection."""
    from shared import hub_sealed

    sealed = sealed_connection(kind, name)
    return hub_sealed.sealed_connection_secret(kind, name, field, sealed) if sealed is not None else None


def _joined_with_keys() -> bool:
    from flowfile_core.auth import issuer_accounts

    return issuer_accounts.joined_with_keys() is not None


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
    is the one asked for. A file-based database, or a stored connection this user cannot reach, names none, and so
    does a stored one on a server joined with keys, where the hub keeps it (``connections_for``).
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
    if _joined_with_keys():
        return None
    with get_db_context() as db:
        row = get_database_connection(db, settings.database_connection_name, user_id)
        secret = db.get(Secret, row.password_id) if row is not None and row.password_id else None
        return secret.name if secret is not None else None


def kafka_secret_names(settings: input_schema.KafkaSourceSettings, user_id: int) -> dict[str, str]:
    """The names of the secrets a Kafka source's stored connection holds, by the ``KafkaReadSettings`` field of each;
    none on a server joined with keys, where the hub keeps the connection (``connections_for``)."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Secret
    from flowfile_core.kafka.connection_manager import get_kafka_connection, get_kafka_connection_by_name

    if _joined_with_keys():
        return {}
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


def names_for(node: FlowNode) -> list[str]:
    """The secret names ``node``'s settings use, each once.

    A database reader's or writer's password, a Kafka source's stored secrets, a REST source's auth secret and a custom
    node's selected secrets; any other node names none.
    """
    settings = node.setting_input
    user_id = getattr(settings, "user_id", None)
    if user_id is None:
        return []
    names: list[str] = []
    if isinstance(settings, input_schema.NodeDatabaseReader | input_schema.NodeDatabaseWriter):
        database = (
            settings.database_settings
            if isinstance(settings, input_schema.NodeDatabaseReader)
            else settings.database_write_settings
        )
        name = database_secret_name(database, user_id)
        if name is not None:
            names.append(name)
    elif isinstance(settings, input_schema.NodeKafkaSource):
        names.extend(kafka_secret_names(settings.kafka_settings, user_id).values())
    elif isinstance(settings, input_schema.NodeRestApiReader):
        auth = settings.rest_api_settings.auth
        if auth.auth_type != "none" and auth.secret_name:
            names.append(auth.secret_name)
    elif isinstance(settings, input_schema.UserDefinedNode):
        names.extend(_custom_node_secret_names(node))
    return list(dict.fromkeys(names))


def connections_for(node: FlowNode) -> list[tuple[str, str]]:
    """The ``(kind, name)`` of each stored connection ``node``'s settings use, on a server joined with keys, where the
    hub keeps them: a database reader's or writer's stored connection, a cloud storage reader's or writer's and a Kafka
    source's. Elsewhere, and for any other node, none.
    """
    settings = node.setting_input
    if getattr(settings, "user_id", None) is None or not _joined_with_keys():
        return []
    if isinstance(settings, input_schema.NodeDatabaseReader | input_schema.NodeDatabaseWriter):
        database = (
            settings.database_settings
            if isinstance(settings, input_schema.NodeDatabaseReader)
            else settings.database_write_settings
        )
        name = database.database_connection_name if database.connection_mode == "reference" else None
        return [("database", name)] if name else []
    if isinstance(settings, input_schema.NodeCloudStorageReader | input_schema.NodeCloudStorageWriter):
        name = settings.cloud_storage_settings.connection_name
        return [("cloud", name)] if name else []
    if isinstance(settings, input_schema.NodeKafkaSource):
        name = settings.kafka_settings.kafka_connection_name
        return [("kafka", name)] if name else []
    return []
