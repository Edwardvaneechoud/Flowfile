"""The secrets a step names, and the form of each that the run was granted for the step running now.

A server with a master key of its own opens no other server's ``$ffsec$`` ciphertext, and a credential provider
(``secret_manager.credential_sources``) keeps values no machine holds. A run asks for those when it starts, and while
the step runs ``use_sealed`` holds what was granted; outside such a step every answer is what the step carried before.
This machine's own connection or secret of a name wins over the provider's.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import TYPE_CHECKING

from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager import credential_sources

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

_sealed: ContextVar[Mapping[str, str]] = ContextVar("sealed_secrets", default=MappingProxyType({}))
_connections: ContextVar[Mapping[tuple[str, str], Mapping[str, str]]] = ContextVar(
    "sealed_connections", default=MappingProxyType({})
)
_fields: ContextVar[Mapping[tuple[str, str], tuple[str, ...]]] = ContextVar(
    "sealed_connection_fields", default=MappingProxyType({})
)


@contextmanager
def use_sealed(
    values: Mapping[str, str],
    connections: Mapping[tuple[str, str], Mapping[str, str]] | None = None,
    fields: Mapping[tuple[str, str], tuple[str, ...]] | None = None,
) -> Iterator[None]:
    """Hold the step's granted secrets, connection maps and each connection's secret field names inside the block."""
    token = _sealed.set(MappingProxyType(dict(values)))
    held = _connections.set(MappingProxyType(dict(connections or {})))
    named = _fields.set(MappingProxyType(dict(fields or {})))
    try:
        yield
    finally:
        _fields.reset(named)
        _connections.reset(held)
        _sealed.reset(token)


def sealed_connection(kind: str, name: str | None) -> Mapping[str, str] | None:
    """The sealed map of the ``kind`` connection ``name`` for the step running now; ``None`` when none was granted."""
    return _connections.get().get((kind, name)) if name else None


NO_STORE = "the hub has no exchange store set to bring its rows back"
NO_SERVER = "you may run on none of the hub's servers"


def refusal(name: str | None, *, secret: bool = False, why: str | None = None) -> str:
    """The provider's sentence for a step using its connection or secret ``name`` where the grant does not reach."""
    return credential_sources.source().refusal(name, secret=secret, why=why)


def granted_connection(kind: str, name: str | None) -> Mapping[str, str]:
    """The granted map of a provider's ``kind`` connection ``name`` for the step running now.

    Raises:
        ValueError: ``refusal``'s sentence when none was granted.
    """
    sealed_map = sealed_connection(kind, name)
    if sealed_map is None:
        raise ValueError(refusal(name))
    return sealed_map


def granted_value(name: str | None, *, kind: str | None = None, field: str | None = None) -> str:
    """The granted form of a provider's secret ``name``, or with ``kind`` of that connection's secret ``field``.

    Raises:
        ValueError: ``refusal``'s sentence when none was granted.
    """
    if kind is not None:
        return credential_sources.source().carried(kind, name, field, granted_connection(kind, name))
    value = sealed(name)
    if value is None:
        raise ValueError(refusal(name, secret=True))
    return value


def granted_fields(kind: str, name: str | None) -> dict[str, str]:
    """Each secret field of a provider's ``kind`` connection ``name`` in its granted form (``granted_value``).

    Raises:
        ValueError: ``refusal``'s sentence when none was granted.
    """
    granted_connection(kind, name)
    return {field: granted_value(name, kind=kind, field=field) for field in _fields.get().get((kind, name), ())}


def provider_row(kind: str, name: str | None, user_id: int) -> dict | None:
    """A provider's row for the ``kind`` connection ``name``; ``None`` when this machine has its own or none."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.flowfile.database_connection_manager import db_connections
    from flowfile_core.kafka.connection_manager import get_kafka_connection_by_name

    if not name:
        return None
    lookups = {
        "cloud": db_connections.get_cloud_connection,
        "database": db_connections.get_database_connection,
        "kafka": get_kafka_connection_by_name,
    }
    with get_db_context() as db:
        if lookups[kind](db, name, user_id) is not None:
            return None
    return credential_sources.connection(user_id, kind, name)


def is_provider_secret(user_id: int, name: str | None) -> bool:
    """Whether the secret ``name`` a step of ``user_id`` uses is a provider's: none of this machine's, and listed."""
    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

    if not name or get_encrypted_secret(current_user_id=user_id, secret_name=name) is not None:
        return False
    return credential_sources.keeps_secret(user_id, name)


def sealed(name: str | None) -> str | None:
    """The granted form of the secret ``name`` for the step running now; ``None`` when the run was granted none."""
    return _sealed.get().get(name) if name else None


def encrypted_secret(user_id: int, name: str) -> str | None:
    """The user's ``$ffsec$`` ciphertext of ``name``, else a provider's granted form; ``None`` when neither has it."""
    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

    own = get_encrypted_secret(current_user_id=user_id, secret_name=name)
    if own is None and is_provider_secret(user_id, name):
        return granted_value(name)
    return own


def database_secret_name(settings: input_schema.DatabaseSettings | input_schema.DatabaseWriteSettings, user_id: int):
    """The name of the secret a database reader's or writer's connection signs in with; ``None`` when it has none."""
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
    """The secret names a Kafka source's stored connection here holds, by ``KafkaReadSettings`` field."""
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


def names_for(node: FlowNode) -> list[str]:
    """The secret names ``node``'s settings use, each once."""
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


def provider_secret_names(node: FlowNode) -> list[str]:
    """The secret names ``node``'s settings use (``names_for``) that are a provider's (``is_provider_secret``)."""
    user_id = getattr(node.setting_input, "user_id", None)
    return [name for name in names_for(node) if is_provider_secret(user_id, name)] if user_id is not None else []


def connections_for(node: FlowNode) -> list[tuple[str, str]]:
    """The ``(kind, name)`` of the provider's connection ``node``'s settings use, when none of this machine's has it."""
    settings = node.setting_input
    user_id = getattr(settings, "user_id", None)
    if user_id is None:
        return []
    ref: tuple[str, str | None] | None = None
    if isinstance(settings, input_schema.NodeDatabaseReader | input_schema.NodeDatabaseWriter):
        database = (
            settings.database_settings
            if isinstance(settings, input_schema.NodeDatabaseReader)
            else settings.database_write_settings
        )
        ref = ("database", database.database_connection_name if database.connection_mode == "reference" else None)
    elif isinstance(settings, input_schema.NodeCloudStorageReader | input_schema.NodeCloudStorageWriter):
        ref = ("cloud", settings.cloud_storage_settings.connection_name)
    elif isinstance(settings, input_schema.NodeKafkaSource):
        ref = ("kafka", settings.kafka_settings.kafka_connection_name)
    if ref is None or provider_row(ref[0], ref[1], user_id) is None:
        return []
    return [ref]
