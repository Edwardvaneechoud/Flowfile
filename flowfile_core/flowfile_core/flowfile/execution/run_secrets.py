"""The secrets a step names, and the form of each that the run was granted for the step running now.

A server with a master key of its own opens no other server's ``$ffsec$`` ciphertext, and a provider of credentials
elsewhere (``secret_manager.credential_sources``) keeps connections and secrets that have no value on any machine. A
run that places a step naming either where this machine's ciphertext does not open asks for them when it starts
(``names_for``, ``provider_secret_names``, ``connections_for``), and while that step runs ``use_sealed`` holds what it
was granted: each secret's form for the server that runs it, each connection's map and the names of its secret fields.
``encrypted_secret`` and ``in_place_of`` hand that on where the step carried a ciphertext, and the server that runs the
step opens it. Outside such a step both answer exactly what the step carried before.

Core gets every value of a provider through ``granted_value`` (a connection's map through ``granted_connection``): the
granted form while the step runs, else the step fails with the provider's sentence (``refusal``). This machine's own
connection or secret of a name wins over the provider's.
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
    """Hold ``values``, secret name to its sealed form, ``connections``, each ``(kind, name)`` of a stored connection
    to its sealed map, and ``fields``, the names of the secret fields its provider says each holds, for the step that
    runs inside the block."""
    token = _sealed.set(MappingProxyType(dict(values)))
    held = _connections.set(MappingProxyType(dict(connections or {})))
    named = _fields.set(MappingProxyType(dict(fields or {})))
    try:
        yield
    finally:
        _fields.reset(named)
        _connections.reset(held)
        _sealed.reset(token)


def active() -> bool:
    """Whether the step running now carries any secret or connection the run was granted for it."""
    return bool(_sealed.get()) or bool(_connections.get())


def sealed_connection(kind: str, name: str | None) -> Mapping[str, str] | None:
    """The sealed map of the ``kind`` connection ``name`` for the step running now; ``None`` when the run was granted
    none."""
    return _connections.get().get((kind, name)) if name else None


def connection_secret(kind: str, name: str | None, field: str) -> str | None:
    """The secret ``field`` of the ``kind`` connection ``name`` as the running step carries it in a ciphertext's place;
    ``None`` when the run was granted no such connection for the step."""
    sealed = sealed_connection(kind, name)
    return credential_sources.source().carried(kind, name, field, sealed) if sealed is not None else None


NO_STORE = "the hub has no exchange store set to bring its rows back"
NO_SERVER = "you may run on none of the hub's servers"


def refusal(name: str | None, *, secret: bool = False, why: str | None = None) -> str:
    """Why a step that uses a provider's connection or secret ``name`` cannot run where it was placed, in the
    provider's one sentence; ``why`` says what kept it from moving by itself."""
    return credential_sources.source().refusal(name, secret=secret, why=why)


def granted_connection(kind: str, name: str | None) -> Mapping[str, str]:
    """The map the run was granted a provider's ``kind`` connection ``name`` in for the step running now.

    Raises:
        ValueError: ``refusal``'s sentence: the step runs where the grant does not reach, or outside a run.
    """
    sealed_map = sealed_connection(kind, name)
    if sealed_map is None:
        raise ValueError(refusal(name))
    return sealed_map


def granted_value(name: str | None, *, kind: str | None = None, field: str | None = None) -> str:
    """What the running step carries in a ciphertext's place for a provider's secret ``name``, or, given ``kind``, for
    the secret ``field`` of its ``kind`` connection ``name``: the form granted for the server that runs it.

    Raises:
        ValueError: ``refusal``'s sentence when the run was granted none for the step.
    """
    if kind is not None:
        return credential_sources.source().carried(kind, name, field, granted_connection(kind, name))
    value = sealed(name)
    if value is None:
        raise ValueError(refusal(name, secret=True))
    return value


def granted_fields(kind: str, name: str | None) -> dict[str, str]:
    """Each secret field a provider's ``kind`` connection ``name`` holds, by its name in the connection's settings, as
    the running step carries it in a ciphertext's place (``granted_value``). The grant names the fields, so no kind is
    told apart here.

    Raises:
        ValueError: ``refusal``'s sentence when the run was granted no such connection for the step.
    """
    granted_connection(kind, name)
    return {field: granted_value(name, kind=kind, field=field) for field in _fields.get().get((kind, name), ())}


def provider_row(kind: str, name: str | None, user_id: int) -> dict | None:
    """A provider's list row of the ``kind`` connection ``name`` a step of ``user_id`` uses, no secret; ``None`` when
    this machine has its own or a granted one of that name, or no provider lists one."""
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


def in_place_of(name: str | None, encrypted: str | None) -> str | None:
    """What the running step carries for the secret ``name`` it holds as ``encrypted``: its sealed form, if any."""
    return sealed(name) or encrypted


def encrypted_secret(user_id: int, name: str) -> str | None:
    """The secret ``name`` as the running step carries it: the user's ``$ffsec$`` ciphertext, else a provider's
    secret's granted form (``granted_value``, which refuses where the grant does not reach); ``None`` when neither has
    it."""
    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

    own = get_encrypted_secret(current_user_id=user_id, secret_name=name)
    if own is None and is_provider_secret(user_id, name):
        return granted_value(name)
    return own


def database_secret_name(settings: input_schema.DatabaseSettings | input_schema.DatabaseWriteSettings, user_id: int):
    """The name of the secret a database reader's or writer's connection signs in with; ``None`` when it has none.

    An inline connection names its ``password_ref``; a stored one keeps its password as a secret of its own, whose name
    is the one asked for. A file-based database, or a stored connection this user cannot reach here, names none; a
    provider's connection's password is its own field (``connections_for``).
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
    """The names of the secrets a Kafka source's stored connection here holds, by the ``KafkaReadSettings`` field of
    each; none for a provider's connection, whose secrets are its own fields (``connections_for``)."""
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


def provider_secret_names(node: FlowNode) -> list[str]:
    """The secret names ``node``'s settings use (``names_for``) that are a provider's (``is_provider_secret``)."""
    user_id = getattr(node.setting_input, "user_id", None)
    return [name for name in names_for(node) if is_provider_secret(user_id, name)] if user_id is not None else []


def connections_for(node: FlowNode) -> list[tuple[str, str]]:
    """The ``(kind, name)`` of the provider's connection ``node``'s settings use, when its name is none of this
    machine's (``provider_row``): a database reader's or writer's stored connection, a cloud storage reader's or
    writer's and a Kafka source's. Any other node names none.
    """
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


# The names flow_graph/execution.py still reads these by.
hub_refusal, hub_secret_names = refusal, provider_secret_names
