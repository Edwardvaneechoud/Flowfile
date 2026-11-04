"""The credential a run over the hub's tables gets from the hub: asked once per run, held in memory, never kept.

A flow that reads or writes tables on the hub holds no credential for them. When it runs, ``request`` asks the hub for
one per storage root the run's namespaces live under (``POST /hub/vend``, which the hub records against the run id),
and the graph holds the answer for that run only. A read's plan carries the options without their secrets and an
``EncryptedCredentialProvider`` encrypted for the user; a write's payload carries a cloud connection whose secrets are
encrypted for the worker. Both travel as a local cloud connection's credentials do, and the worker decrypts them with
the master key it shares with core. Nothing here is cached, stored or logged beyond the hub's URL and the status.
"""

from dataclasses import dataclass

from pydantic import SecretStr

from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.hub_client import catalog, client
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from flowfile_core.schemas.cloud_storage_schemas import (
    FullCloudStorageConnection,
    FullCloudStorageConnectionWorkerInterface,
)
from shared.cloud_storage.storage_options import ALLOW_INVALID_CERTIFICATES

REFUSED = "The hub did not give this run access to its tables"
_S3_FIELDS = ("aws_access_key_id", "aws_region", "endpoint_url")
_S3_SECRETS = ("aws_secret_access_key", "aws_session_token")


@dataclass(frozen=True)
class HubCredential:
    """One credential the hub vended to a run, for the tables under ``base_uri``.

    ``storage_options`` are what Polars and deltalake take, secrets included; ``repr`` never shows them.
    """

    base_uri: str
    storage_options: dict[str, str]
    expires_at: str
    mode: str
    read_prefixes: tuple[str, ...]
    write_prefixes: tuple[str, ...]

    def __repr__(self) -> str:
        return f"HubCredential(base_uri={self.base_uri!r}, mode={self.mode!r})"

    def covers(self, uri: str) -> bool:
        base = self.base_uri.rstrip("/")
        return uri.rstrip("/") == base or uri.startswith(f"{base}/")


@dataclass(frozen=True)
class RunVend:
    """What the hub vended to the run ``run_id``: its ``vend_id``, the credentials, and where each namespace lives.

    ``bases`` is the storage root of each hub namespace id the run names, ``names`` its ``catalog.schema`` name.
    """

    run_id: str
    vend_id: int
    credentials: tuple[HubCredential, ...]
    bases: dict[int, str]
    names: dict[int, str]

    def for_namespace(self, hub_namespace_id: int) -> HubCredential:
        """The credential for the tables of the hub namespace ``hub_namespace_id``.

        Raises:
            LookupError: The run did not ask for that namespace, or the hub gave no credential for its root.
        """
        if hub_namespace_id not in self.bases:
            raise LookupError(f"This run did not ask the hub for namespace {hub_namespace_id}")
        return self.for_uri(self.bases[hub_namespace_id])

    def for_uri(self, uri: str) -> HubCredential:
        """The credential whose ``base_uri`` holds ``uri``.

        Raises:
            LookupError: No credential of this run covers ``uri``.
        """
        for credential in self.credentials:
            if credential.covers(uri):
                return credential
        raise LookupError(f"The hub gave this run no credential for {uri}")

    def scan_kwargs(self, uri: str, user_id: int | None) -> dict:
        """``storage_options`` and ``credential_provider`` for a scan of ``uri``: the secrets encrypted for ``user_id``.

        The options the plan carries hold no secret, so a serialized plan does not either.
        """
        return CloudStorageReader.get_secure_scan_kwargs(self.for_uri(uri).storage_options, user_id)

    def worker_connection(self, uri: str, user_id: int) -> FullCloudStorageConnectionWorkerInterface:
        """The credential for ``uri`` as an S3 access-key connection, its secrets encrypted for the worker.

        Raises:
            ValueError: The credential is for storage other than S3.
        """
        credential = self.for_uri(uri)
        scheme = catalog.scheme(credential.base_uri)
        if scheme != "s3":
            raise ValueError(catalog.other_scheme(scheme))
        options = credential.storage_options
        fields: dict = {name: options[name] for name in _S3_FIELDS if options.get(name)}
        fields.update({name: SecretStr(options[name]) for name in _S3_SECRETS if options.get(name)})
        if "aws_allow_http" in options:
            fields["aws_allow_unsafe_html"] = str(options["aws_allow_http"]).lower() == "true"
        if ALLOW_INVALID_CERTIFICATES in options:
            fields["verify_ssl"] = str(options[ALLOW_INVALID_CERTIFICATES]).lower() != "true"
        connection = FullCloudStorageConnection(
            storage_type="s3", auth_method="access_key", connection_name=None, **fields
        )
        return connection.get_worker_interface(user_id)


def refusal_sentence(detail: str, status: int) -> str:
    """The run's error for a vend the hub refused with ``detail`` (its HTTP ``status``).

    A hub that says what to do answers a sentence of its own, capitalised as one ("You can read main.default but not
    write to it; ask an admin or your team's manager for write access"), and the run's error is that sentence alone. A
    bare reason ("no manage grant on namespace 6"), or no text at all, reads after ``REFUSED``.
    """
    if detail[:1].isupper() and detail != client.answered(status):
        return detail
    return f"{REFUSED}: {detail}"


def request(user_id: int, run_id: str, readers: set[int], writers: set[int]) -> RunVend:
    """Ask the hub, through ``user_id``'s hub session, for the credentials of a run over its tables.

    ``readers`` and ``writers`` are the hub namespace ids the run reads and writes. Every namespace is found in one
    walk of the hub's catalogs; one on the hub's own disk, or on storage other than S3, is refused before the vend
    with the sentence the catalog tree marks it with (``catalog.unreachable``), which says what to do.

    Raises:
        LookupError: A sentence for the user: not connected to the hub, a namespace the hub keeps on its own disk or
            on other storage, the hub's refusal with its reason, or a hub that does not answer.
    """
    try:
        hub_url, access_token = catalog.signed_in(user_id)
        found = catalog.namespaces(hub_url, access_token)
        names: dict[int, str] = {}
        bases: dict[int, str] = {}
        for namespace_id in sorted(readers | writers):
            namespace = found.get(namespace_id)
            if namespace is None:
                raise LookupError(f"{REFUSED}: the hub lists no catalog namespace {namespace_id} for this account")
            refusal = catalog.unreachable(namespace.storage_uri, namespace.full_name)
            if refusal is not None:
                raise LookupError(refusal)
            names[namespace_id] = namespace.full_name
            bases[namespace_id] = namespace.storage_uri.rstrip("/")
        answer = client.vend(hub_url, access_token, run_id, sorted(readers), sorted(writers))
    except HubRefused as e:
        if e.status == 401:
            raise LookupError(catalog.NOT_CONNECTED) from e
        raise LookupError(refusal_sentence(e.detail, e.status)) from e
    except HubVersionRefused as e:
        raise LookupError(refusal_sentence(e.detail, 426)) from e
    except HubUnreachable as e:
        raise LookupError(str(e)) from e
    credentials = tuple(
        HubCredential(
            base_uri=credential["base_uri"].rstrip("/"),
            storage_options=dict(credential["storage_options"]),
            expires_at=str(credential.get("expires_at")),
            mode=str(credential.get("mode")),
            read_prefixes=tuple(credential.get("read_prefixes") or ()),
            write_prefixes=tuple(credential.get("write_prefixes") or ()),
        )
        for credential in answer["credentials"]
    )
    return RunVend(run_id=run_id, vend_id=answer["vend_id"], credentials=credentials, bases=bases, names=names)
