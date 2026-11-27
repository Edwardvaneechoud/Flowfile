"""Two Iceberg REST catalogs on one MinIO, for tests that use the storage keys a catalog hands out.

Lakekeeper asks for no sign-in. Apache Polaris (the "signed" catalog) asks for one: a client id and secret of one of
its principals, or a bearer token fetched with them, and every token it issues lives ``TOKEN_LIFETIME``.

The catalog shares MinIO's network namespace, so ``http://127.0.0.1:<S3_PORT>`` is the same store from inside the
catalog's container and from the host. That matters because the catalog hands clients the storage address it uses
itself. Every container, network and volume is named ``iceberg-proof-*``; ``stop()`` removes only those.

The root keys below are throwaway values for a store that only listens on 127.0.0.1; they live here and nowhere else.
"""

import json
import logging
import secrets as _secrets
import uuid
import os
import re
import subprocess
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import requests

from test_utils.docker_images import docker_daemon_responds
from test_utils.s3.fixtures import DEFAULT_MINIO_IMAGE

logger = logging.getLogger("iceberg_rest_fixture")

S3_PORT = int(os.environ.get("ICEBERG_PROOF_S3_PORT", 19900))
CATALOG_PORT = int(os.environ.get("ICEBERG_PROOF_CATALOG_PORT", 18181))
S3_ENDPOINT = f"http://127.0.0.1:{S3_PORT}"
CATALOG_URL = f"http://127.0.0.1:{CATALOG_PORT}"
CATALOG_URI = f"{CATALOG_URL}/catalog"
WAREHOUSE = "proof"
BUCKET = "iceberg-proof"
REGION = "us-east-1"
CATALOG_IMAGE = os.environ.get("ICEBERG_PROOF_CATALOG_IMAGE", "quay.io/lakekeeper/catalog:v0.15.1")
MINIO_IMAGE = os.environ.get("ICEBERG_PROOF_MINIO_IMAGE", DEFAULT_MINIO_IMAGE)
CATALOG_CONTAINER = "iceberg-proof-catalog"
MINIO_CONTAINER = "iceberg-proof-minio"
POLARIS_PORT = int(os.environ.get("ICEBERG_PROOF_POLARIS_PORT", 18191))
POLARIS_HEALTH_PORT = int(os.environ.get("ICEBERG_PROOF_POLARIS_HEALTH_PORT", 18192))
POLARIS_IMAGE = os.environ.get("ICEBERG_PROOF_POLARIS_IMAGE", "apache/polaris:1.9.0")
POLARIS_URL = f"http://127.0.0.1:{POLARIS_PORT}"
POLARIS_CONTAINER = "iceberg-proof-polaris"
SIGNED_CATALOG_URI = f"{POLARIS_URL}/api/catalog"
SIGNED_WAREHOUSE = "signed"
SIGNED_TOKEN_ENDPOINT = f"{SIGNED_CATALOG_URI}/v1/oauth/tokens"
SIGNED_SCOPE = "PRINCIPAL_ROLE:ALL"
SIGNED_LOCATION = f"s3://iceberg-proof/{SIGNED_WAREHOUSE}"
TOKEN_LIFETIME = os.environ.get("ICEBERG_PROOF_TOKEN_LIFETIME", "PT12S")
_MANAGEMENT = f"{POLARIS_URL}/api/management/v1"
PROJECT = "iceberg-proof"
COMPOSE_FILE = Path(__file__).with_name("docker-compose.yml")
REPO_ROOT = Path(__file__).resolve().parents[2]

MINIO_ROOT_USER = "icebergproofroot"
MINIO_ROOT_PASSWORD = "icebergproof-root-secret-only-local"
PG_PASSWORD = "icebergproof-pg-only-local"
ENCRYPTION_KEY = "icebergproof-encryption-key-only-local"
POLARIS_ROOT_SECRET = "icebergproof-polaris-root-only-local"
DEFAULT_PROJECT_ID = "00000000-0000-0000-0000-000000000000"


SKIP_REASON = (
    f"Iceberg REST catalog is not running at {CATALOG_URI}; "
    "start it with: python -m test_utils.iceberg_rest start "
    "(if it is paused after an interrupted run: stop, then start)"
)

SIGNED_SKIP_REASON = (
    f"The Iceberg REST catalog that asks for a sign-in (Apache Polaris) is not running at {SIGNED_CATALOG_URI}; "
    "start it with: python -m test_utils.iceberg_rest start "
    "(if it is paused after an interrupted run: stop, then start)"
)


def catalog_properties() -> dict[str, str]:
    """What a client needs to reach the catalog: its address and warehouse, nothing about storage."""
    return {"type": "rest", "uri": CATALOG_URI, "warehouse": WAREHOUSE, "token": "dummy"}


def secret_values() -> list[str]:
    """The throwaway secrets, for masking anything shown from a child process."""
    return [MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, PG_PASSWORD, ENCRYPTION_KEY, POLARIS_ROOT_SECRET]


def _answers(url: str, timeout: float = 2.0) -> bool:
    try:
        return requests.get(url, timeout=timeout).status_code == 200
    except requests.RequestException:
        return False


def catalog_ready() -> bool:
    """True when the catalog answers and knows the warehouse. Never starts anything."""
    return _answers(f"{CATALOG_URL}/health") and _answers(f"{CATALOG_URI}/v1/config?warehouse={WAREHOUSE}")


def _compose_env() -> dict[str, str]:
    return {
        **os.environ,
        "ICEBERG_PROOF_MINIO_IMAGE": MINIO_IMAGE,
        "ICEBERG_PROOF_CATALOG_IMAGE": CATALOG_IMAGE,
        "ICEBERG_PROOF_S3_PORT": str(S3_PORT),
        "ICEBERG_PROOF_CATALOG_PORT": str(CATALOG_PORT),
        "ICEBERG_PROOF_MINIO_USER": MINIO_ROOT_USER,
        "ICEBERG_PROOF_MINIO_PASSWORD": MINIO_ROOT_PASSWORD,
        "ICEBERG_PROOF_PG_PASSWORD": PG_PASSWORD,
        "ICEBERG_PROOF_PG_URL": f"postgresql://postgres:{PG_PASSWORD}@iceberg-proof-postgres:5432/postgres",
        "ICEBERG_PROOF_ENCRYPTION_KEY": ENCRYPTION_KEY,
        "ICEBERG_PROOF_POLARIS_IMAGE": POLARIS_IMAGE,
        "ICEBERG_PROOF_POLARIS_PORT": str(POLARIS_PORT),
        "ICEBERG_PROOF_POLARIS_HEALTH_PORT": str(POLARIS_HEALTH_PORT),
        "ICEBERG_PROOF_POLARIS_ROOT_SECRET": POLARIS_ROOT_SECRET,
        "ICEBERG_PROOF_TOKEN_LIFETIME": TOKEN_LIFETIME,
    }


def _compose(*args: str, timeout: int = 600) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args],
        env=_compose_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _wait_for(url: str, seconds: float = 120.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if _answers(url):
            return True
        time.sleep(1)
    return False


def s3_client(properties: dict[str, str] | None = None):
    """A boto3 client on this stack's MinIO: with its root keys, or with a table's handed-out storage properties."""
    import boto3
    from botocore.client import Config

    keys = {"aws_access_key_id": MINIO_ROOT_USER, "aws_secret_access_key": MINIO_ROOT_PASSWORD}
    if properties is not None:
        keys = {
            "aws_access_key_id": properties["s3.access-key-id"],
            "aws_secret_access_key": properties["s3.secret-access-key"],
            "aws_session_token": properties.get("s3.session-token"),
        }
    return boto3.client(
        "s3",
        endpoint_url=(properties or {}).get("s3.endpoint", S3_ENDPOINT),
        region_name=REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        **keys,
    )


def warehouse_body() -> dict:
    """The warehouse the catalog vends keys for: one bucket on this stack's MinIO, keys from MinIO's own STS."""
    return {
        "warehouse-name": WAREHOUSE,
        "project-id": DEFAULT_PROJECT_ID,
        "storage-profile": {
            "type": "s3",
            "bucket": BUCKET,
            "key-prefix": "warehouse",
            "endpoint": S3_ENDPOINT,
            "sts-endpoint": S3_ENDPOINT,
            "region": REGION,
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": True,
            "remote-signing-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": MINIO_ROOT_USER,
            "secret-access-key": MINIO_ROOT_PASSWORD,
        },
        "delete-profile": {"type": "hard"},
    }


def warehouse_id() -> str | None:
    """The id of this stack's warehouse, or None when it does not exist yet."""
    answer = requests.get(f"{CATALOG_URL}/management/v1/warehouse", timeout=10)
    answer.raise_for_status()
    for warehouse in answer.json().get("warehouses", []):
        if warehouse.get("name") == WAREHOUSE:
            return warehouse.get("id") or warehouse.get("warehouse-id")
    return None


def bootstrap() -> str:
    """Create the bucket, bootstrap the catalog and create the warehouse (safe to run again); returns its id."""
    client = s3_client()
    try:
        client.create_bucket(Bucket=BUCKET)
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    info = requests.get(f"{CATALOG_URL}/management/v1/info", timeout=10)
    info.raise_for_status()
    if not info.json().get("bootstrapped"):
        answer = requests.post(
            f"{CATALOG_URL}/management/v1/bootstrap", json={"accept-terms-of-use": True}, timeout=10
        )
        if answer.status_code >= 400:
            raise RuntimeError(mask(f"bootstrap answered {answer.status_code}: {answer.text[:500]}"))
    existing = warehouse_id()
    if existing is None:
        answer = requests.post(f"{CATALOG_URL}/management/v1/warehouse", json=warehouse_body(), timeout=60)
        if answer.status_code >= 400:
            raise RuntimeError(mask(f"creating the warehouse answered {answer.status_code}: {answer.text[:1000]}"))
        existing = answer.json()["warehouse-id"]
    if not _answers(f"{CATALOG_URI}/v1/config?warehouse={WAREHOUSE}", timeout=10):
        raise RuntimeError("the catalog does not answer /v1/config for the warehouse")
    return existing


def start() -> bool:
    """Start MinIO, Postgres and the catalog, wait for them, then bootstrap the warehouse."""
    if not docker_daemon_responds(logger):
        logger.error("Docker is not available")
        return False
    result = _compose("up", "-d")
    if result.returncode != 0:
        logger.error("docker compose up failed:\n%s", mask(result.stderr[-3000:]))
        return False
    if not _wait_for(f"{S3_ENDPOINT}/minio/health/live"):
        logger.error("MinIO did not become healthy at %s", S3_ENDPOINT)
        return False
    if not _wait_for(f"{CATALOG_URL}/health"):
        logger.error("The catalog did not answer /health; see: docker logs %s", CATALOG_CONTAINER)
        return False
    bootstrap()
    if not _wait_for(f"http://127.0.0.1:{POLARIS_HEALTH_PORT}/q/health"):
        logger.error("Polaris did not answer /q/health; see its log through fixtures.mask (it holds the root secret)")
        return False
    bootstrap_signed()
    return True


def stop() -> None:
    """Remove this stack's containers, network and volumes, then list what is left with its name (expected: nothing)."""
    for container in (CATALOG_CONTAINER, POLARIS_CONTAINER, MINIO_CONTAINER):
        subprocess.run(["docker", "unpause", container], capture_output=True, check=False)
    result = _compose("down", "-v", "--remove-orphans")
    if result.returncode != 0:
        logger.error("docker compose down failed:\n%s", mask(result.stderr[-3000:]))
    listings = ((["ps", "-a"], "{{.Names}}"), (["volume", "ls"], "{{.Name}}"), (["network", "ls"], "{{.Name}}"))
    for kind, name_format in listings:
        command = ["docker", *kind, "--filter", "name=iceberg-proof-", "--format", name_format]
        listing = subprocess.run(command, capture_output=True, text=True, check=False)
        print(f"docker {' '.join(kind)} --filter name=iceberg-proof-: {listing.stdout.strip() or '(nothing)'}")


def catalog_is_paused() -> bool:
    """Whether Docker reports the catalog's container as paused."""
    state = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Paused}}", CATALOG_CONTAINER],
        capture_output=True,
        text=True,
        check=False,
    )
    return state.stdout.strip() == "true"


@contextmanager
def catalog_paused() -> Generator[None, None, None]:
    """Freeze the catalog's process while MinIO keeps serving, so nothing inside the block can reach the catalog."""
    subprocess.run(["docker", "pause", CATALOG_CONTAINER], capture_output=True, check=True)
    try:
        yield
    finally:
        subprocess.run(["docker", "unpause", CATALOG_CONTAINER], capture_output=True, check=False)


@contextmanager
def minio_paused() -> Generator[None, None, None]:
    """Freeze the store's process while the catalog keeps answering, so nothing inside the block can read a file."""
    subprocess.run(["docker", "pause", MINIO_CONTAINER], capture_output=True, check=True)
    try:
        yield
    finally:
        subprocess.run(["docker", "unpause", MINIO_CONTAINER], capture_output=True, check=False)


def attach_lake(connection, extension_dir: str | Path | None = None) -> None:
    """Attach the catalog to a DuckDB connection as ``lake``, again if it is attached, so no snapshot is cached.

    With ``extension_dir`` the httpfs and iceberg extensions are installed there first (a download by DuckDB). DuckDB
    reaches the table files with the keys the catalog hands it, as Polars does.
    """
    if extension_dir is not None:
        connection.execute(f"SET extension_directory = '{extension_dir}'")
        for statement in ("INSTALL httpfs", "INSTALL iceberg", "LOAD httpfs", "LOAD iceberg"):
            connection.execute(statement)
    connection.execute("DETACH DATABASE IF EXISTS lake")
    connection.execute(
        f"ATTACH '{WAREHOUSE}' AS lake (TYPE iceberg, ENDPOINT '{CATALOG_URI}', AUTHORIZATION_TYPE 'none')"
    )


# An STS access key, a secret key or a session token, so a key a child loaded itself is masked too.
_KEY_LIKE = re.compile(r"\b[A-Z0-9]{20}\b|[A-Za-z0-9+/=._-]{40,}")
_SECRET_NAMES = ("s3.access-key-id", "s3.secret-access-key", "s3.session-token")


def mask(text: str, secrets=()) -> str:
    """``text`` with every given secret, the store's root keys and anything shaped like a key replaced by ``***``."""
    for secret in sorted({*secret_values(), *secrets}, key=len, reverse=True):
        if secret:
            text = text.replace(secret, "***")
    return _KEY_LIKE.sub("***", text)


def key_values(properties: dict) -> list[str]:
    """The key values among a table's storage properties: access key, secret key and session token."""
    return [properties[name] for name in _SECRET_NAMES if properties.get(name)]


def clean_env(home: Path) -> dict[str, str]:
    """The whole environment of a child process: no AWS key, no profile, an empty home and a closed AWS endpoint."""
    (home / "storage").mkdir(parents=True, exist_ok=True)
    (home / "credentials").write_text("")
    (home / "config").write_text("")
    return {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(home),
        "FLOWFILE_STORAGE_DIR": str(home / "storage"),
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_SHARED_CREDENTIALS_FILE": str(home / "credentials"),
        "AWS_CONFIG_FILE": str(home / "config"),
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9",
    }


def sign_in_values(plains) -> list[str]:
    """Each sign-in as given and, for a client id and secret, the secret and the ``id:secret`` pair on their own."""
    values: list[str] = []
    for plain in plains:
        values.append(plain)
        try:
            parsed = json.loads(plain)
        except ValueError:
            continue
        if isinstance(parsed, dict) and parsed.get("client_secret"):
            values += [parsed["client_secret"], f"{parsed.get('client_id')}:{parsed['client_secret']}"]
    return values


def run_child(module: str, payload: dict, env: dict[str, str], secrets=()) -> dict:
    """Run ``module`` in a fresh Python with exactly ``env`` and ``payload`` on stdin; returns its last JSON line.

    A failure raises ``AssertionError`` with the child's masked stderr; the error texts it reports are masked too.
    """
    secrets = [
        *secrets,
        *key_values(payload.get("access", {}).get("storage_properties", {})),
        *sign_in_values(payload.get("decrypts", {}).values()),
    ]
    result = subprocess.run(
        [sys.executable, "-m", module],
        cwd=REPO_ROOT,
        input=json.dumps(payload),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise AssertionError(mask(f"{module} exited {result.returncode}:\n{result.stderr[-3000:]}", secrets))
    answer = json.loads(result.stdout.strip().splitlines()[-1])
    if "readers" in answer:
        answer["readers"] = {name: mask(text, secrets) for name, text in answer["readers"].items()}
    for listed in ("warnings", "notes"):
        if listed in answer:
            answer[listed] = [mask(text, secrets) for text in answer[listed]]
    return answer


def first_data_file(table) -> tuple[str, str]:
    """The bucket and key of one of the table's data files."""
    path = table.inspect.data_files().column("file_path")[0].as_py()
    bucket, _, key = path.removeprefix("s3://").partition("/")
    return bucket, key


def refusal_code(properties: dict[str, str], bucket: str, key: str) -> str | None:
    """The error code storage answers when these handed-out keys ask for one object, or None when it is served."""
    from botocore.exceptions import ClientError

    try:
        s3_client(properties).get_object(Bucket=bucket, Key=key, Range="bytes=0-0")
    except ClientError as exc:
        return exc.response["Error"]["Code"]
    return None


def _token_answer(client_id: str, client_secret: str) -> requests.Response:
    return requests.post(
        SIGNED_TOKEN_ENDPOINT,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials", "scope": SIGNED_SCOPE},
        timeout=10,
    )


def _root_headers() -> dict[str, str]:
    answer = _token_answer("root", POLARIS_ROOT_SECRET)
    if answer.status_code != 200:
        raise RuntimeError(f"Polaris refused its root principal ({answer.status_code})")
    return {"Authorization": f"Bearer {answer.json()['access_token']}", "Content-Type": "application/json"}


def _manage(method: str, path: str, headers: dict[str, str], body: dict | None = None, ok=(200, 201, 204)) -> dict:
    answer = requests.request(method, f"{_MANAGEMENT}{path}", headers=headers, json=body, timeout=30)
    if answer.status_code not in ok:
        raise RuntimeError(mask(f"Polaris {method} {path} answered {answer.status_code}: {answer.text[:300]}"))
    return answer.json() if answer.content else {}


def signed_catalog_ready() -> bool:
    """True when Polaris answers and its root principal reaches the warehouse. Never starts anything."""
    if not _answers(f"http://127.0.0.1:{POLARIS_HEALTH_PORT}/q/health"):
        return False
    try:
        config = requests.get(
            f"{SIGNED_CATALOG_URI}/v1/config", params={"warehouse": SIGNED_WAREHOUSE}, headers=_root_headers(), timeout=5
        )
    except (requests.RequestException, RuntimeError):
        return False
    return config.status_code == 200


def bootstrap_signed() -> None:
    """Create the Polaris catalog ``signed`` on this stack's bucket, keys from MinIO's own STS (safe to run again)."""
    headers = _root_headers()
    body = {
        "catalog": {
            "name": SIGNED_WAREHOUSE,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {"default-base-location": SIGNED_LOCATION},
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [SIGNED_LOCATION],
                "endpoint": S3_ENDPOINT,
                "stsEndpoint": S3_ENDPOINT,
                "pathStyleAccess": True,
                "region": REGION,
            },
        }
    }
    _manage("POST", "/catalogs", headers, body, ok=(200, 201, 409))


@dataclass
class Principal:
    """A Polaris principal of the signed catalog; its client id and secret stay out of ``repr``."""

    name: str
    client_id: str = field(repr=False)
    client_secret: str = field(repr=False)

    @property
    def credential(self) -> str:
        return f"{self.client_id}:{self.client_secret}"


def new_principal(
    name: str | None = None, *, privilege: str = "CATALOG_MANAGE_CONTENT", catalog: str = SIGNED_WAREHOUSE
) -> Principal:
    """A new principal holding ``privilege`` on a Polaris catalog, through a principal role and a catalog role."""
    name = name or f"flowfile_{uuid.uuid4().hex[:8]}"
    headers = _root_headers()
    created = _manage("POST", "/principals", headers, {"principal": {"name": name}})
    credentials = created["credentials"]
    _manage("POST", "/principal-roles", headers, {"principalRole": {"name": f"{name}_role"}})
    _manage("PUT", f"/principals/{name}/principal-roles", headers, {"principalRole": {"name": f"{name}_role"}})
    _manage("POST", f"/catalogs/{catalog}/catalog-roles", headers, {"catalogRole": {"name": f"{name}_cat"}})
    _manage(
        "PUT",
        f"/catalogs/{catalog}/catalog-roles/{name}_cat/grants",
        headers,
        {"grant": {"type": "catalog", "privilege": privilege}},
    )
    _manage(
        "PUT",
        f"/principal-roles/{name}_role/catalog-roles/{catalog}",
        headers,
        {"catalogRole": {"name": f"{name}_cat"}},
    )
    return Principal(name, credentials["clientId"], credentials["clientSecret"])


def drop_principal(name: str, catalog: str = SIGNED_WAREHOUSE) -> None:
    """Remove a principal made by ``new_principal`` and its two roles; what is already gone is skipped."""
    headers = _root_headers()
    for path in (f"/principals/{name}", f"/principal-roles/{name}_role", f"/catalogs/{catalog}/catalog-roles/{name}_cat"):
        _manage("DELETE", path, headers, ok=(200, 204, 404))


def fetch_token(principal: Principal) -> tuple[str, int]:
    """A bearer token for ``principal`` and the seconds it lives, from the signed catalog's own token endpoint."""
    answer = _token_answer(principal.client_id, principal.client_secret)
    if answer.status_code != 200:
        raise RuntimeError(f"Polaris refused the principal's client id and secret ({answer.status_code})")
    body = answer.json()
    return body["access_token"], int(body.get("expires_in") or 0)


def token_seconds() -> int:
    """How long a token of the running signed catalog lives, as its token endpoint says."""
    answer = _token_answer("root", POLARIS_ROOT_SECRET)
    answer.raise_for_status()
    return int(answer.json().get("expires_in") or 0)


def signed_catalog(principal: Principal, warehouse: str = SIGNED_WAREHOUSE):
    """A PyIceberg client on a Polaris catalog as ``principal``, for seeding and for checking what Flowfile wrote."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "signed",
        uri=SIGNED_CATALOG_URI,
        warehouse=warehouse,
        credential=principal.credential,
        scope=SIGNED_SCOPE,
        **{"oauth2-server-uri": SIGNED_TOKEN_ENDPOINT},
    )


_SEALED: dict[str, str] = {}


def seal(plain: str) -> str:
    """A ``$ffsec$``-shaped stand-in for a stored secret's ciphertext, opened by ``open_sealed`` in this process only."""
    ciphertext = f"$ffsec$1$0$test-{_secrets.token_hex(8)}"
    _SEALED[ciphertext] = plain
    return ciphertext


def open_sealed(ciphertext: str) -> str:
    """The plain text ``seal`` was given; a decryptor for ``register_secret_decryptor``."""
    return _SEALED[ciphertext]


def sealed_map(*ciphertexts: str) -> dict[str, str]:
    """``{ciphertext: plain}`` for a child process that registers its own decryptor from it."""
    return {ciphertext: _SEALED[ciphertext] for ciphertext in ciphertexts}
