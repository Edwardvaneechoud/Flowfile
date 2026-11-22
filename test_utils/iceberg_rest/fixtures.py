"""An Iceberg REST catalog (Lakekeeper) on its own MinIO, for tests that use the storage keys the catalog hands out.

The catalog shares MinIO's network namespace, so ``http://127.0.0.1:<S3_PORT>`` is the same store from inside the
catalog's container and from the host. That matters because the catalog hands clients the storage address it uses
itself. Every container, network and volume is named ``iceberg-proof-*``; ``stop()`` removes only those.

The root keys below are throwaway values for a store that only listens on 127.0.0.1; they live here and nowhere else.
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager
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
PROJECT = "iceberg-proof"
COMPOSE_FILE = Path(__file__).with_name("docker-compose.yml")
REPO_ROOT = Path(__file__).resolve().parents[2]

MINIO_ROOT_USER = "icebergproofroot"
MINIO_ROOT_PASSWORD = "icebergproof-root-secret-only-local"
PG_PASSWORD = "icebergproof-pg-only-local"
ENCRYPTION_KEY = "icebergproof-encryption-key-only-local"
DEFAULT_PROJECT_ID = "00000000-0000-0000-0000-000000000000"


SKIP_REASON = (
    f"Iceberg REST catalog is not running at {CATALOG_URI}; "
    "start it with: python -m test_utils.iceberg_rest start "
    "(if it is paused after an interrupted run: stop, then start)"
)


def catalog_properties() -> dict[str, str]:
    """What a client needs to reach the catalog: its address and warehouse, nothing about storage."""
    return {"type": "rest", "uri": CATALOG_URI, "warehouse": WAREHOUSE, "token": "dummy"}


def secret_values() -> list[str]:
    """The throwaway secrets, for masking anything shown from a child process."""
    return [MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, PG_PASSWORD, ENCRYPTION_KEY]


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
    return True


def stop() -> None:
    """Remove this stack's containers, network and volumes, then list what is left with its name (expected: nothing)."""
    for container in (CATALOG_CONTAINER, MINIO_CONTAINER):
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


def run_child(module: str, payload: dict, env: dict[str, str], secrets=()) -> dict:
    """Run ``module`` in a fresh Python with exactly ``env`` and ``payload`` on stdin; returns its last JSON line.

    A failure raises ``AssertionError`` with the child's masked stderr; the error texts it reports are masked too.
    """
    secrets = [*secrets, *key_values(payload.get("access", {}).get("storage_properties", {}))]
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
