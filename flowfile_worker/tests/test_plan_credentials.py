"""The worker decrypts the cloud credentials core encrypts into a query plan.

Core ships cloud reads as plans whose credentials ride in an ``EncryptedCredentialProvider``.
``flowfile_worker.utils`` (imported by every child that collects a plan) registers the worker's own
``$ffsec$`` decryption, so the provider resolves here without core.
"""

import json
import subprocess
import sys

import pytest

from flowfile_worker import utils
from flowfile_worker.secrets import encrypt_secret
from shared import cloud_credential_provider as cp
from shared.cloud_credential_provider import EncryptedCredentialProvider


def _provider(credentials: dict, user_id: int = 7) -> EncryptedCredentialProvider:
    return EncryptedCredentialProvider(encrypt_secret(json.dumps(credentials), user_id=user_id))


@pytest.fixture
def worker_decryptor():
    """Install the worker's decryptor for one test.

    The registry is process-global and the last import wins, so a test in this session that imported
    flowfile_core has replaced it with core's, which resolves a different test master key.
    """
    previous = cp._decrypt
    cp.register_secret_decryptor(utils._decrypt_plan_credentials)
    yield
    cp._decrypt = previous


@pytest.mark.worker
def test_a_fresh_worker_process_registers_its_own_decryptor():
    """What a spawned child does: importing flowfile_worker.utils registers the worker's decryptor."""
    code = (
        "import flowfile_worker.utils as u, shared.cloud_credential_provider as cp, sys\n"
        "assert cp._decrypt is u._decrypt_plan_credentials\n"
        "assert 'flowfile_core' not in sys.modules\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr


@pytest.mark.worker
def test_worker_decryptor_round_trips_worker_encrypted_credentials(worker_decryptor):
    assert _provider({"aws_secret_access_key": "s3cr3t"})() == ({"aws_secret_access_key": "s3cr3t"}, None)
