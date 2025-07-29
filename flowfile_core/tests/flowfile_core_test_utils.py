# flowfile_core/tests/utils.py

import subprocess
import platform
from flowfile_core.secret_manager.secret_manager import store_secret, get_encrypted_secret
from flowfile_core.database.connection import get_db_context
from flowfile_core.auth.models import SecretInput



def is_docker_available():
    """Check if Docker is running."""
    if platform.system() == "Windows":
        return False
    try:
        subprocess.run(["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def ensure_password_is_available():
    if not get_encrypted_secret(1, 'test_database_pw'):
        secret = SecretInput(name='test_database_pw', value='testpass')
        with get_db_context() as db:
            store_secret(db, secret, 1)



from contextlib import contextmanager

@contextmanager
def generator_func():
    try:
        import os
        os.environ["TESTING"] = "True"
        yield None
    finally:
        os.environ["TESTING"] = "False"


def run_generator():
    with generator_func() as value:
        import os
        from time import sleep
        print(os.environ["TESTING"])
        sleep(1)
        # Do something with the value

    print(os.environ["TESTING"])