# flowfile_core/tests/utils.py

import platform
import subprocess
from contextlib import contextmanager

from flowfile_core.auth.models import SecretInput
from flowfile_core.database.connection import get_db_context
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import get_encrypted_secret, store_secret


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


def ensure_db_connection_is_available():
    from flowfile_core.flowfile.database_connection_manager.db_connections import (
        get_database_connection,
        store_database_connection,
    )

    connection = input_schema.FullDatabaseConnection(username='testuser', password='testpass',
                                                     connection_name='test_connection_endpoint', host='localhost',
                                                     port=5433, database='testdb',
                                                     database_type='postgresql', ssl_enabled=False)

    with get_db_context() as db:
        existing_database_connection = get_database_connection(db, connection.connection_name, user_id=1)
        if not existing_database_connection:
            store_database_connection(db, connection, user_id=1)


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
