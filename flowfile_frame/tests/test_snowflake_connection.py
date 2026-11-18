"""Snowflake connections created from Python: stored like the app's form stores them, no driver needed."""

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from flowfile_frame.database.connection_manager import (
    create_database_connection,
    create_database_connection_if_not_exists,
    del_database_connection,
    get_all_available_database_connections,
)


def _encrypted_key_pem(passphrase: str) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(passphrase.encode()),
    ).decode()


@pytest.fixture
def cleanup():
    names: list[str] = []
    yield names
    for name in names:
        del_database_connection(name)


def _listed(name: str):
    return next(c for c in get_all_available_database_connections() if c.connection_name == name)


def test_access_token_is_the_default_method(cleanup):
    cleanup.append("frame_sf_token")
    create_database_connection(
        "frame_sf_token",
        database_type="snowflake",
        host="myorg-myaccount",
        username="ETL_USER",
        password="pat-token",
        warehouse="COMPUTE_WH",
        role="ANALYST",
    )
    listed = _listed("frame_sf_token")
    assert listed.host == "myorg-myaccount.snowflakecomputing.com"
    assert listed.port is None
    assert (listed.auth_method, listed.role, listed.warehouse) == ("access_token", "ANALYST", "COMPUTE_WH")


def test_key_pair_with_a_passphrase(cleanup):
    cleanup.append("frame_sf_key")
    create_database_connection_if_not_exists(
        "frame_sf_key",
        database_type="snowflake",
        host="myorg-myaccount.snowflakecomputing.com",
        username="ETL_SERVICE",
        password=_encrypted_key_pem("open-sesame"),
        auth_method="key_pair",
        private_key_passphrase="open-sesame",
    )
    assert _listed("frame_sf_key").auth_method == "key_pair"


def test_wrong_passphrase_is_refused(cleanup):
    cleanup.append("frame_sf_badkey")
    with pytest.raises(ValueError):
        create_database_connection(
            "frame_sf_badkey",
            database_type="snowflake",
            host="myorg-myaccount",
            username="ETL_SERVICE",
            password=_encrypted_key_pem("right"),
            auth_method="key_pair",
            private_key_passphrase="wrong",
        )


def test_browser_sign_in_is_refused_from_python():
    with pytest.raises(ValueError, match="only available in the app"):
        create_database_connection(
            "frame_sf_oauth", database_type="snowflake", host="myorg-myaccount", auth_method="oauth"
        )


def test_snowflake_settings_on_another_type_are_refused():
    with pytest.raises(ValueError, match="only applies to Snowflake"):
        create_database_connection(
            "frame_pg_role", database_type="postgresql", host="h", username="u", password="p", role="ANALYST"
        )
