from shared.cloud_credential_provider import register_secret_decryptor


def _decrypt_plan_credentials(encrypted: str) -> str:
    """Decrypt the cloud credentials a core-built plan carries (``EncryptedCredentialProvider``)."""
    # Lazy: flowfile_worker.secrets pulls pydantic, which spawned children must not load eagerly.
    from flowfile_worker.secrets import decrypt_secret

    return decrypt_secret(encrypted).get_secret_value()


register_secret_decryptor(_decrypt_plan_credentials)
