from shared.cloud_credential_provider import register_secret_decryptor
from shared.iceberg_utils import register_run_log


def _decrypt_plan_credentials(encrypted: str) -> str:
    """Decrypt the cloud credentials a core-built plan carries (``EncryptedCredentialProvider``)."""
    # Lazy: flowfile_worker.secrets pulls pydantic, which spawned children must not load eagerly.
    from flowfile_worker.secrets import decrypt_secret

    return decrypt_secret(encrypted).get_secret_value()


register_secret_decryptor(_decrypt_plan_credentials)


def _log_for_node(flow_id: int, node_id: int | str, message: str) -> None:
    """Write a line an Iceberg read or write says about itself to the node's run log."""
    from flowfile_worker.flow_logger import get_worker_logger

    get_worker_logger(flow_id, node_id).info(message)


register_run_log(_log_for_node)
