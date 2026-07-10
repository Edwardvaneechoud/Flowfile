"""Core-side helpers for dispatching custom-node execution to the worker."""

from shared.node_designer import CustomNodeBase, SecretSelector


def resolve_secret_payload(custom_node: CustomNodeBase, user_id: int | None) -> dict[str, str]:
    """Resolve every selected SecretSelector to its ``$ffsec$`` ciphertext.

    Ciphertext only — the worker decrypts locally via the owner id embedded in
    the format. A missing secret fails here, before any worker call.
    """
    if custom_node.settings_schema is None:
        return {}

    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

    payload: dict[str, str] = {}
    for field_name, component in custom_node.settings_schema.get_all_components().items():
        if isinstance(component, SecretSelector) and component.value:
            if user_id is None:
                raise ValueError(f"Secret '{component.value}' (field '{field_name}') requires a user context")
            encrypted = get_encrypted_secret(current_user_id=user_id, secret_name=component.value)
            if encrypted is None:
                raise ValueError(f"Secret '{component.value}' (field '{field_name}') not found for this user")
            payload[component.value] = encrypted
    return payload
