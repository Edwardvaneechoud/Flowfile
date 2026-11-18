"""What a spawned worker child reports about the server extension, for ``test_server_extension``."""


def report(queue) -> None:
    """Import the worker as every spawned child does, then open one sealed secret."""
    import flowfile_worker  # noqa: F401
    from shared import sealed, server_extension

    loaded_at_import = server_extension._resolved
    opened = sealed.open_secret(sealed.sealed_secret("toy", {"a-server": "sealed"}))
    queue.put((loaded_at_import, server_extension.load().__name__, opened))
