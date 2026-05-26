"""Master-key string normalization shared by core and worker.

A ``FLOWFILE_MASTER_KEY`` value commonly arrives wrapped in quotes or padded
with whitespace, because ``/setup/generate-key`` tells operators to write
``FLOWFILE_MASTER_KEY="<key>"`` into ``.env`` and some shells/loaders keep the
surrounding quotes. Core and worker must agree byte-for-byte on how that raw
value maps to a Fernet key, so the normalization lives here and both services
call it on every key source (env var and Docker secret file).
"""


def normalize_master_key(raw: str) -> str:
    """Strip surrounding whitespace and matching single/double quotes.

    A Fernet key is URL-safe base64, so it never legitimately contains quote
    characters — stripping them from both ends can't corrupt a valid key.
    """
    return raw.strip().strip('"').strip("'")
