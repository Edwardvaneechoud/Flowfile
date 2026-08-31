"""Encode/decode the share envelope the way the browser build expects.

``#flow=<base64url(deflate-raw(JSON))>`` — raw DEFLATE with no zlib header
(``wbits=-15``), base64url with the padding stripped. Built with
``zlib.compressobj`` rather than ``zlib.compress(wbits=...)``, which only exists
on Python 3.11+ while this repo supports 3.10.
"""

import base64
import json
import zlib

WASM_DESIGNER_URL = "https://demo.flowfile.org/designer"
SHARE_HASH_PREFIX = "#flow="

# Wide enough for any legitimate flow; a hostile hash cannot make us allocate.
_MAX_DECOMPRESSED_BYTES = 32 * 1024 * 1024


def encode_envelope(envelope: dict) -> str:
    """The base64url blob that goes after ``#flow=``."""
    payload = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
    compressed = compressor.compress(payload) + compressor.flush()
    return base64.urlsafe_b64encode(compressed).decode("ascii").rstrip("=")


def build_share_url(envelope: dict) -> tuple[str, int]:
    """``(url, hash_chars)`` — the link and the length of its encoded payload."""
    blob = encode_envelope(envelope)
    return f"{WASM_DESIGNER_URL}{SHARE_HASH_PREFIX}{blob}", len(blob)


def decode_share_hash(hash_or_url: str) -> dict | None:
    """The envelope inside a share link, or ``None``. Never raises."""
    if not isinstance(hash_or_url, str):
        return None
    blob = hash_or_url.rsplit(SHARE_HASH_PREFIX, 1)[-1].strip()
    if not blob:
        return None
    try:
        raw = base64.urlsafe_b64decode(blob + "=" * (-len(blob) % 4))
        decompressor = zlib.decompressobj(-15)
        payload = decompressor.decompress(raw, _MAX_DECOMPRESSED_BYTES)
        if decompressor.unconsumed_tail:
            return None
        envelope = json.loads(payload)
    except (ValueError, zlib.error, UnicodeDecodeError):
        return None
    return envelope if isinstance(envelope, dict) else None
