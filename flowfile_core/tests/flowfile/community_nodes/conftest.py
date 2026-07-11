"""Shared helpers for community-node bundle tests.

The canonical valid bundle lives on disk at ``bundle_corpus/valid``; tests copy
it into a folder named after its ``manifest.id`` and mutate for broken variants.
PNGs are generated with stdlib zlib/struct so no Pillow dependency is needed.
"""

import shutil
import struct
import zlib
from pathlib import Path

import pytest

BUNDLE_CORPUS = Path(__file__).parent / "bundle_corpus"
VALID_BUNDLE = BUNDLE_CORPUS / "valid"
VALID_ID = "trim_text"


def make_png(width: int, height: int, rgb: tuple[int, int, int] = (80, 140, 200)) -> bytes:
    """Return the bytes of a real (decodable) RGB PNG of the given dimensions."""

    def chunk(type_: bytes, data: bytes) -> bytes:
        body = type_ + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    raw = b"".join(b"\x00" + bytes(rgb) * width for _ in range(height))
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")


def copy_valid_bundle(dst: Path, *, name: str = VALID_ID) -> Path:
    """Copy the canonical valid bundle into ``dst/name`` and return the folder."""
    dst = Path(dst)
    dst.mkdir(parents=True, exist_ok=True)
    folder = dst / name
    shutil.copytree(VALID_BUNDLE, folder)
    return folder


@pytest.fixture
def valid_bundle(tmp_path: Path) -> Path:
    """A correctly-named copy of the canonical valid bundle in a temp dir."""
    return copy_valid_bundle(tmp_path)
