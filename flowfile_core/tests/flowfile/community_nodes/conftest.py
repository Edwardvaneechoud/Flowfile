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

from flowfile_core.flowfile.user_defined.registry import registry
from shared import storage

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


# ------------------------------------------------ designed-node fixtures (publish / PR)
# Shared so the publish and community-github route suites write nodes identically.

NODE_TEMPLATE = '''
import polars as pl
from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class {class_name}(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "Community Test"
    node_icon: str = "{node_icon}"
    intro: str = "{intro}"
    author: str = "{author}"
    version: str = "{version}"
    tags: list = {tags}
{examples}
    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", value=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''

EXAMPLES_BLOCK = (
    '    example_inputs: list = [{"value": ["a", "b"]}]\n'
    '    example_settings: dict = {"main": {"value": "x"}}\n'
)


def _valid_png(width: int = 32, height: int = 32) -> bytes:
    """Minimal PNG whose IHDR carries the given dimensions (png_dimensions-parseable)."""
    return b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 13) + b"IHDR" + struct.pack(">II", width, height) + b"\x00" * 20


@pytest.fixture
def isolated_storage(tmp_path, monkeypatch):
    original_base = storage._base_dir
    storage._base_dir = tmp_path / "storage"
    storage.user_defined_nodes_icons.mkdir(parents=True, exist_ok=True)
    storage.user_defined_nodes_screenshots.mkdir(parents=True, exist_ok=True)
    # Fixture-mode path that doesn't exist: index lookups fail fast offline instead
    # of hitting the live registry (tests that need an index setenv their own).
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(tmp_path / "no_index.json"))
    registry.scan()
    yield tmp_path
    storage._base_dir = original_base
    registry.scan()


def _write_node(
    *,
    node_name: str = "Mood Emoji",
    class_name: str = "MoodEmoji",
    node_icon: str = "user-defined-icon.png",
    intro: str = "Adds a mood emoji column to your rows",
    author: str = "octocat",
    version: str = "1.0.0",
    tags: list | None = None,
    with_examples: bool = True,
):
    source = NODE_TEMPLATE.format(
        class_name=class_name,
        node_name=node_name,
        node_icon=node_icon,
        intro=intro,
        author=author,
        version=version,
        tags=repr(tags or ["fun"]),
        examples=EXAMPLES_BLOCK if with_examples else "",
    )
    file_name = node_name.replace(" ", "_").lower() + ".py"
    path = storage.user_defined_nodes_directory / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    registry.load_file(path)
    return registry.get_by_file(file_name)


def _add_icon(name: str, data: bytes) -> None:
    storage.user_defined_nodes_icons.mkdir(parents=True, exist_ok=True)
    (storage.user_defined_nodes_icons / name).write_bytes(data)


def _add_screenshot(file_stem: str, name: str, data: bytes) -> None:
    d = storage.user_defined_nodes_screenshots / file_stem
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(data)
