import ast
import importlib.util
from pathlib import Path

_GENERATOR_PATH = Path(__file__).resolve().parents[1] / "submodule_stub_generator.py"
_spec = importlib.util.spec_from_file_location("submodule_stub_generator", _GENERATOR_PATH)
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)

_SOURCE = """
class Outputs:
    def __init__(self, names: list[str]) -> None:
        self._names = names

    def __getitem__(self, name: str) -> int:
        return 0

    def __iter__(self):
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    def __getattr__(self, name: str) -> int:
        return 0

    def __contains__(self, name: object) -> bool:
        return name in self._names

    def __dir__(self) -> list[str]:
        return list(self._names)

    def __call__(self, *inputs: int, trim: bool = False) -> int:
        return 0

    def __repr__(self) -> str:
        return "Outputs"

    def _helper(self) -> None:
        pass
"""


def test_render_class_keeps_allow_listed_dunders():
    rendered = "\n".join(generator._render_class(ast.parse(_SOURCE).body[0]))

    assert "def __init__(self, names: list[str]) -> None: ..." in rendered
    assert "def __getitem__(self, name: str) -> int: ..." in rendered
    assert "def __iter__(self) -> Any: ..." in rendered
    assert "def __len__(self) -> int: ..." in rendered
    assert "def __getattr__(self, name: str) -> int: ..." in rendered
    assert "def __contains__(self, name: object) -> bool: ..." in rendered
    assert "def __dir__(self) -> list[str]: ..." in rendered
    assert "def __call__(self, *inputs: int, trim: bool=False) -> int: ..." in rendered
    assert "__repr__" not in rendered
    assert "_helper" not in rendered


_ENUM_SOURCE = """
class Mode(str, Enum):
    \"\"\"Modes.\"\"\"

    FULL = "full"
    QUICK = "quick"
    _hidden = "x"


class Plain:
    LIMIT = 3
"""


def test_render_class_keeps_enum_members():
    enum_cls, plain_cls = ast.parse(_ENUM_SOURCE).body
    rendered = "\n".join(generator._render_class(enum_cls))

    assert "class Mode(str, Enum):" in rendered
    assert "    FULL = 'full'" in rendered
    assert "    QUICK = 'quick'" in rendered
    assert "_hidden" not in rendered
    assert "LIMIT" not in "\n".join(generator._render_class(plain_cls))


def test_init_stub_skips_a_submodule_shadowed_by_an_export(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text("from pkg.factory import factory\nfrom pkg.other import Thing\n")
    (package / "factory.py").write_text("def factory() -> int:\n    return 1\n")
    (package / "other.py").write_text("class Thing:\n    pass\n")

    stub = generator.generate_stub(package / "__init__.py", "pkg").read_text()

    assert "from . import factory as factory" not in stub
    assert "from pkg.factory import factory as factory" in stub
    assert "from . import other as other" in stub
