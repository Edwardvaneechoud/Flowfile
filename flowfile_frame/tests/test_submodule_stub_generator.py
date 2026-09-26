import ast
import importlib.util
import inspect
from pathlib import Path

from flowfile_frame.flow_frame import FlowFrame


def _load_generator(file_name: str):
    spec = importlib.util.spec_from_file_location(file_name.removesuffix(".py"), Path(__file__).parents[1] / file_name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = _load_generator("submodule_stub_generator.py")

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


def test_private_modules_get_no_stub_and_no_reexport(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text(
        "from pkg import _hook  # noqa: F401\nfrom pkg._version import get_version\nfrom pkg.other import Thing\n"
    )
    (package / "_hook.py").write_text("def install() -> None:\n    pass\n")
    (package / "_version.py").write_text("def get_version() -> str:\n    return '1'\n")
    (package / "other.py").write_text("class Thing:\n    pass\n")

    sources = {module for _, module in generator.discover_sources(package)}
    stub = generator.generate_stub(package / "__init__.py", "pkg").read_text()

    assert sources == {"pkg", "pkg.other"}
    assert "_hook" not in stub
    assert "from pkg._version import get_version as get_version" in stub
    assert '__all__ = ["Thing", "get_version", "other"]' in stub


def test_flow_frame_stub_keeps_keyword_only_parameters(tmp_path):
    flow_frame_generator = _load_generator("flow_frame_stub_generator.py")
    stub = Path(flow_frame_generator.generate_improved_type_stub(FlowFrame, str(tmp_path / "flow_frame.pyi")))
    stub_class = next(n for n in ast.parse(stub.read_text()).body if isinstance(n, ast.ClassDef))
    stub_args = {n.name: n.args for n in stub_class.body if isinstance(n, ast.FunctionDef)}

    assert [a.arg for a in stub_args["sql"].args] == ["self", "query"]
    assert [a.arg for a in stub_args["sql"].kwonlyargs] == ["table_name", "description"]
    assert [a.arg for a in stub_args["to_flow_output"].kwonlyargs] == ["description"]
    for name, method in vars(FlowFrame).items():
        if name not in stub_args or not inspect.isfunction(method):
            continue
        params = inspect.signature(method).parameters.values()
        keyword_only = {p.name for p in params if p.kind is inspect.Parameter.KEYWORD_ONLY}
        positional = {p.name for p in params if p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD}
        assert keyword_only <= {a.arg for a in stub_args[name].kwonlyargs}, name
        assert not positional & {a.arg for a in stub_args[name].kwonlyargs}, name
