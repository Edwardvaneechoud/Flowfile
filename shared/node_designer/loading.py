"""Load custom-node .py files and make legacy import paths resolve to the SDK.

Custom node files historically import from ``flowfile_core.flowfile.node_designer``
or ``flowfile.node_designer``. Importing the real ``flowfile_core`` runs DB
migrations on import, and the worker image does not ship it at all —
``install_import_aliases()`` maps those spellings onto the SDK instead so node
files load in any process without core's side effects.
"""

import importlib.util
import inspect
import itertools
import sys
import types

from shared.node_designer import _type_registry as _sdk_type_registry
from shared.node_designer import custom_node as _sdk_custom_node
from shared.node_designer import types as _sdk_types
from shared.node_designer import ui_components as _sdk_ui_components
from shared.node_designer.custom_node import CustomNodeBase

_FORBIDDEN_IMPORT_MSG = (
    "'{name}' is not available in this execution context. "
    "Custom nodes may only import the node designer SDK "
    "(from flowfile import node_designer as nd) — not other flowfile internals."
)

_load_counter = itertools.count()


class NodeLoadError(Exception):
    """Raised when a custom node file cannot be loaded or contains no valid node class."""


def _make_alias_package(name: str) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__path__ = []

    def __getattr__(attr: str, _name: str = name):  # noqa: N807 - PEP 562 module getattr
        if attr.startswith("__") and attr.endswith("__"):
            raise AttributeError(attr)
        raise ImportError(_FORBIDDEN_IMPORT_MSG.format(name=f"{_name}.{attr}"))

    module.__getattr__ = __getattr__
    return module


class _ForbiddenCoreImportFinder:
    """Meta-path finder that turns non-SDK flowfile imports into contract errors."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".", 1)[0] in {"flowfile_core", "flowfile"}:
            raise ImportError(_FORBIDDEN_IMPORT_MSG.format(name=fullname))
        return None


def install_import_aliases() -> None:
    """Alias legacy custom-node import paths onto the SDK in ``sys.modules``.

    No-op when the real ``flowfile_core`` is already imported (i.e. inside core
    itself). Intended for worker / dry-run child processes before exec'ing any
    user node file.
    """
    real_core = sys.modules.get("flowfile_core")
    if real_core is not None and getattr(real_core, "__file__", None):
        return

    sdk_package = sys.modules[__package__]

    aliases: dict[str, types.ModuleType] = {}

    flowfile_core = _make_alias_package("flowfile_core")
    flowfile_core_flowfile = _make_alias_package("flowfile_core.flowfile")
    flowfile_pkg = _make_alias_package("flowfile")

    flowfile_core.flowfile = flowfile_core_flowfile
    flowfile_core.types = _sdk_types
    flowfile_core_flowfile.node_designer = sdk_package
    flowfile_pkg.node_designer = sdk_package

    aliases["flowfile_core"] = flowfile_core
    aliases["flowfile_core.flowfile"] = flowfile_core_flowfile
    aliases["flowfile_core.flowfile.node_designer"] = sdk_package
    aliases["flowfile_core.flowfile.node_designer.custom_node"] = _sdk_custom_node
    aliases["flowfile_core.flowfile.node_designer.ui_components"] = _sdk_ui_components
    aliases["flowfile_core.flowfile.node_designer._type_registry"] = _sdk_type_registry
    aliases["flowfile_core.types"] = _sdk_types
    aliases["flowfile"] = flowfile_pkg
    aliases["flowfile.node_designer"] = sdk_package

    for name, module in aliases.items():
        sys.modules.setdefault(name, module)

    # Everything alias-registered above resolves from sys.modules; any other
    # flowfile_core.* / flowfile.* import reaches this finder and fails with
    # the contract message instead of a bare ModuleNotFoundError.
    if not any(isinstance(f, _ForbiddenCoreImportFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _ForbiddenCoreImportFinder())


def load_node_module(
    *, source: str | None = None, path: str | None = None, module_name: str | None = None
) -> types.ModuleType:
    """Load a custom-node module from source text or a file path.

    Each call gets a unique module name so reloads never hit a stale
    ``sys.modules`` entry.
    """
    if (source is None) == (path is None):
        raise ValueError("Provide exactly one of 'source' or 'path'")

    if module_name is None:
        stem = "inline" if path is None else str(path).rsplit("/", 1)[-1].removesuffix(".py")
        module_name = f"flowfile_udn_{stem}_{next(_load_counter)}"

    if path is not None:
        with open(path, encoding="utf-8") as f:
            source = f.read()

    spec = importlib.util.spec_from_loader(module_name, loader=None)
    module = importlib.util.module_from_spec(spec)
    module.__file__ = str(path) if path is not None else f"<{module_name}>"
    sys.modules[module_name] = module
    try:
        code = compile(source, module.__file__, "exec")
        exec(code, module.__dict__)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def find_custom_node_class(module: types.ModuleType, class_name: str | None = None) -> type[CustomNodeBase]:
    """Locate the CustomNodeBase subclass defined in ``module``.

    With ``class_name`` set, that exact class is returned. Otherwise the module
    must define exactly one CustomNodeBase subclass of its own (imported ones
    are ignored).
    """
    if class_name is not None:
        obj = getattr(module, class_name, None)
        if obj is None or not (inspect.isclass(obj) and issubclass(obj, CustomNodeBase)):
            raise NodeLoadError(f"Class '{class_name}' not found or not a CustomNodeBase subclass")
        return obj

    candidates = [
        obj
        for _name, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, CustomNodeBase) and obj is not CustomNodeBase and obj.__module__ == module.__name__
    ]
    if not candidates:
        raise NodeLoadError("No CustomNodeBase subclass found in module")
    if len(candidates) > 1:
        names = ", ".join(sorted(c.__name__ for c in candidates))
        raise NodeLoadError(f"Multiple CustomNodeBase subclasses found ({names}); a node file must define exactly one")
    return candidates[0]
