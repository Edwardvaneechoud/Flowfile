import ast

import polars as pl

dtype_to_pl = {
    "int": pl.Int64,
    "integer": pl.Int64,
    "char": pl.String,
    "fixed decimal": pl.Float32,
    "double": pl.Float64,
    "float": pl.Float64,
    "bool": pl.Boolean,
    "byte": pl.UInt8,
    "bit": pl.Binary,
    "date": pl.Date,
    "datetime": pl.Datetime,
    "string": pl.String,
    "str": pl.String,
    "time": pl.Time,
}


_BARE_DTYPE_NAMES = (
    "List",
    "Array",
    "Struct",
    "Field",
    "Decimal",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "Float16",
    "Float32",
    "Float64",
    "Boolean",
    "String",
    "Utf8",
    "Binary",
    "Date",
    "Time",
    "Datetime",
    "Duration",
    "Categorical",
    "Enum",
    "Null",
    "Object",
    "Extension",
)
_BARE_DTYPES = {name: getattr(pl, name) for name in _BARE_DTYPE_NAMES if hasattr(pl, name)}


def _is_dtype(value) -> bool:
    return isinstance(value, pl.DataType) or (isinstance(value, type) and issubclass(value, pl.DataType))


def _is_dtype_constructor(value) -> bool:
    return value is pl.Field or (isinstance(value, type) and issubclass(value, pl.DataType))


def _build_dtype(node: ast.AST, bare_names: bool):
    """Build the value one node of a dtype expression spells; only literals and dtype constructors."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.List | ast.Tuple):
        items = [_build_dtype(item, bare_names) for item in node.elts]
        return items if isinstance(node, ast.List) else tuple(items)
    if isinstance(node, ast.Dict) and None not in node.keys:
        pairs = zip(node.keys, node.values, strict=True)
        return {_build_dtype(key, bare_names): _build_dtype(value, bare_names) for key, value in pairs}
    if isinstance(node, ast.Name) and bare_names and node.id in _BARE_DTYPES:
        return _BARE_DTYPES[node.id]
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "pl":
        value = None if node.attr.startswith("_") else getattr(pl, node.attr, None)
        if _is_dtype_constructor(value):
            return value
    if isinstance(node, ast.Call) and all(keyword.arg is not None for keyword in node.keywords):
        func = _build_dtype(node.func, bare_names)
        if _is_dtype_constructor(func):
            args = [_build_dtype(arg, bare_names) for arg in node.args]
            kwargs = {keyword.arg: _build_dtype(keyword.value, bare_names) for keyword in node.keywords}
            return func(*args, **kwargs)
    raise ValueError(f"unsupported syntax in a dtype: {type(node).__name__}")


def safe_eval_pl_type(type_string: str, *, bare_names: bool = True):
    """Build the Polars dtype a type string spells, without evaluating it as Python.

    Accepts ``pl.List(pl.Int64)`` and, with ``bare_names``, ``List(Int64)``: dtype names, calls to
    dtype constructors (and ``Field``) and literal arguments. Anything else raises ``ValueError``,
    because these strings come from node settings and flow files any user can write.
    """
    try:
        dtype = _build_dtype(ast.parse(type_string.strip(), mode="eval").body, bare_names)
        if not _is_dtype(dtype):
            raise ValueError("not a Polars dtype")
        return dtype
    except Exception as e:
        raise ValueError(f"Failed to safely evaluate type string '{type_string}': {e}") from e


dtype_to_pl_str = {k: v.__name__ for k, v in dtype_to_pl.items()}


def get_polars_type(dtype: str):
    if "pl." in dtype:
        try:
            return safe_eval_pl_type(dtype)
        except Exception:
            return pl.String
    pl_datetype = dtype_to_pl.get(dtype.lower())
    if pl_datetype is not None:
        return pl_datetype
    try:
        return safe_eval_pl_type(dtype)
    except Exception:
        return pl.String  # Fallback to String if evaluation fails


def cast_str_to_polars_type(dtype: str) -> pl.DataType:
    pl_type = get_polars_type(dtype)
    if callable(pl_type):
        return pl_type()
    else:
        return pl_type
