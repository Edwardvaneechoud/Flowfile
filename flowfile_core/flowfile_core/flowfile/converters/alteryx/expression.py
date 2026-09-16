"""Fail-closed translator from Alteryx formula expressions to the Flowfile formula dialect.

An expression is only translated when it tokenizes, parses, resolves every function through
``FUNCTION_MAP`` and the rendered result is accepted by the real Flowfile formula parser.
Anything else comes back as ``TranslationOutcome(translated=None, reason=...)`` — never a guess.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import polars as pl
from polars_expr_transformer import simple_function_to_expr

__all__ = [
    "TranslationOutcome",
    "try_translate",
    "FUNCTION_MAP",
    "REJECTED_FUNCTIONS",
    "regex_rejection",
    "unsupported_construct",
]


@dataclass
class TranslationOutcome:
    """Result of a translation attempt. ``translated is None`` means untranslatable and ``reason`` is set.

    ``caveats`` are the sentences a *successful* translation still owes its reader: a function whose
    Flowfile equivalent agrees with Alteryx on some inputs and not on others. Every caller has to put
    them on its row and stop calling the tool ``converted``, which is why they travel with the result
    rather than being written into the formula as a comment nobody reads.

    ``fields`` are the column names the expression reads, bracketed and bare alike. A caller that
    knows the types of its input columns needs them to decide what the expression will compute in;
    it cannot get them from the rendered text, where a column name and a string literal look alike.
    It is only filled in on a successful translation — a refusal stops part-way through the tree,
    so the set it would carry is the answer to a different question.
    """

    translated: str | None = None
    reason: str | None = None
    caveats: list[str] = field(default_factory=list)
    fields: frozenset[str] = frozenset()


class _Untranslatable(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# Caveats collected while emitting the expression currently being translated. A module-level list
# because `_emit` is a plain recursive function and threading a collector through every emitter
# would touch each of them for one feature; `try_translate` owns the reset and the read.
_CAVEATS: list[str] = []
# The column names that expression reads, the ones the caller asked to be read as Float64, and
# whether integer literals render as floats. Same reason, same owner: `try_translate` resets all
# three and reads them back.
_FIELDS: set[str] = set()
_FLOAT_FIELDS: frozenset[str] = frozenset()
_FLOAT_LITERALS: bool = False

# Alteryx evaluates a whole expression in the type it declares for the output, so an assignment
# declared Double computes in floating point even where every operand is an integer column. The
# caller names the integer columns and this wraps each reference, because a cast applied to the
# finished value is too late: `-20 * pow(x, 7)` has already wrapped in Int64 by then.
FLOAT_CAST = "to_number"

# A literal needs the same treatment for the same reason, and a cast on the column cannot give it:
# `POW(2, 70)` reads no column at all and still wraps, to 0 in Int32, where Alteryx prints 1.18e21.
# Rendering the literals as floats — `power(2.0, 70.0)` — is what makes the arithmetic float.
_INTEGER_LITERAL_RE = re.compile(r"\d+")
# Only where an integer literal is an arithmetic operand. A positional or flag argument
# (`Substring([s], 0, 5)`, `REGEX_Match([s], p, 1)`) means an integer and must stay one.
_ARITHMETIC_OPS = frozenset({"+", "-", "*", "/"})
_FLOAT_LITERAL_FUNCTIONS = frozenset({"pow"})


@dataclass(frozen=True)
class FunctionSpec:
    """One verified Alteryx -> Flowfile function mapping.

    ``caveat`` marks a mapping that is exact on some inputs and not on others. It is not a hedge:
    a mapping whose answer is simply different is refused instead, and one that always agrees
    carries no caveat at all.
    """

    alteryx_name: str
    target: str | None
    min_args: int
    max_args: int | None
    special: str | None = None
    caveat: str | None = None


def _spec(alteryx_name: str, target: str, args: int) -> FunctionSpec:
    return FunctionSpec(alteryx_name, target, args, args)


FUNCTION_MAP: dict[str, FunctionSpec] = {
    # conditional
    "iif": FunctionSpec("IIF", None, 3, 3, special="iif"),
    "switch": FunctionSpec("Switch", None, 4, None, special="switch"),
    # string
    "uppercase": _spec("Uppercase", "uppercase", 1),
    "lowercase": _spec("Lowercase", "lowercase", 1),
    "titlecase": _spec("TitleCase", "titlecase", 1),
    "left": _spec("Left", "left", 2),
    "right": _spec("Right", "right", 2),
    "substring": _spec("Substring", "substring", 3),
    "trim": _spec("Trim", "trim", 1),
    "trimleft": _spec("TrimLeft", "left_trim", 1),
    "trimright": _spec("TrimRight", "right_trim", 1),
    "length": _spec("Length", "length", 1),
    "contains": FunctionSpec("Contains", "contains", 2, 2, special="search"),
    "regex_match": FunctionSpec("REGEX_Match", "contains", 2, 3, special="regexmatch"),
    "startswith": FunctionSpec("StartsWith", "starts_with", 2, 2, special="search"),
    "endswith": FunctionSpec("EndsWith", "ends_with", 2, 2, special="search"),
    "findstring": _spec("FindString", "find_position", 2),
    "replace": _spec("Replace", "replace", 3),
    "replacechar": FunctionSpec("ReplaceChar", None, 3, 3, special="replacechar"),
    "padleft": _spec("PadLeft", "pad_left", 3),
    "padright": _spec("PadRight", "pad_right", 3),
    "reversestring": _spec("ReverseString", "reverse", 1),
    # null / logic
    "isnull": _spec("IsNull", "is_empty", 1),
    "isempty": FunctionSpec("IsEmpty", None, 1, 1, special="isempty"),
    # type conversion
    "tonumber": _spec("ToNumber", "to_number", 1),
    "tostring": _spec("ToString", "to_string", 1),
    # hashing; md5() hashes UTF-8 bytes, so only Alteryx's UTF8 variant produces the same digest
    "md5_utf8": _spec("MD5_UTF8", "md5", 1),
    # base64 over the UTF-8 bytes of the text, both directions; identical for ASCII under any encoding
    "base64encode": _spec("Base64Encode", "base64_encode", 1),
    "base64decode": _spec("Base64Decode", "base64_decode", 1),
    # math
    "abs": _spec("Abs", "abs", 1),
    "ceil": _spec("Ceil", "ceil", 1),
    "floor": _spec("Floor", "floor", 1),
    "sqrt": _spec("Sqrt", "sqrt", 1),
    "exp": _spec("Exp", "exp", 1),
    "log": _spec("Log", "log", 1),
    "log10": _spec("Log10", "log10", 1),
    "pow": _spec("Pow", "power", 2),
    "mod": _spec("Mod", "mod", 2),
    "sin": _spec("Sin", "sin", 1),
    "cos": _spec("Cos", "cos", 1),
    "tan": _spec("Tan", "tan", 1),
    "asin": _spec("ASin", "asin", 1),
    "acos": _spec("ACos", "acos", 1),
    "atan": _spec("ATan", "atan", 1),
    "min": FunctionSpec("Min", "least", 2, None),
    "max": FunctionSpec("Max", "greatest", 2, None),
    "round": FunctionSpec("Round", None, 2, 2, special="round"),
    # date / time
    "datetimeadd": FunctionSpec("DateTimeAdd", None, 3, 3, special="datetimeadd"),
    "datetimediff": FunctionSpec("DateTimeDiff", None, 3, 3, special="datetimediff"),
    "datetimetrim": FunctionSpec("DateTimeTrim", None, 2, 2, special="datetimetrim"),
    "datetimeformat": FunctionSpec("DateTimeFormat", None, 2, 2, special="datetimeformat"),
    "datetimeparse": FunctionSpec("DateTimeParse", None, 2, 2, special="datetimeparse"),
    "datetimeyear": _spec("DateTimeYear", "year", 1),
    "datetimemonth": _spec("DateTimeMonth", "month", 1),
    "datetimeday": _spec("DateTimeDay", "day", 1),
    "datetimehour": _spec("DateTimeHour", "hour", 1),
    "datetimeminute": _spec("DateTimeMinute", "minute", 1),
    "datetimeminutes": _spec("DateTimeMinutes", "minute", 1),
    "datetimesecond": _spec("DateTimeSecond", "second", 1),
    "datetimeseconds": _spec("DateTimeSeconds", "second", 1),
    "datetimenow": FunctionSpec("DateTimeNow", "now", 0, 0),
    "datetimetoday": FunctionSpec("DateTimeToday", "today", 0, 0),
    "datetimefirstofmonth": FunctionSpec("DateTimeFirstOfMonth", None, 0, 0, special="firstofmonth"),
    "datetimelastofmonth": FunctionSpec("DateTimeLastOfMonth", None, 0, 0, special="lastofmonth"),
    # date / time, short forms
    "year": _spec("Year", "year", 1),
    "month": _spec("Month", "month", 1),
    "day": _spec("Day", "day", 1),
    "hour": _spec("Hour", "hour", 1),
    "minute": _spec("Minute", "minute", 1),
    "second": _spec("Second", "second", 1),
}

REJECTED_FUNCTIONS: dict[str, str] = {
    "null": "the Alteryx NULL() literal has no Flowfile formula equivalent",
    "rowcount": "RowCount() has no Flowfile formula equivalent (use a Record ID node instead)",
    "getword": "GetWord() has no Flowfile formula equivalent",
    "spellnumber": "SpellNumber() has no Flowfile formula equivalent",
    "randint": "RandInt() is non-deterministic and has no verified Flowfile equivalent",
    "rand": "Rand() is non-deterministic and has no verified Flowfile equivalent",
    "md5_ascii": (
        "MD5_ASCII() hashes the ASCII bytes of the text while Flowfile's md5() hashes UTF-8 bytes; "
        "the digests differ for any non-ASCII character, so use MD5_UTF8() instead"
    ),
    "md5_unicode": (
        "MD5_UNICODE() hashes the UTF-16LE bytes of the text while Flowfile's md5() hashes UTF-8 bytes; "
        "the digests never match, so use MD5_UTF8() instead"
    ),
}

_DATETIME_ADD_UNITS = {
    "year": "add_years",
    "month": "add_months",
    "week": "add_weeks",
    "day": "add_days",
    "hour": "add_hours",
    "minute": "add_minutes",
    "second": "add_seconds",
}

_DATETIME_DIFF_UNITS = {
    "day": "date_diff_days",
    "second": "datetime_diff_seconds",
}

_DATETIME_TRIM_PARTS = {"year", "month", "day", "hour", "minute", "second"}
_DATETIME_TRIM_CALLS = {"firstofmonth": "start_of_month", "lastofmonth": "end_of_month"}

# Date-format codes that are byte-identical in the Alteryx dialect and in chrono's strftime.
_DATE_FORMAT_CODES = frozenset("YymdHIMSpbBaAj%")
_PARSE_TIME_CODES = frozenset("HIMSp")

# Regex metacharacters, split by whether a one-character class can neutralise them.
_REGEX_META = "$()*+.?{|"
_REGEX_UNESCAPABLE = "[^"

# Constructs Polars' regex engine (Rust `regex`) has no support for, and the dunder shape the
# polars_code node rejects. They live here because REGEX_Match() below screens a pattern with them;
# `mappers.py` imports them back, because it imports this module and never the other way round.
#
# These are *wording*, never the decision — `regex_rejection` decides, and this only names the
# construct when the engine has already refused the pattern. The lookbehind tokens spell out their
# `=` and `!` for that reason: a bare `(?<` also matches `(?<name>...)`, a named group Polars
# accepts, and screening on it refused `(?<n>a)b` as "lookbehind" while `(?P<n>a)b` converted.
REGEX_UNSUPPORTED = (
    ("(?=", "lookahead"),
    ("(?!", "negative lookahead"),
    ("(?<=", "lookbehind"),
    ("(?<!", "negative lookbehind"),
)
DUNDER_RE = re.compile(r"__\w+__")


def unsupported_construct(pattern: str) -> str | None:
    """The name of the construct in ``pattern`` a reader is likeliest to have meant, or ``None``.

    Only for phrasing a refusal the engine has already made. Answering ``None`` costs the reader a
    friendlier sentence and never costs a pattern its conversion.
    """
    for token, label in REGEX_UNSUPPORTED:
        if token in pattern:
            return label
    return None


def regex_rejection(pattern: str) -> str | None:
    """Polars' own complaint about a pattern, or ``None`` when its regex engine accepts it.

    The three tokens above are the constructs a reader is likeliest to write and they keep a sentence
    that names them. Everything else the engine lacks is only known to the engine: `(?>ab)c` (atomic
    group) and `(?#c)abc` (inline comment) are legal Perl, pass a substring blocklist, convert green
    and then raise `ComputeError: unrecognized flag` the first time the flow collects. Asking the
    engine at import time is the only screen that cannot be out of date with it.

    The pattern handed here is the *finished* one — anchors, inline flags and all — because that is
    the string `str.contains` will be given, and a wrapper can be what the engine rejects.
    """
    try:
        pl.DataFrame({"_": [""]}).select(pl.col("_").str.contains(pattern))
    except Exception as exc:
        # Polars appends the whole `col("_").str.contains(...)` expression; the complaint is the head.
        return _clean_reason(str(exc).split("This error occurred in the following expression")[0])
    return None


# Keys whose translation provably returns a non-string; IsEmpty() drops its '= ""' arm for these.
_NON_STRING_FUNCTIONS = frozenset(
    {
        "datetimeparse",
        "datetimeadd",
        "datetimetrim",
        "datetimefirstofmonth",
        "datetimelastofmonth",
        "datetimenow",
        "datetimetoday",
        "datetimediff",
        "datetimeyear",
        "datetimemonth",
        "datetimeday",
        "datetimehour",
        "datetimeminute",
        "datetimeminutes",
        "datetimesecond",
        "datetimeseconds",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "tonumber",
        "abs",
        "ceil",
        "floor",
        "sqrt",
        "exp",
        "log",
        "log10",
        "pow",
        "mod",
        "sin",
        "cos",
        "tan",
        "asin",
        "acos",
        "atan",
        "round",
        "length",
        "findstring",
        "isnull",
        "isempty",
        "contains",
        "startswith",
        "endswith",
    }
)

_KEYWORDS = {"if", "then", "elseif", "else", "endif", "and", "or", "not", "in", "true", "false", "null"}

_REASON_IN = (
    "the IN operator has no Flowfile formula equivalent; rewrite it as a chain of '=' comparisons joined by 'or'"
)


_MULTI_CHAR_OPS = (">=", "<=", "!=", "<>", "==", "&&", "||")
_SINGLE_CHAR_OPS = "=<>+-*/"
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?|\.\d+")
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass
class _Token:
    kind: str  # number | string | field | ident | op | lparen | rparen | comma
    value: str
    pos: int


def _tokenize(text: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "[":
            end = text.find("]", i + 1)
            if end == -1:
                raise _Untranslatable(f"unterminated field reference starting at position {i + 1}")
            tokens.append(_Token("field", text[i + 1 : end], i))
            i = end + 1
            continue
        if ch in "\"'":
            value, end = _read_string(text, i)
            tokens.append(_Token("string", value, i))
            i = end
            continue
        if ch == "(":
            tokens.append(_Token("lparen", ch, i))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Token("rparen", ch, i))
            i += 1
            continue
        if ch == ",":
            tokens.append(_Token("comma", ch, i))
            i += 1
            continue
        if ch == "!":
            if text[i : i + 2] == "!=":
                tokens.append(_Token("op", "!=", i))
                i += 2
                continue
            tokens.append(_Token("op", "!", i))
            i += 1
            continue
        if ch == "^":
            raise _Untranslatable("the '^' power operator is not supported; rewrite it as Pow(base, exponent)")
        two = text[i : i + 2]
        if two in _MULTI_CHAR_OPS:
            tokens.append(_Token("op", two, i))
            i += 2
            continue
        if ch in _SINGLE_CHAR_OPS:
            tokens.append(_Token("op", ch, i))
            i += 1
            continue
        match = _NUMBER_RE.match(text, i)
        if match:
            tokens.append(_Token("number", match.group(0), i))
            i = match.end()
            continue
        match = _IDENT_RE.match(text, i)
        if match:
            tokens.append(_Token("ident", match.group(0), i))
            i = match.end()
            continue
        raise _Untranslatable(f"unsupported character {ch!r} at position {i + 1}")
    return tokens


def _read_string(text: str, start: int) -> tuple[str, int]:
    quote = text[start]
    i = start + 1
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "\\":
            raise _Untranslatable(
                f"the string literal starting at position {start + 1} contains a backslash escape, "
                "which cannot be converted safely"
            )
        if ch in "\r\n":
            raise _Untranslatable(f"the string literal starting at position {start + 1} spans multiple lines")
        if ch == quote:
            if i + 1 < n and text[i + 1] == quote:
                raise _Untranslatable(
                    f"the string literal starting at position {start + 1} uses a doubled-quote escape, "
                    "which cannot be converted safely"
                )
            return text[start + 1 : i], i + 1
        if ch == '"':
            raise _Untranslatable(
                f"the string literal starting at position {start + 1} contains a nested double quote, "
                "which cannot be converted safely"
            )
        i += 1
    raise _Untranslatable(f"unterminated string literal starting at position {start + 1}")


_PREC_IF = 0
_PREC_OR = 1
_PREC_AND = 2
_PREC_CMP = 3
_PREC_ADD = 4
_PREC_MUL = 5
_PREC_UNARY = 6
_PREC_ATOM = 7


@dataclass
class _Node:
    pass


@dataclass
class _Literal(_Node):
    text: str


@dataclass
class _Field(_Node):
    name: str


@dataclass
class _Binary(_Node):
    op: str
    left: _Node
    right: _Node
    prec: int


@dataclass
class _Unary(_Node):
    operand: _Node


@dataclass
class _Not(_Node):
    operand: _Node


@dataclass
class _Call(_Node):
    name: str
    args: list[_Node]
    pos: int


@dataclass
class _If(_Node):
    branches: list[tuple[_Node, _Node]] = field(default_factory=list)
    otherwise: _Node | None = None


_CMP_OPS = {"=": "=", "==": "=", "!=": "!=", "<>": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}


class _Parser:
    def __init__(
        self,
        tokens: list[_Token],
        allowed_specials: frozenset[str] = frozenset(),
        known_columns: frozenset[str] = frozenset(),
    ) -> None:
        self._tokens = tokens
        self._i = 0
        self._allowed_specials = frozenset(special.lower() for special in allowed_specials)
        self._known_columns = known_columns

    def parse(self) -> _Node:
        node = self._expr()
        if self._peek() is not None:
            tok = self._peek()
            raise _Untranslatable(f"unexpected trailing input {tok.value!r} at position {tok.pos + 1}")
        return node

    def _peek(self) -> _Token | None:
        return self._tokens[self._i] if self._i < len(self._tokens) else None

    def _next(self) -> _Token:
        tok = self._peek()
        if tok is None:
            raise _Untranslatable("the expression ends unexpectedly")
        self._i += 1
        return tok

    def _peek_ident(self) -> str | None:
        tok = self._peek()
        return tok.value.lower() if tok is not None and tok.kind == "ident" else None

    def _peek_op(self) -> str | None:
        tok = self._peek()
        return tok.value if tok is not None and tok.kind == "op" else None

    def _expect_keyword(self, keyword: str, context: str) -> None:
        if self._peek_ident() != keyword:
            tok = self._peek()
            found = "the end of the expression" if tok is None else repr(tok.value)
            raise _Untranslatable(f"expected {keyword.upper()!r} in {context} but found {found}")
        self._i += 1

    def _expr(self) -> _Node:
        return self._or_expr()

    def _or_expr(self) -> _Node:
        node = self._and_expr()
        while self._peek_ident() == "or" or self._peek_op() == "||":
            self._i += 1
            node = _Binary("or", node, self._and_expr(), _PREC_OR)
        return node

    def _and_expr(self) -> _Node:
        node = self._cmp_expr()
        while self._peek_ident() == "and" or self._peek_op() == "&&":
            self._i += 1
            node = _Binary("and", node, self._cmp_expr(), _PREC_AND)
        return node

    def _cmp_expr(self) -> _Node:
        node = self._add_expr()
        while True:
            if self._peek_ident() == "in":
                raise _Untranslatable(_REASON_IN)
            op = self._peek_op()
            if op not in _CMP_OPS:
                return node
            self._i += 1
            node = _Binary(_CMP_OPS[op], node, self._add_expr(), _PREC_CMP)

    def _add_expr(self) -> _Node:
        node = self._mul_expr()
        while self._peek_op() in ("+", "-"):
            op = self._next().value
            node = _Binary(op, node, self._mul_expr(), _PREC_ADD)
        return node

    def _mul_expr(self) -> _Node:
        node = self._unary()
        while self._peek_op() in ("*", "/"):
            op = self._next().value
            node = _Binary(op, node, self._unary(), _PREC_MUL)
        return node

    def _unary(self) -> _Node:
        if self._peek_ident() == "not" or self._peek_op() == "!":
            self._i += 1
            return _Not(self._unary())
        if self._peek_op() == "-":
            self._i += 1
            return _Unary(self._unary())
        return self._primary()

    def _primary(self) -> _Node:
        tok = self._peek()
        if tok is None:
            raise _Untranslatable("the expression ends unexpectedly")
        if tok.kind == "number":
            self._i += 1
            return _Literal(tok.value)
        if tok.kind == "string":
            self._i += 1
            return _Literal(f'"{tok.value}"')
        if tok.kind == "field":
            self._i += 1
            return _Field(_check_field_name(tok.value, self._allowed_specials))
        if tok.kind == "lparen":
            self._i += 1
            node = self._expr()
            closing = self._peek()
            if closing is None or closing.kind != "rparen":
                raise _Untranslatable(f"unbalanced parentheses: the '(' at position {tok.pos + 1} is never closed")
            self._i += 1
            return node
        if tok.kind == "ident":
            return self._ident_primary(tok)
        raise _Untranslatable(f"unexpected token {tok.value!r} at position {tok.pos + 1}")

    def _ident_primary(self, tok: _Token) -> _Node:
        low = tok.value.lower()
        if low in ("true", "false"):
            self._i += 1
            return _Literal(low)
        if low == "in":
            raise _Untranslatable(_REASON_IN)
        if low == "if":
            return self._if_expr()
        if low in _KEYWORDS and low != "null":
            raise _Untranslatable(f"unexpected keyword {tok.value!r} at position {tok.pos + 1}")
        following = self._tokens[self._i + 1] if self._i + 1 < len(self._tokens) else None
        if following is None or following.kind != "lparen":
            if low == "null":
                raise _Untranslatable(REJECTED_FUNCTIONS["null"])
            if self._resolves_to_a_column(tok.value):
                self._i += 1
                return _Field(_check_field_name(tok.value, self._allowed_specials))
            raise _Untranslatable(
                f"unbracketed field reference {tok.value!r} at position {tok.pos + 1}; "
                "Alteryx field references must be written as [Field] to be converted"
            )
        return self._call(tok)

    def _resolves_to_a_column(self, name: str) -> bool:
        """Whether a bare identifier is a column the caller can vouch for, not a guess at one.

        Alteryx accepts an unbracketed field name — `EXP(x/2)` — and the corpus writes it, so the
        blanket refusal cost real conversions. It is resolved only against the exact column names the
        caller passed in, and only when nothing in the function table wears that name: `Min` is a
        function whether or not a column is called Min, and reading it as a column would change the
        expression rather than fail to convert it. Case is not folded, because a column name's case
        is part of its identity in Polars.
        """
        low = name.lower()
        return name in self._known_columns and low not in FUNCTION_MAP and low not in REJECTED_FUNCTIONS

    def _call(self, name_tok: _Token) -> _Node:
        self._i += 2  # name + '('
        args: list[_Node] = []
        if self._peek() is not None and self._peek().kind == "rparen":
            self._i += 1
            return _Call(name_tok.value, args, name_tok.pos)
        while True:
            args.append(self._expr())
            tok = self._peek()
            if tok is None:
                raise _Untranslatable(f"the argument list of {name_tok.value!r} is never closed")
            if tok.kind == "comma":
                self._i += 1
                continue
            if tok.kind == "rparen":
                self._i += 1
                return _Call(name_tok.value, args, name_tok.pos)
            raise _Untranslatable(
                f"unexpected token {tok.value!r} at position {tok.pos + 1} in the arguments of {name_tok.value!r}"
            )

    def _if_expr(self) -> _Node:
        self._i += 1  # 'if'
        node = _If()
        condition = self._expr()
        self._expect_keyword("then", "an IF expression")
        node.branches.append((condition, self._expr()))
        while self._peek_ident() == "elseif":
            self._i += 1
            branch_condition = self._expr()
            self._expect_keyword("then", "an ELSEIF branch")
            node.branches.append((branch_condition, self._expr()))
        if self._peek_ident() != "else":
            raise _Untranslatable("the IF expression has no ELSE branch; the Flowfile formula language requires one")
        self._i += 1
        node.otherwise = self._expr()
        self._expect_keyword("endif", "an IF expression")
        return node


def _check_field_name(name: str, allowed_specials: frozenset[str] = frozenset()) -> str:
    """Validate one ``[field]`` reference; ``allowed_specials`` holds lowercased `_..._` names to keep."""
    if not name.strip():
        raise _Untranslatable("empty field reference '[]'")
    if ":" in name:
        raise _Untranslatable(
            f"row-offset field reference '[{name}]' has no Flowfile formula equivalent "
            "(formulas cannot look at other rows)"
        )
    if re.fullmatch(r"_.+_", name) and name.lower() not in allowed_specials:
        raise _Untranslatable(f"special Alteryx field reference '[{name}]' has no Flowfile formula equivalent")
    if "]" in name or "[" in name:
        raise _Untranslatable(f"malformed field reference '[{name}]'")
    return name


def _emit(node: _Node, numeric: bool = False) -> tuple[str, int]:
    """Render one node. ``numeric`` marks an arithmetic position, where an integer literal becomes a
    float so the expression computes in the floating type the caller's tool declared."""
    if isinstance(node, _Literal):
        if numeric and _FLOAT_LITERALS and _INTEGER_LITERAL_RE.fullmatch(node.text):
            return f"{node.text}.0", _PREC_ATOM
        return node.text, _PREC_ATOM
    if isinstance(node, _Field):
        _FIELDS.add(node.name)
        if node.name in _FLOAT_FIELDS:
            return f"{FLOAT_CAST}([{node.name}])", _PREC_ATOM
        return f"[{node.name}]", _PREC_ATOM
    if isinstance(node, _Unary):
        return f"-{_emit_child(node.operand, _PREC_UNARY, numeric=True)}", _PREC_UNARY
    if isinstance(node, _Not):
        return f"not({_emit_child(node.operand, _PREC_IF)})", _PREC_ATOM
    if isinstance(node, _Binary):
        if node.op == "=":
            # The Flowfile parser gives '=' maximum binding power, so compound operands need parens.
            return f"{_emit_child(node.left, _PREC_ATOM)} = {_emit_child(node.right, _PREC_ATOM)}", node.prec
        arithmetic = node.op in _ARITHMETIC_OPS
        left = _emit_child(node.left, node.prec, numeric=arithmetic)
        right = _emit_child(node.right, node.prec, right=True, numeric=arithmetic)
        return f"{left} {node.op} {right}", node.prec
    if isinstance(node, _If):
        parts = []
        for index, (condition, value) in enumerate(node.branches):
            keyword = "if" if index == 0 else "elseif"
            parts.append(f"{keyword} {_emit_child(condition, _PREC_OR)} then {_emit_child(value, _PREC_OR)}")
        parts.append(f"else {_emit_child(node.otherwise, _PREC_OR)} endif")
        return " ".join(parts), _PREC_IF
    if isinstance(node, _Call):
        return _emit_call(node)
    raise _Untranslatable("the expression contains a construct that cannot be converted")


def _emit_child(node: _Node, parent_prec: int, right: bool = False, numeric: bool = False) -> str:
    text, prec = _emit(node, numeric)
    if prec < parent_prec or (right and prec == parent_prec):
        return f"({text})"
    return text


def _emit_call(node: _Call) -> tuple[str, int]:
    low = node.name.lower()
    if low.startswith("regex_") and low not in FUNCTION_MAP:
        raise _Untranslatable(f"regular-expression function {node.name}() has no Flowfile formula equivalent")
    if low in REJECTED_FUNCTIONS:
        raise _Untranslatable(REJECTED_FUNCTIONS[low])
    spec = FUNCTION_MAP.get(low)
    if spec is None:
        raise _Untranslatable(f"Alteryx function {node.name}() has no verified Flowfile formula equivalent")
    count = len(node.args)
    if count < spec.min_args or (spec.max_args is not None and count > spec.max_args):
        raise _Untranslatable(f"Alteryx function {spec.alteryx_name}() expects {_arity_text(spec)} but got {count}")
    if spec.caveat is not None and spec.caveat not in _CAVEATS:
        _CAVEATS.append(spec.caveat)
    if spec.special == "iif":
        condition, when_true, when_false = node.args
        return (
            f"if {_emit_child(condition, _PREC_OR)} then {_emit_child(when_true, _PREC_OR)} "
            f"else {_emit_child(when_false, _PREC_OR)} endif"
        ), _PREC_IF
    if spec.special == "switch":
        return _emit_switch(node), _PREC_IF
    if spec.special == "replacechar":
        return _emit_replace_char(node), _PREC_ATOM
    if spec.special == "search":
        return _emit_search(node, spec), _PREC_ATOM
    if spec.special == "regexmatch":
        return _emit_regex_match(node), _PREC_ATOM
    if spec.special == "firstofmonth":
        return "start_of_month(today())", _PREC_ATOM
    if spec.special == "lastofmonth":
        return "end_of_month(today())", _PREC_ATOM
    if spec.special == "round":
        return _emit_round(node), _PREC_ATOM
    if spec.special == "datetimeadd":
        return _emit_datetime_add(node), _PREC_ATOM
    if spec.special == "datetimediff":
        return _emit_datetime_diff(node), _PREC_ATOM
    if spec.special == "datetimetrim":
        return _emit_datetime_trim(node), _PREC_ATOM
    if spec.special == "datetimeformat":
        return _emit_datetime_format(node), _PREC_ATOM
    if spec.special == "datetimeparse":
        return _emit_datetime_parse(node), _PREC_ATOM
    if spec.special == "isempty":
        # Alteryx IsEmpty() is true for null *and* the empty string; is_empty() only covers null.
        arg = node.args[0]
        rendered = _emit_child(arg, _PREC_IF)
        if isinstance(arg, _Call) and arg.name.lower() in _NON_STRING_FUNCTIONS:
            # A non-string operand cannot be the empty string, and `= ""` on it raises at run time.
            return f"is_empty({rendered})", _PREC_ATOM
        return f'(is_empty({rendered}) or {rendered} = "")', _PREC_ATOM
    numeric = low in _FLOAT_LITERAL_FUNCTIONS
    rendered_args = ", ".join(_emit_child(arg, _PREC_IF, numeric=numeric) for arg in node.args)
    return f"{spec.target}({rendered_args})", _PREC_ATOM


def _arity_text(spec: FunctionSpec) -> str:
    if spec.max_args is None:
        return f"at least {spec.min_args} argument(s)"
    if spec.min_args == spec.max_args:
        return f"{spec.min_args} argument(s)"
    return f"between {spec.min_args} and {spec.max_args} arguments"


def _literal_string(node: _Node, function_name: str, position: str) -> str:
    if not isinstance(node, _Literal) or not node.text.startswith('"'):
        raise _Untranslatable(
            f"{function_name}() can only be converted when the {position} argument is a literal string"
        )
    return node.text[1:-1]


def _emit_switch(node: _Call) -> str:
    """Switch(v, default, c1, r1, ...) becomes an if/elseif chain.

    A null value makes every emitted '=' comparison null, which falls through to the
    ELSE default — exactly Alteryx's 'null matches no case' behaviour.
    """
    value, default, *pairs = node.args
    if len(pairs) % 2:
        raise _Untranslatable("Switch() expects case/result pairs after the default, but one case has no result")
    branches = [
        (_Binary("=", value, case, _PREC_CMP), result) for case, result in zip(pairs[::2], pairs[1::2], strict=True)
    ]
    rendered, _ = _emit(_If(branches=branches, otherwise=default))
    return rendered


def _emit_replace_char(node: _Call) -> str:
    """ReplaceChar(x, chars, repl) nests one literal replace() per character.

    Alteryx replaces every character in ``chars`` with the *first* character of ``repl``
    (an empty ``repl`` deletes). Because all characters map to the same single target,
    the nested sequential replaces are equivalent to Alteryx's simultaneous pass.
    """
    chars = _literal_string(node.args[1], "ReplaceChar", "second")
    replacement = _literal_string(node.args[2], "ReplaceChar", "third")[:1]
    if not chars:
        raise _Untranslatable("ReplaceChar() with no characters to replace cannot be converted")
    rendered = _emit_child(node.args[0], _PREC_IF)
    for char in dict.fromkeys(chars):
        rendered = f'replace({rendered}, "{char}", "{replacement}")'
    return rendered


def _escape_regex(text: str) -> str:
    """Neutralise regex metacharacters as one-character classes, e.g. 'a.c' -> 'a[.]c'.

    A backslash escape would work too, but the formula parser resolves a string token through
    ``eval()``, so every emitted ``\\.`` would ride on Python's deprecated invalid-escape passthrough.
    Character classes stay literal on both sides. '[' and '^' are the two metacharacters a class cannot
    hold ('[[]' and '[^]' are both invalid), so a pattern containing either is rejected instead.
    """
    for char in text:
        if char in _REGEX_UNESCAPABLE:
            raise _Untranslatable(
                f"the search string contains {char!r}, a regular-expression character that the Flowfile "
                "contains() cannot be made to match literally"
            )
    return "".join(f"[{char}]" if char in _REGEX_META else char for char in text)


def _emit_search(node: _Call, spec: FunctionSpec) -> str:
    """Contains/StartsWith/EndsWith fold both operands to lower case, and Contains escapes its pattern.

    Alteryx searches case-insensitively unless the optional third argument turns that off (the 2-argument
    arity pin rejects the explicit form), while the target functions are case-sensitive — hence the
    ``lowercase()`` wrappers. Wrapping the text operand also pins contains() to its regex branch: given a
    bare string it falls back to a plain Python substring test, where escaping would be wrong.

    contains() reaches ``pl.Expr.str.contains`` without ``literal=True``, so an Alteryx literal substring
    is a regex here — 'a.c' matches 'abc' and an unbalanced '(' throws at run time. Its pattern is
    therefore required to be a literal string, which is what makes escaping possible.
    """
    text = f"lowercase({_emit_child(node.args[0], _PREC_IF)})"
    search = node.args[1]
    is_regex = spec.target == "contains"
    if isinstance(search, _Literal):
        # A simple-mode Filter renders a numeric operand unquoted; it has no case to fold.
        pattern = search.text[1:-1].lower() if search.text.startswith('"') else search.text
        return f'{spec.target}({text}, "{_escape_regex(pattern) if is_regex else pattern}")'
    if is_regex:
        raise _Untranslatable(
            f"{spec.alteryx_name}() can only be converted when the search argument is a literal string, "
            "because the Flowfile contains() reads its pattern as a regular expression"
        )
    return f"{spec.target}({text}, lowercase({_emit_child(search, _PREC_IF)}))"


def _emit_regex_match(node: _Call) -> str:
    """REGEX_Match(text, pattern[, case_insensitive]) is an *anchored* match, unlike Contains().

    Alteryx returns true only when the whole value matches, so the pattern is wrapped as
    ``^(?:pat)$`` — a non-capturing group, because ``^a|b$`` would otherwise anchor only one arm.
    The pattern is emitted verbatim rather than through ``_escape_regex``: here it really is a
    regular expression and neutralising its metacharacters would change what it matches. That is
    only safe because the tokenizer refuses any backslash inside a string literal, so no escape and
    no backreference can reach this at all — which leaves lookaround as the one unsupported
    construct a pattern could still spell out, and the engine below is what screens it.

    The text operand is *not* folded to lower case the way Contains() folds it. Alteryx documents
    ``REGEX_Match(String, pattern, icase)`` with "By default icase=1 (meaning ignore case)"
    (help.alteryx.com, string functions), so the match ignores case unless the third argument is a
    literal false or 0 — and that is expressed as the inline ``(?i)`` flag rather than by folding
    both operands, because folding would also change what the pattern's own character classes mean.
    """
    pattern_node = node.args[1]
    if not (isinstance(pattern_node, _Literal) and pattern_node.text.startswith('"')):
        raise _Untranslatable(
            "REGEX_Match() can only be converted when the pattern is a literal string, because a "
            "pattern built at run time cannot be checked for constructs Polars' regex engine lacks"
        )
    pattern = pattern_node.text[1:-1]
    if DUNDER_RE.search(pattern):
        raise _Untranslatable("the REGEX_Match() pattern contains a dunder pattern, which is rejected")
    ignore_case = True
    if len(node.args) == 3:
        case_insensitive = node.args[2]
        if not (isinstance(case_insensitive, _Literal) and case_insensitive.text.strip().lower() in _BOOLEAN_LITERALS):
            raise _Untranslatable(
                "REGEX_Match() can only be converted when its case-sensitivity argument is a literal true or false"
            )
        ignore_case = _BOOLEAN_LITERALS[case_insensitive.text.strip().lower()]
    flags = "(?i)" if ignore_case else ""
    anchored = f"{flags}^(?:{pattern})$"
    rejection = regex_rejection(anchored)
    if rejection is not None:
        label = unsupported_construct(anchored)
        if label is not None:
            raise _Untranslatable(
                f"the REGEX_Match() pattern uses {label}, which Polars' regex engine does not support"
            )
        raise _Untranslatable(f"Polars' regex engine rejected the REGEX_Match() pattern: {rejection}")
    return f'contains({_emit_child(node.args[0], _PREC_IF)}, "{anchored}")'


_BOOLEAN_LITERALS = {"1": True, "0": False, "true": True, "false": False, '"true"': True, '"false"': False}


def _emit_round(node: _Call) -> str:
    multiple = node.args[1]
    if not isinstance(multiple, _Literal):
        raise _Untranslatable(
            "Round() can only be converted when the second argument is a literal power of ten (1, 0.1, 0.01, ...)"
        )
    text = multiple.text
    if text == "1":
        digits = 0
    else:
        match = re.fullmatch(r"0?\.(0*)1", text)
        if match is None:
            raise _Untranslatable(
                f"Round() rounds to the nearest multiple of {text}, which has no Flowfile equivalent; "
                "only literal powers of ten (1, 0.1, 0.01, ...) can be converted"
            )
        digits = len(match.group(1)) + 1
    return f"round({_emit_child(node.args[0], _PREC_IF)}, {digits})"


def _literal_unit(node: _Node, function_name: str) -> tuple[str, str]:
    if not isinstance(node, _Literal) or not node.text.startswith('"'):
        raise _Untranslatable(
            f'{function_name}() can only be converted when the unit is a literal string such as "days"'
        )
    raw = node.text[1:-1]
    return raw, raw.strip().lower().rstrip("s")


def _emit_datetime_add(node: _Call) -> str:
    raw, unit = _literal_unit(node.args[2], "DateTimeAdd")
    target = _DATETIME_ADD_UNITS.get(unit)
    if target is None:
        raise _Untranslatable(f"DateTimeAdd() unit {raw!r} has no Flowfile formula equivalent")
    return f"{target}({_emit_child(node.args[0], _PREC_IF)}, {_emit_child(node.args[1], _PREC_IF)})"


def _emit_datetime_trim(node: _Call) -> str:
    raw, unit = _literal_unit(node.args[1], "DateTimeTrim")
    target = _DATETIME_TRIM_CALLS.get(unit)
    if target is not None:
        return f"{target}({_emit_child(node.args[0], _PREC_IF)})"
    if unit not in _DATETIME_TRIM_PARTS:
        raise _Untranslatable(f"DateTimeTrim() unit {raw!r} has no Flowfile formula equivalent")
    return f'date_trim({_emit_child(node.args[0], _PREC_IF)}, "{unit}")'


def _emit_datetime_diff(node: _Call) -> str:
    raw, unit = _literal_unit(node.args[2], "DateTimeDiff")
    target = _DATETIME_DIFF_UNITS.get(unit)
    if target is None:
        raise _Untranslatable(
            f"DateTimeDiff() unit {raw!r} has no Flowfile formula equivalent; "
            'only "days" and "seconds" can be converted'
        )
    return f"{target}({_emit_child(node.args[0], _PREC_IF)}, {_emit_child(node.args[1], _PREC_IF)})"


def _check_format_string(node: _Node, function_name: str, for_parse: bool) -> tuple[str, set[str]]:
    """Validate an Alteryx date-format literal as a chrono strftime string and pass it through unchanged.

    This is a whitelist of the codes verified to mean the same thing on both sides, not a translation
    table: every '%' must introduce one of ``_DATE_FORMAT_CODES``, so anything Alteryx-specific
    (ordinal suffixes, subsecond digits) or merely unverified (%e, %T, %U, %w, %z) is refused.
    ``%y`` is additionally refused when parsing, because chrono's 00-68/69-99 century pivot is not
    verified to match Alteryx's; formatting a two-digit year is unambiguous and stays allowed.

    Returns the format text and the set of codes it uses, which is what picks the parse target.
    """
    text = _literal_string(node, function_name, "second")
    codes: set[str] = set()
    i = 0
    while i < len(text):
        if text[i] != "%":
            i += 1
            continue
        code = text[i + 1 : i + 2]
        if for_parse and code == "y":
            raise _Untranslatable(
                f"{function_name}() cannot be converted with the two-digit year '%y', because the century "
                "it expands to is not verified to be the same in Alteryx and Flowfile; use '%Y' instead"
            )
        if code not in _DATE_FORMAT_CODES:
            offender = f"'%{code}'" if code else "a trailing '%'"
            raise _Untranslatable(
                f"{function_name}() format {text!r} uses {offender}, which is not one of the date-format "
                "codes verified to mean the same thing in Alteryx and Flowfile"
            )
        codes.add(code)
        i += 2
    return text, codes


def _emit_datetime_format(node: _Call) -> str:
    fmt, _ = _check_format_string(node.args[1], "DateTimeFormat", for_parse=False)
    return f'format_date({_emit_child(node.args[0], _PREC_IF)}, "{fmt}")'


def _emit_datetime_parse(node: _Call) -> str:
    """DateTimeParse reaches to_datetime() only when the format carries a time part, else to_date().

    Both targets pass ``strict=False`` down to ``str.to_date``/``str.to_datetime``, so a value the
    format does not match becomes null — which is what Alteryx's DateTimeParse does too.
    """
    fmt, codes = _check_format_string(node.args[1], "DateTimeParse", for_parse=True)
    target = "to_datetime" if codes & _PARSE_TIME_CODES else "to_date"
    return f'{target}({_emit_child(node.args[0], _PREC_IF)}, "{fmt}")'


def _clean_reason(reason: str) -> str:
    # Reasons land verbatim in a single-line `//` comment on the generated formula node.
    collapsed = re.sub(r"\s+", " ", reason).strip()
    return collapsed[:297] + "..." if len(collapsed) > 300 else collapsed


def try_translate(
    alteryx_expr: str,
    *,
    allowed_specials: frozenset[str] = frozenset(),
    known_columns: frozenset[str] = frozenset(),
    float_fields: frozenset[str] = frozenset(),
    float_literals: bool = False,
) -> TranslationOutcome:
    """Translate an Alteryx expression, or explain why it cannot be translated.

    ``allowed_specials`` names the `_..._` field references the caller binds itself — the
    Multi-Field Formula placeholders, which stay in the rendered formula as written instead
    of being rejected. Every other special field reference is still refused.

    ``known_columns`` are the column names the caller can vouch for, which is what lets an
    unbracketed field reference — Alteryx accepts `EXP(x/2)` for `EXP([x]/2)` — be resolved instead
    of refused. An empty set keeps the refusal, so a caller that does not know its input columns
    cannot accidentally turn a misspelled function name into a column.

    ``float_fields`` are the columns to read as Float64, for a caller whose tool declares a floating
    output type. Only a caller that knows a column really is numeric may name it: the cast is strict,
    so a text column in this set stops the flow at run time instead of computing something else.

    ``float_literals`` renders integer literals in arithmetic positions as floats, for that same
    caller. It needs no such promise — a literal's type is written in the expression — and it is
    what makes an expression with no column in it, `POW(2, 70)`, compute the way Alteryx does.
    """
    global _FLOAT_FIELDS, _FLOAT_LITERALS
    if not isinstance(alteryx_expr, str) or not alteryx_expr.strip():
        return TranslationOutcome(None, "the Alteryx expression is empty")
    _CAVEATS.clear()
    _FIELDS.clear()
    _FLOAT_FIELDS = frozenset(float_fields)
    _FLOAT_LITERALS = float_literals
    try:
        tokens = _tokenize(alteryx_expr)
        if not tokens:
            return TranslationOutcome(None, "the Alteryx expression is empty")
        rendered, _ = _emit(_Parser(tokens, allowed_specials, frozenset(known_columns)).parse())
    except _Untranslatable as exc:
        return TranslationOutcome(None, _clean_reason(exc.reason))
    except RecursionError:
        return TranslationOutcome(None, "the Alteryx expression is nested too deeply to convert")
    if not rendered.strip():
        return TranslationOutcome(None, "the Alteryx expression produced an empty Flowfile formula")
    try:
        simple_function_to_expr(rendered)
    except Exception as exc:  # the formula parser raises ExpressionSyntaxError, TypeError and IndexError
        return TranslationOutcome(
            None,
            _clean_reason(f"the converted formula {rendered!r} was rejected by the Flowfile formula parser: {exc}"),
        )
    return TranslationOutcome(rendered, None, list(_CAVEATS), frozenset(_FIELDS))
