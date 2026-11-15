"""Pure query matching and scoring: no database, no Flowfile imports.

A query is split into tokens; an item is a list of ``(field, text)`` pairs with its name first. An item matches when
every token is found in at least one of its fields, and it scores by how well each token matched (whole name, start
of the name, start of a word, anywhere) weighted by the field it matched in. Separators are interchangeable: a token
``sales-orders`` finds ``sales_orders`` and ``sales orders``.
"""

from __future__ import annotations

import re
import string
from dataclasses import dataclass

MAX_TOKENS = 8
MAX_MATCH_VALUE = 200

_TOKEN_SPLIT = re.compile(r"[\s.,;:/\\]+")
_TOKEN_TRIM = "\"'`*?"
_WORD_SEPARATORS = frozenset(" _-./:")
_NAME_SEPARATORS = re.compile(r"[\s_\-.]+")
_INNER_SEPARATORS = frozenset("_-")
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)

FIELD_WEIGHTS: dict[str, float] = {
    "name": 1.0,
    "id": 0.8,
    "path": 0.55,
    "flow": 0.5,
    "table": 0.5,
    "description": 0.35,
}
DEFAULT_FIELD_WEIGHT = 0.45

EXACT, PREFIX, WORD_START, SUBSTRING = 1.0, 0.85, 0.7, 0.5


def tokenize(query: str) -> list[str]:
    """Lower-cased words of ``query``: split on whitespace and ``. , ; : / \\``, quotes and ``* ?`` trimmed, words of
    only ``_`` and ``-`` dropped, deduplicated, at most ``MAX_TOKENS`` (later words are ignored)."""
    tokens: list[str] = []
    for raw in _TOKEN_SPLIT.split(query.strip().lower()):
        token = raw.strip(_TOKEN_TRIM)
        if token and _normalized(token) and token not in tokens:
            tokens.append(token)
    return tokens[:MAX_TOKENS]


def _normalized(text: str) -> str:
    return _NAME_SEPARATORS.sub(" ", text.lower()).strip()


def has_inner_separator(token: str) -> bool:
    """Whether ``token`` holds ``_`` or ``-``, so it also matches across ``.`` and spaces in the text."""
    return bool(_INNER_SEPARATORS & set(token))


def _fold(text: str, token: str) -> str:
    # An ASCII token is matched the way SQL's LIKE folds case, ASCII letters only, so the SQL filter keeps every row
    # the scorer accepts; any other token gets full Unicode lower-casing.
    return text.translate(_ASCII_LOWER) if token.isascii() else text.lower()


def _quality(token: str, text: str) -> float:
    if text == token:
        return EXACT
    if text.startswith(token):
        return PREFIX
    position = text.find(token)
    if position < 0:
        return 0.0
    while position >= 0:
        if text[position - 1] in _WORD_SEPARATORS:
            return WORD_START
        position = text.find(token, position + 1)
    return SUBSTRING


def match_quality(token: str, text: str) -> float:
    """How well ``token`` (lower case) matches ``text``: 1 for the whole text, down to 0.5 anywhere, 0 for no match.

    A token holding ``_`` or ``-`` also matches with every separator treated alike.
    """
    lowered = _fold(text, token)
    quality = _quality(token, lowered)
    if quality or not has_inner_separator(token):
        return quality
    return _quality(_normalized(token), _normalized(lowered))


@dataclass(frozen=True)
class Scored:
    score: float
    matches: tuple[tuple[str, str], ...]


def score_fields(tokens: list[str], fields: list[tuple[str, str | None]]) -> Scored | None:
    """Score an item whose first field is its name; ``None`` unless every token matches some field.

    The score is the mean over tokens of the best ``quality × field weight`` (0-100), plus 25 when the whole query is
    the name (separators ignored) and 10 when the name starts with it. ``matches`` lists each field that gave a
    token its best match, in field order.
    """
    present = [(field, text) for field, text in fields if text]
    if not tokens or not present:
        return None
    total = 0.0
    used: set[int] = set()
    for token in tokens:
        best, best_index = 0.0, -1
        for index, (field, text) in enumerate(present):
            value = match_quality(token, text) * FIELD_WEIGHTS.get(field, DEFAULT_FIELD_WEIGHT)
            if value > best:
                best, best_index = value, index
        if best_index < 0:
            return None
        total += best
        used.add(best_index)
    score = total / len(tokens) * 100
    name_field, name = fields[0]
    if name_field == "name" and name:
        whole, normalized_name = _normalized(" ".join(tokens)), _normalized(name)
        if normalized_name == whole:
            score += 25
        elif normalized_name.startswith(whole):
            score += 10
    matches = tuple((present[i][0], present[i][1][:MAX_MATCH_VALUE]) for i in sorted(used))
    return Scored(round(score, 3), matches)


def sql_narrowable(token: str) -> bool:
    """Whether a database can pre-filter by ``token``: SQLite folds case for ASCII only, and JSON text columns
    escape non-ASCII characters and quotes, so other tokens are left to ``score_fields``."""
    return token.isascii() and '"' not in token


def like_pattern(token: str) -> str:
    """A ``LIKE`` pattern (escape ``\\``) finding ``token`` anywhere, each run of ``_`` and ``-`` standing for any text.

    That makes the pattern a superset of what ``match_quality`` accepts: ``sales-orders`` finds ``sales_orders``,
    ``sales orders`` and ``sales__orders``.
    """
    escaped = token.replace("\\", "\\\\").replace("%", "\\%")
    return "%" + re.sub(r"[-_]+", "%", escaped) + "%"
