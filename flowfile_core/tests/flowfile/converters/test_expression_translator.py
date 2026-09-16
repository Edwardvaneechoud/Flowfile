import hashlib
import warnings
from datetime import date, datetime

import polars as pl
import pytest
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx.expression import (
    FUNCTION_MAP,
    FunctionSpec,
    TranslationOutcome,
    regex_rejection,
    try_translate,
    unsupported_construct,
)
from flowfile_core.schemas.transform_schema import MULTI_FIELD_PLACEHOLDERS

# (FUNCTION_MAP key, Alteryx expression, expected Flowfile formula)
FUNCTION_CASES: list[tuple[str, str, str]] = [
    ("iif", 'IIF([Amount] > 100, "big", "small")', 'if [Amount] > 100 then "big" else "small" endif'),
    (
        "switch",
        'Switch([Code], "other", 1, "one", 2, "two")',
        'if [Code] = 1 then "one" elseif [Code] = 2 then "two" else "other" endif',
    ),
    ("uppercase", "Uppercase([Name])", "uppercase([Name])"),
    ("lowercase", "Lowercase([Name])", "lowercase([Name])"),
    ("titlecase", "TitleCase([Name])", "titlecase([Name])"),
    ("left", "Left([Name], 3)", "left([Name], 3)"),
    ("right", "Right([Name], 3)", "right([Name], 3)"),
    ("substring", "Substring([Name], 0, 3)", "substring([Name], 0, 3)"),
    ("trim", "Trim([Name])", "trim([Name])"),
    ("trimleft", "TrimLeft([Name])", "left_trim([Name])"),
    ("trimright", "TrimRight([Name])", "right_trim([Name])"),
    ("length", "Length([Name])", "length([Name])"),
    # Alteryx searches case-insensitively by default, so both operands are folded to lower case.
    ("contains", 'Contains([Name], "abc")', 'contains(lowercase([Name]), "abc")'),
    # Anchored, and the pattern is a real regex — not escaped the way Contains() escapes its needle.
    # Alteryx documents icase=1 as the default, so a two-argument call ignores case.
    ("regex_match", 'REGEX_Match([Name], ".*west")', 'contains([Name], "(?i)^(?:.*west)$")'),
    ("startswith", 'StartsWith([Name], "abc")', 'starts_with(lowercase([Name]), "abc")'),
    ("endswith", 'EndsWith([Name], "abc")', 'ends_with(lowercase([Name]), "abc")'),
    ("findstring", 'FindString([Name], "-")', 'find_position([Name], "-")'),
    ("replace", 'Replace([Name], "a", "b")', 'replace([Name], "a", "b")'),
    ("replacechar", 'ReplaceChar([Name], "ab", "-")', 'replace(replace([Name], "a", "-"), "b", "-")'),
    ("padleft", 'PadLeft([Name], 5, "0")', 'pad_left([Name], 5, "0")'),
    ("padright", 'PadRight([Name], 5, "0")', 'pad_right([Name], 5, "0")'),
    ("isnull", "IsNull([Amount])", "is_empty([Amount])"),
    # Alteryx IsEmpty() is true for null *and* the empty string; is_empty() only covers null.
    ("isempty", "IsEmpty([Name])", '(is_empty([Name]) or [Name] = "")'),
    ("reversestring", "ReverseString([Name])", "reverse([Name])"),
    ("tonumber", "ToNumber([Name])", "to_number([Name])"),
    ("tostring", "ToString([Amount])", "to_string([Amount])"),
    ("md5_utf8", "MD5_UTF8([Name])", "md5([Name])"),
    ("base64encode", "Base64Encode([Name])", "base64_encode([Name])"),
    ("base64decode", "Base64Decode([Name])", "base64_decode([Name])"),
    ("abs", "Abs([Amount])", "abs([Amount])"),
    ("ceil", "Ceil([Amount])", "ceil([Amount])"),
    ("floor", "Floor([Amount])", "floor([Amount])"),
    ("sqrt", "Sqrt([Amount])", "sqrt([Amount])"),
    ("exp", "Exp([Amount])", "exp([Amount])"),
    ("log", "Log([Amount])", "log([Amount])"),
    ("log10", "Log10([Amount])", "log10([Amount])"),
    ("pow", "Pow([Amount], 2)", "power([Amount], 2)"),
    ("mod", "Mod([Amount], 2)", "mod([Amount], 2)"),
    ("sin", "Sin([Amount])", "sin([Amount])"),
    ("cos", "Cos([Amount])", "cos([Amount])"),
    ("tan", "Tan([Amount])", "tan([Amount])"),
    ("asin", "ASin([Amount])", "asin([Amount])"),
    ("acos", "ACos([Amount])", "acos([Amount])"),
    ("atan", "ATan([Amount])", "atan([Amount])"),
    ("min", "Min([Amount], 10)", "least([Amount], 10)"),
    ("max", "Max([Amount], 10, 20)", "greatest([Amount], 10, 20)"),
    ("round", "Round([Amount], 0.01)", "round([Amount], 2)"),
    ("datetimeadd", 'DateTimeAdd([OrderDate], 3, "days")', "add_days([OrderDate], 3)"),
    ("datetimediff", 'DateTimeDiff([ShipDate], [OrderDate], "days")', "date_diff_days([ShipDate], [OrderDate])"),
    ("datetimeyear", "DateTimeYear([OrderDate])", "year([OrderDate])"),
    ("datetimemonth", "DateTimeMonth([OrderDate])", "month([OrderDate])"),
    ("datetimeday", "DateTimeDay([OrderDate])", "day([OrderDate])"),
    ("datetimehour", "DateTimeHour([OrderDate])", "hour([OrderDate])"),
    ("datetimeminute", "DateTimeMinute([OrderDate])", "minute([OrderDate])"),
    ("datetimeminutes", "DateTimeMinutes([OrderDate])", "minute([OrderDate])"),
    ("datetimesecond", "DateTimeSecond([OrderDate])", "second([OrderDate])"),
    ("datetimeseconds", "DateTimeSeconds([OrderDate])", "second([OrderDate])"),
    ("datetimenow", "DateTimeNow()", "now()"),
    ("datetimetoday", "DateTimeToday()", "today()"),
    ("datetimefirstofmonth", "DateTimeFirstOfMonth()", "start_of_month(today())"),
    ("datetimelastofmonth", "DateTimeLastOfMonth()", "end_of_month(today())"),
    ("datetimetrim", 'DateTimeTrim([OrderDate], "month")', 'date_trim([OrderDate], "month")'),
    # Re-run case-swapped below, so both casings of each code must stay whitelisted (%y is parse-rejected).
    ("datetimeformat", 'DateTimeFormat([OrderDate], "%Y-%m")', 'format_date([OrderDate], "%Y-%m")'),
    ("datetimeparse", 'DateTimeParse([DateText], "%m-%b")', 'to_date([DateText], "%m-%b")'),
    ("year", "Year([OrderDate])", "year([OrderDate])"),
    ("month", "Month([OrderDate])", "month([OrderDate])"),
    ("day", "Day([OrderDate])", "day([OrderDate])"),
    ("hour", "Hour([OrderDate])", "hour([OrderDate])"),
    ("minute", "Minute([OrderDate])", "minute([OrderDate])"),
    ("second", "Second([OrderDate])", "second([OrderDate])"),
]

EXTRA_FUNCTION_CASES: list[tuple[str, str]] = [
    ("Round([Amount], 1)", "round([Amount], 0)"),
    ("Round([Amount], 0.001)", "round([Amount], 3)"),
    ('REGEX_Match([Name], "a.c", 1)', 'contains([Name], "(?i)^(?:a.c)$")'),
    ('REGEX_Match([Name], "a.c", 0)', 'contains([Name], "^(?:a.c)$")'),
    ('REGEX_Match(Uppercase([Region]), ".*WEST")', 'contains(uppercase([Region]), "(?i)^(?:.*WEST)$")'),
    ('DateTimeAdd([OrderDate], 1, "years")', "add_years([OrderDate], 1)"),
    ('DateTimeAdd([OrderDate], 1, "month")', "add_months([OrderDate], 1)"),
    ('DateTimeAdd([OrderDate], 2, "weeks")', "add_weeks([OrderDate], 2)"),
    ('DateTimeAdd([OrderDate], 2, "hours")', "add_hours([OrderDate], 2)"),
    ('DateTimeAdd([OrderDate], 2, "minutes")', "add_minutes([OrderDate], 2)"),
    ('DateTimeAdd([OrderDate], 2, "seconds")', "add_seconds([OrderDate], 2)"),
    (
        'DateTimeDiff([ShipDate], [OrderDate], "seconds")',
        "datetime_diff_seconds([ShipDate], [OrderDate])",
    ),
    ('DateTimeFormat([OrderDate], "%d/%m/%Y")', 'format_date([OrderDate], "%d/%m/%Y")'),
    ('DateTimeFormat([OrderDate], "%A %d %B %Y")', 'format_date([OrderDate], "%A %d %B %Y")'),
    ('DateTimeFormat([OrderDate], "%I:%M %p")', 'format_date([OrderDate], "%I:%M %p")'),
    # Formatting a two-digit year is unambiguous, so %y is whitelisted in this direction only.
    ('DateTimeFormat([OrderDate], "%d-%b-%y")', 'format_date([OrderDate], "%d-%b-%y")'),
    ('DateTimeParse([DateText], "%d/%m/%Y")', 'to_date([DateText], "%d/%m/%Y")'),
    ('DateTimeParse([DateText], "%Y-%m-%d %H:%M:%S")', 'to_datetime([DateText], "%Y-%m-%d %H:%M:%S")'),
    ('DateTimeParse([DateText], "%d %b %Y %I:%M %p")', 'to_datetime([DateText], "%d %b %Y %I:%M %p")'),
    # '%%' is an escaped percent, not a time code, so this stays on the to_date() branch.
    ('DateTimeParse([DateText], "%d/%m/%Y 100%%H")', 'to_date([DateText], "%d/%m/%Y 100%%H")'),
    ('DateTimeTrim([OrderDate], "firstofmonth")', "start_of_month([OrderDate])"),
    ('DateTimeTrim([OrderDate], "lastofmonth")', "end_of_month([OrderDate])"),
    ('DateTimeTrim([OrderDate], "day")', 'date_trim([OrderDate], "day")'),
    ("Switch([A] + 1, 0, 2, 20)", "if ([A] + 1) = 2 then 20 else 0 endif"),
    ('ReplaceChar([Name], "aab", "xy")', 'replace(replace([Name], "a", "x"), "b", "x")'),
    ('ReplaceChar([Name], "-", "")', 'replace([Name], "-", "")'),
    # Contains() reaches a regex engine, so a literal search string is lower-cased and escaped.
    ('Contains([Name], "A.C")', 'contains(lowercase([Name]), "a[.]c")'),
    ('Contains([Price], "$")', 'contains(lowercase([Price]), "[$]")'),
    ('Contains([Name], "(")', 'contains(lowercase([Name]), "[(]")'),
    ('Contains([Name], "Smith & Co")', 'contains(lowercase([Name]), "smith & co")'),
    ('Contains(Uppercase([Name]), "AB")', 'contains(lowercase(uppercase([Name])), "ab")'),
    # StartsWith/EndsWith are literal in the target, so a non-literal search only needs the case fold.
    ("StartsWith([Name], [Prefix])", "starts_with(lowercase([Name]), lowercase([Prefix]))"),
    ("EndsWith([Name], [Suffix])", "ends_with(lowercase([Name]), lowercase([Suffix]))"),
    ('StartsWith([Name], "SM")', 'starts_with(lowercase([Name]), "sm")'),
    ('EndsWith([Name], "CO.")', 'ends_with(lowercase([Name]), "co.")'),
    # A simple-mode Filter renders a numeric operand unquoted.
    ("Contains([OrderID], 2024)", 'contains(lowercase([OrderID]), "2024")'),
    ("Contains([Price], 1.50)", 'contains(lowercase([Price]), "1[.]50")'),
    # IsEmpty() over a provably non-string call drops the '= ""' arm, which would raise at run time.
    ('IsEmpty(DateTimeParse([D], "%m/%d/%Y"))', 'is_empty(to_date([D], "%m/%d/%Y"))'),
    ("IsEmpty(ToNumber([X]))", "is_empty(to_number([X]))"),
    ("IsEmpty(Trim([Name]))", '(is_empty(trim([Name])) or trim([Name]) = "")'),
]

OPERATOR_CASES: list[tuple[str, str]] = [
    ("[Amount] + 1", "[Amount] + 1"),
    ("[Amount] - 1", "[Amount] - 1"),
    ("[Amount] * 2", "[Amount] * 2"),
    ("[Amount] / 2", "[Amount] / 2"),
    ("[Amount] = 1", "[Amount] = 1"),
    ("[Amount] == 1", "[Amount] = 1"),
    ("[Amount] != 1", "[Amount] != 1"),
    ("[Amount] <> 1", "[Amount] != 1"),
    ("[Amount] > 1", "[Amount] > 1"),
    ("[Amount] < 1", "[Amount] < 1"),
    ("[Amount] >= 1", "[Amount] >= 1"),
    ("[Amount] <= 1", "[Amount] <= 1"),
    ("[Flag] AND [Other]", "[Flag] and [Other]"),
    ("[Flag] OR [Other]", "[Flag] or [Other]"),
    ("[Flag] && [Other]", "[Flag] and [Other]"),
    ("[Flag] || [Other]", "[Flag] or [Other]"),
    ("-[Amount]", "-[Amount]"),
    ("[Amount] - -1", "[Amount] - -1"),
    ("NOT [Flag]", "not([Flag])"),
    ("NOT([Flag])", "not([Flag])"),
    ("!([Flag])", "not([Flag])"),
    ("![Flag]", "not([Flag])"),
    ("[A] = 1 AND NOT [B]", "[A] = 1 and not([B])"),
    ("NOT IsNull([Amount])", "not(is_empty([Amount]))"),
    # `!` binds tighter than a comparison in Alteryx, so the NOT applies to the field alone.
    ("NOT [Flag] = 1", "not([Flag]) = 1"),
]

PRECEDENCE_CASES: list[tuple[str, str]] = [
    ("[Amount] + 1 * 2", "[Amount] + 1 * 2"),
    ("([Amount] + 1) * 2", "([Amount] + 1) * 2"),
    ("(([Amount] + 1) * 2) - 3", "([Amount] + 1) * 2 - 3"),
    ("[Amount] - ([Other] - [Third])", "[Amount] - ([Other] - [Third])"),
    ("[Amount] - [Other] - [Third]", "[Amount] - [Other] - [Third]"),
    ("[A] > 1 AND [B] < 3 OR [C] = 2", "[A] > 1 and [B] < 3 or [C] = 2"),
    ("[A] > 1 AND ([B] < 3 OR [C] = 2)", "[A] > 1 and ([B] < 3 or [C] = 2)"),
    ("[A] + 1 > 2", "[A] + 1 > 2"),
    ("[A] = [B] + 1", "[A] = ([B] + 1)"),
    ("[A] + 1 = [B]", "([A] + 1) = [B]"),
    ("[A] * 2 == [B] - 3", "([A] * 2) = ([B] - 3)"),
    ("IIF([A] = 1, 1, 2) + 1", "(if [A] = 1 then 1 else 2 endif) + 1"),
    ("Uppercase(IIF([A] = 1, [B], [C]))", "uppercase(if [A] = 1 then [B] else [C] endif)"),
    ("Left(Trim([Name]), 3)", "left(trim([Name]), 3)"),
]

CONDITIONAL_CASES: list[tuple[str, str]] = [
    (
        'IF [A] = 1 THEN "one" ELSE "many" ENDIF',
        'if [A] = 1 then "one" else "many" endif',
    ),
    (
        'IF [A] = 1 THEN "one" ELSEIF [A] = 2 THEN "two" ELSE "many" ENDIF',
        'if [A] = 1 then "one" elseif [A] = 2 then "two" else "many" endif',
    ),
    (
        'if [A]=1 then "a" elseif [A]=2 then "b" elseif [A]=3 then "c" else "d" endif',
        'if [A] = 1 then "a" elseif [A] = 2 then "b" elseif [A] = 3 then "c" else "d" endif',
    ),
    (
        'IF [A] = 1 THEN "one" ELSE IF [A] = 2 THEN "two" ELSE "many" ENDIF ENDIF',
        'if [A] = 1 then "one" else (if [A] = 2 then "two" else "many" endif) endif',
    ),
    (
        'IIF([A] > 0, IIF([B] > 0, "both", "a"), "none")',
        'if [A] > 0 then (if [B] > 0 then "both" else "a" endif) else "none" endif',
    ),
]

LITERAL_CASES: list[tuple[str, str]] = [
    ("42", "42"),
    ("1.50", "1.50"),
    ("-7", "-7"),
    ('"hello"', '"hello"'),
    ("'hello'", '"hello"'),
    ('"it\'s here"', '"it\'s here"'),
    ("True", "true"),
    ("FALSE", "false"),
    ("[Field With Spaces]", "[Field With Spaces]"),
    ("[Field With Spaces] * 2", "[Field With Spaces] * 2"),
    ('Uppercase([Field With Spaces]) + "!"', 'uppercase([Field With Spaces]) + "!"'),
]

REJECTED_CASES: list[tuple[str, str]] = [
    # Flowfile's `in` is substring containment, not set membership, so IN can never be translated.
    ("in-operator", '[Status] IN ("a", "b")'),
    ("datetimetrim-unsupported-unit", 'DateTimeTrim([D], "fortnight")'),
    ("datetimetrim-non-literal-unit", "DateTimeTrim([D], [Unit])"),
    ("null-literal", "NULL"),
    ("null-call", "Null()"),
    ("null-in-expression", "[Amount] = NULL()"),
    ("regex-replace", 'REGEX_Replace([Name], "a", "b")'),
    # A pattern built at run time cannot be screened for what Polars' regex engine lacks.
    ("regex-match-non-literal-pattern", "REGEX_Match([Name], [Pattern])"),
    ("regex-match-lookahead", 'REGEX_Match([Name], "(?=a)b")'),
    ("regex-match-lookbehind", 'REGEX_Match([Name], "(?<a)b")'),
    ("regex-match-negative-lookahead", 'REGEX_Match([Name], "(?!a)b")'),
    ("regex-match-non-literal-case-flag", 'REGEX_Match([Name], "a", [Flag])'),
    # Legal Perl that Rust's regex crate has no support for: a substring blocklist passes both,
    # and the engine says "unrecognized flag" only when the flow collects.
    ("regex-match-atomic-group", 'REGEX_Match([Name], "(?>ab)c")'),
    ("regex-match-inline-comment", 'REGEX_Match([Name], "(?#c)abc")'),
    ("regex-match-unclosed-group", 'REGEX_Match([Name], "(ab")'),
    ("regex-match-backwards-repetition", 'REGEX_Match([Name], "a{2,1}")'),
    ("unknown-function", "Frobnicate([Name])"),
    ("row-offset-reference", "[Row-1:Amount]"),
    ("current-field-reference", "[_CurrentField_]"),
    ("round-non-power-of-ten", "Round([Amount], 7)"),
    ("round-fractional-multiple", "Round([Amount], 0.3)"),
    ("datetimeadd-non-literal-unit", "DateTimeAdd([Date], [N], [Unit])"),
    ("datetimediff-unsupported-unit", 'DateTimeDiff([A], [B], "months")'),
    ("unterminated-string", '"unterminated'),
    ("escaped-quote", "'a''b'"),
    ("backslash-escape", '"a\\"b"'),
    ("trailing-garbage", "[Amount] + 1 garbage"),
    ("empty-string", ""),
    ("whitespace-only", "   "),
    ("arity-too-few", "Left([Name])"),
    ("arity-too-many", "Left([Name], 1, 2)"),
    ("unbracketed-field", "Amount + 1"),
    ("power-operator", "[Amount] ^ 2"),
    ("rowcount", "RowCount()"),
    # Only MD5_UTF8 hashes the same bytes as Flowfile's md5(); the other two encodings differ.
    ("md5-ascii", "MD5_ASCII([Name])"),
    ("md5-unicode", "MD5_UNICODE([Name])"),
    # Only the format codes verified byte-identical in both dialects pass the whitelist.
    ("datetimeformat-unverified-code", 'DateTimeFormat([D], "%e %b %Y")'),
    ("datetimeformat-trailing-percent", 'DateTimeFormat([D], "%Y-%m-%")'),
    ("datetimeformat-non-literal-format", "DateTimeFormat([D], [Fmt])"),
    ("datetimeformat-language-argument", 'DateTimeFormat([D], "%d/%m/%Y", "English")'),
    ("datetimeparse-unverified-code", 'DateTimeParse([D], "%Y-%m-%d %T")'),
    # chrono's two-digit-year century pivot is not verified to match Alteryx's.
    ("datetimeparse-two-digit-year", 'DateTimeParse([D], "%d/%m/%y")'),
    ("datetimeparse-non-literal-format", "DateTimeParse([D], [Fmt])"),
    ("datetimeparse-language-argument", 'DateTimeParse([D], "%d/%m/%Y", "English")'),
    ("if-without-else", 'IF [A] = 1 THEN "one" ENDIF'),
    ("unbalanced-parenthesis", "([Amount] + 1"),
    ("dangling-operator", "[Amount] +"),
    ("empty-field-reference", "[]"),
    ("unterminated-field-reference", "[Amount"),
    ("unsupported-character", "[Amount] @ 1"),
    ("string-concat-of-nested-quotes", "'he said \"hi\"'"),
    ("switch-odd-arguments", 'Switch([Code], "other", 1, "one", 2)'),
    ("replacechar-non-literal-chars", 'ReplaceChar([Name], [Chars], "-")'),
    ("replacechar-empty-chars", 'ReplaceChar([Name], "", "-")'),
    # A column pattern cannot be regex-escaped at translation time.
    ("contains-non-literal-search", "Contains([Name], [Other])"),
    # '[' and '^' are the metacharacters a one-character class cannot neutralise.
    ("contains-open-bracket", 'Contains([Name], "[tag]")'),
    ("contains-caret", 'Contains([Name], "a^b")'),
    # The explicit CaseInsensitive argument asks for semantics the emitted lowercase() fold cannot express.
    ("contains-explicit-case-flag", 'Contains([Name], "abc", 0)'),
    ("startswith-explicit-case-flag", 'StartsWith([Name], "abc", 0)'),
    ("endswith-explicit-case-flag", 'EndsWith([Name], "abc", 0)'),
]


def _assert_reparses(formula: str) -> None:
    simple_function_to_expr(formula)


def test_translation_outcome_shape():
    outcome = try_translate("[Amount] + 1")
    assert isinstance(outcome, TranslationOutcome)
    assert outcome.translated == "[Amount] + 1"
    assert outcome.reason is None


def test_every_function_map_entry_has_a_case():
    covered = {key for key, _, _ in FUNCTION_CASES}
    assert covered == set(FUNCTION_MAP), f"uncovered FUNCTION_MAP keys: {sorted(set(FUNCTION_MAP) - covered)}"


@pytest.mark.parametrize(("key", "alteryx", "expected"), FUNCTION_CASES, ids=[c[0] for c in FUNCTION_CASES])
def test_function_map_entries_translate_and_reparse(key: str, alteryx: str, expected: str):
    outcome = try_translate(alteryx)
    assert outcome.translated is not None, f"{key}: {outcome.reason}"
    assert outcome.reason is None
    assert outcome.translated == expected
    _assert_reparses(outcome.translated)


@pytest.mark.parametrize(("alteryx", "expected"), EXTRA_FUNCTION_CASES)
def test_extra_function_variants(alteryx: str, expected: str):
    outcome = try_translate(alteryx)
    assert outcome.translated == expected, outcome.reason
    _assert_reparses(outcome.translated)


@pytest.mark.parametrize("alteryx", [c[1] for c in FUNCTION_CASES])
def test_function_names_are_case_insensitive(alteryx: str):
    assert try_translate(alteryx.upper()).translated is not None
    assert try_translate(alteryx.lower()).translated is not None


@pytest.mark.parametrize(("alteryx", "expected"), OPERATOR_CASES)
def test_operators(alteryx: str, expected: str):
    outcome = try_translate(alteryx)
    assert outcome.translated == expected, outcome.reason
    _assert_reparses(outcome.translated)


@pytest.mark.parametrize(("alteryx", "expected"), PRECEDENCE_CASES)
def test_precedence_and_parenthesization(alteryx: str, expected: str):
    outcome = try_translate(alteryx)
    assert outcome.translated == expected, outcome.reason
    _assert_reparses(outcome.translated)


@pytest.mark.parametrize(("alteryx", "expected"), CONDITIONAL_CASES)
def test_conditionals(alteryx: str, expected: str):
    outcome = try_translate(alteryx)
    assert outcome.translated == expected, outcome.reason
    _assert_reparses(outcome.translated)


@pytest.mark.parametrize(("alteryx", "expected"), LITERAL_CASES)
def test_literals_and_field_references(alteryx: str, expected: str):
    outcome = try_translate(alteryx)
    assert outcome.translated == expected, outcome.reason
    _assert_reparses(outcome.translated)


def test_translated_output_is_stable_under_retranslation():
    for _, alteryx, expected in FUNCTION_CASES:
        assert try_translate(alteryx).translated == expected


@pytest.mark.parametrize(("case_id", "alteryx"), REJECTED_CASES, ids=[c[0] for c in REJECTED_CASES])
def test_fail_closed(case_id: str, alteryx: str):
    outcome = try_translate(alteryx)
    assert outcome.translated is None, f"{case_id} unexpectedly translated to {outcome.translated!r}"
    assert outcome.reason
    assert outcome.reason.strip()


@pytest.mark.parametrize(("case_id", "alteryx"), REJECTED_CASES, ids=[c[0] for c in REJECTED_CASES])
def test_rejection_reasons_are_single_line_and_comment_safe(case_id: str, alteryx: str):
    # Reasons are embedded verbatim in a `//` comment on the generated formula node.
    reason = try_translate(alteryx).reason
    assert "\n" not in reason and "\r" not in reason
    assert len(reason) <= 300


def test_in_reason_mentions_the_in_operator():
    assert "IN" in try_translate('[Status] IN ("a")').reason


def test_unknown_function_reason_names_the_function():
    assert "Frobnicate" in try_translate("Frobnicate([Name])").reason


def test_arity_reason_names_the_function():
    reason = try_translate("Left([Name])").reason
    assert "Left" in reason and "2" in reason


def test_none_and_non_string_input_fail_closed():
    assert try_translate(None).translated is None  # type: ignore[arg-type]
    assert try_translate(None).reason  # type: ignore[arg-type]


def test_equality_with_compound_operands_evaluates_as_a_comparison():
    """polars_expr_transformer gives '=' maximum binding power, so without operand parens
    '[A] = [B] + 1' silently evaluates as ([A] = [B]) + 1 and yields numbers."""
    expr = simple_function_to_expr(try_translate("[A] = [B] + 1").translated)
    values = pl.DataFrame({"A": [2, 3], "B": [1, 3]}).select(expr.alias("out"))["out"].to_list()
    assert values == [True, False]


def _evaluate(alteryx: str, frame: pl.DataFrame) -> list:
    outcome = try_translate(alteryx)
    assert outcome.translated is not None, outcome.reason
    return frame.select(simple_function_to_expr(outcome.translated).alias("out"))["out"].to_list()


def test_contains_search_string_is_matched_literally_not_as_a_regex():
    """The target contains() reaches pl.Expr.str.contains without literal=True, so an unescaped
    Alteryx search string would be read as a regex: '$' matched every row and 'a.c' matched 'abc'."""
    frame = pl.DataFrame({"Name": ["a$b", "abc", "a.c", "plain"]})
    assert _evaluate('Contains([Name], "$")', frame) == [True, False, False, False]
    assert _evaluate('Contains([Name], "a.c")', frame) == [False, False, True, False]


def test_contains_unbalanced_metacharacter_evaluates_instead_of_raising():
    # The fail-closed gate only parses, so an unescaped '(' reaches polars' regex compiler.
    frame = pl.DataFrame({"Name": ["x(y", "plain"]})
    assert _evaluate('Contains([Name], "(")', frame) == [True, False]


@pytest.mark.parametrize(
    ("alteryx", "expected"),
    [
        ('Contains([Name], "MITH")', [True, True, False]),
        ('StartsWith([Name], "smith")', [True, True, False]),
        ('EndsWith([Name], "CO")', [True, True, False]),
    ],
)
def test_search_functions_are_case_insensitive_like_alteryx(alteryx: str, expected: list):
    # Alteryx defaults these to case-insensitive; the target functions are case-sensitive.
    frame = pl.DataFrame({"Name": ["Smith & Co", "SMITH & CO", "Jones Ltd"]})
    assert _evaluate(alteryx, frame) == expected


def test_datetime_format_and_parse_evaluate_against_real_columns():
    """format_date reaches dt.to_string (chrono strftime) and to_date reaches str.to_date with
    strict=False, so a value the format does not match becomes null, like Alteryx's DateTimeParse."""
    frame = pl.DataFrame(
        {"D": [date(2021, 3, 15), date(2020, 12, 1)], "S": ["15/03/2021", "not a date"]},
    )
    assert _evaluate('DateTimeFormat([D], "%d/%m/%Y")', frame) == ["15/03/2021", "01/12/2020"]
    assert _evaluate('DateTimeFormat([D], "%b %Y")', frame) == ["Mar 2021", "Dec 2020"]
    assert _evaluate('DateTimeParse([S], "%d/%m/%Y")', frame) == [date(2021, 3, 15), None]


def test_datetimeparse_with_a_time_code_yields_a_datetime():
    frame = pl.DataFrame({"S": ["2021-03-15 14:30:00"]})
    assert _evaluate('DateTimeParse([S], "%Y-%m-%d %H:%M:%S")', frame) == [datetime(2021, 3, 15, 14, 30)]


def test_unverified_format_code_rejection_names_the_code():
    assert "%T" in try_translate('DateTimeParse([D], "%Y-%m-%d %T")').reason
    assert "%e" in try_translate('DateTimeFormat([D], "%e %b %Y")').reason


def test_two_digit_year_is_rejected_only_in_the_parse_direction():
    assert try_translate('DateTimeFormat([D], "%d-%b-%y")').translated == 'format_date([D], "%d-%b-%y")'
    assert "%y" in try_translate('DateTimeParse([D], "%d-%b-%y")').reason


def test_startswith_with_a_column_search_is_case_insensitive():
    frame = pl.DataFrame({"Name": ["Smith & Co", "Jones Ltd"], "Prefix": ["SMITH", "smith"]})
    assert _evaluate("StartsWith([Name], [Prefix])", frame) == [True, False]


_SEARCHABLE_ASCII = [chr(c) for c in range(32, 127) if chr(c) not in '"\\[^']


@pytest.mark.parametrize("char", _SEARCHABLE_ASCII)
def test_every_printable_ascii_search_character_matches_itself(char: str):
    # The four excluded chars are tokenizer-unreachable or covered by REJECTED_CASES.
    frame = pl.DataFrame({"Name": [f"x{char}y", "§§§"]})
    assert _evaluate(f'Contains([Name], "{char}")', frame) == [True, False]


def test_escaped_search_patterns_do_not_rely_on_python_escape_sequences():
    """The formula parser resolves a string token through eval(), so a backslash-escaped pattern would
    ride on Python's deprecated invalid-escape passthrough. Character-class escaping keeps it clean."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        warnings.simplefilter("error", SyntaxWarning)
        for char in _SEARCHABLE_ASCII:
            translated = try_translate(f'Contains([Name], "{char}")').translated
            assert translated is not None and "\\" not in translated
            _assert_reparses(translated)


def test_unary_minus_over_parenthesised_expression_is_rejected_by_verification():
    # The Flowfile formula parser cannot handle `-( ... )`; the verification stage must catch it.
    outcome = try_translate("-([Amount] + 1)")
    assert outcome.translated is None
    assert "formula parser" in outcome.reason


def test_deeply_nested_expression_fails_closed_instead_of_raising():
    outcome = try_translate("Abs(" * 300 + "[Amount]" + ")" * 300)
    assert outcome.translated is None
    assert "deeply" in outcome.reason


def test_multiline_alteryx_expression_translates():
    outcome = try_translate('IF [A] = 1\n  THEN "one"\n  ELSE "many"\nENDIF')
    assert outcome.translated == 'if [A] = 1 then "one" else "many" endif'
    _assert_reparses(outcome.translated)


def test_comment_wrapped_untranslated_body_still_parses():
    # The mapper wraps an untranslated formula in `//` comments plus an identity stub.
    outcome = try_translate('REGEX_Replace([Name], "a", "b")')
    body = f'// could not be converted: {outcome.reason}\n// Original: REGEX_Replace([Name], "a", "b")\n[Name]'
    _assert_reparses(body)


def test_isempty_of_a_parsed_date_evaluates_without_a_string_comparison():
    """The '= ""' arm on a date expression raises 'cannot compare date to string' at run time
    (2026 Grand Prix Round 1 regression), so a non-string operand gets plain is_empty()."""
    outcome = try_translate('IsEmpty(DateTimeParse([D], "%m/%d/%Y"))')
    assert outcome.translated == 'is_empty(to_date([D], "%m/%d/%Y"))'
    expr = simple_function_to_expr(outcome.translated)
    values = pl.DataFrame({"D": ["03/15/2021", "not a date"]}).select(expr.alias("o"))["o"].to_list()
    assert values == [False, True]


def test_md5_utf8_evaluates_to_the_alteryx_documented_digest():
    """Alteryx MD5_UTF8 hashes the UTF-8 bytes of the text, which is exactly what md5() hashes.
    'Lá' -> 68f0... is the worked example from Alteryx's own String Functions page."""
    frame = pl.DataFrame({"Name": ["John", "Lá"]})
    assert _evaluate("MD5_UTF8([Name])", frame) == [
        hashlib.md5(b"John").hexdigest(),
        "68f00289dc3be140b1dfd4e031d733f1",
    ]


def test_md5_ascii_and_unicode_stay_fail_closed_because_they_hash_other_bytes():
    """Alteryx documents Md5_Ascii('Lá') as the digest of the narrow bytes and Md5_Unicode('Lá')
    as the digest of the UTF-16LE bytes — neither equals the UTF-8 digest md5() would produce."""
    documented = {
        "latin-1": "0c0ee86cc87d87125ad8923562be952e",  # Md5_Ascii
        "utf-16-le": "aa9969dfcca04249842cc457e5b3dd01",  # Md5_Unicode
        "utf-8": "68f00289dc3be140b1dfd4e031d733f1",  # Md5_Utf8, the only one md5() reproduces
    }
    for encoding, digest in documented.items():
        assert hashlib.md5("Lá".encode(encoding)).hexdigest() == digest

    for expression in ("MD5_ASCII([Name])", "MD5_UNICODE([Name])"):
        outcome = try_translate(expression)
        assert outcome.translated is None
        assert "MD5_UTF8" in outcome.reason


MULTI_FIELD_SPECIALS = frozenset(MULTI_FIELD_PLACEHOLDERS)


@pytest.mark.parametrize(
    "expression",
    [*(f"[{placeholder}]" for placeholder in MULTI_FIELD_PLACEHOLDERS), "[_currentfield_] + 1"],
)
def test_allowed_specials_keep_the_multi_field_placeholders_verbatim(expression: str):
    """The multi-field mapper binds these itself, so the translator must pass them through as written."""
    assert try_translate(expression, allowed_specials=MULTI_FIELD_SPECIALS).translated == expression


def test_allowed_specials_still_reject_other_specials():
    outcome = try_translate("[_RecordID_]", allowed_specials=MULTI_FIELD_SPECIALS)
    assert outcome.translated is None
    assert "[_RecordID_]" in outcome.reason


# --- an unbracketed field reference, resolved only against columns the caller vouches for ---


@pytest.mark.parametrize(
    ("alteryx", "known", "expected"),
    [
        ("EXP(x/2)", ["x"], "exp([x] / 2)"),
        ("ABS(x-500)", ["x"], "abs([x] - 500)"),
        ("POW(x, 7)", ["x"], "power([x], 7)"),
        ("x + [y]", ["x", "y"], "[x] + [y]"),
        ("IF x<0 THEN 27 ELSE 26 ENDIF", ["x"], "if [x] < 0 then 27 else 26 endif"),
        # A name that differs only in case is a different column in Polars.
        ("Uppercase(name)", ["name"], "uppercase([name])"),
    ],
    ids=["exp", "abs", "pow", "mixed_with_bracketed", "inside_if", "case_sensitive_hit"],
)
def test_a_bare_identifier_resolves_against_a_known_column(alteryx: str, known: list[str], expected: str):
    outcome = try_translate(alteryx, known_columns=frozenset(known))
    assert outcome.translated == expected, outcome.reason
    _assert_reparses(outcome.translated)


@pytest.mark.parametrize(
    ("alteryx", "known"),
    [
        ("EXP(x/2)", []),
        ("EXP(x/2)", ["y"]),
        ("EXP(X/2)", ["x"]),
        ("[a] + total", ["a"]),
    ],
    ids=["nothing_known", "different_column", "different_case", "unknown_upstream_name"],
)
def test_a_bare_identifier_with_nothing_to_resolve_against_is_still_refused(alteryx: str, known: list[str]):
    """The resolution is a lookup, never a guess: an unknown name keeps the refusal it always had."""
    outcome = try_translate(alteryx, known_columns=frozenset(known))
    assert outcome.translated is None
    assert "must be written as [Field]" in (outcome.reason or "")


@pytest.mark.parametrize("name", ["Min", "Round", "RandInt", "Contains"])
def test_a_function_name_is_a_function_even_when_a_column_wears_it(name: str):
    """Reading `Min` as a column because one is called Min would change the expression, not fail to
    convert it — and `Min(...)` is a call whatever the schema says."""
    outcome = try_translate(f"[a] + {name}", known_columns=frozenset([name]))
    assert outcome.translated is None
    assert "must be written as [Field]" in (outcome.reason or "")


def test_the_power_operator_is_still_refused_with_the_rewrite_that_works():
    """`^` has no corpus instance and Alteryx's own sample writes the formula as POW(x, 7).

    `Pearson_Correlation.yxmd` uses `x^2` and `-20*x^7` in its prose comment boxes and
    `POW(x, 7)` in the formula the tool really runs — so the operator's binding against unary minus
    is unverified here, and inventing a precedence would change results silently where the refusal
    points at a spelling that already converts.
    """
    outcome = try_translate("x ^ 2", known_columns=frozenset(["x"]))
    assert outcome.translated is None
    assert outcome.reason == "the '^' power operator is not supported; rewrite it as Pow(base, exponent)"
    assert try_translate("Pow(x, 2)", known_columns=frozenset(["x"])).translated == "power([x], 2)"


# --- a mapping that is exact on some inputs and not on others owes its reader a sentence ---
# No shipped FunctionSpec carries a caveat today (MD5_UTF8 is exact; MD5_ASCII/MD5_UNICODE are
# refused; Base64 is not a Desktop formula function), so the plumbing is pinned on a patched spec.

CAVEAT_SENTENCE = "exact on ASCII input and different above it; check a non-ASCII value against Designer"


@pytest.fixture
def caveated_md5(monkeypatch: pytest.MonkeyPatch) -> str:
    spec = FUNCTION_MAP["md5_utf8"]
    monkeypatch.setitem(FUNCTION_MAP, "md5_utf8", FunctionSpec("MD5_UTF8", "md5", 1, 1, caveat=CAVEAT_SENTENCE))
    assert spec.caveat is None
    return "MD5_UTF8([Name])"


def test_a_caveated_function_translates_and_says_what_it_does_not_promise(caveated_md5: str):
    outcome = try_translate(caveated_md5)
    assert outcome.translated == "md5([Name])" and outcome.reason is None
    assert outcome.caveats == [CAVEAT_SENTENCE]


def test_an_uncaveated_translation_carries_no_caveats():
    """Otherwise the demotion would fire on every formula and `partial` would stop meaning anything."""
    assert try_translate("Uppercase([Name])").caveats == []
    assert try_translate("[Amount] + 1").caveats == []
    assert try_translate("MD5_UTF8([Name])").caveats == []


def test_caveats_do_not_leak_from_one_translation_into_the_next(caveated_md5: str):
    """They are collected in module state, so the reset is the part worth pinning."""
    assert try_translate(caveated_md5).caveats != []
    assert try_translate("Uppercase([Name])").caveats == []


def test_one_caveat_is_reported_once_however_often_the_function_appears(caveated_md5: str):
    outcome = try_translate("MD5_UTF8([A]) + MD5_UTF8([B])")
    assert len(outcome.caveats) == 1


# --- reading a column as Float64 ---

FLOAT_CAST_CASES: list[tuple[str, frozenset[str], str]] = [
    ("-20*POW([x], 7)", frozenset({"x"}), "-20 * power(to_number([x]), 7)"),
    ("[x] + [y]", frozenset({"x"}), "to_number([x]) + [y]"),
    ("[x] + [y]", frozenset({"x", "y"}), "to_number([x]) + to_number([y])"),
    ("ABS([x] - 500)", frozenset({"x"}), "abs(to_number([x]) - 500)"),
    (
        "IF [x]<0 THEN 27 ELSEIF [x]<20 THEN 26 ELSE -20*POW([x], 7) ENDIF",
        frozenset({"x"}),
        "if to_number([x]) < 0 then 27 elseif to_number([x]) < 20 then 26 else -20 * power(to_number([x]), 7) endif",
    ),
    # A column nobody named is left exactly as it was.
    ("[x] * 2", frozenset({"other"}), "[x] * 2"),
]


@pytest.mark.parametrize(("alteryx", "float_fields", "expected"), FLOAT_CAST_CASES)
def test_a_named_column_is_read_as_float64(alteryx: str, float_fields: frozenset[str], expected: str):
    outcome = try_translate(alteryx, float_fields=float_fields)
    assert outcome.reason is None
    assert outcome.translated == expected


def test_the_cast_is_what_stops_the_int64_wrap():
    """-20 * 500^7 is -1.5625e20; Int64 holds 500^7 and wraps on the multiply, to -8676047410323587072."""
    frame = pl.DataFrame({"x": [500]}, schema={"x": pl.Int64})
    wrapped = frame.select(simple_function_to_expr(try_translate("-20*POW([x], 7)").translated).alias("r"))
    assert wrapped["r"].to_list() == [-8676047410323587072]

    cast = try_translate("-20*POW([x], 7)", float_fields=frozenset({"x"})).translated
    assert frame.select(simple_function_to_expr(cast).alias("r"))["r"].to_list() == [-1.5625e20]


def test_a_translation_reports_the_columns_it_reads():
    """Bracketed and bare alike — the caller cannot read them off the rendered text."""
    assert try_translate("[a] + [b] * [a]").fields == {"a", "b"}
    assert try_translate("EXP(x/2)", known_columns=frozenset({"x"})).fields == {"x"}
    assert try_translate('Uppercase("literal")').fields == frozenset()


def test_the_float_field_set_does_not_leak_from_one_translation_into_the_next():
    """Module state again, so the reset is the part worth pinning."""
    assert try_translate("[x] * 2", float_fields=frozenset({"x"})).translated == "to_number([x]) * 2"
    assert try_translate("[x] * 2").translated == "[x] * 2"


# --- the integer literals float as well, and only in arithmetic positions ---


FLOAT_LITERAL_CASES: list[tuple[str, str]] = [
    # No column to cast at all, which is why the floating rule could not reach these.
    ("POW(2, 70)", "power(2.0, 70.0)"),
    ("[x]*POW(2,70)", "[x] * power(2.0, 70.0)"),
    ("[x]+POW(60,6)", "[x] + power(60.0, 6.0)"),
    ("-20*POW([x], 7)", "-20.0 * power([x], 7.0)"),
    ("ABS([x] - 500)", "abs([x] - 500.0)"),
    ("[x] / 2", "[x] / 2.0"),
    # A positional argument means an integer; `substring([s], 0.0, 5.0)` is not the same call.
    ("Substring([s], 0, 5)", "substring([s], 0, 5)"),
    # Nor is a flag argument, nor a unit count.
    ('REGEX_Match([s], "a", 1)', 'contains([s], "(?i)^(?:a)$")'),
    ('DateTimeAdd([d], 3, "days")', "add_days([d], 3)"),
    # A comparison is not arithmetic and cannot overflow, so its literal is left alone.
    ("IF [x] < 1 THEN 1 ELSE 0 ENDIF", "if [x] < 1 then 1 else 0 endif"),
]


@pytest.mark.parametrize(("alteryx", "expected"), FLOAT_LITERAL_CASES)
def test_an_integer_literal_floats_only_where_it_is_an_arithmetic_operand(alteryx: str, expected: str):
    outcome = try_translate(alteryx, known_columns=frozenset({"x", "s", "d"}), float_literals=True)
    assert outcome.reason is None
    assert outcome.translated == expected


def test_the_literal_rendering_is_what_stops_the_int32_wrap():
    """2^70 is 1.18e21. Polars computes `power(2, 70)` in Int32 and returns 0 — no column involved."""
    frame = pl.DataFrame({"unused": [1]})
    wrapped = try_translate("POW(2, 70)").translated
    assert frame.select(simple_function_to_expr(wrapped).alias("r"))["r"].to_list() == [0]

    floated = try_translate("POW(2, 70)", float_literals=True).translated
    assert frame.select(simple_function_to_expr(floated).alias("r"))["r"].to_list() == [1.1805916207174113e21]


def test_the_literal_flag_does_not_leak_from_one_translation_into_the_next():
    """Module state again, so the reset is the part worth pinning."""
    assert try_translate("POW(2, 70)", float_literals=True).translated == "power(2.0, 70.0)"
    assert try_translate("POW(2, 70)").translated == "power(2, 70)"


# --- the regex screen is the engine, not a substring blocklist ---


@pytest.mark.parametrize(
    ("pattern", "construct"),
    [("(?>ab)c", "atomic group"), ("(?#c)abc", "inline comment")],
    ids=["atomic_group", "inline_comment"],
)
def test_legal_perl_the_rust_engine_lacks_is_refused_rather_than_converted(pattern: str, construct: str):
    """Both are legal Perl, both pass the three-token blocklist, and both raise on collect.

    The refusal quotes Polars, because Polars is the only thing that knows what Polars supports —
    naming the constructs one by one is what let these two through in the first place.
    """
    outcome = try_translate(f'REGEX_Match([Name], "{pattern}")')
    assert outcome.translated is None, construct
    assert "Polars' regex engine rejected" in outcome.reason
    assert "unrecognized flag" in outcome.reason


def test_the_screen_reads_the_finished_pattern_including_its_anchors():
    """'a)b' is a fine substring and an unbalanced group once wrapped in `^(?:...)$`."""
    outcome = try_translate('REGEX_Match([Name], "a)b")')
    assert outcome.translated is None
    assert "Polars' regex engine rejected" in outcome.reason


def test_a_pattern_the_engine_accepts_still_converts():
    """Otherwise the screen would be a refusal with extra steps."""
    assert try_translate('REGEX_Match([Name], "^[A-Z]{2}-\\\\d+")').translated is None  # backslash: tokenizer
    assert try_translate('REGEX_Match([Name], "[A-Z]+|west")').translated == 'contains([Name], "(?i)^(?:[A-Z]+|west)$")'


# --- the engine decides and the construct names are only wording ---


NAMED_GROUP_CASES: list[tuple[str, str | None]] = [
    # Both spellings of a named group are legal in Polars, and only one of them used to convert.
    ("(?<n>a)b", 'contains([Name], "(?i)^(?:(?<n>a)b)$")'),
    ("(?P<n>a)b", 'contains([Name], "(?i)^(?:(?P<n>a)b)$")'),
    ("(?<=a)b", None),
    ("(?<!a)b", None),
    ("(?=a)b", None),
    ("(?!a)b", None),
]


@pytest.mark.parametrize(("pattern", "expected"), NAMED_GROUP_CASES, ids=[case[0] for case in NAMED_GROUP_CASES])
def test_a_named_group_converts_and_only_real_lookaround_is_refused(pattern: str, expected: str | None):
    outcome = try_translate(f'REGEX_Match([Name], "{pattern}")')
    assert outcome.translated == expected, outcome.reason


@pytest.mark.parametrize(
    ("pattern", "label"),
    [
        ("(?<=a)b", "lookbehind"),
        ("(?<!a)b", "negative lookbehind"),
        ("(?=a)b", "lookahead"),
        ("(?!a)b", "negative lookahead"),
    ],
    ids=["lookbehind", "negative_lookbehind", "lookahead", "negative_lookahead"],
)
def test_real_lookaround_keeps_the_sentence_that_names_it(pattern: str, label: str):
    """The names survive as wording; what changed is that the engine has to agree first."""
    outcome = try_translate(f'REGEX_Match([Name], "{pattern}")')
    assert outcome.translated is None
    assert outcome.reason == f"the REGEX_Match() pattern uses {label}, which Polars' regex engine does not support"


def test_the_construct_names_can_only_phrase_a_refusal_the_engine_already_made():
    """The screen the names used to be is the bug: a name matched a pattern Polars accepts."""
    assert unsupported_construct("(?<=a)b") == "lookbehind"
    # A named group is not lookbehind, and nothing in it is a construct worth naming.
    assert unsupported_construct("(?<n>a)b") is None
    assert unsupported_construct("(?>ab)c") is None
    assert regex_rejection("(?<n>a)b") is None
    assert regex_rejection("(?<=a)b") is not None
