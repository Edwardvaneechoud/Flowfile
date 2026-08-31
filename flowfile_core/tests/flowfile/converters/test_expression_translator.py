import warnings
from datetime import date, datetime

import polars as pl
import pytest
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.converters.alteryx.expression import (
    FUNCTION_MAP,
    TranslationOutcome,
    try_translate,
)

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
    # These are re-run upper- and lower-cased below, so both casings of the format must stay whitelisted
    # (%Y<->%y and %m<->%M are; the parse case avoids %Y because %y is rejected when parsing).
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
    ("regex-match", 'REGEX_Match([Name], "^a")'),
    ("regex-replace", 'REGEX_Replace([Name], "a", "b")'),
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
    # An unescaped '(' passed the fail-closed gate (the parser only parses) and raised
    # ComputeError once polars compiled the regex at run time.
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
    # `"` and `\` are unreachable (the Alteryx tokenizer rejects nested quotes and backslash escapes);
    # `[` and `^` are rejected by the translator and covered in REJECTED_CASES.
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
    outcome = try_translate("REGEX_Match([Name], \"^a\")")
    body = f"// could not be converted: {outcome.reason}\n// Original: REGEX_Match([Name], \"^a\")\n[Name]"
    _assert_reparses(body)
