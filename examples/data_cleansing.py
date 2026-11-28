"""Fix nulls, whitespace, casing and stray characters in one call with data_cleansing."""

# --8<-- [start:example]
import flowfile as ff

raw = ff.from_dict(
    {
        "name": ["  alice smith", "BOB   JONES ", None, None],
        "city": ["amsterdam", " Rotterdam  ", "utrecht", None],
        "score": [10, None, None, None],
    }
)

# Defaults already fill nulls (blank for text, 0 for numbers) and trim whitespace.
cleaned = raw.data_cleansing(
    remove_null_rows=True,
    normalize_whitespace=True,
    case_mode="titlecase",
).collect()

# Character rules apply only to the columns you name; other columns pass through.
phones = (
    ff.from_dict({"phone": ["+31 (0)20-123 4567", "06 1234 5678"], "id": [1, 2]})
    .data_cleansing(["phone"], remove_punctuation=True, remove_all_whitespace=True)
    .collect()
)
# --8<-- [end:example]

assert cleaned.height == 3
assert cleaned["name"].to_list() == ["Alice Smith", "Bob Jones", ""]
assert cleaned["city"].to_list() == ["Amsterdam", "Rotterdam", "Utrecht"]
assert cleaned["score"].to_list() == [10, 0, 0]

assert phones["phone"].to_list() == ["310201234567", "0612345678"]
assert phones["id"].to_list() == [1, 2]
