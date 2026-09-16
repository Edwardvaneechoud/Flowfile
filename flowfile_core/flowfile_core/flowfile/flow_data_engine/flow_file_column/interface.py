from typing import Literal

DataTypeGroup = Literal["numeric", "str", "date"]
ReadableDataTypeGroup = Literal["Numeric", "String", "Date", "Other", "Boolean", "Binary", "Complex"]
# What the values mean beyond their storage dtype; derived from a declared dtype only, never sampled.
SemanticType = Literal["geometry"]
