# Alteryx `.yxmd` fixtures

| Fixture | Covers |
|---|---|
| `all_supported.yxmd` | every tool with a dedicated mapper, wired end to end |
| `formulas.yxmd` | translatable and untranslatable Formula expressions |
| `containers.yxmd` | tool containers, nested tools, text boxes (imported as canvas comments) |
| `unsupported.yxmd` | tools with no mapper, plus a macro node |
| `dynamic_rename.yxmd` | Dynamic Rename formula / first-row / prefix+suffix / unsupported modes |
| `multi_field_formula.yxmd` | the maintainer's Multi-Field Formula sample: overwrite every text field, a `New_` prefixed copy, and a percent-of-total over 12 listed numeric fields with a FixedDecimal output type |
| `multi_field_formula_runs.yxmd` | the same three Multi-Field Formula shapes plus a `[_CurrentFieldType_]` tool, an untranslatable expression, a rejected `[_RecordID_]` special and an untranslatable expression carrying a declared `Int32` output type, fed by a Text Input, so the imported flow can actually be executed |
| `regex_and_multifield.yxmd` | RegEx parse and match, plus a Multi-Field Formula over an explicitly listed subset of fields |
| `simple_filter.yxmd` | simple-mode Filter operators, including an unsupported one |
| `price_paid.yxmd` | a real published Alteryx workflow (UK Price Paid extract) |
| `extra_tools.yxmd` | Record ID, Running Total, Transpose, Cross Tab, Append Fields, Data Cleansing (macro) |
| `zero_tools.yxmd`, `invalid.xml` | parse failures |

Real workflows to test against come from:

- Alteryx Designer's own samples, under `C:\Program Files\Alteryx\Samples\` on an install.
- The Alteryx Community Weekly Challenges, which publish `.yxzp` packages — a `.yxzp`
  (and a `.yxi`) is a zip archive, so `unzip x.yxzp` yields the `.yxmd` inside.
- Public GitHub repositories: GitHub code search for `path:*.yxmd`.

`.yxmd` is plain XML, so a fixture can also be written by hand — the ones above were.
