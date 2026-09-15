# Alteryx `.yxmd` fixtures

| Fixture | Covers |
|---|---|
| `all_supported.yxmd` | every tool with a dedicated mapper, wired end to end |
| `formulas.yxmd` | translatable and untranslatable Formula expressions |
| `containers.yxmd` | tool containers, nested tools, text boxes (imported as canvas comments) |
| `unsupported.yxmd` | tools with no mapper, plus a macro node |
| `dynamic_rename.yxmd` | Dynamic Rename formula / first-row / prefix+suffix / unsupported modes |
| `multi_field_formula_runs.yxmd` | the same three Multi-Field Formula shapes plus a `[_CurrentFieldType_]` tool, an untranslatable expression, a rejected `[_RecordID_]` special and an untranslatable expression carrying a declared `Int32` output type, fed by a Text Input, so the imported flow can actually be executed |
| `regex_and_multifield.yxmd` | RegEx parse and match, plus a Multi-Field Formula over an explicitly listed subset of fields |
| `simple_filter.yxmd` | simple-mode Filter operators, including an unsupported one |
| `price_paid.yxmd` | a real published Alteryx workflow (UK Price Paid extract) |
| `extra_tools.yxmd` | Record ID, Running Total, Transpose, Cross Tab, Append Fields, Data Cleansing (macro) |
| `out_of_scope.yxmd` | the scope registry's three shapes — an official spatial/reporting plugin, a vendor namespace that collapses to `custom_plugin`, a macro that collapses to `user_macro` — plus two no-ops (a Message that passes its data on and a Test, which is a sink) and an in-scope tool with no mapper |
| `no_op_passthrough.yxmd` | a Block Until Done feeding three anchors, one of them behind a Message, with two consumers written before that Message so the column resolution cannot depend on document order |
| `detour.yxmd` | both Detour sides — one `<DetourRight>`, one without — each with a live consumer, a dead consumer and a Detour End |
| `explorer_box.yxmd` | Explorer Boxes: an https address, a Windows path, an empty one, one carrying a query-string token, one wired into the flow, beside an ordinary text box |
| `api_output.yxmd` | an API Output sink behind a Text Input |
| `field_info.yxmd` | Field Info with one input, with none and with two |
| `make_group.yxmd` | Make Group over a key pair, plus one missing a key and one carrying an option Flowfile does not read |
| `map_input.yxmd` | Map Input: a drawn point, a point and a polygon with a reference file and a wrong `NumRows`, a labels-only tool, and one in `Select` mode |
| `zero_tools.yxmd`, `invalid.xml` | parse failures |

Real workflows to test against come from:

- Alteryx Designer's own samples, under `C:\Program Files\Alteryx\Samples\` on an install.
- The Alteryx Community Weekly Challenges, which publish `.yxzp` packages — a `.yxzp`
  (and a `.yxi`) is a zip archive, so `unzip x.yxzp` yields the `.yxmd` inside.
- Public GitHub repositories: GitHub code search for `path:*.yxmd`.

**None of them is committed here.** Designer's samples are proprietary and this repo is MIT, so a
test that needs a real workflow reads it from the private corpus beside the checkout and skips
where that is absent — `needs_corpus` in `test_convert.py` and `test_scope.py`. `.yxmd` is plain
XML, so every fixture in the table above is written by hand, except `price_paid.yxmd`, whose
origin is being confirmed with the maintainer.
