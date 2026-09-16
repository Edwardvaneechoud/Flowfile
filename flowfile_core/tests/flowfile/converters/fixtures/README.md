# Alteryx `.yxmd` fixtures

| Fixture | Covers |
|---|---|
| `all_supported.yxmd` | twelve of the tools with a dedicated mapper — Text Input, Select, Filter, Formula, Sort, Summarize, Sample, Unique, Text To Columns, Join, Union, Output Data — wired end to end |
| `formulas.yxmd` | translatable and untranslatable Formula expressions |
| `containers.yxmd` | tool containers, nested tools, text boxes (imported as canvas comments) |
| `unsupported.yxmd` | tools with no mapper, plus a macro node |
| `dynamic_rename.yxmd` | Dynamic Rename formula / first-row / prefix+suffix / unsupported modes |
| `dynamic_rename_affix.yxmd` | Dynamic Rename `Add` and `Remove` in the shape Alteryx really writes (`<Type>`/`<Text>`/`<OnError>`), a suffix whose text starts with a space, both remove directions, and a `NamesFromRows/InputMode=Mapped` table holding a self-rename, a column it does not name and a name that is no column |
| `multi_field_formula_runs.yxmd` | the same three Multi-Field Formula shapes plus a `[_CurrentFieldType_]` tool, an untranslatable expression, a rejected `[_RecordID_]` special and an untranslatable expression carrying a declared `Int32` output type, fed by a Text Input, so the imported flow can actually be executed |
| `regex_and_multifield.yxmd` | RegEx parse and match, plus a Multi-Field Formula over an explicitly listed subset of fields |
| `simple_filter.yxmd` | simple-mode Filter operators, including an unsupported one |
| `filter_regex_period.yxmd` | the W6 Filter reach — `REGEX_Match` anchored, case-insensitive by default and explicitly either way, with a lookahead and with a pattern taken from a column; `PeriodAfter`/`PeriodBefore` windows; a dynamic `DateType` (tomorrow) beside a fixed one, each carrying the same stale `<Operand>` Alteryx leaves behind; an unknown period unit, a fixed-anchored period, an unknown date type and a string operator carrying a dynamic date (refused) beside one carrying a fixed date (converted). A Formula parses the text date first, because Alteryx only offers the dynamic operators on a date column |
| `pearson_correlation.yxmd` | Pearson Correlation: three numeric columns, the same with `Covariance=True`, `*Unknown` selected beside a string column, `*Unknown` selected beside an unticked numeric one, a string column selected by name, a selected column that does not arrive, one field alone and one field twice |
| `spearman_correlation.yxmd` | the Spearman macro: a monotone-but-not-linear pair, tied ranks, a grouped run, a stale grouping field under an off switch, and the five refusals (same field twice, a missing variable, a group field that is a variable, a string variable, a variable that does not arrive) |
| `field_profile.yxmd` | Field Summary Report — an all-selected list, a partial one, a sampled run, a sample that is both a count and a percentage, one that is neither, an empty selection, and a Browse on the `Reports` anchor — plus a Basic Data Profile on the same input |
| `price_paid.yxmd` | a real published Alteryx workflow (UK Price Paid extract) |
| `extra_tools.yxmd` | Record ID, Running Total, Transpose, Cross Tab, Append Fields, Data Cleansing (macro) |
| `out_of_scope.yxmd` | the scope registry's three shapes — an official spatial/reporting plugin, a vendor namespace that collapses to `custom_plugin`, a macro that collapses to `user_macro` — plus two no-ops (a Message that passes its data on and a Test, which is a sink) and an in-scope tool with no mapper |
| `no_op_passthrough.yxmd` | a Block Until Done feeding three anchors, one of them behind a Message, with two consumers written before that Message so the column resolution cannot depend on document order |
| `detour.yxmd` | both Detour sides — one `<DetourRight>`, one without — each with a live consumer, a dead consumer and a Detour End |
| `explorer_box.yxmd` | six Explorer Boxes: an https address, a Windows path, an empty one, one carrying a query-string token, one wired into the flow and one that is both wired and carries a token, beside an ordinary text box |
| `api_output.yxmd` | an API Output sink behind a Text Input |
| `field_info.yxmd` | Field Info with one input, with none and with two |
| `make_group.yxmd` | Make Group over a key pair, plus one missing a key and one carrying an option Flowfile does not read |
| `map_input.yxmd` | Map Input: a drawn point, a point and a polygon with a reference file and a wrong `NumRows`, a labels-only tool, and one in `Select` mode |
| `data_cleanse_pro.yxmd` | Data Cleanse Pro: whitespace and number removal, tab/line-break cleanup, numeric-column replacement, and punctuation removal with a case change |
| `imputation.yxmd` | the Imputation macro: nulls replaced by a fixed value in place, into Alteryx's `_ImputedValue` field and beside an `_Indicator` column, by mean, by mode, a median over a String column, two replace-with radios on at once, a chosen value replaced by a statistic, and an `_Indicator` name a column arriving already holds |
| `weighted_average.yxmd` | the Weighted Average macro: ungrouped and grouped, a column weighted by itself, a String value column, an output name that is already the group field, and a group field the stream does not carry |
| `create_samples.yxmd` | the Create Samples macro: a 70/20/10 split with all three anchors wired, a 50/50 split whose `Holdout` anchor is declared empty and whose consumer reports the dropped wire, and the three refusals (over 100%, an unreadable percentage, two zeroes) |
| `generate_rows.yxmd` | Generate Rows as a range: a start node with no input, a bound read from an input column, a date range, an exclusive condition with a step of 3, and the five refusals (a descending condition, a data-dependent step, a record-count cap, `+1` on a date column, and a created column that already arrives) |
| `datetime_tokens.yxmd` | Date Time both directions (parse and format), a time-only format, and four formats carrying eight of the tokenizer's twelve tokens — `Mon`, `dy`, `hh` and `tt` are covered by the parametrised token test instead |
| `date_time_now.yxmd` | Date Time Now as a start node: a date format feeding a Select that renames the invented column, a time-only format, a language Flowfile cannot write, and a letter run that is not one token |
| `random_records.yxmd` | the Random Records macro: a record count, an unseeded percentage, a seeded one, and a tool that selects neither |
| `rank_modes.yxmd` | every Rank mode, one grouped, one behind a Sort that breaks the tie the other way round, a null in the ranked field, and an input that already carries a `Rank` column |
| `sample_modes.yxmd` | the Sample mode table — First, Last, Skip, every-Nth, one grouped — plus one behind a Sort |
| `select_records.yxmd` | the Select Records macro's range forms (`N`, `N-M`, `N+`, `-N`) and one behind a Sort |
| `summarize_exotic.yxmd` | Summarize actions the group-by node cannot name (Mode, Longest, Shortest, Count Blank/Non Blank), a numeric-looking string column, First/Last with and without a stated order, and a `Trim` that turns the whitespace row into an empty string — the only way a Text Input's frame can hold one, since Alteryx writes every empty cell as `<c />` and no XML parser can tell that from `<c></c>` |
| `transpose_unknown.yxmd` | Transpose over `*Unknown` and over a static selection, with `ErrorWarn` set to Warn on the unknown path and to Ignore and Warn on the static one |
| `injected_comments.yxmd` | line breaks carrying Python inside a tool name, a setting value and a Record ID group field, so the sanitiser is proved on a workflow that still dispatches to the real mappers |
| `join_select.yxmd` | the Join's `SelectConfiguration`: Alteryx's own output names, a kept right-hand key and one without, a deselected field, `*Unknown`, a join by record position, and a Sort reading a Join's `Left` (unjoined-left) output |
| `text_to_columns.yxmd` | seven Text To Columns tools in a chain: `NumFields` as the split-to-columns-or-rows discriminator, a named root, a missing one and an empty `<RootName />`, escaped and brace delimiters, and three `Flags` values |
| `zero_tools.yxmd` | a comment-only workflow — a tool container holding one text box and no tool — which imports as a flow with no nodes and a single canvas comment |
| `empty_canvas.yxmd`, `invalid.xml` | the two parse failures: a canvas with neither tools nor comments, and a file that is not XML |

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
