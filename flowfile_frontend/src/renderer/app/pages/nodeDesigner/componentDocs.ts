/**
 * Canonical reference for the Node Designer's UI components.
 *
 * Single source of truth for the help popover, the component-reference modal,
 * and the palette lists (constants.ts, AddControlMenu.vue derive from this).
 * Facts are grounded in shared/node_designer/ui_components.py — keep the docs
 * page (docs/for-developers/creating-custom-nodes.md) mirrored by hand.
 */
import type { ComponentType } from "./designerState";

export interface ComponentReturns {
  /** How the value is read in `process`. */
  accessor: ".value" | ".secret_value";
  /** Python type of the returned value. */
  type: string;
  /** Full shape for complex returns (e.g. ColumnActionInput's dict). */
  shape?: string;
}

export interface ComponentConfigProp {
  prop: string;
  desc: string;
}

export interface ComponentDoc {
  type: ComponentType;
  label: string;
  icon: string;
  /** What the control renders / what the user does with it. */
  whatItDoes: string;
  /** What the value is and how it is read in `process`. */
  returns: ComponentReturns;
  /** Key constructor properties. */
  configure: ComponentConfigProp[];
  /** Whether it needs a connected input frame to be useful. */
  needsInput: boolean;
  /** Short `process()` snippet reading the value. */
  howToUse: string;
  /** One-liner contrasting it with the closest sibling component. */
  whenToUse: string;
  /** Optional gotcha. */
  note?: string;
}

/** The `.value → str` chip shown in collapsed headers and popover rows. */
export function returnsChip(returns: ComponentReturns): string {
  return `${returns.accessor} → ${returns.type}`;
}

/**
 * Canonical Python type of a component's value as written in code — the display
 * `returns.type` without a trailing ` | None`. ControlInspector's Insert Variable
 * derives from this so it can't drift from the reference. (The Polars autocompletion
 * keeps its own map, tuned for CodeMirror.)
 */
export function pyValueType(type: ComponentType): string {
  const doc = componentDocs.find((c) => c.type === type);
  return (doc?.returns.type ?? "Any").replace(/\s*\|\s*None$/, "");
}

export const componentDocs: ComponentDoc[] = [
  {
    type: "TextInput",
    label: "Text Input",
    icon: "fa-solid fa-font",
    whatItDoes: "A single-line text field for capturing a free-form string.",
    returns: { accessor: ".value", type: "str" },
    configure: [
      { prop: "default", desc: "Initial text (defaults to an empty string)." },
      { prop: "placeholder", desc: "Hint text shown while the field is empty." },
    ],
    needsInput: false,
    howToUse: `prefix = self.settings_schema.options.prefix.value
return lf.with_columns(
    (pl.lit(prefix) + pl.col("name")).alias("greeting")
)`,
    whenToUse: "Free-form text, or a column name typed by hand.",
  },
  {
    type: "NumericInput",
    label: "Numeric Input",
    icon: "fa-solid fa-hashtag",
    whatItDoes: "A number field with optional minimum and maximum bounds.",
    returns: { accessor: ".value", type: "float" },
    configure: [
      { prop: "default", desc: "Initial value (None if unset)." },
      { prop: "min_value", desc: "Lowest value the field accepts." },
      { prop: "max_value", desc: "Highest value the field accepts." },
    ],
    needsInput: false,
    howToUse: `threshold = self.settings_schema.options.threshold.value
return lf.filter(pl.col("amount") > threshold)`,
    whenToUse: "Exact numeric entry with optional bounds.",
    note: "The value is always a float, even when a whole number is entered; cast with int(...) if you need an integer.",
  },
  {
    type: "ToggleSwitch",
    label: "Toggle Switch",
    icon: "fa-solid fa-toggle-on",
    whatItDoes: "An on/off switch for enabling or disabling behaviour.",
    returns: { accessor: ".value", type: "bool" },
    configure: [
      { prop: "default", desc: "Initial state (defaults to off / False)." },
      { prop: "description", desc: "Helper text shown beneath the switch." },
    ],
    needsInput: false,
    howToUse: `if self.settings_schema.options.drop_nulls.value:
    lf = lf.drop_nulls()
return lf`,
    whenToUse: "A single on/off feature flag.",
  },
  {
    type: "SingleSelect",
    label: "Single Select",
    icon: "fa-solid fa-list",
    whatItDoes: "A dropdown for picking one option from a list of choices.",
    returns: { accessor: ".value", type: "str" },
    configure: [
      {
        prop: "options",
        desc: "Static list of strings/(value, label) tuples, or the IncomingColumns / AvailableArtifacts marker for dynamic options.",
      },
      { prop: "default", desc: "Initially selected value." },
    ],
    needsInput: false,
    howToUse: `op = self.settings_schema.options.aggregation.value  # e.g. "sum"
return lf.select(getattr(pl.col("amount"), op)())`,
    whenToUse:
      "One choice from a fixed list. Use options=IncomingColumns for a lightweight one-column dropdown.",
    note: "With options=IncomingColumns (or AvailableArtifacts) the choices are populated from a connected input at runtime.",
  },
  {
    type: "MultiSelect",
    label: "Multi Select",
    icon: "fa-solid fa-list-check",
    whatItDoes: "A dropdown for picking several options from a list of choices.",
    returns: { accessor: ".value", type: "list[str]" },
    configure: [
      { prop: "options", desc: "Same as SingleSelect: static list or a dynamic marker." },
      { prop: "default", desc: "Initially selected values (defaults to an empty list)." },
    ],
    needsInput: false,
    howToUse: `ops = self.settings_schema.cleaning.operations.value  # ["lowercase", "trim"]
expr = pl.col("text")
if "lowercase" in ops:
    expr = expr.str.to_lowercase()
return lf.with_columns(expr.alias("clean"))`,
    whenToUse: "Several choices from a fixed list (SingleSelect for exactly one).",
  },
  {
    type: "ColumnSelector",
    label: "Column Selector",
    icon: "fa-solid fa-table-columns",
    whatItDoes:
      "A picker for one or more columns from the input frame, optionally filtered by data type.",
    returns: { accessor: ".value", type: "str | list[str]" },
    configure: [
      { prop: "required", desc: "Whether a selection is mandatory (defaults to False)." },
      {
        prop: "multiple",
        desc: "Allow selecting more than one column — makes the value a list[str].",
      },
      {
        prop: "data_types",
        desc: 'Restrict selectable columns by type, e.g. nd.Types.Numeric, [nd.Types.String, nd.Types.Categorical], or "ALL".',
      },
    ],
    needsInput: true,
    howToUse: `cols = self.settings_schema.config.columns.value  # str, or list[str] when multiple=True
return lf.select(cols)`,
    whenToUse:
      "The purpose-built, type-filterable column picker — prefer it over SingleSelect(IncomingColumns) whenever you are choosing real input columns.",
  },
  {
    type: "ColumnActionInput",
    label: "Column Action",
    icon: "fa-solid fa-table-list",
    whatItDoes:
      "A table where the user pairs input columns with an action and an output name, with optional group-by and order-by pickers. Built for aggregations, rolling windows, string operations, and type casts.",
    returns: {
      accessor: ".value",
      type: "ColumnActionValue",
      shape: `cfg.rows              # list[nd.ColumnActionRow] — each .column / .action / .output_name (str)
cfg.group_by_columns  # list[str]   ([] unless show_group_by=True)
cfg.order_by_column   # str | None  (None unless show_order_by=True)`,
    },
    configure: [
      {
        prop: "actions",
        desc: "Available actions — plain strings or ActionOption(value, label) tuples.",
      },
      {
        prop: "output_name_template",
        desc: 'Default output naming, using {column} and {action} placeholders (default "{column}_{action}").',
      },
      { prop: "show_group_by", desc: "Add a group-by column selector (defaults to False)." },
      { prop: "show_order_by", desc: "Add an order-by column selector (defaults to False)." },
      { prop: "data_types", desc: "Restrict selectable columns by type." },
    ],
    needsInput: true,
    howToUse: `cfg: nd.ColumnActionValue = self.settings_schema.agg.column_actions.value
return lf.group_by(cfg.group_by_columns).agg([
    getattr(pl.col(r.column), r.action)().alias(r.output_name)
    for r in cfg.rows
])`,
    whenToUse:
      "Pair many columns with an operation and an output name in one table. ColumnSelector only returns the column names — this returns column + action + output rows.",
    note: "The value is a typed nd.ColumnActionValue (attribute access: cfg.rows, r.column); annotate cfg: nd.ColumnActionValue for editor autocomplete. The action strings are yours to interpret in process — the component just collects the rows.",
  },
  {
    type: "SliderInput",
    label: "Slider",
    icon: "fa-solid fa-sliders",
    whatItDoes: "A slider for choosing a numeric value within a fixed range.",
    returns: { accessor: ".value", type: "float" },
    configure: [
      { prop: "min_value", desc: "Range minimum (defaults to 0)." },
      { prop: "max_value", desc: "Range maximum (defaults to 100)." },
      { prop: "step", desc: "Increment between stops (defaults to 1)." },
      { prop: "default", desc: "Initial value (defaults to min_value)." },
    ],
    needsInput: false,
    howToUse: `top_n = self.settings_schema.options.top_n.value
return lf.head(int(top_n))`,
    whenToUse:
      "A bounded numeric value where a slider is friendlier than typing (NumericInput for unbounded/exact entry).",
  },
  {
    type: "SecretSelector",
    label: "Secret Selector",
    icon: "fa-solid fa-key",
    whatItDoes:
      "A dropdown listing the secrets the current user has stored (API keys, tokens, credentials). Only the name is shown in the UI; the value is resolved at run time.",
    returns: { accessor: ".secret_value", type: "SecretStr | None" },
    configure: [
      { prop: "required", desc: "Whether a secret must be selected (defaults to False)." },
      { prop: "description", desc: "Helper text shown beneath the dropdown." },
      {
        prop: "name_prefix",
        desc: 'Only list secrets whose name starts with this prefix, e.g. "API_KEY_".',
      },
    ],
    needsInput: false,
    howToUse: `secret = self.settings_schema.connection.api_key.secret_value
if secret is not None:
    api_key = secret.get_secret_value()  # the decrypted string
    ...
return lf`,
    whenToUse: "Reference a stored credential without hard-coding it in the node file.",
    note: 'Resolvable only during execution (returns None when nothing is selected), and unavailable in kernel nodes — use environment="local".',
  },
];
