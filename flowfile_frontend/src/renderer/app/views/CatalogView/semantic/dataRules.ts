import type { DataRule, DataRuleResult } from "../../../types/semantic.types";

/** One input of a rule's form; every input is text and `buildRule` reads it. */
interface RuleField {
  key: string;
  label: string;
  placeholder: string;
}

/** A kind of data rule a column can hold: `kind` is what the ledger stores as `check`. */
export interface RuleKind {
  kind: string;
  label: string;
  hint: string;
  fields: RuleField[];
  numbersOnly?: boolean;
}

const RANGE: RuleField[] = [
  { key: "min", label: "Min", placeholder: "no lower end" },
  { key: "max", label: "Max", placeholder: "no upper end" },
];

/** Adding a kind here offers it; core must run it too (`catalog/semantic/data_rules.py`). */
export const RULE_KINDS: RuleKind[] = [
  { kind: "not_null", label: "Not null", hint: "Every row has a value.", fields: [] },
  { kind: "unique", label: "Unique", hint: "No value occurs twice.", fields: [] },
  {
    kind: "one_of",
    label: "One of",
    hint: "Only the values listed, separated by commas.",
    fields: [{ key: "values", label: "Values", placeholder: "open, shipped, cancelled" }],
  },
  {
    kind: "matches",
    label: "Pattern",
    hint: "The value matches a regular expression.",
    fields: [{ key: "pattern", label: "Pattern", placeholder: "^[A-Z]{2}$" }],
  },
  {
    kind: "between",
    label: "Between",
    hint: "A number within a range; either end may be left open.",
    fields: RANGE,
    numbersOnly: true,
  },
  {
    kind: "length",
    label: "Length",
    hint: "The number of characters; either end may be left open.",
    fields: RANGE,
  },
  {
    kind: "formula",
    label: "Formula",
    hint: "One formula line that is true for a row that passes.",
    fields: [{ key: "expression", label: "Formula", placeholder: "[amount] >= 0" }],
  },
];

export const isNumberType = (dtype: string | null | undefined): boolean =>
  /^(U?Int|Float|Decimal)/.test(dtype ?? "");

/** The kinds offered for a column of `dtype`. */
export const ruleKindsFor = (dtype: string | null | undefined): RuleKind[] =>
  RULE_KINDS.filter((k) => !k.numbersOnly || isNumberType(dtype));

/** The stored rules of a `checks` fact's value; anything else is no rules. */
export const rulesOf = (value: unknown): DataRule[] =>
  Array.isArray(value)
    ? value.filter((r): r is DataRule => !!r && typeof (r as DataRule).check === "string")
    : [];

const isSet = (value: unknown): boolean => value !== null && value !== undefined && value !== "";

function range(rule: DataRule, unit: string): string {
  const { min, max } = rule;
  if (isSet(min) && isSet(max))
    return min === max && unit ? `exactly ${min}${unit}` : `${min} to ${max}${unit}`;
  if (isSet(min)) return `at least ${min}${unit}`;
  if (isSet(max)) return `at most ${max}${unit}`;
  return unit ? `any length` : "any value";
}

const SHOWN_VALUES = 4;

/** A rule as a person reads it: "not null", "0 to 100", "at most 10 characters". */
export function describeRule(rule: DataRule): string {
  switch (rule.check) {
    case "not_null":
      return "not null";
    case "unique":
      return "unique";
    case "matches":
      return `matches ${rule.pattern ?? ""}`;
    case "one_of": {
      const values = Array.isArray(rule.values) ? rule.values.map(String) : [];
      const more = values.length - SHOWN_VALUES;
      const shown = values.slice(0, SHOWN_VALUES).join(", ");
      return `one of ${shown}${more > 0 ? ` +${more} more` : ""}`;
    }
    case "between":
      return range(rule, "");
    case "length":
      return range(rule, " characters");
    case "formula":
      return String(rule.expression ?? "");
    default:
      return rule.check;
  }
}

export const describeRules = (value: unknown): string =>
  rulesOf(value).map(describeRule).join(" · ");

function sorted(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sorted);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((k) => [k, sorted((value as Record<string, unknown>)[k])]),
    );
  return value;
}

/** One rule of one column, whatever order its keys came in: how a result finds its rule. */
export const ruleKey = (column: string, rule: DataRule): string =>
  `${column}\u0000${JSON.stringify(sorted(rule))}`;

export type RuleOutcome = "pass" | "fail" | "error";

export const outcomeOf = (result: DataRuleResult): RuleOutcome =>
  result.error ? "error" : result.passed ? "pass" : "fail";

const rows = (n: number): string => `${n.toLocaleString()} ${n === 1 ? "row" : "rows"}`;

/** What a run found for one rule, as a sentence. */
export function resultText(result: DataRuleResult): string {
  if (result.error) return `Could not run: ${result.error}`;
  if (result.passed) return `Passes on all ${rows(result.total_rows)}`;
  return `${result.failed_rows.toLocaleString()} of ${rows(result.total_rows)} fail`;
}

/** A table's rules against its results: "3 of 4 rules pass", or that they did not run yet. */
export function rulesSummary(
  rules: { column: string; rule: DataRule }[],
  results: Record<string, DataRuleResult> | undefined,
): string {
  if (rules.length === 0) return "";
  const count = `${rules.length} ${rules.length === 1 ? "rule" : "rules"}`;
  const found = rules.map((r) => results?.[ruleKey(r.column, r.rule)]).filter((r) => !!r);
  if (found.length === 0) return `${count} · not run yet`;
  const passing = found.filter((r) => outcomeOf(r as DataRuleResult) === "pass").length;
  const waiting = rules.length - found.length;
  const text = `${passing} of ${count} pass`;
  return waiting > 0 ? `${text} · ${waiting} not run yet` : text;
}

const asNumber = (text: string): number | null => {
  const n = Number(text);
  return text.trim() !== "" && Number.isFinite(n) ? n : null;
};

/** The rule a filled-in form describes, or what is wrong with it. */
export function buildRule(
  kind: string,
  draft: Record<string, string>,
  dtype: string | null | undefined,
): { rule: DataRule } | { error: string } {
  const text = (key: string) => (draft[key] ?? "").trim();
  if (kind === "matches")
    return text("pattern")
      ? { rule: { check: kind, pattern: text("pattern") } }
      : { error: "Give a pattern." };
  if (kind === "formula")
    return text("expression")
      ? { rule: { check: kind, expression: text("expression") } }
      : { error: "Give a formula." };
  if (kind === "one_of") {
    const words = [
      ...new Set(
        text("values")
          .split(",")
          .map((w) => w.trim()),
      ),
    ].filter(Boolean);
    if (words.length === 0) return { error: "Give at least one value." };
    if (!isNumberType(dtype)) return { rule: { check: kind, values: words } };
    const numbers = words.map(asNumber);
    return numbers.some((n) => n === null)
      ? { error: "This column holds numbers; give numbers." }
      : { rule: { check: kind, values: numbers } };
  }
  if (kind === "between" || kind === "length") {
    const ends: { min?: number; max?: number } = {};
    for (const end of ["min", "max"] as const) {
      if (!text(end)) continue;
      const n = asNumber(text(end));
      if (n === null || (kind === "length" && (!Number.isInteger(n) || n < 0)))
        return { error: kind === "length" ? "Give whole numbers of 0 or more." : "Give numbers." };
      ends[end] = n;
    }
    if (ends.min === undefined && ends.max === undefined) return { error: "Give a min or a max." };
    if (ends.min !== undefined && ends.max !== undefined && ends.min > ends.max)
      return { error: "Min must be at most max." };
    return { rule: { check: kind, ...ends } };
  }
  return { rule: { check: kind } };
}
