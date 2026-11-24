import type {
  SemanticFact,
  SemanticFactStatus,
  SemanticSourceKind,
} from "../../../types/semantic.types";

/** The catalog page of a table (the same location CatalogView pushes when a table is selected). */
export const tablePageLocation = (tableId: number) => ({
  name: "catalog",
  query: { tab: "catalog", tableId: String(tableId) },
});

export type ChipState = "accepted" | "pending" | "stale";

export const COLUMN_ROLES = ["key", "time", "category", "amount"] as const;
export const CONFIDENTIALITY_LEVELS = ["public", "internal", "confidential", "restricted"] as const;

/** What a column is used for when a number is asked for; `value` is what the ledger stores. */
export const ROLE_OPTIONS: { value: (typeof COLUMN_ROLES)[number]; label: string; hint: string }[] =
  [
    { value: "key", label: "Identifier", hint: "Tells rows apart. Counted, never added up." },
    { value: "time", label: "Date", hint: "When it happened. Used for trends over time." },
    { value: "category", label: "Group by", hint: "A label to split a number by." },
    { value: "amount", label: "Amount", hint: "A number to add up or average." },
  ];

/** What each level means for AI; `value` is what the ledger stores. People see every level. */
export const CONFIDENTIALITY_OPTIONS: {
  value: (typeof CONFIDENTIALITY_LEVELS)[number];
  label: string;
  hint: string;
}[] = [
  { value: "public", label: "Public", hint: "AI may read it." },
  { value: "internal", label: "Internal", hint: "AI may read it." },
  {
    value: "confidential",
    label: "Confidential",
    hint: "Kept from AI: it sees the name and type only.",
  },
  {
    value: "restricted",
    label: "Restricted",
    hint: "Kept from AI: it sees the name and type only.",
  },
];

export const CONFIDENTIALITY_HELP =
  "Confidential and restricted columns are kept from AI: no value, description or synonym " +
  "is sent to a model, only the column's name and type. This applies as soon as the level is " +
  "proposed. People who can open this schema still see everything.";

export const ROLE_HELP =
  "How this column is used when you ask for a number: an identifier is counted, " +
  "an amount is added up, a date gives a trend, and a group-by column splits the result.";

const ATTRIBUTE_LABELS: Record<string, string> = { role: "used as" };

export const attributeLabel = (attribute: string): string =>
  ATTRIBUTE_LABELS[attribute] ?? attribute;

/** A fact's value as a person reads it: a role by its label, anything else as text. */
export const displayValue = (attribute: string, value: unknown): string => {
  if (attribute === "role") {
    const option = ROLE_OPTIONS.find((o) => o.value === value);
    if (option) return option.label;
  }
  // A metric's formula is stored as {sql}; show the SQL, not the wrapper.
  const sql = (value as { sql?: unknown } | null)?.sql;
  if (typeof sql === "string") return sql;
  return valueText(value);
};

/** True for a fact that still needs a person: proposed or stale. An accepted fact shows no chip. */
export const needsReview = (fact: SemanticFact | undefined): boolean => {
  if (!fact) return false;
  const state = chipState(fact.status);
  return state === "pending" || state === "stale";
};

export const chipState = (status: SemanticFactStatus): ChipState | null => {
  if (status === "accepted") return "accepted";
  if (status === "proposed" || status === "reviewed") return "pending";
  if (status === "stale") return "stale";
  return null;
};

export const SOURCE_LABELS: Record<SemanticSourceKind, string> = {
  human: "Person",
  ai: "AI",
  inferred: "Rules",
  catalog: "Catalog",
  import: "Import",
  system: "System",
  other: "Other",
};

export const SOURCE_ICONS: Record<SemanticSourceKind, string> = {
  human: "fa-solid fa-user",
  ai: "fa-solid fa-wand-magic-sparkles",
  inferred: "fa-solid fa-gears",
  catalog: "fa-solid fa-database",
  import: "fa-solid fa-file-import",
  system: "fa-solid fa-microchip",
  other: "fa-solid fa-circle-question",
};

export const valueText = (value: unknown): string => {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(valueText).join(", ");
  return JSON.stringify(value);
};

export const factText = (fact: SemanticFact | undefined): string => valueText(fact?.value);

/** One sentence on who stated a fact that is waiting for review, for a tooltip. */
export const sourceHint = (fact: SemanticFact): string => {
  const parts = [`Proposed by ${SOURCE_LABELS[fact.source_kind].toLowerCase()}`];
  if (fact.confidence !== null) parts.push(`${Math.round(fact.confidence * 100)}% confident`);
  if (fact.rationale) parts.push(fact.rationale);
  return parts.join(" · ");
};

export const shortTime = (iso: string): string => {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
};

export const errorText = (e: unknown, fallback: string): string => {
  const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  return (e as { message?: string })?.message ?? fallback;
};
