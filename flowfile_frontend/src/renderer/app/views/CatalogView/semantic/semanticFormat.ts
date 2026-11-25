import { ElMessage } from "element-plus";
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

/** What a column can be set to: `value` is what the ledger stores, `hint` what it means. */
interface Option {
  value: string;
  label: string;
  hint: string;
}

export const ROLE_OPTIONS: Option[] = [
  { value: "key", label: "Identifier", hint: "Tells rows apart. Counted, never added up." },
  { value: "time", label: "Date", hint: "When it happened. Used for trends over time." },
  { value: "category", label: "Group by", hint: "A label to split a number by." },
  { value: "amount", label: "Amount", hint: "A number to add up or average." },
];

// People see every level; the hint says what AI sees.
const KEPT = "Kept from AI: it sees the name and type only.";
export const CONFIDENTIALITY_OPTIONS: Option[] = [
  { value: "public", label: "Public", hint: "AI may read it." },
  { value: "internal", label: "Internal", hint: "AI may read it." },
  { value: "confidential", label: "Confidential", hint: KEPT },
  { value: "restricted", label: "Restricted", hint: KEPT },
];

export const CONFIDENTIALITY_HELP =
  "Confidential and restricted columns are kept from AI: no value, description or synonym " +
  "is sent to a model, only the column's name and type. This applies as soon as the level is " +
  "proposed. People who can open this schema still see everything.";

export const ROLE_HELP =
  "How this column is used when you ask for a number: an identifier is counted, " +
  "an amount is added up, a date gives a trend, and a group-by column splits the result.";

export const attributeLabel = (attribute: string): string =>
  attribute === "role" ? "used as" : attribute;

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

export const chipState = (status: SemanticFactStatus): "accepted" | "pending" | "stale" | null => {
  if (status === "proposed" || status === "reviewed") return "pending";
  return status === "accepted" || status === "stale" ? status : null;
};

/** Whether a fact, join or metric still needs a person: proposed or stale. */
export const needsReview = (of: { status: SemanticFactStatus } | undefined): boolean =>
  !!of && ["pending", "stale"].includes(chipState(of.status) ?? "");

export const SOURCES: Record<SemanticSourceKind, { label: string; icon: string }> = {
  human: { label: "Person", icon: "fa-solid fa-user" },
  ai: { label: "AI", icon: "fa-solid fa-wand-magic-sparkles" },
  inferred: { label: "Rules", icon: "fa-solid fa-gears" },
  catalog: { label: "Catalog", icon: "fa-solid fa-database" },
  import: { label: "Import", icon: "fa-solid fa-file-import" },
  system: { label: "System", icon: "fa-solid fa-microchip" },
  other: { label: "Other", icon: "fa-solid fa-circle-question" },
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
  const parts = [`Proposed by ${SOURCES[fact.source_kind].label.toLowerCase()}`];
  if (fact.confidence !== null) parts.push(`${Math.round(fact.confidence * 100)}% confident`);
  if (fact.rationale) parts.push(fact.rationale);
  return parts.join(" · ");
};

/** The words of a comma-separated list, each once. */
export const splitWords = (text: string): string[] => [
  ...new Set(
    text
      .split(",")
      .map((w) => w.trim())
      .filter(Boolean),
  ),
];

/** A table's own name: the last part of its key. */
export const shortName = (key: string): string => key.slice(key.lastIndexOf(".") + 1);

/** Say that a stored proposal waits for a person; nothing when all of it was accepted at once. */
export function notePending(facts: SemanticFact[], text = "Proposed. It waits in the Review tab.") {
  const waits = facts.some((f) => f.status !== "accepted");
  if (waits) ElMessage.info(text);
  return waits;
}
