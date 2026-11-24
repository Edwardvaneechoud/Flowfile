import type {
  SemanticFact,
  SemanticFactStatus,
  SemanticSourceKind,
} from "../../../types/semantic.types";

export type ChipState = "accepted" | "pending" | "stale";

export const COLUMN_ROLES = ["key", "time", "category", "amount"] as const;
export const CONFIDENTIALITY_LEVELS = ["public", "internal", "confidential", "restricted"] as const;

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
