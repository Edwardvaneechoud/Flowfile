import { ref } from "vue";
import { useSemanticModelStore } from "../stores/semantic-model-store";
import {
  isAiNotSetUp,
  type SemanticSuggestKind,
  type SemanticSuggestRequestOut,
  type SemanticSuggestionSkipped,
  type SuggestCounts,
  type SuggestOut,
} from "../types/semantic.types";
import { useSemanticAi } from "./useSemanticAi";

/** What one suggest press ended in, worded for a person: nothing in it is live yet. */
export interface SuggestOutcome {
  line: string;
  proposed: number;
  notes: string[];
  skipped: SemanticSuggestionSkipped[];
  warn: boolean;
}

const NOUNS: Record<SemanticSuggestKind, [string, string]> = {
  join: ["join", "joins"],
  metric: ["metric", "metrics"],
  description: ["description", "descriptions"],
  role: ["column role", "column roles"],
};

const COUNT_KEYS: Record<SemanticSuggestKind, keyof SuggestCounts> = {
  join: "joins",
  metric: "metrics",
  description: "descriptions",
  role: "roles",
};

const ORDER: SemanticSuggestKind[] = ["metric", "join", "description", "role"];

const REASONS: Record<string, string> = {
  timeout: "The model did not answer in time.",
  provider_error: "The AI call failed.",
  invalid_answer: "The model's answer could not be read.",
  all_refused: "Nothing was proposed: every suggestion failed its checks.",
};

const listOf = (parts: string[]) =>
  parts.length <= 1
    ? (parts[0] ?? "")
    : `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;

/** "2 joins and 1 metric" from counts per kind; empty when all are zero. */
export function countsText(counts: Partial<Record<SemanticSuggestKind, number>>): string {
  return listOf(
    ORDER.filter((k) => (counts[k] ?? 0) > 0).map((k) => {
      const n = counts[k] ?? 0;
      return `${n} ${NOUNS[k][n === 1 ? 0 : 1]}`;
    }),
  );
}

/** "joins", "metrics", "descriptions or column roles": what a scoped press looks for. */
export function kindsText(kinds: SemanticSuggestKind[] | null | undefined, single = false): string {
  const list = (kinds && kinds.length ? kinds : ORDER).map((k) => NOUNS[k][single ? 0 : 1]);
  return list.length <= 1
    ? (list[0] ?? "")
    : `${list.slice(0, -1).join(", ")} or ${list[list.length - 1]}`;
}

function waits(n: number): string {
  return n === 1 ? "It waits in Review." : "They wait in Review.";
}

/** Why none of the AI's suggestions became a proposal: off the request's subject, or failed a check. */
function refusedText(skipped: { why: string }[]): string {
  const n = skipped.length;
  if (skipped.every((s) => s.why.startsWith("Not what was asked"))) {
    return n === 1
      ? "the AI's suggestion was about something other than what you asked for"
      : `the AI's ${n} suggestions were about something other than what you asked for`;
  }
  return n === 1
    ? "the AI's suggestion did not pass the checks"
    : `none of the AI's ${n} suggestions passed the checks`;
}

/** The one result line of an AI request (and its notes), never claiming anything is live. */
export function describeAiOutcome(out: SemanticSuggestRequestOut): SuggestOutcome {
  const counts: Partial<Record<SemanticSuggestKind, number>> = {};
  for (const p of out.proposed) counts[p.kind] = (counts[p.kind] ?? 0) + 1;
  const named = out.proposed
    .filter((p) => p.name && (p.kind === "metric" || p.kind === "join"))
    .map((p) => p.name as string)
    .slice(0, 3);
  const notes = [...out.notes];
  let line: string;
  let warn = false;
  if (out.proposed.length > 0) {
    const names =
      named.length && named.length === out.proposed.length ? `: ${named.join(", ")}` : "";
    line = `Proposed ${countsText(counts)}${names}. ${waits(out.proposed.length)}`;
  } else if (out.nothing_to_suggest) {
    line = out.why ? `Nothing to suggest: ${out.why}` : "The AI found nothing to suggest for this.";
  } else if (out.degraded) {
    line = REASONS[out.reason ?? ""] ?? "The AI could not make a suggestion.";
    warn = true;
  } else if (out.skipped.length > 0) {
    line = `Nothing was proposed, so nothing changed: ${refusedText(out.skipped)}.`;
    warn = true;
  } else {
    line = "Nothing new to suggest: the model already holds what the AI found.";
  }
  if (out.withheld_columns > 0) {
    notes.push(
      `${out.withheld_columns} confidential column${out.withheld_columns === 1 ? " was" : "s were"} sent by name and type only.`,
    );
  }
  return { line, proposed: out.proposed.length, notes, skipped: out.skipped, warn };
}

/** The result of a tab's "Suggest …" press: the rules' part plus, when it ran, the AI's. */
export function describeScopedOutcome(
  kinds: SemanticSuggestKind[],
  rules: SuggestOut | null,
  ai: SemanticSuggestRequestOut | null,
  aiNote: string | null,
): SuggestOutcome {
  const counts: Partial<Record<SemanticSuggestKind, number>> = {};
  for (const k of kinds) counts[k] = rules ? rules.added[COUNT_KEYS[k]] : 0;
  for (const p of ai?.proposed ?? []) counts[p.kind] = (counts[p.kind] ?? 0) + 1;
  const total = Object.values(counts).reduce((a, b) => a + (b ?? 0), 0);
  const notes: string[] = [];
  if (aiNote) notes.push(aiNote);
  let warn = false;
  if (ai?.degraded && !ai.proposed.length) {
    // The result line already says so when every suggestion was refused.
    if (ai.reason !== "all_refused") {
      notes.push(`The AI part: ${REASONS[ai.reason ?? ""] ?? "it could not make a suggestion."}`);
    }
    warn = true;
  }
  if (ai) notes.push(...ai.notes);
  if (rules && !rules.stats_available) {
    notes.push("Column statistics were not available, so the rules used names and types only.");
  }
  const who = ai ? "the rules and the AI" : "the rules";
  let line: string;
  if (total > 0) {
    line = `Proposed ${countsText(counts)}. ${waits(total)}`;
  } else if (ai && ai.skipped.length > 0) {
    line = `Nothing new: the rules found no more ${kindsText(kinds)}, and ${refusedText(ai.skipped)}.`;
  } else {
    line = `Nothing new to suggest: every ${kindsText(kinds, true)} ${who} found is already in the model.`;
  }
  return { line, proposed: total, notes, skipped: ai?.skipped ?? [], warn };
}

/** One suggest surface's state: a free-text request to the AI, or a tab's scoped "Suggest …" press. */
export function useSemanticSuggest() {
  const store = useSemanticModelStore();
  const { aiStore, usable, canOfferLocal, pick } = useSemanticAi();
  const running = ref(false);
  const step = ref("");
  const outcome = ref<SuggestOutcome | null>(null);
  const notSetUp = ref(false);
  const error = ref("");

  function begin(text: string) {
    running.value = true;
    step.value = text;
    outcome.value = null;
    error.value = "";
  }

  /** The AI alone: there is no rules version of a request in words. */
  async function ask(request: string, kinds: SemanticSuggestKind[] | null = null): Promise<void> {
    const text = request.trim();
    if (!text || running.value) return;
    begin("Asking the AI, then checking each suggestion against your data…");
    try {
      outcome.value = describeAiOutcome(await store.suggestOnRequest(text, kinds, pick()));
    } catch (e) {
      if (isAiNotSetUp(e)) notSetUp.value = true;
      else error.value = errorMessage(e, "Could not ask for suggestions");
    } finally {
      running.value = false;
    }
  }

  /** Rules scoped to `kinds` first (no AI needed), then the AI for what rules cannot find. */
  async function suggestMore(kinds: SemanticSuggestKind[]): Promise<void> {
    if (running.value) return;
    begin(`Looking for ${kindsText(kinds)} with the rules…`);
    try {
      const rules = await store.suggestModel(kinds);
      let ai: SemanticSuggestRequestOut | null = null;
      let aiNote: string | null = null;
      if (usable.value) {
        step.value = `Asking the AI for more ${kindsText(kinds)} and checking each against your data…`;
        try {
          ai = await store.suggestOnRequest(null, kinds, pick());
        } catch (e) {
          if (!isAiNotSetUp(e)) throw e;
          notSetUp.value = true;
          aiNote = "AI is not set up, so only the rules looked.";
        }
      }
      outcome.value = describeScopedOutcome(kinds, rules, ai, aiNote);
    } catch (e) {
      error.value = errorMessage(e, "Could not suggest");
    } finally {
      running.value = false;
    }
  }

  function clear() {
    outcome.value = null;
    error.value = "";
  }

  return {
    aiStore,
    usable,
    canOfferLocal,
    running,
    step,
    outcome,
    notSetUp,
    error,
    ask,
    suggestMore,
    clear,
  };
}

function errorMessage(e: unknown, fallback: string): string {
  const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail) && detail[0] && typeof detail[0].msg === "string") return detail[0].msg;
  return (e as { message?: string })?.message ?? fallback;
}
