import { ref } from "vue";
import { useSemanticModelStore } from "../stores/semantic-model-store";
import {
  errorText,
  isAiNotSetUp,
  isSuggestRunning,
  type SemanticSuggestKind,
  type SemanticSuggestRequestOut,
  type SemanticSuggestionSkipped,
  type SuggestOut,
} from "../types/semantic.types";
import { SUGGEST_BUSY_TEXT, useSemanticActivity } from "./useSemanticActivity";
import { useSemanticAi } from "./useSemanticAi";
import { SEMANTIC_SUGGEST_SURFACE } from "./useTaskModels";

/** What one suggest press ended in, worded for a person: nothing in it is live yet. */
export interface SuggestOutcome {
  line: string;
  proposed: number;
  notes: string[];
  skipped: SemanticSuggestionSkipped[];
  warn: boolean;
}

type Counts = Partial<Record<SemanticSuggestKind, number>>;

const NOUNS: Record<SemanticSuggestKind, [string, string]> = {
  metric: ["metric", "metrics"],
  join: ["join", "joins"],
  description: ["description", "descriptions"],
  role: ["column role", "column roles"],
  synonyms: ["set of synonyms", "sets of synonyms"],
};
const ORDER = Object.keys(NOUNS) as SemanticSuggestKind[];

/** The rules' counts under the kinds' own names. */
export const ruleCounts = ({ added }: SuggestOut): Counts => ({
  metric: added.metrics,
  join: added.joins,
  description: added.descriptions,
  role: added.roles,
});

const REASONS: Record<string, string> = {
  timeout: "The model did not answer in time.",
  provider_error: "The AI call failed.",
  invalid_answer: "The model's answer could not be read.",
  all_refused: "Nothing was proposed: every suggestion failed its checks.",
};

const listOf = (parts: string[], last = "and") =>
  parts.length <= 1
    ? (parts[0] ?? "")
    : `${parts.slice(0, -1).join(", ")} ${last} ${parts[parts.length - 1]}`;

/** "2 joins and 1 metric" from counts per kind; empty when all are zero. */
export const countsText = (counts: Counts): string =>
  listOf(
    ORDER.filter((k) => counts[k]).map((k) => `${counts[k]} ${NOUNS[k][counts[k] === 1 ? 0 : 1]}`),
  );

/** "joins", "metrics", "descriptions or column roles": what a scoped press looks for. */
const kindsText = (kinds: SemanticSuggestKind[], single = false): string =>
  listOf(
    kinds.map((k) => NOUNS[k][single ? 0 : 1]),
    "or",
  );

const tally = (counts: Counts, proposed: { kind: SemanticSuggestKind }[]): Counts => {
  for (const p of proposed) counts[p.kind] = (counts[p.kind] ?? 0) + 1;
  return counts;
};

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
function describeAiOutcome(out: SemanticSuggestRequestOut): SuggestOutcome {
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
    line = `Proposed ${countsText(tally({}, out.proposed))}${names}. ${waits(out.proposed.length)}`;
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
function describeScopedOutcome(
  kinds: SemanticSuggestKind[],
  rules: SuggestOut,
  ai: SemanticSuggestRequestOut | null,
  aiNote: string | null,
): SuggestOutcome {
  const found = ruleCounts(rules);
  const counts = tally(
    Object.fromEntries(kinds.map((k) => [k, found[k] ?? 0])),
    ai?.proposed ?? [],
  );
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
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
  if (!rules.stats_available) {
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

/** One suggest surface's state: the header box's request in words, or a tab's scoped "Suggest …" press. */
export function useSemanticSuggest() {
  const store = useSemanticModelStore();
  const { aiStore, usable, pick } = useSemanticAi();
  const { busy, track } = useSemanticActivity();
  const running = ref(false);
  const outcome = ref<SuggestOutcome | null>(null);
  const notSetUp = ref(false);
  const error = ref("");

  function begin() {
    running.value = true;
    outcome.value = null;
    error.value = "";
  }

  function failed(e: unknown, fallback: string) {
    error.value = isSuggestRunning(e) ? SUGGEST_BUSY_TEXT : errorText(e, fallback);
  }

  /** The AI alone: there is no rules version of a request in words. */
  async function ask(request: string): Promise<void> {
    const text = request.trim();
    if (!text || running.value || busy.value) return;
    begin();
    try {
      const out = await track(
        () => store.suggestOnRequest({ request: text }, pick(SEMANTIC_SUGGEST_SURFACE)),
        {
          ownOutcome: true,
        },
      );
      outcome.value = describeAiOutcome(out);
    } catch (e) {
      if (isAiNotSetUp(e)) notSetUp.value = true;
      else failed(e, "Could not ask for suggestions");
    } finally {
      running.value = false;
    }
  }

  /** Rules scoped to `kinds` first (no AI needed), then the AI for what rules cannot find. */
  async function suggestMore(kinds: SemanticSuggestKind[]): Promise<void> {
    if (running.value || busy.value) return;
    begin();
    try {
      outcome.value = await track(
        async () => {
          const rules = await store.suggestModel(kinds);
          let ai: SemanticSuggestRequestOut | null = null;
          let aiNote: string | null = null;
          if (usable.value) {
            try {
              ai = await store.suggestOnRequest({ kinds }, pick(SEMANTIC_SUGGEST_SURFACE));
            } catch (e) {
              if (!isAiNotSetUp(e)) throw e;
              notSetUp.value = true;
              aiNote = "AI is not set up, so only the rules looked.";
            }
          }
          return describeScopedOutcome(kinds, rules, ai, aiNote);
        },
        { ownOutcome: true },
      );
    } catch (e) {
      failed(e, "Could not suggest");
    } finally {
      running.value = false;
    }
  }

  function clear() {
    outcome.value = null;
    error.value = "";
  }

  return { aiStore, usable, running, busy, outcome, notSetUp, error, ask, suggestMore, clear };
}
