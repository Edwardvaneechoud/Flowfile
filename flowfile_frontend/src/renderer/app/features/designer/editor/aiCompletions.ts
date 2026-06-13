// — CodeMirror autocomplete source backed by the AI settings-autocomplete
// endpoint.
//
// Returns a `CompletionSource` factory that the existing FunctionEditor wires
// alongside the static `polarsCompletions` source. Both run in parallel; AI
// suggestions arrive ~400-800 ms after the static ones so the dropdown
// updates with AI options appearing alongside static matches.
//
// Behaviour gates:
//   - sub-3 chars → no AI call (static source still runs)
//   - 300 ms debounce per keystroke
//   - prior in-flight request cancelled via the store's AbortController
//   - timeout / parse error / hallucinated columns → soft fallback (no
//     AI suggestions; static `polarsCompletions` keeps showing what it had)
//
// Schema-grounding promise: any AI suggestion the backend returns has either
//   - `verified=true` — its literal `pl.col("X")` / `[X]` refs all resolved
//     against the upstream schema, or
//   - `verified=false` — extraction was incomplete (complex expression),
//     so we render a small "?" icon to flag it as AI-but-unverified.
// Suggestions whose extracted refs miss the schema are filtered server-side
// and never reach the dropdown.

import type {
  CompletionContext,
  CompletionResult,
  CompletionSource,
} from "@codemirror/autocomplete";
import type { EditorView } from "@codemirror/view";

import { useAiAutocompleteStore } from "../../../stores/ai-autocomplete-store";
import type { FormulaSuggestion } from "../../../api/ai.api";

const MIN_CHARS_BEFORE_AI = 3;
const DEBOUNCE_MS = 300;

interface CreateAiCompletionSourceOptions {
  // The flow id whose schema the backend resolves against. AI source
  // short-circuits to `null` when this is undefined / null / 0 — autocomplete
  // can only ground when we know which flow.
  getFlowId: () => number | null | undefined;
  // The downstream node id (e.g. the formula node). Backend walks
  // `node.all_inputs[0].predicted_schema` to ground references.
  getNodeId: () => number | string | null | undefined;
  // Optional override for the BYOK provider used; defaults to the backend
  // route's default ("google" via Gemini Flash).
  getProvider?: () => string | undefined;
  // Optional explicit model — most callers leave this unset and accept the
  // surface-keyed default (Haiku 4.5 / Gemini Flash / etc.).
  getModel?: () => string | null | undefined;
}

/** Build a CodeMirror `CompletionSource` that calls the AI backend. */
export function createAiCompletionSource(opts: CreateAiCompletionSourceOptions): CompletionSource {
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let resolveWaiter: ((value: CompletionResult | null) => void) | null = null;

  const cancelDebounce = (): void => {
    if (debounceTimer !== null) {
      clearTimeout(debounceTimer);
      debounceTimer = null;
    }
    if (resolveWaiter !== null) {
      resolveWaiter(null);
      resolveWaiter = null;
    }
  };

  return async (context: CompletionContext): Promise<CompletionResult | null> => {
    cancelDebounce();

    // AI suggestions only fire on an explicit request (Cmd/Ctrl+Space) — never
    // automatically while typing. This keeps the LLM from running in the
    // background on every keystroke; the static `polarsCompletions` source still
    // populates the dropdown as you type.
    if (!context.explicit) {
      return null;
    }

    // Determine the active prefix — match a contiguous run of word / dot /
    // bracket / paren chars before the cursor. We don't try to be clever
    // about Polars syntax; the backend gets the prefix string and decides.
    const word = context.matchBefore(/[\w.[\]("']*$/);
    const partial = word?.text ?? "";
    if (partial.length < MIN_CHARS_BEFORE_AI) {
      return null;
    }

    const flowId = opts.getFlowId();
    const nodeId = opts.getNodeId();
    if (flowId == null || flowId === 0 || nodeId == null) {
      return null;
    }

    const debouncePromise = new Promise<CompletionResult | null>((resolve) => {
      resolveWaiter = resolve;
      debounceTimer = setTimeout(() => {
        resolveWaiter = null;
        debounceTimer = null;
        resolve(null); // sentinel — `null` means "proceed past debounce"
      }, DEBOUNCE_MS);
    });

    const debounced = await debouncePromise;
    if (debounced !== null) {
      // We resolved early because a sibling keystroke superseded us.
      return null;
    }

    const store = useAiAutocompleteStore();
    const response = await store.getFormulaSuggestions({
      flowId: Number(flowId),
      nodeId: nodeId,
      partialText: partial,
      provider: opts.getProvider?.() ?? undefined,
      model: opts.getModel?.() ?? undefined,
    });
    if (response === null || response.degraded) {
      return null;
    }

    const from = word?.from ?? context.pos;
    const to = word?.to ?? context.pos;

    return {
      from,
      to,
      options: response.suggestions.map((suggestion: FormulaSuggestion) => ({
        label: suggestion.label,
        type: "ai-suggestion",
        boost: -1, // sort below static `polarsCompletions` matches
        info: buildInfo(suggestion),
        apply: (view: EditorView) => {
          view.dispatch({
            changes: { from, to, insert: suggestion.insertText },
            selection: { anchor: from + suggestion.insertText.length },
          });
        },
      })),
    };
  };
}

const buildInfo = (suggestion: FormulaSuggestion): string => {
  const tag = suggestion.verified ? "AI" : "AI · unverified";
  if (suggestion.description && suggestion.description.length > 0) {
    return `${tag}: ${suggestion.description}`;
  }
  return tag;
};
