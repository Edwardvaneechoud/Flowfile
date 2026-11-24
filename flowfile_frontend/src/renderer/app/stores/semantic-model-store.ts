// Semantic model store: the model of the selected schema or catalog, its review queue and history
import { defineStore } from "pinia";
import { ref } from "vue";
import { SemanticModelApi } from "../api/semanticModel.api";
import { useCatalogStore } from "./catalog-store";
import type {
  JoinCheck,
  MetricQuery,
  SemanticAskOut,
  SemanticDraftOut,
  SemanticSuggestKind,
  SemanticSuggestRequestOut,
  SuggestOut,
  SemanticEvent,
  SemanticFact,
  SemanticModel,
  SemanticModelUpdate,
  SemanticProposal,
  SemanticReviewAction,
  SemanticReviewItem,
  SemanticReviewResult,
  SemanticSyncResult,
} from "../types/semantic.types";

export const semanticErrorMessage = (e: any, fallback: string): string => {
  const detail = e?.response?.data?.detail;
  return typeof detail === "string" ? detail : (e?.message ?? fallback);
};

export const useSemanticModelStore = defineStore("semanticModel", () => {
  const namespaceId = ref<number | null>(null);
  const model = ref<SemanticModel | null>(null);
  const loading = ref(false);
  const error = ref<string | null>(null);
  const reviewQueue = ref<SemanticReviewItem[]>([]);
  const reviewLoading = ref(false);
  const history = ref<SemanticEvent[]>([]);
  const historyLoading = ref(false);
  const historyError = ref<string | null>(null);
  const busy = ref(false);
  // A query another part of the UI wants the Ask tab to set and run; the tab consumes it.
  const pendingAsk = ref<MetricQuery | null>(null);
  // A table another part of the UI wants the Tables tab to open; the tab consumes it.
  const focusTableId = ref<number | null>(null);
  // Each join measured against the data, by join key; filled by checkJoins, empty until it answers.
  const joinChecks = ref<Record<string, JoinCheck>>({});

  let loadToken = 0;
  let joinCheckRunning = false;
  let joinCheckAgain = false;
  let historyToken = 0;

  async function refreshReview() {
    const id = namespaceId.value;
    if (id === null) return;
    reviewLoading.value = true;
    try {
      const items = await SemanticModelApi.getReviewQueue(id);
      if (namespaceId.value === id) reviewQueue.value = items;
    } catch (e: any) {
      if (namespaceId.value === id) error.value = semanticErrorMessage(e, "Could not load review");
    } finally {
      reviewLoading.value = false;
    }
  }

  async function load(id: number) {
    const token = ++loadToken;
    if (namespaceId.value !== id) {
      model.value = null;
      reviewQueue.value = [];
      history.value = [];
      joinChecks.value = {};
    }
    namespaceId.value = id;
    loading.value = true;
    error.value = null;
    try {
      const loaded = await SemanticModelApi.get(id);
      if (token !== loadToken) return;
      model.value = loaded;
      await refreshReview();
    } catch (e: any) {
      if (token !== loadToken) return;
      model.value = null;
      error.value = semanticErrorMessage(e, "Could not load the semantic model");
    } finally {
      if (token === loadToken) loading.value = false;
    }
  }

  // The tree's pending badges come from the catalog tree, so a change reloads it.
  async function reloadAfterChange() {
    const id = namespaceId.value;
    if (id === null) return;
    const [loaded] = await Promise.all([
      SemanticModelApi.get(id),
      refreshReview(),
      useCatalogStore().loadTree(),
    ]);
    if (namespaceId.value === id) model.value = loaded;
  }

  async function run<T>(work: (id: number) => Promise<T>): Promise<T> {
    const id = namespaceId.value;
    if (id === null) throw new Error("No semantic model is open");
    busy.value = true;
    try {
      const result = await work(id);
      await reloadAfterChange();
      return result;
    } finally {
      busy.value = false;
    }
  }

  const updateModel = (body: SemanticModelUpdate) => run((id) => SemanticModelApi.update(id, body));

  const propose = (proposals: SemanticProposal[]): Promise<SemanticFact[]> =>
    run((id) => SemanticModelApi.propose(id, proposals));

  const review = (action: SemanticReviewAction): Promise<SemanticReviewResult> =>
    run((id) => SemanticModelApi.review(id, action));

  /** Remove a whole metric or join by its key; the history keeps it. */
  const removeSubject = (subject: string): Promise<SemanticReviewResult> =>
    run((id) => SemanticModelApi.review(id, { action: "retract", subject }));

  const reviewBatch = (actions: SemanticReviewAction[]): Promise<SemanticReviewResult> =>
    run(async (id) => {
      const merged: SemanticReviewResult = { changed: [], errors: [] };
      for (const action of actions) {
        const result = await SemanticModelApi.review(id, action);
        merged.changed.push(...result.changed);
        merged.errors.push(...result.errors);
      }
      return merged;
    });

  const suggestModel = (kinds?: SemanticSuggestKind[]): Promise<SuggestOut> =>
    run((id) => SemanticModelApi.suggest(id, kinds ? { kinds } : undefined));

  /** A request in the person's words (or more of `kinds`) for the AI; what it stores waits for review. */
  const suggestOnRequest = (
    request: string | null,
    kinds: SemanticSuggestKind[] | null,
    pick: { provider?: string; model?: string } = {},
  ): Promise<SemanticSuggestRequestOut> =>
    run((id) =>
      SemanticModelApi.suggestOnRequest({
        namespace_id: id,
        ...(request ? { request } : {}),
        ...(kinds ? { kinds } : {}),
        ...pick,
      }),
    );

  const draftModel = (
    pick: { provider?: string; model?: string } = {},
  ): Promise<SemanticDraftOut> =>
    run((id) => SemanticModelApi.draft({ namespace_id: id, ...pick }));

  async function askQuestion(
    question: string,
    pick: { provider?: string; model?: string } = {},
  ): Promise<SemanticAskOut> {
    const id = namespaceId.value;
    if (id === null) throw new Error("No semantic model is open");
    return SemanticModelApi.ask({ namespace_id: id, question, ...pick });
  }

  const sync = (): Promise<SemanticSyncResult> => run((id) => SemanticModelApi.sync(id));

  async function exportYaml(): Promise<string> {
    const id = namespaceId.value;
    if (id === null) throw new Error("No semantic model is open");
    return SemanticModelApi.exportYaml(id);
  }

  async function loadHistory(subject: string) {
    const id = namespaceId.value;
    if (id === null) return;
    const token = ++historyToken;
    historyLoading.value = true;
    historyError.value = null;
    try {
      const events = await SemanticModelApi.getHistory(id, subject);
      if (token === historyToken) history.value = events;
    } catch (e: any) {
      if (token === historyToken) {
        history.value = [];
        historyError.value = semanticErrorMessage(e, "Could not load history");
      }
    } finally {
      if (token === historyToken) historyLoading.value = false;
    }
  }

  /** Measure the model's joins; one request at a time, a call made meanwhile runs once after it, failures are
   * silent (the list then shows no warnings). */
  async function checkJoins() {
    if (joinCheckRunning) {
      joinCheckAgain = true;
      return;
    }
    joinCheckRunning = true;
    try {
      do {
        joinCheckAgain = false;
        const id = namespaceId.value;
        if (id === null) return;
        try {
          const checks = await SemanticModelApi.checkJoins(id);
          if (namespaceId.value === id && !joinCheckAgain) {
            joinChecks.value = Object.fromEntries(checks.map((c) => [c.key, c]));
          }
        } catch {
          // a failed check leaves the joins unmarked
        }
      } while (joinCheckAgain);
    } finally {
      joinCheckRunning = false;
    }
  }

  const requestAsk = (query: MetricQuery) => {
    pendingAsk.value = query;
  };

  const requestFocusTable = (tableId: number | null) => {
    focusTableId.value = tableId;
  };

  function clear() {
    loadToken++;
    historyToken++;
    namespaceId.value = null;
    model.value = null;
    error.value = null;
    reviewQueue.value = [];
    history.value = [];
    pendingAsk.value = null;
    focusTableId.value = null;
    joinChecks.value = {};
    loading.value = false;
  }

  return {
    namespaceId,
    model,
    loading,
    error,
    reviewQueue,
    reviewLoading,
    history,
    historyLoading,
    historyError,
    busy,
    pendingAsk,
    requestAsk,
    focusTableId,
    requestFocusTable,
    joinChecks,
    checkJoins,
    load,
    updateModel,
    propose,
    review,
    removeSubject,
    reviewBatch,
    suggestModel,
    suggestOnRequest,
    draftModel,
    askQuestion,
    sync,
    exportYaml,
    loadHistory,
    clear,
  };
});
