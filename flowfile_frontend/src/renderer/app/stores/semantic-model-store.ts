// Semantic model store: the model of the selected schema or catalog, its review queue and history
import { defineStore } from "pinia";
import { ref } from "vue";
import { SemanticModelApi } from "../api/semanticModel.api";
import { useCatalogStore } from "./catalog-store";
import type {
  MetricQuery,
  SemanticAskOut,
  SemanticDraftOut,
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

  let loadToken = 0;
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

  const suggestModel = (): Promise<SuggestOut> => run((id) => SemanticModelApi.suggest(id));

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

  const requestAsk = (query: MetricQuery) => {
    pendingAsk.value = query;
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
    load,
    updateModel,
    propose,
    review,
    reviewBatch,
    suggestModel,
    draftModel,
    askQuestion,
    sync,
    exportYaml,
    loadHistory,
    clear,
  };
});
