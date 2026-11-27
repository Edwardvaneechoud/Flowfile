// Semantic model store: the model of the selected schema or catalog and its review queue
import { defineStore } from "pinia";
import { ref } from "vue";
import { SemanticModelApi } from "../api/semanticModel.api";
import { errorText } from "../types/semantic.types";
import { useCatalogStore } from "./catalog-store";
import { ruleKey } from "../views/CatalogView/semantic/dataRules";
import type {
  DataRuleResult,
  DataRuleTable,
  DataRulesRun,
  JoinCheck,
  SemanticFact,
  SemanticModel,
  SemanticModelPick,
  SemanticModelUpdate,
  SemanticProposal,
  SemanticReviewAction,
  SemanticReviewItem,
  SemanticReviewResult,
  SemanticSuggestKind,
} from "../types/semantic.types";

export const useSemanticModelStore = defineStore("semanticModel", () => {
  const namespaceId = ref<number | null>(null);
  const model = ref<SemanticModel | null>(null);
  const loading = ref(false);
  const error = ref<string | null>(null);
  const reviewQueue = ref<SemanticReviewItem[]>([]);
  const reviewLoading = ref(false);
  const busy = ref(false);
  // A table another part of the UI wants the Tables tab to open; the tab consumes it.
  const focusTableId = ref<number | null>(null);
  // Each join measured against the data, by join key; filled by checkJoins, empty until it answers.
  const joinChecks = ref<Record<string, JoinCheck>>({});
  // What each table's last run of its data rules found, by table id then `ruleKey`.
  const ruleResults = ref<Record<number, Record<string, DataRuleResult>>>({});
  // The tables whose rules are being run now.
  const rulesRunning = ref<number[]>([]);

  let loadToken = 0;
  let rulesRuns = 0;
  let joinCheckRunning = false;
  let joinCheckAgain = false;

  async function refreshReview() {
    const id = namespaceId.value;
    if (id === null) return;
    reviewLoading.value = true;
    try {
      const items = await SemanticModelApi.getReviewQueue(id);
      if (namespaceId.value === id) reviewQueue.value = items;
    } catch (e) {
      if (namespaceId.value === id) error.value = errorText(e, "Could not load review");
    } finally {
      reviewLoading.value = false;
    }
  }

  async function load(id: number) {
    const token = ++loadToken;
    if (namespaceId.value !== id) {
      model.value = null;
      reviewQueue.value = [];
      joinChecks.value = {};
      ruleResults.value = {};
    }
    namespaceId.value = id;
    loading.value = true;
    error.value = null;
    try {
      const loaded = await SemanticModelApi.get(id);
      if (token !== loadToken) return;
      model.value = loaded;
      await refreshReview();
    } catch (e) {
      if (token !== loadToken) return;
      model.value = null;
      error.value = errorText(e, "Could not load the semantic model");
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

  function openId(): number {
    if (namespaceId.value === null) throw new Error("No semantic model is open");
    return namespaceId.value;
  }

  async function run<T>(work: (id: number) => Promise<T>): Promise<T> {
    const id = openId();
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

  /** Remove a whole metric or join by its key; the history keeps it. */
  const removeSubject = (subject: string) =>
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

  const suggestModel = (kinds?: SemanticSuggestKind[]) =>
    run((id) => SemanticModelApi.suggest(id, kinds));

  /** A request in the person's words (or more of `kinds`) for the AI; what it stores waits for review. */
  const suggestOnRequest = (
    scope: { request?: string; kinds?: SemanticSuggestKind[] },
    pick: SemanticModelPick,
  ) => run((id) => SemanticModelApi.suggestOnRequest(id, { ...scope, ...pick }));

  const draftModel = (pick: SemanticModelPick) => run((id) => SemanticModelApi.draft(id, pick));

  const sync = () => run((id) => SemanticModelApi.sync(id));

  const askQuestion = async (question: string, pick: SemanticModelPick) =>
    SemanticModelApi.ask(openId(), question, pick);

  const exportYaml = async () => SemanticModelApi.exportYaml(openId());

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

  const keyed = (table: DataRuleTable) =>
    Object.fromEntries(table.results.map((r) => [ruleKey(r.column, r.rule), r]));

  /** The last results of the model's data rules; a failed read leaves the rules unmarked. */
  async function loadRuleResults() {
    const id = namespaceId.value;
    if (id === null) return;
    const runsBefore = rulesRuns;
    try {
      const tables = await SemanticModelApi.getDataRuleResults(id);
      // a run that answered meanwhile is newer than what was read
      if (namespaceId.value !== id || rulesRuns !== runsBefore) return;
      ruleResults.value = Object.fromEntries(tables.map((t) => [t.table_id, keyed(t)]));
    } catch {
      // the rules then show without a result
    }
  }

  /** Run the accepted data rules of `tableIds` and keep what each table answered. */
  async function runDataRules(tableIds: number[]): Promise<DataRulesRun> {
    const id = openId();
    rulesRunning.value = [...rulesRunning.value, ...tableIds];
    try {
      const run = await SemanticModelApi.runDataRules(id, tableIds);
      rulesRuns += 1;
      if (namespaceId.value === id) {
        const next = { ...ruleResults.value };
        for (const table of run.tables) if (!table.skipped) next[table.table_id] = keyed(table);
        ruleResults.value = next;
      }
      return run;
    } finally {
      rulesRunning.value = rulesRunning.value.filter((t) => !tableIds.includes(t));
    }
  }

  return {
    namespaceId,
    model,
    loading,
    error,
    reviewQueue,
    reviewLoading,
    busy,
    focusTableId,
    joinChecks,
    checkJoins,
    ruleResults,
    rulesRunning,
    loadRuleResults,
    runDataRules,
    load,
    updateModel,
    propose,
    removeSubject,
    reviewBatch,
    suggestModel,
    suggestOnRequest,
    draftModel,
    askQuestion,
    sync,
    exportYaml,
  };
});
