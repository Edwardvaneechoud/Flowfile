// One table's semantic data, loaded into local state so an open model panel's store is left alone
import { computed, ref, watch, type Ref } from "vue";
import { SemanticModelApi } from "../api/semanticModel.api";
import { useCatalogStore } from "../stores/catalog-store";
import { semanticErrorMessage } from "../stores/semantic-model-store";
import type { ColumnSchema } from "../types";
import type { SemanticProposal, SemanticTable } from "../types/semantic.types";

interface TableRef {
  id: number;
  namespace_id?: number | null;
  source?: string | null;
  schema_columns?: ColumnSchema[] | null;
}

export function useTableSemantics(table: Ref<TableRef>) {
  const entry = ref<SemanticTable | null>(null);
  let token = 0;

  const applies = () => {
    const t = table.value;
    return t.id > 0 && t.source !== "hub" && t.namespace_id != null;
  };

  async function fetchEntry(): Promise<SemanticTable | null> {
    const t = table.value;
    const model = await SemanticModelApi.get(t.namespace_id as number);
    return model.tables.find((x) => x.table_id === t.id) ?? null;
  }

  async function load() {
    const mine = ++token;
    entry.value = null;
    if (!applies()) return;
    try {
      const found = await fetchEntry();
      if (mine === token) entry.value = found;
    } catch {
      // the plain schema list stays
    }
  }

  watch(() => [table.value.id, table.value.namespace_id, table.value.source], load, {
    immediate: true,
  });

  /** The table's columns with the model's facts; a column the ledger has not synced shows with empty cells. */
  const semanticTable = computed<SemanticTable | null>(() => {
    const e = entry.value;
    if (!e) return null;
    const known = new Map(e.fields.map((f) => [f.name, f]));
    const columns = table.value.schema_columns ?? [];
    const fields = columns.map(
      (c) => known.get(c.name) ?? { name: c.name, dtype: c.dtype, facts: {}, pending: [] },
    );
    return { ...e, fields: fields.length ? fields : e.fields };
  });

  /** Propose facts, reload this table's data and the tree's counts; resolves true when all were accepted. */
  async function propose(proposals: SemanticProposal[]): Promise<boolean> {
    const mine = ++token;
    const namespaceId = table.value.namespace_id as number;
    try {
      const facts = await SemanticModelApi.propose(namespaceId, proposals);
      const [found] = await Promise.all([fetchEntry(), useCatalogStore().loadTree()]);
      if (mine === token) entry.value = found;
      return facts.every((f) => f.status === "accepted");
    } catch (e) {
      throw new Error(semanticErrorMessage(e, "Could not save the change"));
    }
  }

  return { semanticTable, propose, reload: load };
}
