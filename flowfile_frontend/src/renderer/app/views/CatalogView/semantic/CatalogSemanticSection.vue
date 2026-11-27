<template>
  <section class="catalog-semantic" data-testid="catalog-semantic-section">
    <h3 class="group-title">Semantic models</h3>
    <p class="hint spaced">
      Each schema has its own model. Open one to describe its tables and define its metrics.
    </p>

    <p v-if="schemas.length === 0" class="hint spaced">This catalog has no schemas yet.</p>
    <ul v-else class="list">
      <li
        v-for="schema in schemas"
        :key="schema.id"
        class="box schema-row"
        data-testid="catalog-semantic-schema"
      >
        <i class="fa-solid fa-sitemap schema-icon"></i>
        <span class="name">{{ schema.name }}</span>
        <span class="hint grow">{{ countsText(schema) }}</span>
        <button
          type="button"
          class="btn btn-secondary btn-sm"
          data-testid="catalog-semantic-open"
          @click="emit('open-semantic-model', schema.id)"
        >
          {{ modelled(schema) ? "Open" : "Start" }}
        </button>
      </li>
    </ul>

    <div class="toolbar joins-header">
      <h3 class="group-title">Joins across schemas</h3>
      <div class="line">
        <button
          type="button"
          class="btn btn-ghost btn-sm"
          data-testid="catalog-semantic-suggest"
          :disabled="suggesting || model === null"
          @click="suggestJoins"
        >
          <i
            :class="suggesting ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-wand-magic-sparkles'"
          ></i>
          Suggest joins
        </button>
        <button
          type="button"
          class="btn btn-primary btn-sm"
          data-testid="catalog-semantic-add-join"
          :disabled="model === null"
          @click="showDialog = true"
        >
          <i class="fa-solid fa-plus"></i> Add join
        </button>
      </div>
    </div>
    <p v-if="outcome" class="hint spaced" data-testid="catalog-semantic-outcome">{{ outcome }}</p>

    <p v-if="store.loading && model === null" class="hint spaced">Loading…</p>
    <p v-else-if="store.error && model === null" class="line error spaced" role="alert">
      {{ store.error }}
      <button type="button" class="btn btn-secondary btn-sm" @click="store.load(catalog.id)">
        Retry
      </button>
    </p>
    <p v-else-if="joins.length === 0" class="hint spaced" data-testid="catalog-semantic-no-joins">
      No joins across schemas yet. A metric in one schema can use a table of another once they are
      joined.
    </p>
    <SemanticJoinList
      v-else
      :joins="joins"
      :namespace-id="catalog.id"
      :busy="deciding"
      :show-layer="false"
      :table-label="tableLabel"
    >
      <template #actions="{ join, removing }">
        <template v-if="chipState(join.status) === 'pending' && model?.can_manage && join.group">
          <button
            type="button"
            class="btn btn-success btn-sm"
            data-testid="catalog-semantic-accept"
            :disabled="deciding || removing"
            @click="decide(join.group, 'accept')"
          >
            Accept
          </button>
          <button
            type="button"
            class="btn btn-secondary btn-sm"
            data-testid="catalog-semantic-reject"
            :disabled="deciding || removing"
            @click="decide(join.group, 'reject')"
          >
            Reject
          </button>
        </template>
      </template>
    </SemanticJoinList>

    <SemanticJoinDialog
      v-model="showDialog"
      :namespace-id="catalog.id"
      pending-note="Proposed. It waits for review."
      @added="noteOwner"
    />
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { useCatalogStore } from "../../../stores/catalog-store";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { NamespaceTree } from "../../../types/catalog.types";
import { errorText } from "../../../types/semantic.types";
import { plural } from "../../../utils/text";
import SemanticJoinDialog from "./SemanticJoinDialog.vue";
import SemanticJoinList from "./SemanticJoinList.vue";
import { chipState } from "./semanticFormat";

const props = defineProps<{ catalog: NamespaceTree }>();
const emit = defineEmits<{ "open-semantic-model": [schemaId: number] }>();

const catalogStore = useCatalogStore();
// The panel of a schema's model is never shown beside this section, so both hold their model in the store.
const store = useSemanticModelStore();
const model = computed(() => (store.model?.namespace_id === props.catalog.id ? store.model : null));
const deciding = ref(false);
const suggesting = ref(false);
const outcome = ref("");
const showDialog = ref(false);

// The tree as last loaded, so the counts follow a change made here or in a schema's model.
const schemas = computed(
  () => (catalogStore.findNamespaceInTree(props.catalog.id) ?? props.catalog).children,
);

const joins = computed(() => (model.value?.joins ?? []).filter((j) => j.layer === "catalog"));

const modelled = ({ semantic_model: m }: NamespaceTree) =>
  !!m && m.accepted + m.pending + m.stale > 0;

function countsText(s: NamespaceTree): string {
  const m = s.semantic_model;
  if (!m || !modelled(s)) return "Not modelled yet";
  return `${m.accepted} accepted · ${m.pending + m.stale} waiting for review`;
}

/** `schema.table` for a dataset key of this catalog; any other key as it is. */
const tableLabel = (key: string): string => {
  const prefix = `${model.value?.key ?? props.catalog.name}.`;
  return key.startsWith(prefix) ? key.slice(prefix.length) : key;
};

/** A join added here between two tables of one schema belongs to that schema's model. */
function noteOwner(name: string, ...ids: number[]) {
  const [from, to] = ids.map((id) =>
    schemas.value.find((s) => (s.tables ?? []).some((t) => t.id === id)),
  );
  if (!from || from !== to) return;
  ElMessage.info(`${name} joins two tables of ${from.name}, so it is kept in that schema's model.`);
}

async function decide(group: string, action: "accept" | "reject") {
  deciding.value = true;
  try {
    const result = await store.reviewBatch([{ action, group }]);
    if (result.errors.length && !result.changed.length) ElMessage.error(result.errors[0].message);
  } catch (e) {
    ElMessage.error(errorText(e, `Could not ${action} the join`));
  } finally {
    deciding.value = false;
  }
}

async function suggestJoins() {
  suggesting.value = true;
  outcome.value = "";
  const before = new Set(joins.value.map((j) => j.key));
  try {
    const added = (await store.suggestModel(["join"])).added.joins;
    if (added === 0) {
      outcome.value = "Nothing new to suggest.";
      return;
    }
    const across = joins.value.filter((j) => !before.has(j.key)).length;
    const within = Math.max(added - across, 0);
    const parts = [];
    if (across) parts.push(`${across} across schemas, waiting for review here`);
    if (within) parts.push(`${within} within one schema, waiting in that schema's Review`);
    outcome.value = `${plural(added, "join")} proposed: ${parts.join("; ")}.`;
  } catch (e) {
    outcome.value = errorText(e, "Could not suggest joins");
  } finally {
    suggesting.value = false;
  }
}

watch(
  () => props.catalog.id,
  (id) => {
    outcome.value = "";
    void store.load(id);
  },
  { immediate: true },
);
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.catalog-semantic {
  margin-top: var(--spacing-6);
}

.joins-header {
  margin-top: var(--spacing-5);
  margin-bottom: var(--spacing-2);
}

.joins-header .group-title {
  margin: 0;
}

.spaced {
  margin-bottom: var(--spacing-3);
}

.schema-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-4);
}

.schema-icon {
  color: var(--color-text-muted);
}
</style>
