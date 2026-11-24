<template>
  <section class="catalog-semantic" data-testid="catalog-semantic-section">
    <div class="section-header">
      <h3 class="section-title">Semantic models</h3>
    </div>
    <p class="hint">
      Each schema has its own model. Open one to describe its tables and define its metrics.
    </p>

    <p v-if="schemas.length === 0" class="empty">This catalog has no schemas yet.</p>
    <ul v-else class="schema-list">
      <li
        v-for="schema in schemas"
        :key="schema.id"
        class="schema-row"
        data-testid="catalog-semantic-schema"
      >
        <i class="fa-solid fa-sitemap schema-icon"></i>
        <span class="schema-name">{{ schema.name }}</span>
        <span class="schema-counts">{{ countsText(schema) }}</span>
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

    <div class="section-header joins-header">
      <h3 class="section-title">Joins across schemas</h3>
      <div class="joins-actions">
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
    <p v-if="outcome" class="outcome" data-testid="catalog-semantic-outcome">{{ outcome }}</p>

    <p v-if="loading && model === null" class="empty">Loading…</p>
    <p v-else-if="error" class="empty error" role="alert">
      {{ error }}
      <button type="button" class="btn btn-secondary btn-sm" @click="load">Retry</button>
    </p>
    <p v-else-if="joins.length === 0" class="empty" data-testid="catalog-semantic-no-joins">
      No joins across schemas yet. A metric in one schema can use a table of another once they are
      joined.
    </p>
    <ul v-else class="join-list">
      <SemanticJoinRow
        v-for="join in joins"
        :key="join.key"
        :join="join"
        :check="checks[join.key]"
        :show-layer="false"
        :table-label="tableLabel"
      >
        <template #actions>
          <template v-if="isPending(join) && model?.can_manage && join.group">
            <button
              type="button"
              class="btn btn-success btn-sm"
              data-testid="catalog-semantic-accept"
              :disabled="busy !== null"
              @click="decide(join, 'accept')"
            >
              Accept
            </button>
            <button
              type="button"
              class="btn btn-secondary btn-sm"
              data-testid="catalog-semantic-reject"
              :disabled="busy !== null"
              @click="decide(join, 'reject')"
            >
              Reject
            </button>
          </template>
          <button
            v-if="join.can_remove"
            type="button"
            class="btn btn-ghost btn-danger btn-sm"
            data-testid="catalog-semantic-remove"
            :disabled="busy !== null"
            @click="removeJoin(join)"
          >
            <i class="fa-solid fa-trash"></i>
            {{ busy === join.key ? "Removing…" : "Remove" }}
          </button>
        </template>
      </SemanticJoinRow>
    </ul>

    <SemanticJoinDialog
      v-model="showDialog"
      :namespace-id="catalog.id"
      :propose="proposeHere"
      pending-note="Proposed. It waits for review."
    />
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { SemanticModelApi } from "../../../api/semanticModel.api";
import { useCatalogStore } from "../../../stores/catalog-store";
import { semanticErrorMessage } from "../../../stores/semantic-model-store";
import type { NamespaceTree } from "../../../types/catalog.types";
import type {
  JoinCheck,
  SemanticFact,
  SemanticJoin,
  SemanticModel,
  SemanticProposal,
} from "../../../types/semantic.types";
import SemanticJoinDialog from "./SemanticJoinDialog.vue";
import SemanticJoinRow from "./SemanticJoinRow.vue";
import { chipState, errorText } from "./semanticFormat";
import { confirmRemoveJoin } from "./semanticRemoval";

const props = defineProps<{ catalog: NamespaceTree }>();
const emit = defineEmits<{ "open-semantic-model": [schemaId: number] }>();

const catalogStore = useCatalogStore();

// The catalog's own model, held here: the semantic-model store belongs to the open model panel.
const model = ref<SemanticModel | null>(null);
const checks = ref<Record<string, JoinCheck>>({});
const loading = ref(false);
const error = ref<string | null>(null);
const busy = ref<string | null>(null);
const suggesting = ref(false);
const outcome = ref("");
const showDialog = ref(false);
let token = 0;

// The tree as last loaded, so the counts follow a change made here or in a schema's model.
const schemas = computed(
  () => (catalogStore.findNamespaceInTree(props.catalog.id) ?? props.catalog).children,
);

const joins = computed(() => (model.value?.joins ?? []).filter((j) => j.layer === "catalog"));

const total = (s: NamespaceTree) =>
  (s.semantic_model?.accepted ?? 0) +
  (s.semantic_model?.pending ?? 0) +
  (s.semantic_model?.stale ?? 0);

const modelled = (s: NamespaceTree) => total(s) > 0;

function countsText(s: NamespaceTree): string {
  if (!modelled(s)) return "Not modelled yet";
  const accepted = s.semantic_model?.accepted ?? 0;
  const waiting = (s.semantic_model?.pending ?? 0) + (s.semantic_model?.stale ?? 0);
  return `${accepted} accepted · ${waiting} waiting for review`;
}

const isPending = (join: SemanticJoin) => chipState(join.status) === "pending";

/** `schema.table` for a dataset key of this catalog; any other key as it is. */
const tableLabel = (key: string): string => {
  const prefix = `${model.value?.key ?? props.catalog.name}.`;
  return key.startsWith(prefix) ? key.slice(prefix.length) : key;
};

async function load() {
  const mine = ++token;
  const id = props.catalog.id;
  loading.value = true;
  error.value = null;
  try {
    const loaded = await SemanticModelApi.get(id);
    if (mine !== token) return;
    model.value = loaded;
    void loadChecks(mine, id);
  } catch (e) {
    if (mine !== token) return;
    model.value = null;
    error.value = semanticErrorMessage(e, "Could not load the joins");
  } finally {
    if (mine === token) loading.value = false;
  }
}

// A failed check leaves the joins unmarked.
async function loadChecks(mine: number, id: number) {
  if (joins.value.length === 0) {
    checks.value = {};
    return;
  }
  try {
    const found = await SemanticModelApi.checkJoins(id);
    if (mine === token) checks.value = Object.fromEntries(found.map((c) => [c.key, c]));
  } catch {
    if (mine === token) checks.value = {};
  }
}

/** Reload this list and the tree's counts after a change. */
async function refresh() {
  await Promise.all([load(), catalogStore.loadTree()]);
}

function schemaOfTable(tableId: number): NamespaceTree | undefined {
  return schemas.value.find((s) => (s.tables ?? []).some((t) => t.id === tableId));
}

async function proposeHere(proposals: SemanticProposal[]): Promise<SemanticFact[]> {
  const facts = await SemanticModelApi.propose(props.catalog.id, proposals);
  await refresh();
  for (const p of proposals) {
    if (p.kind !== "join") continue;
    const from = schemaOfTable(p.from_table_id);
    if (from && from === schemaOfTable(p.to_table_id))
      ElMessage.info(
        `${p.name} joins two tables of ${from.name}, so it is kept in that schema's model.`,
      );
  }
  return facts;
}

async function decide(join: SemanticJoin, action: "accept" | "reject") {
  if (!join.group) return;
  busy.value = join.key;
  try {
    const result = await SemanticModelApi.review(props.catalog.id, { action, group: join.group });
    if (result.errors.length && !result.changed.length) ElMessage.error(result.errors[0].message);
    await refresh();
  } catch (e) {
    ElMessage.error(errorText(e, `Could not ${action} the join`));
  } finally {
    busy.value = null;
  }
}

async function removeJoin(join: SemanticJoin) {
  if (!(await confirmRemoveJoin(props.catalog.id, join))) return;
  busy.value = join.key;
  try {
    await SemanticModelApi.review(props.catalog.id, { action: "retract", subject: join.key });
    ElMessage.success(`Removed ${join.name}.`);
    await refresh();
  } catch (e) {
    ElMessage.error(errorText(e, "Could not remove the join"));
  } finally {
    busy.value = null;
  }
}

async function suggestJoins() {
  suggesting.value = true;
  outcome.value = "";
  const before = new Set(joins.value.map((j) => j.key));
  try {
    const out = await SemanticModelApi.suggest(props.catalog.id, { kinds: ["join"] });
    await refresh();
    const added = out.added.joins;
    if (added === 0) {
      outcome.value = "Nothing new to suggest.";
      return;
    }
    const across = joins.value.filter((j) => !before.has(j.key)).length;
    const within = Math.max(added - across, 0);
    const parts = [];
    if (across) parts.push(`${across} across schemas, waiting for review here`);
    if (within) parts.push(`${within} within one schema, waiting in that schema's Review`);
    outcome.value = `${added} ${added === 1 ? "join" : "joins"} proposed: ${parts.join("; ")}.`;
  } catch (e) {
    outcome.value = errorText(e, "Could not suggest joins");
  } finally {
    suggesting.value = false;
  }
}

watch(
  () => props.catalog.id,
  () => {
    model.value = null;
    checks.value = {};
    outcome.value = "";
    void load();
  },
);
onMounted(load);
</script>

<style scoped>
.catalog-semantic {
  margin-top: var(--spacing-6);
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-2);
}

.joins-header {
  margin-top: var(--spacing-5);
}

.section-title {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.joins-actions {
  display: flex;
  gap: var(--spacing-2);
}

.hint,
.empty,
.outcome {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.empty.error {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  color: var(--color-danger);
}

.schema-list,
.join-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  margin: 0;
  padding: 0;
  list-style: none;
}

.schema-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.schema-icon {
  color: var(--color-text-muted);
}

.schema-name {
  font-weight: var(--font-weight-semibold);
}

.schema-counts {
  flex: 1;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
</style>
