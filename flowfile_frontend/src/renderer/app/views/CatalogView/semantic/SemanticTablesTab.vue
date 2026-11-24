<template>
  <div class="tables-tab">
    <SemanticSuggestMore
      v-if="model.tables.length > 0"
      :kinds="['description', 'role']"
      label="Suggest descriptions and roles"
      @review="emit('open-tab', 'review')"
    >
      <template #hint>
        <p class="muted hint">Say what each table and column holds, and how each column is used.</p>
      </template>
    </SemanticSuggestMore>
    <p v-if="model.tables.length === 0" class="muted">This schema has no tables yet.</p>

    <section
      v-for="table in model.tables"
      :id="`semantic-table-${table.table_id}`"
      :key="table.table_id"
      class="table-card"
      :class="{ focused: focusedId === table.table_id }"
    >
      <header class="table-head">
        <button class="collapse-btn" type="button" @click="toggle(table.table_id)">
          <i
            :class="
              isOpen(table.table_id) ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'
            "
          ></i>
        </button>
        <i class="fa-solid fa-table table-icon"></i>
        <h3 class="table-name">{{ table.name }}</h3>
        <SemanticStatusChip
          v-if="needsReview(table.facts.description)"
          :status="table.facts.description!.status"
          :fact="table.facts.description"
        />
        <span class="table-count">{{ table.fields.length }} columns</span>
        <el-tooltip content="Open table" placement="top" :show-after="300">
          <button
            class="open-table-btn"
            type="button"
            aria-label="Open table"
            :data-testid="`semantic-open-table-${table.table_id}`"
            @click="router.push(tablePageLocation(table.table_id))"
          >
            <i class="fa-solid fa-arrow-up-right-from-square"></i>
          </button>
        </el-tooltip>
      </header>
      <div class="table-description">
        <SemanticInlineText
          :value="table.description ?? ''"
          placeholder="Describe what one row of this table is"
          multiline
          @commit="saveTable(table, $event)"
        />
      </div>

      <SemanticColumnGrid v-if="isOpen(table.table_id)" :table="table" @propose="send" />
    </section>
  </div>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { SemanticModel, SemanticProposal, SemanticTable } from "../../../types/semantic.types";
import SemanticColumnGrid from "./SemanticColumnGrid.vue";
import SemanticInlineText from "./SemanticInlineText.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import SemanticSuggestMore from "./SemanticSuggestMore.vue";
import { errorText, needsReview, tablePageLocation } from "./semanticFormat";

const props = defineProps<{ model: SemanticModel }>();
const emit = defineEmits<{ "open-tab": [tab: string] }>();

const store = useSemanticModelStore();
const router = useRouter();
const closed = ref(new Set<number>());
const focusedId = ref<number | null>(null);
let focusTimer: ReturnType<typeof setTimeout> | undefined;

async function focusRequested() {
  const id = store.focusTableId;
  if (id === null) return;
  store.requestFocusTable(null);
  if (!props.model.tables.some((t) => t.table_id === id)) return;
  if (closed.value.has(id)) toggle(id);
  focusedId.value = id;
  await nextTick();
  document
    .getElementById(`semantic-table-${id}`)
    ?.scrollIntoView({ behavior: "smooth", block: "start" });
  clearTimeout(focusTimer);
  focusTimer = setTimeout(() => {
    if (focusedId.value === id) focusedId.value = null;
  }, 2000);
}

watch([() => store.focusTableId, () => props.model], focusRequested, { immediate: true });
onBeforeUnmount(() => clearTimeout(focusTimer));

const isOpen = (id: number) => !closed.value.has(id);
const toggle = (id: number) => {
  const next = new Set(closed.value);
  if (!next.delete(id)) next.add(id);
  closed.value = next;
};

async function send(proposal: SemanticProposal) {
  try {
    const facts = await store.propose([proposal]);
    if (facts.some((f) => f.status !== "accepted")) {
      ElMessage.info("Proposed. It waits in the Review tab.");
    }
  } catch (e) {
    ElMessage.error(errorText(e, "Could not save the change"));
  }
}

const saveTable = (table: SemanticTable, value: string) =>
  send({ kind: "table", table_id: table.table_id, attribute: "description", value });
</script>

<style scoped>
.tables-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.table-card {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  padding: var(--spacing-3) var(--spacing-4);
}

.table-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.collapse-btn {
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  width: 20px;
}

.table-icon {
  color: var(--color-primary);
}

.table-card {
  transition:
    border-color var(--transition-fast),
    box-shadow var(--transition-fast);
}

.table-card.focused {
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px var(--color-primary-light, rgba(59, 130, 246, 0.25));
}

.open-table-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  font-size: var(--font-size-xs);
}

.open-table-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-primary);
}

.table-name {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
}

.table-count {
  margin-left: auto;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.table-description {
  margin: var(--spacing-1) 0 var(--spacing-3) 28px;
}

.muted {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.hint {
  margin: 0;
}
</style>
