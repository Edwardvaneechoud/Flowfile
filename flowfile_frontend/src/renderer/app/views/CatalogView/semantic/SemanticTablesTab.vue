<template>
  <div class="stack">
    <SemanticSuggestMore
      v-if="model.tables.length > 0"
      :kinds="['description', 'role']"
      label="Suggest descriptions and roles"
      @review="emit('open-tab', 'review')"
    >
      <template #hint>
        <p class="hint muted">Say what each table and column holds, and how each column is used.</p>
      </template>
    </SemanticSuggestMore>
    <p v-if="model.tables.length === 0" class="muted">This schema has no tables yet.</p>

    <section
      v-for="table in model.tables"
      :id="`semantic-table-${table.table_id}`"
      :key="table.table_id"
      class="box table-card"
      :class="{ focused: focusedId === table.table_id }"
    >
      <header class="line">
        <button class="icon-btn collapse-btn" type="button" @click="toggle(table.table_id)">
          <i
            :class="
              isOpen(table.table_id) ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'
            "
          ></i>
        </button>
        <i class="fa-solid fa-table table-icon"></i>
        <h3 class="table-name">{{ table.name }}</h3>
        <SemanticStatusChip :fact="table.facts.description" />
        <span class="table-count">{{ table.fields.length }} columns</span>
        <template v-if="rulesIn(table).length > 0">
          <span class="rule-summary" :data-testid="`rules-summary-${table.table_id}`">
            {{ summaryOf(table) }}
          </span>
          <el-button
            size="small"
            :loading="store.rulesRunning.includes(table.table_id)"
            :data-testid="`rules-run-${table.table_id}`"
            @click="runRules(table)"
          >
            Run rules
          </el-button>
        </template>
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

      <SemanticColumnGrid
        v-if="isOpen(table.table_id)"
        :table="table"
        :rule-results="store.ruleResults[table.table_id]"
        @propose="send"
      />
    </section>
  </div>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import {
  errorText,
  type SemanticModel,
  type SemanticProposal,
  type SemanticTable,
} from "../../../types/semantic.types";
import SemanticColumnGrid from "./SemanticColumnGrid.vue";
import SemanticInlineText from "./SemanticInlineText.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import SemanticSuggestMore from "./SemanticSuggestMore.vue";
import { lastRun, ranText, ruleKey, rulesOf, rulesSummary } from "./dataRules";
import { notePending, noteUncheckedRules, tablePageLocation } from "./semanticFormat";

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
  store.focusTableId = null;
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

watch(() => store.namespaceId, store.loadRuleResults, { immediate: true });

const rulesIn = (table: SemanticTable) =>
  table.fields.flatMap((f) =>
    rulesOf(f.facts.checks?.value).map((rule) => ({ column: f.name, rule })),
  );

const summaryOf = (table: SemanticTable) => {
  const results = store.ruleResults[table.table_id];
  const shown = rulesIn(table);
  const ran = shown.some((r) => results?.[ruleKey(r.column, r.rule)])
    ? ranText(lastRun(results))
    : "";
  return [rulesSummary(shown, results), ran].filter(Boolean).join(" · ");
};

async function runRules(table: SemanticTable) {
  try {
    const run = await store.runDataRules([table.table_id]);
    const answered = run.tables.find((t) => t.table_id === table.table_id);
    if (!answered) ElMessage.info("No accepted rules to run. A proposed rule runs once accepted.");
    else if (answered.skipped) ElMessage.info(answered.skipped);
  } catch (e) {
    ElMessage.error(errorText(e, "Could not run the rules"));
  }
}

const isOpen = (id: number) => !closed.value.has(id);
const toggle = (id: number) => {
  const next = new Set(closed.value);
  if (!next.delete(id)) next.add(id);
  closed.value = next;
};

async function send(proposal: SemanticProposal) {
  try {
    const stored = await store.propose([proposal]);
    noteUncheckedRules(stored);
    notePending(stored);
  } catch (e) {
    ElMessage.error(errorText(e, "Could not save the change"));
  }
}

const saveTable = (table: SemanticTable, value: string) =>
  send({ kind: "table", table_id: table.table_id, attribute: "description", value });
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.collapse-btn {
  width: 20px;
}

.collapse-btn:hover {
  color: var(--color-text-muted);
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

.rule-summary {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.table-description {
  margin: var(--spacing-1) 0 var(--spacing-3) 28px;
}

.muted {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}
</style>
