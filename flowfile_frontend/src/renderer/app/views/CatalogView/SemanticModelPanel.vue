<template>
  <div class="semantic-panel">
    <button class="back-btn" @click="emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>

    <div v-if="store.loading && !model" class="state">Loading the semantic model…</div>
    <div v-else-if="store.error && !model" class="state error" role="alert">
      <span>{{ store.error }}</span>
      <button class="btn btn-secondary btn-sm" @click="store.load(namespaceId)">Retry</button>
    </div>

    <template v-else-if="model">
      <header class="panel-header">
        <div class="header-main">
          <div class="header-title">
            <i class="fa-solid fa-sitemap header-icon"></i>
            <SemanticInlineText
              class="title-edit"
              :value="displayName"
              placeholder="Name this model"
              @commit="saveModel({ name: $event })"
            />
            <el-tooltip :content="modeHint" placement="top">
              <span
                class="mode-chip"
                :class="`mode-${model.review_mode}`"
                data-testid="review-mode"
              >
                {{ model.review_mode === "team" ? "Team review" : "Solo" }}
              </span>
            </el-tooltip>
          </div>
          <SemanticInlineText
            :value="model.description ?? ''"
            placeholder="Describe what this data is about"
            multiline
            @commit="saveModel({ description: $event })"
          />
        </div>
        <div class="header-actions">
          <button
            v-if="!isEmpty || started"
            class="btn btn-secondary btn-sm"
            :disabled="store.busy || suggesting"
            title="Propose roles, joins and metrics for you to review"
            data-testid="semantic-suggest-small"
            @click="suggest()"
          >
            <i class="fa-solid fa-wand-magic-sparkles"></i> Suggest
          </button>
          <button
            v-if="!isEmpty || started"
            class="btn btn-secondary btn-sm"
            :class="{ active: showAskBox }"
            title="Ask the AI for one particular thing, in your own words"
            data-testid="semantic-suggest-specific"
            @click="showAskBox = !showAskBox"
          >
            <i class="fa-solid fa-comment-dots"></i> Ask for something specific
          </button>
          <button class="btn btn-secondary btn-sm" :disabled="store.busy" @click="resync">
            <i class="fa-solid fa-rotate"></i> Re-sync
          </button>
          <button class="btn btn-secondary btn-sm" @click="exportYaml">
            <i class="fa-solid fa-file-export"></i> Export YAML
          </button>
        </div>
      </header>

      <SemanticSuggestBox
        v-if="showAskBox && (!isEmpty || started)"
        class="ask-box"
        :model="model"
        @review="openReview"
      />

      <div class="progress" data-testid="semantic-progress">
        <el-progress
          :percentage="percentAccepted"
          :stroke-width="8"
          :show-text="false"
          status="success"
        />
        <div class="progress-legend">
          <span
            ><strong>{{ model.progress.accepted }}</strong> accepted</span
          >
          <span
            ><strong>{{ model.progress.pending }}</strong> pending</span
          >
          <span
            ><strong>{{ model.progress.stale }}</strong> stale</span
          >
        </div>
      </div>

      <p v-if="suggesting" class="suggest-status" role="status" data-testid="suggest-status">
        <i class="fa-solid fa-spinner fa-spin"></i> {{ suggestStep }}
      </p>
      <div v-else-if="suggestNotice" class="suggest-notice" data-testid="suggest-notice">
        <p>{{ suggestNotice }}</p>
        <p v-for="line in suggestWarnings" :key="line" class="suggest-warning">{{ line }}</p>
      </div>

      <LocalModelSetupCard
        v-if="showLocalOffer"
        compact
        class="local-offer"
        title="Let AI on this computer write the descriptions"
        @installed="onLocalInstalled"
        @open-settings="router.push({ name: 'ai' })"
      />

      <section v-if="isEmpty && !started" class="empty-state" data-testid="semantic-empty">
        <h3>What is a semantic model?</h3>
        <p>
          It records what your data means: what each column describes, how tables join, and which
          metrics (like total revenue) everyone should calculate the same way. Every fact is
          reviewed before it counts, so you always know who said what.
        </p>
        <div class="empty-actions">
          <button class="btn btn-secondary" @click="start">
            <i class="fa-solid fa-table"></i> Describe your tables
          </button>
          <button
            class="btn btn-primary"
            :disabled="suggesting"
            data-testid="semantic-suggest"
            @click="suggest()"
          >
            <i class="fa-solid fa-wand-magic-sparkles"></i> Suggest a model
          </button>
          <slot name="empty-actions" />
        </div>
      </section>

      <template v-else>
        <nav class="tab-bar" role="tablist">
          <button
            v-for="tab in tabs"
            :key="tab.key"
            class="tab"
            :class="{ active: activeTab === tab.key }"
            role="tab"
            :aria-selected="activeTab === tab.key"
            :data-testid="`semantic-tab-${tab.key}`"
            @click="activeTab = tab.key"
          >
            <i :class="tab.icon"></i>
            {{ tab.label }}
            <span v-if="tab.count" class="tab-count">{{ tab.count }}</span>
          </button>
        </nav>
        <div class="tab-body">
          <component :is="activeComponent" :model="model" @open-tab="activeTab = $event" />
        </div>
      </template>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch, type Component } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import LocalModelSetupCard from "../../components/ai/LocalModelSetupCard.vue";
import { useSemanticAi } from "../../composables/useSemanticAi";
import { useCatalogStore } from "../../stores/catalog-store";
import { useSemanticModelStore } from "../../stores/semantic-model-store";
import { saveFile } from "../../utils/tableExport";
import {
  isAiNotSetUp,
  type SemanticDraftOut,
  type SemanticModelUpdate,
  type SuggestOut,
} from "../../types/semantic.types";
import SemanticAskTab from "./semantic/SemanticAskTab.vue";
import SemanticHistoryTab from "./semantic/SemanticHistoryTab.vue";
import SemanticInlineText from "./semantic/SemanticInlineText.vue";
import SemanticJoinsTab from "./semantic/SemanticJoinsTab.vue";
import SemanticMetricsTab from "./semantic/SemanticMetricsTab.vue";
import SemanticReviewTab from "./semantic/SemanticReviewTab.vue";
import SemanticSuggestBox from "./semantic/SemanticSuggestBox.vue";
import SemanticTablesTab from "./semantic/SemanticTablesTab.vue";
import { errorText } from "./semantic/semanticFormat";

const props = defineProps<{ namespaceId: number }>();
const emit = defineEmits<{ close: [] }>();

interface SemanticTabDef {
  key: string;
  label: string;
  icon: string;
  component: Component;
  count?: number;
}

const store = useSemanticModelStore();
const catalogStore = useCatalogStore();
const model = computed(() => store.model);

const router = useRouter();
const { aiStore, usable, canOfferLocal, pick } = useSemanticAi();

const activeTab = ref("tables");
const started = ref(false);
const suggesting = ref(false);
const suggestStep = ref("");
const suggestNotice = ref("");
const suggestWarnings = ref<string[]>([]);
const showAskBox = ref(false);
const showLocalOffer = computed(() => canOfferLocal.value && !!model.value);

// A later step appends its own entry here (for example an "Ask" tab).
const tabs = computed<SemanticTabDef[]>(() => [
  { key: "tables", label: "Tables", icon: "fa-solid fa-table", component: SemanticTablesTab },
  { key: "ask", label: "Ask", icon: "fa-solid fa-circle-question", component: SemanticAskTab },
  { key: "joins", label: "Joins", icon: "fa-solid fa-link", component: SemanticJoinsTab },
  {
    key: "metrics",
    label: "Metrics",
    icon: "fa-solid fa-chart-line",
    component: SemanticMetricsTab,
  },
  {
    key: "review",
    label: "Review",
    icon: "fa-solid fa-list-check",
    component: SemanticReviewTab,
    count: store.reviewQueue.length,
  },
  {
    key: "history",
    label: "History",
    icon: "fa-solid fa-clock-rotate-left",
    component: SemanticHistoryTab,
  },
]);

const activeComponent = computed(
  () => tabs.value.find((t) => t.key === activeTab.value)?.component ?? SemanticTablesTab,
);

const displayName = computed(
  () => model.value?.name || catalogStore.getNamespaceName(props.namespaceId) || "Semantic model",
);

const isEmpty = computed(() => {
  const m = model.value;
  if (!m) return false;
  const p = m.progress;
  return (
    p.accepted + p.pending + p.stale === 0 &&
    m.joins.length === 0 &&
    m.tables.every((t) => t.metrics.length === 0)
  );
});

const percentAccepted = computed(() => {
  const p = model.value?.progress;
  if (!p) return 0;
  const total = p.accepted + p.pending + p.stale;
  return total === 0 ? 0 : Math.round((p.accepted / total) * 100);
});

const modeHint = computed(() =>
  model.value?.review_mode === "team"
    ? "Changes wait for a reviewer before they count."
    : "You are the only person here, so your changes count right away.",
);

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

function summarize(out: SuggestOut | SemanticDraftOut): string {
  const parts = [
    out.added.roles && plural(out.added.roles, "column role"),
    out.added.joins && plural(out.added.joins, "join"),
    out.added.metrics && plural(out.added.metrics, "metric"),
    out.added.descriptions && plural(out.added.descriptions, "description"),
  ].filter(Boolean) as string[];
  if (parts.length === 0) {
    return "Nothing new to propose: the model already holds every suggestion.";
  }
  const list =
    parts.length === 1
      ? parts[0]
      : `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;
  return `Proposed ${list}.`;
}

function warningsFor(out: SuggestOut | SemanticDraftOut): string[] {
  const lines: string[] = [];
  if (!out.stats_available) {
    lines.push("Column statistics were not available, so roles come from names and types only.");
  }
  if ("table_outcomes" in out) {
    const failed = out.table_outcomes.filter((t) => !t.ok).map((t) => t.table);
    if (failed.length) {
      lines.push(
        `The model could not describe ${failed.join(", ")}; those keep their rule-based suggestions.`,
      );
    }
  }
  return lines;
}

async function suggest(withNewModel = false) {
  if (suggesting.value) return;
  suggesting.value = true;
  suggestNotice.value = "";
  suggestWarnings.value = [];
  const useAi = usable.value;
  try {
    suggestStep.value = useAi
      ? "Reading your tables, proposing roles, joins and metrics, then writing descriptions…"
      : "Reading your tables and proposing roles, joins and metrics…";
    let out: SuggestOut | SemanticDraftOut;
    try {
      out = useAi ? await store.draftModel(pick()) : await store.suggestModel();
    } catch (e) {
      if (!useAi || !isAiNotSetUp(e)) throw e;
      suggestStep.value = "AI is not set up, so using rules only…";
      out = await store.suggestModel();
    }
    const prefix = withNewModel ? "AI on this computer is ready and wrote the descriptions. " : "";
    suggestNotice.value = prefix + summarize(out);
    suggestWarnings.value = warningsFor(out);
    started.value = true;
    activeTab.value = "review";
  } catch (e) {
    ElMessage.error(errorText(e, "Could not suggest a model"));
  } finally {
    suggesting.value = false;
  }
}

function onLocalInstalled() {
  void suggest(true);
}

function start() {
  started.value = true;
  activeTab.value = "tables";
}

function openReview() {
  started.value = true;
  activeTab.value = "review";
}

async function saveModel(body: SemanticModelUpdate) {
  try {
    await store.updateModel(body);
  } catch (e) {
    ElMessage.error(errorText(e, "Could not save the change"));
  }
}

async function resync() {
  try {
    const r = await store.sync();
    ElMessage.success(
      r.new.length + r.changed.length + r.gone.length === 0
        ? "Already up to date"
        : `Synced: ${r.new.length} new, ${r.changed.length} changed, ${r.gone.length} removed`,
    );
  } catch (e) {
    ElMessage.error(errorText(e, "Could not re-sync"));
  }
}

async function exportYaml() {
  try {
    const yaml = await store.exportYaml();
    const file = `${displayName.value.replace(/[^\w.-]+/g, "_")}.semantic-model.yaml`;
    await saveFile(new Blob([yaml], { type: "application/yaml" }), file, {
      name: "YAML",
      extensions: ["yaml"],
    });
  } catch (e) {
    ElMessage.error(errorText(e, "Could not export the model"));
  }
}

onMounted(() => {
  void aiStore.loadProviders();
});

watch(
  () => store.pendingAsk,
  (query) => {
    if (query) {
      started.value = true;
      activeTab.value = "ask";
    }
  },
);

watch(
  () => store.focusTableId,
  (id) => {
    if (id !== null) {
      started.value = true;
      activeTab.value = "tables";
    }
  },
  { immediate: true },
);

watch(
  () => props.namespaceId,
  (id) => {
    started.value = store.focusTableId !== null;
    activeTab.value = "tables";
    suggestNotice.value = "";
    suggestWarnings.value = [];
    showAskBox.value = false;
    void store.load(id);
  },
  { immediate: true },
);
</script>

<style scoped>
.semantic-panel {
  max-width: 1100px;
  margin: 0 auto;
}

.state {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-6) 0;
  color: var(--color-text-secondary);
}

.state.error {
  color: var(--color-danger);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: var(--spacing-4);
  margin-bottom: var(--spacing-4);
}

.header-main {
  flex: 1;
  min-width: 0;
}

.header-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.header-icon {
  font-size: 20px;
  color: var(--color-primary);
}

.title-edit {
  flex: 0 1 auto;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
}

.title-edit :deep(.inline-display) {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
}

.mode-chip {
  padding: 0 var(--spacing-2);
  border-radius: 999px;
  border: 1px solid var(--color-border-primary);
  font-size: var(--font-size-xs);
  line-height: 20px;
  color: var(--color-text-secondary);
}

.mode-team {
  color: var(--color-primary);
  border-color: var(--color-primary);
}

.header-actions {
  display: flex;
  gap: var(--spacing-2);
  flex-shrink: 0;
}

.header-actions .btn.active {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

.ask-box {
  margin-bottom: var(--spacing-4);
}

.progress {
  margin-bottom: var(--spacing-5, var(--spacing-4));
}

.progress-legend {
  display: flex;
  gap: var(--spacing-4);
  margin-top: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.suggest-status,
.suggest-notice {
  margin: 0 0 var(--spacing-4);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  font-size: var(--font-size-sm);
}

.suggest-notice p,
.suggest-status {
  margin: 0;
}

.suggest-warning {
  margin-top: var(--spacing-1);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.local-offer {
  margin-bottom: var(--spacing-4);
}

.empty-state {
  padding: var(--spacing-6);
  border: 1px dashed var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
}

.empty-state h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
}

.empty-state p {
  max-width: 640px;
  margin: 0 0 var(--spacing-4);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  line-height: 1.5;
}

.empty-actions {
  display: flex;
  gap: var(--spacing-2);
}

.tab-bar {
  display: flex;
  gap: var(--spacing-1);
  margin-bottom: var(--spacing-4);
  border-bottom: 1px solid var(--color-border-light);
}

.tab {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border: none;
  border-bottom: 2px solid transparent;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.tab:hover {
  color: var(--color-text-primary);
}

.tab.active {
  color: var(--color-primary);
  border-bottom-color: var(--color-primary);
  font-weight: var(--font-weight-medium);
}

.tab-count {
  min-width: 18px;
  padding: 0 var(--spacing-1);
  border-radius: 9px;
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
  font-size: var(--font-size-xs);
  text-align: center;
}
</style>
