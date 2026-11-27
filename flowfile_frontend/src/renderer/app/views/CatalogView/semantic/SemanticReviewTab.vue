<template>
  <div class="stack">
    <div class="toolbar">
      <p>
        <template v-if="store.reviewQueue.length === 0">Nothing is waiting for review.</template>
        <template v-else>
          {{ store.reviewQueue.length }} item{{ store.reviewQueue.length === 1 ? "" : "s" }} waiting
          for review.
        </template>
      </p>
      <el-tooltip :content="blockedReason" :disabled="model.can_manage" placement="top">
        <span>
          <button
            class="btn btn-primary btn-sm"
            :disabled="!model.can_manage || store.busy || store.reviewQueue.length === 0"
            @click="acceptAll"
          >
            <i class="fa-solid fa-check-double"></i> Accept all
          </button>
        </span>
      </el-tooltip>
    </div>

    <div v-if="store.reviewLoading && store.reviewQueue.length === 0" class="hint">Loading…</div>

    <ul class="list">
      <li v-for="item in store.reviewQueue" :key="item.id" class="box queue-item">
        <div class="item-main">
          <div class="line item-title">
            <i :class="KIND_ICONS[item.subject_kind]"></i>
            <span class="name">{{ item.subject_label }}</span>
            <span class="item-kind">{{ item.subject_kind }}</span>
            <span class="source-badge" :class="`kind-${item.source_kind}`" :title="item.source">
              <i :class="SOURCES[item.source_kind].icon"></i>
              {{ SOURCES[item.source_kind].label }}
            </span>
            <span v-if="item.confidence !== null" class="confidence">
              {{ Math.round(item.confidence * 100) }}% confident
            </span>
            <SemanticStatusChip v-if="item.status === 'stale'" status="stale" />
          </div>
          <ul class="facts">
            <li v-for="fact in factsOf(item)" :key="fact.id">
              <span class="fact-attr">{{ attributeLabel(fact.attribute) }}</span>
              <span class="fact-value">{{ displayValue(fact.attribute, fact.value) }}</span>
            </li>
          </ul>
          <p v-if="item.rationale" class="rationale">{{ item.rationale }}</p>
        </div>
        <div class="item-actions">
          <el-tooltip
            v-for="action in ACTIONS"
            :key="action.name"
            :content="blockedReason"
            :disabled="model.can_manage"
            placement="top"
          >
            <span>
              <button
                class="btn btn-sm"
                :class="action.style"
                :disabled="!model.can_manage || store.busy"
                @click="run([actionFor(item, action.name)], `${action.label}ed`)"
              >
                {{ action.label }}
              </button>
            </span>
          </el-tooltip>
        </div>
      </li>
    </ul>
  </div>
</template>

<script setup lang="ts">
import { ElMessage } from "element-plus";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import {
  errorText,
  type SemanticFact,
  type SemanticModel,
  type SemanticReviewAction,
  type SemanticReviewItem,
} from "../../../types/semantic.types";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { SOURCES, attributeLabel, displayValue } from "./semanticFormat";

defineProps<{ model: SemanticModel }>();

const store = useSemanticModelStore();

const blockedReason = "Only people who manage this schema can review changes.";

const KIND_ICONS: Record<SemanticReviewItem["subject_kind"], string> = {
  model: "fa-solid fa-sitemap",
  dataset: "fa-solid fa-table",
  field: "fa-solid fa-columns",
  relationship: "fa-solid fa-link",
  metric: "fa-solid fa-chart-line",
  type: "fa-solid fa-tag",
};

const ACTIONS = [
  { name: "reject", label: "Reject", style: "btn-secondary" },
  { name: "accept", label: "Accept", style: "btn-primary" },
] as const;

const factsOf = (item: SemanticReviewItem): SemanticFact[] =>
  item.group_facts.length > 0 ? item.group_facts : [item];

const actionFor = (item: SemanticReviewItem, action: "accept" | "reject"): SemanticReviewAction =>
  item.group ? { action, group: item.group } : { action, ids: [item.id] };

async function run(actions: SemanticReviewAction[], done: string) {
  try {
    const result = await store.reviewBatch(actions);
    if (result.errors.length > 0) ElMessage.warning(result.errors[0].message);
    else ElMessage.success(done);
  } catch (e) {
    ElMessage.error(errorText(e, "Could not save the review"));
  }
}

function acceptAll() {
  const groups = new Set<string>();
  const ids: string[] = [];
  for (const item of store.reviewQueue) {
    if (item.group) groups.add(item.group);
    else ids.push(item.id);
  }
  const actions: SemanticReviewAction[] = [...groups].map((group) => ({ action: "accept", group }));
  if (ids.length > 0) actions.unshift({ action: "accept", ids });
  return run(actions, "Accepted everything");
}
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.queue-item {
  display: flex;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.item-main {
  min-width: 0;
}

.item-title {
  flex-wrap: wrap;
}

.item-kind,
.confidence {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.source-badge {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: 0 var(--spacing-2);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  line-height: 18px;
  white-space: nowrap;
}

.kind-ai {
  color: var(--color-primary);
}

.facts {
  margin: var(--spacing-2) 0 0;
  padding: 0;
  list-style: none;
  font-size: var(--font-size-sm);
}

.facts li {
  display: flex;
  gap: var(--spacing-3);
}

.fact-attr {
  min-width: 90px;
  color: var(--color-text-muted);
}

.fact-value {
  word-break: break-word;
}

.rationale {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  font-style: italic;
}

.item-actions {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  flex-shrink: 0;
}
</style>
