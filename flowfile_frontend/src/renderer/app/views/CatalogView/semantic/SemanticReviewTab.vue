<template>
  <div class="review-tab">
    <div class="toolbar">
      <p class="hint">
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

    <div v-if="store.reviewLoading && store.reviewQueue.length === 0" class="muted">Loading…</div>

    <ul class="queue">
      <li v-for="item in store.reviewQueue" :key="item.id" class="queue-item">
        <div class="item-main">
          <div class="item-title">
            <i :class="KIND_ICONS[item.subject_kind]"></i>
            <span class="item-label">{{ item.subject_label }}</span>
            <span class="item-kind">{{ item.subject_kind }}</span>
            <SemanticSourceBadge :kind="item.source_kind" :source="item.source" />
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
          <el-tooltip :content="blockedReason" :disabled="model.can_manage" placement="top">
            <span>
              <button
                class="btn btn-secondary btn-sm"
                :disabled="!model.can_manage || store.busy"
                @click="decide(item, 'reject')"
              >
                Reject
              </button>
            </span>
          </el-tooltip>
          <el-tooltip :content="blockedReason" :disabled="model.can_manage" placement="top">
            <span>
              <button
                class="btn btn-primary btn-sm"
                :disabled="!model.can_manage || store.busy"
                @click="decide(item, 'accept')"
              >
                Accept
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
import type {
  SemanticFact,
  SemanticModel,
  SemanticReviewAction,
  SemanticReviewItem,
} from "../../../types/semantic.types";
import SemanticSourceBadge from "./SemanticSourceBadge.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { attributeLabel, displayValue, errorText } from "./semanticFormat";

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

const decide = (item: SemanticReviewItem, action: "accept" | "reject") =>
  run([actionFor(item, action)], action === "accept" ? "Accepted" : "Rejected");

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

<style scoped>
.review-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.hint,
.muted {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.queue {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  margin: 0;
  padding: 0;
  list-style: none;
}

.queue-item {
  display: flex;
  justify-content: space-between;
  gap: var(--spacing-4);
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.item-main {
  min-width: 0;
}

.item-title {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2);
}

.item-label {
  font-weight: var(--font-weight-semibold);
}

.item-kind,
.confidence {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
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
