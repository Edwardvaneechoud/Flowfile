<template>
  <div
    v-if="store.conflicts.length || store.settled"
    class="merge-bar nokey"
    data-canvas-overlay
    data-testid="merge-conflicts-bar"
  >
    <template v-if="store.conflicts.length">
      <div class="merge-bar__head">
        <i class="fa-solid fa-code-merge merge-bar__icon" aria-hidden="true"></i>
        <span class="merge-bar__title">{{ conflictsTitle(store.conflicts.length) }}</span>
        <el-button
          size="small"
          link
          type="primary"
          data-testid="merge-conflicts-toggle"
          @click="store.expanded = !store.expanded"
        >
          {{ store.expanded ? "Hide" : "Settle them" }}
        </el-button>
      </div>
      <p v-if="store.error" class="merge-bar__error">{{ store.error }}</p>
      <div v-if="store.expanded" class="merge-bar__list">
        <merge-conflict-item
          v-for="item in store.conflicts"
          :key="item.key"
          :item="item"
          :item-key="item.key"
          testid="merge-bar"
          :busy="store.busy"
          :theirs-first="!fromProject"
          @take="store.resolve(item.key, $event)"
        >
          <template #title>
            <strong>{{ item.label }}</strong>
          </template>
        </merge-conflict-item>
      </div>
    </template>
    <div v-else-if="store.settled" class="merge-bar__head" data-testid="merge-conflicts-settled">
      <i
        class="fa-solid fa-circle-check merge-bar__icon merge-bar__icon--done"
        aria-hidden="true"
      ></i>
      <span class="merge-bar__title">{{ store.settled.message }}</span>
      <el-button
        v-if="store.settled.toProject"
        size="small"
        type="primary"
        data-testid="merge-conflicts-to-project"
        @click="toProject"
      >
        Open the project page
      </el-button>
      <el-button size="small" link aria-label="Dismiss" @click="store.dismissSettled()">
        <i class="fa-solid fa-xmark" aria-hidden="true"></i>
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, watch } from "vue";
import { useRouter } from "vue-router";
import MergeConflictItem from "../../../components/merge/MergeConflictItem.vue";
import { useMergeConflictsStore } from "../../../stores/merge-conflicts-store";
import { useFlowStore } from "../../../stores/flow-store";
import { useProjectStore } from "../../../stores/project-store";
import { conflictsTitle } from "../mergeConflicts";

const store = useMergeConflictsStore();
const flowStore = useFlowStore();
const projectStore = useProjectStore();
const router = useRouter();

// A hub merge calls the person's copy "theirs", a project pull "ours": show that copy first either way.
const fromProject = computed(() => projectStore.isMergeFlow(flowStore.flowId));

watch(
  () => [flowStore.flowId, flowStore.pendingReloadCounter] as const,
  ([flowId]) => void store.load(flowId),
  { immediate: true },
);

const toProject = () => {
  store.dismissSettled();
  void router.push({ name: "project" });
};
</script>

<style scoped>
.merge-bar {
  position: absolute;
  top: 12px;
  left: 50%;
  z-index: 5;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  width: min(560px, calc(100% - 32px));
  max-height: calc(100% - 24px);
  padding: var(--spacing-2) var(--spacing-3);
  transform: translateX(-50%);
  border: 1px solid var(--color-warning, #e6a23c);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  font-size: var(--font-size-sm);
}

.merge-bar__head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.merge-bar__icon {
  color: var(--color-warning-dark, #b88230);
}

.merge-bar__icon--done {
  color: var(--color-success, #67c23a);
}

.merge-bar__title {
  flex: 1;
  min-width: 0;
  color: var(--color-text-primary);
}

.merge-bar__error {
  margin: 0;
  color: var(--color-danger);
}

.merge-bar__list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  overflow-y: auto;
}
</style>
