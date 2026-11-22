<template>
  <div class="merge-conflict" :data-testid="testid ? `${testid}-${itemKey}` : undefined">
    <p v-if="$slots.title" class="merge-conflict__title"><slot name="title" /></p>
    <p v-if="item.message" class="merge-conflict__message">{{ item.message }}</p>
    <div class="merge-conflict__sides">
      <div v-for="side in sides" :key="side.side" class="merge-conflict__side">
        <h4 class="merge-conflict__name">{{ side.name }}</h4>
        <p v-for="(line, i) in side.lines" :key="i" class="merge-conflict__line">{{ line }}</p>
        <el-button
          size="small"
          :disabled="disabled"
          :loading="busy"
          :data-testid="testid ? `${testid}-${side.side}-${itemKey}` : undefined"
          @click="emit('take', side.side)"
        >
          {{ side.action }}
        </el-button>
      </div>
    </div>
    <div v-if="$slots.actions" class="merge-conflict__actions"><slot name="actions" /></div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { conflictSides, type MergeConflictView, type MergeSide } from "./mergeConflict";

const props = defineProps<{
  item: MergeConflictView;
  /** Names the row and its buttons in test ids: `<testid>-<key>`, `<testid>-ours-<key>`. */
  testid?: string;
  itemKey?: string;
  busy?: boolean;
  disabled?: boolean;
  theirsFirst?: boolean;
  actionLabel?: (side: MergeSide, name: string) => string;
}>();

const emit = defineEmits<{ (e: "take", side: MergeSide): void }>();

const sides = computed(() =>
  conflictSides(props.item, { theirsFirst: props.theirsFirst, actionLabel: props.actionLabel }),
);
</script>

<style scoped>
.merge-conflict__title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-sm);
}

.merge-conflict__message {
  margin: 0 0 6px;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.merge-conflict__sides {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-2);
}

.merge-conflict__side {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  min-width: 0;
  padding: var(--spacing-2);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
}

.merge-conflict__name {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-secondary);
  overflow-wrap: anywhere;
}

.merge-conflict__line {
  margin: 0 0 var(--spacing-1);
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  overflow-wrap: anywhere;
}

.merge-conflict__side .el-button {
  margin-top: auto;
}

.merge-conflict__actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
}
</style>
