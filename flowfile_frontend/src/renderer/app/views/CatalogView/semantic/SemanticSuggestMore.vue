<template>
  <div class="suggest-more">
    <div class="toolbar">
      <div class="toolbar-hint"><slot name="hint" /></div>
      <div class="toolbar-actions">
        <el-tooltip
          :content="busy ? SUGGEST_BUSY_TEXT : buttonHint"
          placement="top"
          :show-after="300"
        >
          <span>
            <button
              class="btn btn-secondary btn-sm"
              type="button"
              :disabled="running || busy"
              :data-testid="`suggest-more-${testId}`"
              @click="suggestMore(kinds)"
            >
              <i class="fa-solid fa-wand-magic-sparkles"></i>
              {{ label }}
            </button>
          </span>
        </el-tooltip>
        <slot />
      </div>
    </div>
    <p v-if="error && !running" class="note warn" role="alert">{{ error }}</p>
    <SemanticSuggestOutcome
      v-else-if="outcome && !running"
      :outcome="outcome"
      @review="emit('review')"
    />
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { SUGGEST_BUSY_TEXT } from "../../../composables/useSemanticActivity";
import { useSemanticSuggest } from "../../../composables/useSemanticSuggest";
import { SEMANTIC_SUGGEST_SURFACE, useTaskModels } from "../../../composables/useTaskModels";
import type { SemanticSuggestKind } from "../../../types/semantic.types";
import SemanticSuggestOutcome from "./SemanticSuggestOutcome.vue";

const props = defineProps<{
  kinds: SemanticSuggestKind[];
  label: string;
}>();
const emit = defineEmits<{ review: [] }>();

const { usable, running, busy, outcome, error, suggestMore } = useSemanticSuggest();
const { describe } = useTaskModels();
const testId = computed(() => props.kinds.join("-"));

const buttonHint = computed(() => {
  if (!usable.value) return "The rules look for these from names, types and the data.";
  const model = describe(SEMANTIC_SUGGEST_SURFACE);
  return `The rules look first, then the AI looks for what rules miss.${model ? ` ${model}.` : ""}`;
});
</script>

<style scoped>
.suggest-more {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.toolbar-hint {
  min-width: 0;
}

.toolbar-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
}

.note {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.note.warn {
  color: var(--color-warning-dark);
}
</style>
