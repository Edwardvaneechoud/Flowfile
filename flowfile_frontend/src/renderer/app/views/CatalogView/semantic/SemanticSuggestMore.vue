<template>
  <div class="suggest-more">
    <div class="toolbar">
      <div class="toolbar-hint"><slot name="hint" /></div>
      <div class="toolbar-actions">
        <a
          href="#"
          class="ask-link"
          :data-testid="`suggest-more-ask-${testId}`"
          @click.prevent="boxOpen = !boxOpen"
          >{{ boxOpen ? "Hide" : askLabel }}</a
        >
        <el-tooltip :content="buttonHint" placement="top" :show-after="300">
          <button
            class="btn btn-secondary btn-sm"
            type="button"
            :disabled="running"
            :data-testid="`suggest-more-${testId}`"
            @click="suggestMore(kinds)"
          >
            <i
              :class="running ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-wand-magic-sparkles'"
            ></i>
            {{ label }}
          </button>
        </el-tooltip>
        <slot />
      </div>
    </div>
    <p v-if="running" class="note" role="status">{{ step }}</p>
    <p v-else-if="error" class="note warn" role="alert">{{ error }}</p>
    <SemanticSuggestOutcome v-else-if="outcome" :outcome="outcome" @review="emit('review')" />
    <SemanticSuggestBox v-if="boxOpen" :model="model" :kinds="kinds" @review="emit('review')" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useSemanticSuggest } from "../../../composables/useSemanticSuggest";
import type { SemanticModel, SemanticSuggestKind } from "../../../types/semantic.types";
import SemanticSuggestBox from "./SemanticSuggestBox.vue";
import SemanticSuggestOutcome from "./SemanticSuggestOutcome.vue";

const props = defineProps<{
  model: SemanticModel;
  kinds: SemanticSuggestKind[];
  label: string;
  askLabel: string;
}>();
const emit = defineEmits<{ review: [] }>();

const { usable, running, step, outcome, error, suggestMore } = useSemanticSuggest();
const boxOpen = ref(false);
const testId = computed(() => props.kinds.join("-"));

const buttonHint = computed(() =>
  usable.value
    ? "The rules look first, then the AI looks for what rules miss. Everything waits in Review."
    : "The rules look for these from names, types and the data. Everything waits in Review.",
);
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

.ask-link {
  font-size: var(--font-size-xs);
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
