<template>
  <div class="suggest-outcome" data-testid="suggest-outcome">
    <p class="outcome-line" :class="{ 'warn-bar': outcome.warn }" role="status">
      {{ outcome.line }}
      <a
        v-if="outcome.proposed > 0"
        href="#"
        data-testid="suggest-outcome-review"
        @click.prevent="emit('review')"
        >Review</a
      >
    </p>
    <ul v-if="outcome.notes.length" class="note outcome-notes">
      <li v-for="note in outcome.notes" :key="note">{{ note }}</li>
    </ul>
    <details
      v-if="outcome.skipped.length"
      class="note outcome-skipped"
      data-testid="suggest-skipped"
    >
      <summary>{{ outcome.skipped.length }} left out, and why</summary>
      <ul>
        <li v-for="s in outcome.skipped" :key="s.what">
          <span class="skipped-what">{{ s.what }}</span
          >: {{ s.why }}
        </li>
      </ul>
    </details>
  </div>
</template>

<script setup lang="ts">
import type { SuggestOutcome } from "../../../composables/useSemanticSuggest";

defineProps<{ outcome: SuggestOutcome }>();
const emit = defineEmits<{ review: [] }>();
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.suggest-outcome {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
}

.outcome-line {
  margin: 0;
}

.outcome-line a {
  margin-left: var(--spacing-1);
}

.outcome-notes,
.outcome-skipped ul {
  padding: 0 0 0 var(--spacing-4);
}

.outcome-skipped summary {
  cursor: pointer;
}

.outcome-skipped ul {
  margin: var(--spacing-1) 0 0;
}

.skipped-what {
  color: var(--color-text-primary);
}
</style>
