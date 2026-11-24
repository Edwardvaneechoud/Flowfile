<template>
  <div class="suggest-outcome" :class="{ warn: outcome.warn }" data-testid="suggest-outcome">
    <p class="outcome-line" role="status">
      {{ outcome.line }}
      <a
        v-if="outcome.proposed > 0 && showReviewLink"
        href="#"
        data-testid="suggest-outcome-review"
        @click.prevent="emit('review')"
        >Review</a
      >
    </p>
    <ul v-if="outcome.notes.length" class="outcome-notes">
      <li v-for="note in outcome.notes" :key="note">{{ note }}</li>
    </ul>
    <details v-if="outcome.skipped.length" class="outcome-skipped" data-testid="suggest-skipped">
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

withDefaults(defineProps<{ outcome: SuggestOutcome; showReviewLink?: boolean }>(), {
  showReviewLink: true,
});
const emit = defineEmits<{ review: [] }>();
</script>

<style scoped>
.suggest-outcome {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
}

.outcome-line {
  margin: 0;
  color: var(--color-text-primary);
}

.outcome-line a {
  margin-left: var(--spacing-1);
}

.suggest-outcome.warn .outcome-line {
  padding-left: var(--spacing-3);
  border-left: 3px solid var(--color-warning-dark);
  color: var(--color-warning-dark);
}

.outcome-notes {
  margin: 0;
  padding: 0 0 0 var(--spacing-4);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.outcome-skipped {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.outcome-skipped summary {
  cursor: pointer;
}

.outcome-skipped ul {
  margin: var(--spacing-1) 0 0;
  padding: 0 0 0 var(--spacing-4);
}

.skipped-what {
  color: var(--color-text-primary);
}
</style>
