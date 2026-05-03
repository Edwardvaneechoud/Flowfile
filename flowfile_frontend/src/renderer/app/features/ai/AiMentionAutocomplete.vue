<script setup lang="ts">
// W24 — autocomplete dropdown for `@`-mentions in the AI chat composer.
// Pure presentation: receives candidates + activeIndex from a parent
// (which owns the parsing via `useMentionAutocomplete`) and emits picks
// or dismissals back. Caret-relative positioning is the parent's call;
// we just render at `position`.

import { computed } from "vue";
import type { MentionCandidate } from "./mentionVocabulary";

interface Props {
  candidates: MentionCandidate[];
  activeIndex: number;
  position: { top: number; left: number } | null;
}

const props = defineProps<Props>();

const emit = defineEmits<{
  (e: "pick", candidate: MentionCandidate): void;
  (e: "dismiss"): void;
  (e: "hover", index: number): void;
}>();

const visible = computed(() => props.candidates.length > 0 && props.position !== null);

const style = computed(() => {
  if (!props.position) return { display: "none" };
  return {
    top: `${props.position.top}px`,
    left: `${props.position.left}px`,
  };
});

const handleClick = (candidate: MentionCandidate): void => {
  emit("pick", candidate);
};

const handleMouseEnter = (index: number): void => {
  emit("hover", index);
};
</script>

<template>
  <div
    v-if="visible"
    class="ai-mention-autocomplete"
    :style="style"
    role="listbox"
    aria-label="Mention candidates"
  >
    <ul class="ai-mention-autocomplete__list">
      <li
        v-for="(candidate, index) in candidates"
        :key="`${candidate.kind}:${candidate.ref ?? ''}:${index}`"
        :class="[
          'ai-mention-autocomplete__item',
          { 'ai-mention-autocomplete__item--active': index === activeIndex },
        ]"
        role="option"
        :aria-selected="index === activeIndex"
        @mousedown.prevent="handleClick(candidate)"
        @mouseenter="handleMouseEnter(index)"
      >
        <span class="ai-mention-autocomplete__label">{{ candidate.label }}</span>
        <span v-if="candidate.hint" class="ai-mention-autocomplete__hint">
          {{ candidate.hint }}
        </span>
      </li>
    </ul>
  </div>
</template>

<style scoped>
.ai-mention-autocomplete {
  position: absolute;
  z-index: 1000;
  min-width: 220px;
  max-width: 320px;
  max-height: 240px;
  overflow-y: auto;
  background-color: var(--color-background-primary, #ffffff);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  border-radius: 6px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
  font-size: 12px;
}

.ai-mention-autocomplete__list {
  list-style: none;
  margin: 0;
  padding: 4px 0;
}

.ai-mention-autocomplete__item {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 6px 12px;
  cursor: pointer;
  color: var(--color-text-primary, #24292e);
}

.ai-mention-autocomplete__item:hover,
.ai-mention-autocomplete__item--active {
  background-color: var(--color-background-secondary, #f6f8fa);
}

.ai-mention-autocomplete__label {
  font-weight: 500;
  font-family: var(--font-family-mono, "SF Mono", Menlo, monospace);
}

.ai-mention-autocomplete__hint {
  color: var(--color-text-muted, #6a737d);
  font-size: 11px;
}
</style>
