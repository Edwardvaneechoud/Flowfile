<script setup lang="ts">
// Single message renderer for the W20 chat surface.
//
// Plain text only — markdown rendering deferred. The assistant placeholder
// streams content into `content` directly while `pending=true`; we render a
// blinking caret as the visible streaming indicator.

import { computed } from "vue";
import type { ChatMessage } from "../../stores/ai-store";

const props = defineProps<{ message: ChatMessage }>();

const isAssistant = computed(() => props.message.role === "assistant");
const isUser = computed(() => props.message.role === "user");
const showCaret = computed(() => props.message.pending && isAssistant.value);
const showEmptyHint = computed(
  () => isAssistant.value && !props.message.content && !props.message.pending,
);
</script>

<template>
  <div class="ai-message" :class="{ 'is-user': isUser, 'is-assistant': isAssistant }">
    <div class="ai-message__role">{{ isUser ? "You" : "Assistant" }}</div>
    <div class="ai-message__body">
      <span v-if="message.content" class="ai-message__text">{{ message.content }}</span>
      <span v-if="showEmptyHint" class="ai-message__hint">[no response]</span>
      <span v-if="showCaret" class="ai-message__caret" aria-hidden="true">▍</span>
      <div v-if="message.error" class="ai-message__error">{{ message.error }}</div>
    </div>
  </div>
</template>

<style scoped>
.ai-message {
  padding: 8px 12px;
  border-radius: 8px;
  margin-bottom: 8px;
  display: flex;
  flex-direction: column;
  gap: 4px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-message.is-user {
  background-color: var(--color-focus-ring-purple-light, #ece7ff);
  align-self: stretch;
}

.ai-message.is-assistant {
  background-color: var(--color-background-secondary, #f6f8fa);
}

.ai-message__role {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  color: var(--color-text-muted, #6a737d);
  letter-spacing: 0.4px;
}

.ai-message__body {
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-primary, #24292e);
  white-space: pre-wrap;
  word-break: break-word;
}

.ai-message__hint {
  font-style: italic;
  color: var(--color-text-muted, #6a737d);
}

.ai-message__caret {
  display: inline-block;
  margin-left: 2px;
  animation: ai-caret-blink 1s steps(1, end) infinite;
}

.ai-message__error {
  margin-top: 6px;
  padding: 6px 8px;
  border-radius: 6px;
  background-color: var(--color-danger-light, #ffe5e5);
  color: var(--color-danger, #c53030);
  font-size: 12px;
}

@keyframes ai-caret-blink {
  0%,
  50% {
    opacity: 1;
  }
  51%,
  100% {
    opacity: 0;
  }
}
</style>
