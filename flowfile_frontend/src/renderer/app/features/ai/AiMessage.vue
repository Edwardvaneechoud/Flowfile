<script setup lang="ts">
// Single message renderer for the chat surface.
//
// Assistant messages render as sanitised markdown (marked + DOMPurify) so
// headings, lists, code blocks, and tables format correctly during streaming.
// User messages stay as plain text — markdown chars the user typed (e.g. a
// literal `*` in their question) shouldn't be silently transformed into
// emphasis. The assistant placeholder streams content into `content` while
// `pending=true`; we render a blinking caret as the visible streaming
// indicator alongside the rendered markdown.

import { computed } from "vue";

import type { ChatMessage } from "../../stores/ai-store";
import AiAvatar from "./AiAvatar.vue";
import AiThinkingDots from "./AiThinkingDots.vue";
import { sanitiseMarkdown } from "./markdown";

const props = defineProps<{ message: ChatMessage }>();
const emit = defineEmits<{ (e: "add-build", messageId: number): void }>();

const isAssistant = computed(() => props.message.role === "assistant");

// Simple-build result: show an inline "Add to canvas" button while the diff is
// pending; flip to a confirmation once applied.
const hasBuild = computed(() => isAssistant.value && !!props.message.buildDiffId);
const buildAdded = computed(() => props.message.buildAdded === true);
const buildLabel = computed(() => {
  const n = props.message.buildOpCount ?? 0;
  return n > 0 ? `Add to canvas (${n} node${n === 1 ? "" : "s"})` : "Add to canvas";
});
const isUser = computed(() => props.message.role === "user");
// Streaming caret only renders once content has started arriving; the
// pre-content "thinking" state shows the dots animation instead.
const showCaret = computed(
  () => props.message.pending && isAssistant.value && !!props.message.content,
);
const showThinking = computed(
  () => props.message.pending && isAssistant.value && !props.message.content,
);
const showEmptyHint = computed(
  () => isAssistant.value && !props.message.content && !props.message.pending,
);

// Assistant content rendered as sanitised HTML via the shared helper —
// see `./markdown.ts` for the marked + DOMPurify pipeline.
const renderedHtml = computed<string>(() => {
  if (!isAssistant.value || !props.message.content) return "";
  return sanitiseMarkdown(props.message.content);
});

// Compact HH:MM time label, locale-aware via Intl.DateTimeFormat.
// Tooltip shows the full date/time so the user can drill into when an
// older message came in. Falls back to empty if `createdAt` was missing
// from a legacy persisted entry (sanitiser should populate it from `id`,
// but be defensive).
const timeLabel = computed<string>(() => {
  const ts = props.message.createdAt;
  if (typeof ts !== "number" || ts <= 0) return "";
  try {
    return new Intl.DateTimeFormat(undefined, {
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(ts));
  } catch {
    return "";
  }
});

const timeTooltip = computed<string>(() => {
  const ts = props.message.createdAt;
  if (typeof ts !== "number" || ts <= 0) return "";
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return "";
  }
});
</script>

<template>
  <div class="ai-message" :class="{ 'is-user': isUser, 'is-assistant': isAssistant }">
    <!-- Assistant header: gradient avatar + sentence-case "AI" label + timestamp.
         Hidden for user messages — right-alignment + soft purple already signals identity. -->
    <div v-if="isAssistant" class="ai-message__header">
      <AiAvatar size="md" />
      <span class="ai-message__role">AI</span>
      <span v-if="timeLabel" class="ai-message__sep" aria-hidden="true">·</span>
      <span v-if="timeLabel" class="ai-message__time" :title="timeTooltip">
        {{ timeLabel }}
      </span>
    </div>
    <div class="ai-message__body">
      <!-- Assistant: rendered markdown, sanitised via DOMPurify before v-html. -->
      <!-- eslint-disable-next-line vue/no-v-html -->
      <div
        v-if="isAssistant && message.content"
        class="ai-message__markdown"
        v-html="renderedHtml"
      />
      <!-- User: plain text. white-space: pre-wrap preserves newlines. -->
      <span v-else-if="message.content" class="ai-message__text">{{ message.content }}</span>
      <AiThinkingDots v-if="showThinking" label="Thinking" />
      <span v-if="showEmptyHint" class="ai-message__hint">[no response]</span>
      <span v-if="showCaret" class="ai-message__caret" aria-hidden="true">▍</span>
      <div v-if="message.error" class="ai-message__error">{{ message.error }}</div>
      <!-- Simple-build: inline apply button (no separate review panel). -->
      <div v-if="hasBuild" class="ai-message__build">
        <button
          v-if="!buildAdded"
          type="button"
          class="ai-message__build-btn"
          @click="emit('add-build', message.id)"
        >
          <i class="fa-solid fa-circle-plus"></i>
          <span>{{ buildLabel }}</span>
        </button>
        <span v-else class="ai-message__build-done">
          <i class="fa-solid fa-circle-check"></i>
          Added to canvas
        </span>
      </div>
    </div>
    <!-- User footer: tiny inline timestamp, right-aligned inside the bubble. -->
    <div v-if="isUser && timeLabel" class="ai-message__user-time" :title="timeTooltip">
      {{ timeLabel }}
    </div>
  </div>
</template>

<style scoped>
.ai-message {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-bottom: 8px;
}

/* User: right-aligned bubble, max 85% width, soft purple, asymmetric
   corners (sharper bottom-right toward "you"), no border. The right
   alignment + tint signals identity, so the role label is dropped. */
.ai-message.is-user {
  align-self: flex-end;
  max-width: 85%;
  margin-left: auto;
  padding: 8px 12px;
  border-radius: 10px 10px 4px 10px;
  background-color: var(--color-focus-ring-purple-light, #ece7ff);
  gap: 2px;
}

/* Assistant: full-width refined card. White surface, subtle border,
   gentle elevation, 10px radius. Roomier padding because the body
   often contains rich markdown (lists, code, tables). */
.ai-message.is-assistant {
  align-self: stretch;
  padding: 10px 14px;
  border-radius: 10px;
  background-color: var(--color-background-primary, #ffffff);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
}

.ai-message__header {
  display: flex;
  align-items: center;
  gap: 6px;
}

/* Sentence-case "AI" — modern apps have moved away from
   uppercase-shouting role labels. */
.ai-message__role {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1;
}

.ai-message__sep {
  color: var(--color-text-tertiary, #a0aec0);
  font-size: 11px;
  line-height: 1;
}

.ai-message__time {
  font-size: 10px;
  color: var(--color-text-tertiary, #a0aec0);
  cursor: default;
  line-height: 1;
}

.ai-message__user-time {
  font-size: 10px;
  color: var(--color-text-tertiary, #a0aec0);
  text-align: right;
  cursor: default;
  line-height: 1;
}

.ai-message.is-user .ai-message__body {
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
}

.ai-message.is-assistant .ai-message__body {
  font-size: 12px;
  color: var(--color-text-primary, #24292e);
}

.ai-message__body {
  line-height: 1.5;
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

.ai-message__build {
  margin-top: 8px;
}

.ai-message__build-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  border: none;
  border-radius: 6px;
  background-color: var(--color-accent, #6b4eff);
  color: #fff;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}

.ai-message__build-btn:hover {
  filter: brightness(1.05);
}

.ai-message__build-done {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 600;
  color: var(--color-success, #16a34a);
}

/* ------------------------------------------------------------------ */
/* Markdown rendering for assistant messages.                          */
/* `:deep(...)` reaches into the v-html content from scoped CSS.       */
/* `white-space: normal` overrides the body's pre-wrap so paragraphs   */
/* don't keep stray indentation from the source markdown.              */
/* ------------------------------------------------------------------ */

.ai-message__markdown {
  white-space: normal;
}

.ai-message__markdown :deep(p) {
  margin: 0 0 8px;
}
.ai-message__markdown :deep(p:last-child) {
  margin-bottom: 0;
}

.ai-message__markdown :deep(h1),
.ai-message__markdown :deep(h2),
.ai-message__markdown :deep(h3),
.ai-message__markdown :deep(h4),
.ai-message__markdown :deep(h5),
.ai-message__markdown :deep(h6) {
  margin: 12px 0 6px;
  font-weight: 600;
  line-height: 1.25;
}
.ai-message__markdown :deep(h1) {
  font-size: 16px;
}
.ai-message__markdown :deep(h2) {
  font-size: 15px;
}
.ai-message__markdown :deep(h3) {
  font-size: 14px;
}
.ai-message__markdown :deep(h4),
.ai-message__markdown :deep(h5),
.ai-message__markdown :deep(h6) {
  font-size: 13px;
}

.ai-message__markdown :deep(ul),
.ai-message__markdown :deep(ol) {
  margin: 4px 0 8px;
  padding-left: 20px;
}
.ai-message__markdown :deep(li) {
  margin: 2px 0;
}
.ai-message__markdown :deep(li > p) {
  margin: 0;
}

.ai-message__markdown :deep(code) {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 11px;
  padding: 1px 5px;
  border-radius: 4px;
  background-color: var(--color-background-tertiary, #f1f3f5);
  color: var(--color-text-primary, #24292e);
}

.ai-message__markdown :deep(pre) {
  margin: 8px 0;
  padding: 8px 10px;
  border-radius: 6px;
  background-color: var(--color-background-tertiary, #f0f1f3);
  overflow-x: auto;
}
.ai-message__markdown :deep(pre code) {
  padding: 0;
  background: transparent;
  font-size: 12px;
  line-height: 1.45;
  white-space: pre;
}

.ai-message__markdown :deep(blockquote) {
  margin: 8px 0;
  padding: 4px 12px;
  border-left: 3px solid var(--color-border-primary, #d0d7de);
  color: var(--color-text-muted, #6a737d);
}

.ai-message__markdown :deep(a) {
  color: var(--color-link, #0366d6);
  text-decoration: underline;
}

.ai-message__markdown :deep(strong) {
  font-weight: 600;
}
.ai-message__markdown :deep(em) {
  font-style: italic;
}

.ai-message__markdown :deep(table) {
  border-collapse: collapse;
  margin: 8px 0;
  font-size: 12px;
}
.ai-message__markdown :deep(th),
.ai-message__markdown :deep(td) {
  border: 1px solid var(--color-border-primary, #d0d7de);
  padding: 4px 8px;
  text-align: left;
}
.ai-message__markdown :deep(th) {
  background-color: var(--color-background-tertiary, rgba(175, 184, 193, 0.15));
  font-weight: 600;
}

.ai-message__markdown :deep(hr) {
  border: 0;
  border-top: 1px solid var(--color-border-primary, #d0d7de);
  margin: 12px 0;
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
