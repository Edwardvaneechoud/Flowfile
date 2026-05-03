<script setup lang="ts">
// Single message renderer for the W20 chat surface.
//
// Assistant messages render as sanitised markdown (marked + DOMPurify) so
// headings, lists, code blocks, and tables format correctly during streaming.
// User messages stay as plain text — markdown chars the user typed (e.g. a
// literal `*` in their question) shouldn't be silently transformed into
// emphasis. The assistant placeholder streams content into `content` while
// `pending=true`; we render a blinking caret as the visible streaming
// indicator alongside the rendered markdown.

import DOMPurify from "dompurify";
import { marked } from "marked";
import { computed } from "vue";

import type { ChatMessage } from "../../stores/ai-store";

const props = defineProps<{ message: ChatMessage }>();

const isAssistant = computed(() => props.message.role === "assistant");
const isUser = computed(() => props.message.role === "user");
const showCaret = computed(() => props.message.pending && isAssistant.value);
const showEmptyHint = computed(
  () => isAssistant.value && !props.message.content && !props.message.pending,
);

// GFM = GitHub-flavoured markdown (tables, fenced code, task lists).
// breaks = treat single newlines as <br> so streamed paragraph chunks render
// naturally without the user needing to remember double-newline semantics.
marked.setOptions({
  gfm: true,
  breaks: true,
});

// Assistant content rendered as sanitised HTML. DOMPurify strips <script>,
// on* handlers, and javascript: URIs by default — LLM output is untrusted
// at this layer (prompt-injection-via-data is a real vector). marked.parse
// returns string under our config; cast for TS.
const renderedHtml = computed<string>(() => {
  if (!isAssistant.value || !props.message.content) return "";
  const raw = marked.parse(props.message.content) as string;
  return DOMPurify.sanitize(raw);
});
</script>

<template>
  <div class="ai-message" :class="{ 'is-user': isUser, 'is-assistant': isAssistant }">
    <div class="ai-message__role">{{ isUser ? "You" : "Assistant" }}</div>
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
  font-size: 12px;
  padding: 1px 4px;
  border-radius: 3px;
  background-color: rgba(175, 184, 193, 0.2);
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
