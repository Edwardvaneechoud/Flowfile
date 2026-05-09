<script setup lang="ts">
// Group of consecutive planner events rendered as a single chat bubble
// with the role label "Agent" — visually parallel to AiMessage.vue's
// assistant bubble. Each event renders inside via the existing
// AiAgentEvent component (which knows how to format thinking events as
// markdown vs other kinds as mechanical summaries).
//
// AiAssistant.vue's `timelineItems` computed groups consecutive events
// between user/assistant messages into one of these bubbles, so the
// chat reads chronologically with each agent run's output collected at
// the position of its first event.

import { computed } from "vue";

import type { AgentEvent } from "../../stores/ai-agent-store";
import AiAgentEvent from "./AiAgentEvent.vue";
import AiAvatar from "./AiAvatar.vue";

const props = defineProps<{ events: AgentEvent[] }>();

// Display HH:MM derived from the run's first event timestamp. Locale-
// aware via Intl.DateTimeFormat — ``hour: "numeric"`` + ``minute: "2-digit"``
// produces "10:23 AM" / "10:23" depending on the user's locale.
const startedAt = computed<number>(() => {
  if (props.events.length === 0) return Date.now();
  return props.events[0].at;
});

const startedAtLabel = computed<string>(() => {
  try {
    return new Intl.DateTimeFormat(undefined, {
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(startedAt.value));
  } catch {
    return "";
  }
});

const startedAtTooltip = computed<string>(() => {
  try {
    return new Date(startedAt.value).toLocaleString();
  } catch {
    return "";
  }
});
</script>

<template>
  <!-- Native <details> wraps the entire agent run so the user can
       collapse a verbose bubble (chain-of-thought + tool steps) to a
       single header line. Default open so new runs render expanded;
       <details> preserves expand state across re-renders without
       extra Vue ref plumbing. -->
  <details class="ai-agent-run" open>
    <summary class="ai-agent-run__header">
      <span class="ai-agent-run__chevron" aria-hidden="true">
        <span class="material-icons">expand_more</span>
      </span>
      <AiAvatar size="md" />
      <span class="ai-agent-run__role">Agent</span>
      <span v-if="startedAtLabel" class="ai-agent-run__sep" aria-hidden="true">·</span>
      <span v-if="startedAtLabel" class="ai-agent-run__time" :title="startedAtTooltip">
        {{ startedAtLabel }}
      </span>
      <!-- Right-aligned quiet text link flips between "Show details" /
           "Hide details" via CSS [open] selector so the affordance is
           always explicit. aria-hidden because the chevron + native
           <summary> already announce the toggle to screen readers. -->
      <span class="ai-agent-run__toggle-hint" aria-hidden="true" />
    </summary>
    <div class="ai-agent-run__body">
      <AiAgentEvent
        v-for="(event, idx) in events"
        :key="`${event.kind}-${idx}-${event.at}`"
        :event="event"
      />
    </div>
  </details>
</template>

<style scoped>
/* Refined card matching the assistant message bubble: white surface,
   1px border, soft elevation, 10px radius. The previous purple left
   stripe competed with the inner-event accents, so we drop it. */
.ai-agent-run {
  padding: 10px 14px;
  border-radius: 10px;
  margin-bottom: 8px;
  background-color: var(--color-background-primary, #ffffff);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
}

.ai-agent-run__header {
  display: flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  user-select: none;
  list-style: none;
  padding: 0;
  border-radius: 4px;
  transition: background-color var(--transition-fast, 120ms ease);
}

/* Hide the default native disclosure triangle — we draw our own
   subtle inline chevron. */
.ai-agent-run__header::-webkit-details-marker {
  display: none;
}

/* Bigger, clearly clickable chevron — uses Material Icons expand_more
   so it visually matches the rest of the toolbar iconography. Rotates
   on [open]. */
.ai-agent-run__chevron {
  display: inline-flex;
  width: 22px;
  height: 22px;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  color: var(--color-text-secondary, #4a5568);
  background-color: transparent;
  transform: rotate(-90deg);
  transition:
    transform var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease),
    background-color var(--transition-fast, 120ms ease);
}

.ai-agent-run__chevron .material-icons {
  font-size: 18px;
  line-height: 1;
}

.ai-agent-run[open] > .ai-agent-run__header > .ai-agent-run__chevron {
  transform: rotate(0deg);
}

.ai-agent-run__header:hover .ai-agent-run__chevron {
  color: var(--color-text-primary, #1a1a2e);
  background-color: var(--color-background-secondary, #f8f9fa);
}

/* Quiet right-aligned text link, no border / pill. CSS ::before flips
   the text via the [open] selector on the parent <details>. */
.ai-agent-run__toggle-hint {
  margin-left: auto;
  flex-shrink: 0;
  font-size: 11px;
  color: var(--color-text-tertiary, #a0aec0);
  line-height: 1;
  transition: color var(--transition-fast, 120ms ease);
}

.ai-agent-run__header:hover .ai-agent-run__toggle-hint {
  color: var(--color-text-secondary, #4a5568);
  text-decoration: underline;
}

.ai-agent-run > .ai-agent-run__header > .ai-agent-run__toggle-hint::before {
  content: "Show details";
}

.ai-agent-run[open] > .ai-agent-run__header > .ai-agent-run__toggle-hint::before {
  content: "Hide details";
}

.ai-agent-run__role {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary, #4a5568);
  line-height: 1;
}

.ai-agent-run__sep {
  color: var(--color-text-tertiary, #a0aec0);
  font-size: 11px;
  line-height: 1;
}

.ai-agent-run__time {
  font-size: 10px;
  color: var(--color-text-tertiary, #a0aec0);
  cursor: default;
  line-height: 1;
}

.ai-agent-run__body {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-top: 8px;
}
</style>
