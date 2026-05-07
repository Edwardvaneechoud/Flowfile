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
      <span class="ai-agent-run__role">Agent</span>
      <span v-if="startedAtLabel" class="ai-agent-run__time" :title="startedAtTooltip">
        {{ startedAtLabel }}
      </span>
      <!-- Right-aligned text pill flips between Show / Hide via CSS
           [open] selector so the affordance is always explicit, not
           just a chevron. aria-hidden because the chevron + native
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
.ai-agent-run {
  padding: 8px 12px;
  border-radius: 8px;
  margin-bottom: 8px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  border-left: 3px solid var(--color-accent, #6f42c1);
}

.ai-agent-run__header {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  user-select: none;
  list-style: none;
  /* Reserve space for the chunky chevron-button on the left so role /
     time stay aligned regardless of open/closed state. */
  padding: 4px 4px 4px 32px;
  margin: -4px -4px 0;
  border-radius: 4px;
  position: relative;
  transition: background-color var(--transition-fast, 120ms ease);
}

/* Hover the whole summary to reinforce that it's an interactive
   region. */
.ai-agent-run__header:hover {
  background-color: var(--color-background-tertiary, rgba(175, 184, 193, 0.15));
}

/* Hide the default disclosure triangle (Chrome/Safari) — we draw our
   own chevron via ::before, sized as a 22 px accent-bordered button so
   the toggle reads as a primary control rather than a subtle hint. */
.ai-agent-run__header::-webkit-details-marker {
  display: none;
}

.ai-agent-run__header::before {
  content: "▸";
  position: absolute;
  left: 4px;
  top: 50%;
  transform: translateY(-50%);
  width: 22px;
  height: 22px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  font-weight: 700;
  line-height: 1;
  color: var(--color-accent, #6f42c1);
  background-color: var(--color-background-primary, #ffffff);
  border: 1px solid var(--color-accent, #6f42c1);
  border-radius: 4px;
  transition:
    background-color var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease);
}

.ai-agent-run[open] > .ai-agent-run__header::before {
  content: "▾";
}

/* Filled accent on hover so the chevron-button looks pressable. */
.ai-agent-run__header:hover::before {
  background-color: var(--color-accent, #6f42c1);
  color: #ffffff;
}

/* Right-aligned Show/Hide pill — reinforces collapse affordance
   beyond the chevron alone. CSS ::before flips text with state via
   the [open] selector on the parent <details>, no Vue ref needed.
   flex-shrink:0 so it stays visible even in narrow drawer widths. */
.ai-agent-run__toggle-hint {
  margin-left: auto;
  flex-shrink: 0;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  color: var(--color-accent, #6f42c1);
  padding: 3px 8px;
  border: 1px solid var(--color-accent, #6f42c1);
  border-radius: 3px;
  line-height: 1.2;
  background-color: var(--color-background-primary, #ffffff);
  transition:
    background-color var(--transition-fast, 120ms ease),
    color var(--transition-fast, 120ms ease);
}

.ai-agent-run__header:hover .ai-agent-run__toggle-hint {
  background-color: var(--color-accent, #6f42c1);
  color: #ffffff;
}

.ai-agent-run > .ai-agent-run__header > .ai-agent-run__toggle-hint::before {
  content: "Show";
}

.ai-agent-run[open] > .ai-agent-run__header > .ai-agent-run__toggle-hint::before {
  content: "Hide";
}

.ai-agent-run__role {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  color: var(--color-accent, #6f42c1);
  letter-spacing: 0.4px;
}

.ai-agent-run__time {
  font-size: 11px;
  color: var(--color-text-muted, #6a737d);
  cursor: default;
}

.ai-agent-run__body {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-top: 6px;
}
</style>
