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
  <div class="ai-agent-run">
    <div class="ai-agent-run__header">
      <span class="ai-agent-run__role">Agent</span>
      <span v-if="startedAtLabel" class="ai-agent-run__time" :title="startedAtTooltip">
        {{ startedAtLabel }}
      </span>
    </div>
    <div class="ai-agent-run__body">
      <AiAgentEvent
        v-for="(event, idx) in events"
        :key="`${event.kind}-${idx}-${event.at}`"
        :event="event"
      />
    </div>
  </div>
</template>

<style scoped>
.ai-agent-run {
  padding: 8px 12px;
  border-radius: 8px;
  margin-bottom: 8px;
  display: flex;
  flex-direction: column;
  gap: 6px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  border-left: 3px solid var(--color-accent, #6f42c1);
}

.ai-agent-run__header {
  display: flex;
  align-items: baseline;
  gap: 8px;
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
}
</style>
