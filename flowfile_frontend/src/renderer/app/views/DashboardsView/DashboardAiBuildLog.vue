<template>
  <details class="build-log" data-testid="dashboard-ai-log" :open="open">
    <summary class="build-log__summary">
      <span class="material-icons build-log__chevron" aria-hidden="true">chevron_right</span>
      <span>Build log</span>
      <span class="build-log__count">{{ logSummary(entries) }}</span>
    </summary>
    <ol class="build-log__entries">
      <li
        v-for="entry in entries"
        :key="entry.key"
        class="build-log__entry"
        :class="`is-${entry.status}`"
        :data-status="entry.status"
        data-testid="dashboard-ai-log-entry"
      >
        <div class="build-log__head">
          <span class="material-icons build-log__icon" aria-hidden="true">{{
            statusIcon(entry.status)
          }}</span>
          <span class="build-log__step">{{ stepLabel(entry.step) }}</span>
          <span class="build-log__title">{{ entry.title }}</span>
          <span class="build-log__meta">
            <span>{{ statusLabel(entry.status) }}</span>
            <span v-if="durationText(entry)">{{ durationText(entry) }}</span>
            <span v-if="modelText(entry)">{{ modelText(entry) }}</span>
          </span>
        </div>
        <dl v-if="entryLines(entry).length" class="build-log__lines">
          <template v-for="(line, i) in entryLines(entry)" :key="i">
            <dt>{{ line.label }}</dt>
            <dd>{{ line.text }}</dd>
          </template>
        </dl>
      </li>
    </ol>
    <div class="build-log__footer">
      <p class="build-log__hint">{{ PROMPT_LOG_HINT }}</p>
      <el-button size="small" data-testid="dashboard-ai-log-copy" @click="onCopy">
        {{ copied ? "Copied" : "Copy log" }}
      </el-button>
    </div>
  </details>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";

import { copyTextEverywhere } from "../../utils/clipboardUtils";
import {
  PROMPT_LOG_HINT,
  durationText,
  entryLines,
  logAsText,
  logSummary,
  modelText,
  statusLabel,
  stepLabel,
  type BuildLogEntry,
  type LogStatus,
} from "./aiBuildLog";

const props = defineProps<{ entries: BuildLogEntry[]; open?: boolean }>();

const COPIED_MS = 2000;
const copied = ref(false);
let copiedTimer: ReturnType<typeof setTimeout> | null = null;
const clearCopied = () => {
  if (copiedTimer) clearTimeout(copiedTimer);
  copiedTimer = null;
  copied.value = false;
};
watch(() => props.entries, clearCopied);
onBeforeUnmount(clearCopied);

const ICONS: Record<LogStatus, string> = {
  running: "autorenew",
  ok: "check",
  warning: "error_outline",
  failed: "close",
  cancelled: "block",
};
const statusIcon = (status: LogStatus): string => ICONS[status];

const onCopy = async () => {
  const ok = await copyTextEverywhere(logAsText(props.entries));
  clearCopied();
  copied.value = ok;
  if (ok) copiedTimer = setTimeout(clearCopied, COPIED_MS);
};
</script>

<style scoped>
.build-log {
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  font-size: 12px;
}
.build-log__summary {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 10px;
  cursor: pointer;
  font-weight: 600;
  color: var(--color-text-primary);
  list-style: none;
}
.build-log__summary::-webkit-details-marker {
  display: none;
}
.build-log__summary:focus-visible {
  outline: 2px solid var(--color-focus-ring-accent, var(--color-accent));
  outline-offset: -2px;
}
.build-log__chevron {
  font-size: 16px;
  transition: transform 0.15s;
}
.build-log[open] .build-log__chevron {
  transform: rotate(90deg);
}
.build-log__count {
  font-weight: 400;
  color: var(--color-text-muted);
}
.build-log__entries {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 280px;
  margin: 0;
  padding: 4px 10px 8px;
  overflow-y: auto;
  list-style: none;
}
.build-log__head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  color: var(--color-text-primary);
}
.build-log__icon {
  font-size: 15px;
  color: var(--color-text-muted);
}
.is-ok .build-log__icon {
  color: var(--el-color-success);
}
.is-warning .build-log__icon {
  color: var(--el-color-warning);
}
.is-failed .build-log__icon,
.is-failed .build-log__title {
  color: var(--el-color-danger);
}
.is-running .build-log__icon {
  color: var(--color-accent);
}
.build-log__step {
  padding: 0 6px;
  border-radius: 4px;
  background: var(--el-fill-color-light);
  color: var(--color-text-muted);
  font-size: 11px;
}
.build-log__title {
  font-weight: 600;
}
.build-log__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-left: auto;
  color: var(--color-text-muted);
}
.build-log__lines {
  display: grid;
  grid-template-columns: max-content 1fr;
  gap: 2px 10px;
  margin: 4px 0 0 21px;
}
.build-log__lines dt {
  color: var(--color-text-muted);
}
.build-log__lines dd {
  margin: 0;
  color: var(--color-text-primary);
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
.build-log__footer {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 6px 10px 8px;
  border-top: 1px solid var(--el-border-color-lighter);
}
.build-log__hint {
  flex: 1;
  margin: 0;
  color: var(--color-text-muted);
  line-height: 16px;
}
@media (max-width: 640px) {
  .build-log__meta {
    margin-left: 21px;
  }
  .build-log__lines {
    grid-template-columns: 1fr;
  }
}
</style>
