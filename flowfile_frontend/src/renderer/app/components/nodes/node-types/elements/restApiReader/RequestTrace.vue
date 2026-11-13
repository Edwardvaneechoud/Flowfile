<template>
  <div class="request-trace" data-testid="request-trace">
    <div
      class="trace-line"
      role="button"
      tabindex="0"
      :aria-expanded="open"
      @click="open = !open"
      @keydown.enter.prevent="open = !open"
      @keydown.space.prevent="open = !open"
    >
      <i class="fa-solid fa-chevron-right trace-chevron" :class="{ 'is-open': open }"></i>
      <span class="trace-status" :class="{ 'is-ok': entry.status < 400 }">{{
        responseLine(entry)
      }}</span>
      <span class="trace-toggle">{{ open ? "Hide request" : "Show request" }}</span>
    </div>

    <div v-show="open" class="trace-detail">
      <div class="trace-head">
        <span class="trace-label">Request sent</span>
        <button type="button" class="trace-copy" @click="copyCurl">
          <i class="fa-regular fa-copy"></i> Copy as cURL
        </button>
      </div>
      <pre class="trace-block" data-testid="trace-request">{{ requestText }}</pre>
      <div class="trace-head">
        <span class="trace-label">Response</span>
        <span v-if="truncated" class="trace-note">first part shown</span>
      </div>
      <pre class="trace-block trace-response" data-testid="trace-response">{{ responseText }}</pre>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import { copyTextEverywhere } from "../../../../../utils/clipboardUtils";
import { responseLine, showSecret, toCurl, type TraceEntry } from "./restApiView";

const props = defineProps<{
  entry: TraceEntry;
  secretName: string;
  responseText: string;
  truncated: boolean;
}>();

const open = ref(false);

const requestText = computed(() => {
  const lines = [`${props.entry.method} ${props.entry.url}`];
  for (const [name, value] of Object.entries(props.entry.headers)) lines.push(`${name}: ${value}`);
  if (props.entry.body !== null && props.entry.body !== undefined) {
    lines.push("", JSON.stringify(props.entry.body, null, 2));
  }
  return showSecret(lines.join("\n"), props.secretName);
});

const copyCurl = async () => {
  if (await copyTextEverywhere(toCurl(props.entry, props.secretName))) {
    ElMessage.success("Copied. The secret is a variable, not its value.");
  } else {
    ElMessage.error("Could not copy to the clipboard.");
  }
};
</script>

<style scoped>
.request-trace {
  margin-top: 8px;
  border: 1px solid var(--color-border-primary, #d0d0d0);
  border-radius: 4px;
  font-size: 12px;
}

.trace-line {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 8px;
  cursor: pointer;
  user-select: none;
}

.trace-chevron {
  font-size: 10px;
  color: var(--color-text-secondary, #888);
  transition: transform 0.15s;
}

.trace-chevron.is-open {
  transform: rotate(90deg);
}

.trace-status {
  font-family: var(--font-family-mono, monospace);
  color: var(--color-danger, #d9534f);
}

.trace-status.is-ok {
  color: var(--color-success-dark);
}

.trace-toggle {
  margin-left: auto;
  color: var(--color-text-secondary, #888);
}

.trace-detail {
  border-top: 1px solid var(--color-border-primary, #d0d0d0);
  padding: 8px;
}

.trace-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
}

.trace-label {
  font-weight: 600;
}

.trace-note {
  color: var(--color-text-secondary, #888);
  font-size: 11px;
}

.trace-copy {
  cursor: pointer;
  border: none;
  background: none;
  padding: 0;
  font-size: 11px;
  color: var(--color-accent, #3b82f6);
}

.trace-block {
  margin: 0 0 10px;
  padding: 6px 8px;
  border-radius: 4px;
  background: var(--color-background-secondary);
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 160px;
  overflow: auto;
}

.trace-response {
  margin-bottom: 0;
  max-height: 220px;
}
</style>
