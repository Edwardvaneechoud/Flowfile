<script setup lang="ts">
import { nextTick, ref, watch } from "vue";
import { useLogStream } from "./useLogStream";

const container = ref<HTMLElement | null>(null);
const autoScroll = ref(true);
const { logs, connectionStatus, errorMessage, startStreamingLogs, stopStreamingLogs, clearLogs } =
  useLogStream();

const scrollToBottom = () => {
  if (!autoScroll.value) return;
  nextTick(() => {
    requestAnimationFrame(() => {
      const el = container.value;
      if (el) el.scrollTop = el.scrollHeight;
    });
  });
};

const handleScroll = (event: Event) => {
  const element = event.target as HTMLElement;
  autoScroll.value = element.scrollHeight - element.scrollTop <= element.clientHeight + 50;
};

defineExpose({ startStreamingLogs, stopStreamingLogs, clearLogs, logs });

const logLines = ref<string[]>([]);
watch(logs, (newLogs) => {
  logLines.value = newLogs
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
  scrollToBottom();
});

const isErrorLine = (line: string): boolean => {
  return / - ERROR - /.test(line);
};

const isWarningLine = (line: string): boolean => {
  return / - WARNING - /.test(line);
};
</script>

<template>
  <div ref="container" class="log-container" @scroll="handleScroll">
    <div class="log-header">
      <div class="log-status">
        <span
          :class="[
            'status-indicator',
            {
              active: connectionStatus === 'connected',
              error: connectionStatus === 'error',
            },
          ]"
        ></span>
        {{
          connectionStatus === "connected"
            ? "Connected"
            : connectionStatus === "error"
              ? "Connection Error"
              : "Disconnected"
        }}
      </div>
      <div class="log-controls">
        <el-button size="small" @click="startStreamingLogs()">Fetch logs</el-button>
        <el-button size="small" :disabled="!logs || autoScroll" @click="scrollToBottom">
          Scroll to Bottom
        </el-button>
        <el-button size="small" type="danger" @click="clearLogs"> Clear </el-button>
      </div>
    </div>

    <div v-if="errorMessage" class="error-banner">
      {{ errorMessage }}
    </div>

    <div v-if="logLines.length === 0 && !errorMessage" class="empty-state">
      No logs available. Start running your flow to see logs appear here.
    </div>

    <div class="logs" :class="{ 'auto-scroll': autoScroll }">
      <div
        v-for="(line, index) in logLines"
        :key="index"
        :class="{ 'error-line': isErrorLine(line), 'warning-line': isWarningLine(line) }"
      >
        {{ line }}
      </div>
    </div>
  </div>
</template>

<style scoped>
.log-container {
  height: 100%;
  display: flex;
  flex-direction: column;
  background-color: var(--color-code-bg);
  color: var(--color-code-text);
  font-family: var(--font-family-mono);
  overflow-y: auto;
}

.log-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px;
  background-color: var(--color-gray-800);
  border-bottom: 1px solid var(--color-border-primary);
  opacity: 0.95;
  position: sticky;
  top: 0;
  z-index: 10; /* Ensures it stays above the logs */
}

.log-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9em;
}

.status-indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: var(--color-gray-500);
}

.status-indicator.active {
  background-color: var(--color-success);
}

.status-indicator.error {
  background-color: var(--color-danger);
}

.log-controls {
  display: flex;
  gap: 8px;
}

.error-banner {
  padding: 8px 12px;
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  font-size: 0.9em;
  border-bottom: 1px solid var(--color-danger);
}

.empty-state {
  padding: 16px;
  text-align: center;
  color: var(--color-text-muted);
  font-style: italic;
}

.logs {
  flex-grow: 1;
  margin: 0;
  padding: 8px;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-size: 0.9em;
  line-height: 1.8; /* Reduced line height */
  font-size: small;
}

.logs.auto-scroll {
  scroll-behavior: smooth;
}

::-webkit-scrollbar {
  width: 12px;
}

::-webkit-scrollbar-track {
  background: var(--color-code-bg);
}

::-webkit-scrollbar-thumb {
  background: var(--color-gray-600);
  border-radius: 6px;
  border: 3px solid var(--color-code-bg);
}

::-webkit-scrollbar-thumb:hover {
  background: var(--color-gray-500);
}

.error-line {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.warning-line {
  background-color: var(--color-warning-light);
  color: var(--color-warning-dark);
}
</style>
