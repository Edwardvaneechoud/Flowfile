<script setup lang="ts">
import { ref, onUnmounted, nextTick, onMounted, watch } from "vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { flowfileCorebaseURL } from "../../../../../../config/constants";

// Props
interface Props {
  flowId: number;
}
const props = defineProps<Props>();

// Store & Refs
const nodeStore = useNodeStore();
const logs = ref<string>("");
const eventSourceRef = ref<EventSource | null>(null);
const autoScroll = ref(true);

// Scroll to bottom function
const scrollToBottom = () => {
  if (!autoScroll.value) return;
  nextTick(() => {
    requestAnimationFrame(() => {
      const container = document.querySelector(".log-container");
      if (container) container.scrollTop = container.scrollHeight;
    });
  });
};

// Watch for node state changes
watch(
  () => nodeStore.isRunning,
  (isRunning) => {
    isRunning ? startStreamingLogs() : stopStreamingLogs();
  },
);

// Start & Stop Log Streaming
const startStreamingLogs = () => {
  if (eventSourceRef.value) eventSourceRef.value.close();

  logs.value = "";
  console.log("Starting log streaming");
  const eventSource = new EventSource(`${flowfileCorebaseURL}logs/${props.flowId}`);
  eventSourceRef.value = eventSource;

  let hasReceivedMessage = false;

  eventSource.onmessage = (event) => {
    hasReceivedMessage = true;
    try {
      logs.value += JSON.parse(event.data) + "\n";
      scrollToBottom();
    } catch (error) {
      console.error("Error parsing log data:", error);
    }
  };

  eventSource.onerror = () => {
    if (!hasReceivedMessage && nodeStore.isRunning) {
      console.log("Retrying log connection...");
      stopStreamingLogs();
      setTimeout(startStreamingLogs, 1000);
    } else {
      console.log("Log connection closed.");
      stopStreamingLogs();
    }
  };
};

const stopStreamingLogs = () => {
  eventSourceRef.value?.close();
  eventSourceRef.value = null;
  console.log("Log streaming stopped");
};

// UI Handlers
const handleScroll = (event: Event) => {
  const element = event.target as HTMLElement;
  autoScroll.value = element.scrollHeight - element.scrollTop <= element.clientHeight + 50;
};

const clearLogs = () => (logs.value = "");

// Lifecycle Hooks
onMounted(startStreamingLogs);
onUnmounted(stopStreamingLogs);

// Expose functions to parent component
defineExpose({ startStreamingLogs, stopStreamingLogs, clearLogs, logs });

// Computed property to split logs into lines and identify errors
const logLines = ref<string[]>([]);
watch(logs, (newLogs) => {
  logLines.value = newLogs
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
});

const isErrorLine = (line: string): boolean => {
  return line.toUpperCase().includes("ERROR");
};
</script>

<template>
  <div class="log-container" @scroll="handleScroll">
    <div class="log-header">
      <div class="log-status">
        <span :class="['status-indicator', { active: eventSourceRef }]"></span>
        {{ eventSourceRef ? "Connected" : "Disconnected" }}
      </div>
      <div class="log-controls">
        <el-button size="small" @click="startStreamingLogs">Fetch logs</el-button>
        <el-button size="small" :disabled="!logs || autoScroll" @click="scrollToBottom">
          Scroll to Bottom
        </el-button>
      </div>
    </div>
    <div class="logs" :class="{ 'auto-scroll': autoScroll }">
      <div
        v-for="(line, index) in logLines"
        :key="index"
        :class="{ 'error-line': isErrorLine(line) }"
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
  background-color: #1e1e1e;
  color: #d4d4d4;
  font-family: "Consolas", "Monaco", "Courier New", monospace;
  overflow-y: auto;
}

.log-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px;
  background-color: #252526;
  border-bottom: 1px solid #333;
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
  background-color: #666;
}

.status-indicator.active {
  background-color: #4caf50;
}

.log-controls {
  display: flex;
  gap: 8px;
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
  background: #1e1e1e;
}

::-webkit-scrollbar-thumb {
  background: #424242;
  border-radius: 6px;
  border: 3px solid #1e1e1e;
}

::-webkit-scrollbar-thumb:hover {
  background: #4f4f4f;
}

.error-line {
  background-color: rgba(255, 0, 0, 0.2); /* Light red background */
  color: #ffcdd2; /* Light red text for better readability */
  /* You can also add a border or other visual cues */
}
</style>
