<script setup lang="ts">
import { ref, onUnmounted, nextTick, onMounted, watch } from "vue";
import { useNodeStore } from "../../../stores/column-store";
import { flowfileCorebaseURL } from "../../../../config/constants";
import authService from "../../../services/auth.service";

// Store & Refs
const nodeStore = useNodeStore();
const logs = ref<string>("");
const eventSourceRef = ref<EventSource | null>(null);
const autoScroll = ref(true);
const connectionRetries = ref(0);
const maxRetries = 5;
const connectionStatus = ref<"connected" | "disconnected" | "error">("disconnected");
const errorMessage = ref<string | null>(null);

const scrollToBottom = () => {
  if (!autoScroll.value) return;
  nextTick(() => {
    requestAnimationFrame(() => {
      const container = document.querySelector(".log-container");
      if (container) container.scrollTop = container.scrollHeight;
    });
  });
};

watch(
  () => nodeStore.isRunning,
  (isRunning) => {
    isRunning ? startStreamingLogs() : stopStreamingLogs();
  },
);

const startStreamingLogs = async () => {
  if (eventSourceRef.value) eventSourceRef.value.close();

  logs.value = "";
  connectionRetries.value = 0;
  errorMessage.value = null;
  connectionStatus.value = "disconnected";

  try {
    // Get the auth token
    const token = await authService.getToken();
    if (!token) {
      console.error("No auth token available for log streaming");
      errorMessage.value = "Authentication failed. Please log in again.";
      connectionStatus.value = "error";
      return;
    }

    // Create URL with token as query parameter
    const url = new URL(`${flowfileCorebaseURL}logs/${nodeStore.flow_id}`);
    url.searchParams.append("access_token", token);

    const eventSource = new EventSource(url.toString());
    eventSourceRef.value = eventSource;

    let hasReceivedMessage = false;

    eventSource.onopen = () => {
      connectionStatus.value = "connected";
      console.log("Log connection established");
    };

    eventSource.onmessage = (event) => {
      hasReceivedMessage = true;
      try {
        logs.value += JSON.parse(event.data) + "\n";
        scrollToBottom();
      } catch (error) {
        console.error("Error parsing log data:", error);
      }
    };

    eventSource.onerror = async (error) => {
      console.error("EventSource error:", error);

      if (!hasReceivedMessage && nodeStore.isRunning) {
        if (connectionRetries.value < maxRetries) {
          connectionRetries.value++;
          errorMessage.value = `Connection failed. Retrying (${connectionRetries.value}/${maxRetries})...`;
          connectionStatus.value = "error";
          stopStreamingLogs();

          // Check if token is still valid, refresh if needed
          if (!authService.hasValidToken()) {
            await authService.getToken();
          }

          setTimeout(startStreamingLogs, 1000 * connectionRetries.value); // Exponential backoff
        } else {
          console.error("Max retries reached for log connection");
          errorMessage.value =
            "Failed to connect after multiple attempts. Try refreshing the page.";
          connectionStatus.value = "error";
          stopStreamingLogs();
        }
      } else {
        console.log("Log connection closed.");
        stopStreamingLogs();
      }
    };
  } catch (error) {
    console.error("Failed to start log streaming:", error);
    errorMessage.value = `Error: ${error instanceof Error ? error.message : "Unknown error"}`;
    connectionStatus.value = "error";
  }
};

const stopStreamingLogs = () => {
  eventSourceRef.value?.close();
  eventSourceRef.value = null;
  if (connectionStatus.value === "connected") {
    connectionStatus.value = "disconnected";
  }
};

// UI Handlers
const handleScroll = (event: Event) => {
  const element = event.target as HTMLElement;
  autoScroll.value = element.scrollHeight - element.scrollTop <= element.clientHeight + 50;
};

const clearLogs = () => (logs.value = "");

// Handle token expiration
let tokenRefreshInterval: number | null = null;

const setupTokenRefresh = () => {
  // Clear existing interval if any
  if (tokenRefreshInterval) {
    clearInterval(tokenRefreshInterval);
  }

  // Check token every 5 minutes
  tokenRefreshInterval = window.setInterval(
    async () => {
      if (eventSourceRef.value && !authService.hasValidToken()) {
        console.log("Token expired, reconnecting log stream");
        stopStreamingLogs();
        await authService.getToken();
        startStreamingLogs();
      }
    },
    5 * 60 * 1000,
  );
};

// Lifecycle Hooks
onMounted(() => {
  startStreamingLogs();
  setupTokenRefresh();
});

onUnmounted(() => {
  stopStreamingLogs();
  if (tokenRefreshInterval) {
    clearInterval(tokenRefreshInterval);
    tokenRefreshInterval = null;
  }
});

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
        <el-button size="small" @click="startStreamingLogs">Fetch logs</el-button>
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

.status-indicator.error {
  background-color: #f44336;
}

.log-controls {
  display: flex;
  gap: 8px;
}

.error-banner {
  padding: 8px 12px;
  background-color: rgba(244, 67, 54, 0.2);
  color: #f44336;
  font-size: 0.9em;
  border-bottom: 1px solid #f44336;
}

.empty-state {
  padding: 16px;
  text-align: center;
  color: #777;
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
}
</style>
