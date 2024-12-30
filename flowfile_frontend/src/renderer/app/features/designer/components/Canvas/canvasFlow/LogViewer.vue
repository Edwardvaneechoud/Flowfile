// LogViewer.vue
<script setup lang="ts">
import { ref, onUnmounted, nextTick, onMounted, watch, stop } from 'vue';
import { useNodeStore } from '../../../../../stores/column-store';
import { flowfileCorebaseURL } from '../../../../../../config/constants';

// Props with TypeScript interface
interface Props {
  flowId: number;
}
const nodeStore = useNodeStore();

const props = defineProps<Props>();

// Store and refs
const logs = ref<string>('');
const eventSourceRef = ref<EventSource | null>(null);
const autoScroll = ref(true);

// Function to handle auto-scrolling
const scrollToBottom = () => {
  if (!autoScroll.value) return;
  
  const container = document.querySelector('.log-container');
  if (container) {
    container.scrollTop = container.scrollHeight;
  }
};


watch(() => nodeStore.isRunning, (newVal) => {
  if (newVal) {
    startStreamingLogs();
  } else {
    stopStreamingLogs();
  }
});

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));


const initialLogViewer = async () => {

  if (nodeStore.isRunning) {
    startStreamingLogs();
  }
  else {
    startStreamingLogs();
    await sleep(2000);
    stopStreamingLogs();
  }
}
const startStreamingLogs = () => {
  if (eventSourceRef.value) {
    eventSourceRef.value.close();
  }

  console.log('Starting log streaming');
  logs.value = '';
  
  const eventSource = new EventSource(`${flowfileCorebaseURL}logs/${props.flowId}`);
  eventSourceRef.value = eventSource;
  console.log('Event source created');
  let hasReceivedMessage = false;

  eventSource.onopen = () => {
  };

  eventSource.onmessage = (event) => {
    hasReceivedMessage = true;
    try {
      const logData = JSON.parse(event.data);
      logs.value += logData + '\n';
      nextTick(() => {
        scrollToBottom();
      });
    } catch (error) {
    }
  };

  eventSource.onerror = (error) => {
    // If we never received a message, the endpoint might not be ready yet
    if (!hasReceivedMessage && nodeStore.isRunning) {
      console.log("Connection failed - endpoint might not be ready yet. Retrying...");
      stopStreamingLogs();
      // Retry after a short delay
      setTimeout(startStreamingLogs, 1000);
      return;
    }

    // If we had messages but now got an error, the endpoint probably closed
    if (hasReceivedMessage) {
      console.log("Log connection closed after receiving messages");
    } else {
      console.log("Log connection failed to establish");
    }
    
    stopStreamingLogs();
  };
};

const stopStreamingLogs = () => {
  if (eventSourceRef.value) {
    eventSourceRef.value.close();
    eventSourceRef.value = null;
    console.log("Event source closed");
  }
};

// Handle scroll events to toggle auto-scroll
const handleScroll = (event: Event) => {
  const element = event.target as HTMLElement;
  const isAtBottom = element.scrollHeight - element.scrollTop <= element.clientHeight + 50;
  autoScroll.value = isAtBottom;
};

// Clear logs
const clearLogs = () => {
  logs.value = '';
};

// Cleanup on component unmount
onUnmounted(() => {
  if (eventSourceRef.value) {
    eventSourceRef.value.close();
  }
});

onMounted(() => {
  initialLogViewer();
});

// Expose methods and properties to parent
defineExpose({
  startStreamingLogs,
  stopStreamingLogs,
  clearLogs,
  logs
});
</script>

<template>
  <div class="log-container" @scroll="handleScroll">
    <div class="log-header">
      <div class="log-status">
        <span v-if="eventSourceRef" class="status-indicator active"></span>
        <span v-else class="status-indicator"></span>
        {{ eventSourceRef ? 'Connected' : 'Disconnected' }}
      </div>
      <div class="log-controls">
        <el-button 
          size="small" 
          @click="initialLogViewer"
          >Fetch logs</el-button>
        <el-button 
          size="small" 
          @click="scrollToBottom"
          :disabled="!logs || autoScroll"
        >
          Scroll to Bottom
        </el-button>
      </div>
    </div>
    <pre class="logs" :class="{ 'auto-scroll': autoScroll }">{{ logs }}</pre>
  </div>
</template>

<style scoped>
.log-container {
  height: 100%;
  display: flex;
  flex-direction: column;
  background-color: #1e1e1e;
  color: #d4d4d4;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
}

.log-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px;
  background-color: #252526;
  border-bottom: 1px solid #333;
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
  background-color: #4CAF50;
}

.log-controls {
  display: flex;
  gap: 8px;
}

.logs {
  flex-grow: 1;
  margin: 0;
  padding: 8px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-size: 0.9em;
  line-height: 1.4;
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
</style>
