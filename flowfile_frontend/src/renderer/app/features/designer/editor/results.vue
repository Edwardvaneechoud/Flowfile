<template>
  <el-card class="run-card" shadow="hover">
    <div class="clearfix">
      <span>Flow: {{ runInformation?.flow_id }}:</span>
      <span class="flow-summary" :class="runStatusClass">
        {{ runStatusText
        }}<template v-if="hasRun"
          >, Nodes: {{ runInformation?.nodes_completed }}/{{
            runInformation?.number_of_nodes
          }}</template
        >
      </span>
    </div>

    <!-- Performance-mode notice: per-step data isn't stored in Performance mode -->
    <div v-if="showPerfNotice" class="perf-mode-notice">
      <el-icon class="perf-mode-notice__icon"><InfoFilled /></el-icon>
      <span class="perf-mode-notice__text">
        This flow ran in <strong>Performance</strong> mode, so data for each step isn't stored. To
        inspect the data step by step, select Development as
        <button type="button" class="perf-mode-notice__link" @click="openFlowSettings">
          execution mode
        </button>
        and run again.
      </span>
      <button
        class="perf-mode-notice__dismiss"
        aria-label="Dismiss notice"
        @click="perfNoticeDismissed = true"
      >
        ×
      </button>
    </div>
    <br />
    <div>
      <el-timeline>
        <el-timeline-item
          v-for="node in runInformation?.node_step_result"
          :key="node.node_id"
          :timestamp="formatTimestamp(node.start_timestamp)"
          :color="calculateColor(node.success)"
          @click="navigateToNode(`node-${node.node_id}`)"
        >
          <el-card class="node-card">
            <div
              v-if="nodeStore.nodeDescriptions[nodeStore.flow_id]?.[node.node_id]?.description"
              class="node-info"
            >
              <h4 class="node-title">
                {{ nodeStore.nodeDescriptions[nodeStore.flow_id][node.node_id].description }}
              </h4>
              <p class="node-description">node type: {{ node.node_name }}</p>
            </div>
            <h4 v-else>{{ `Node ${node.node_id}` }}: {{ node.node_name }}</h4>
            <div class="node-details">
              <p>
                Duration:
                {{ formatRunTime(node.run_time_ms, node.start_timestamp, node.is_running) }}
              </p>
              <p>
                Status:
                <span
                  :class="{
                    running: node.success === null,
                    success: node.success === true,
                    failure: node.success === false,
                  }"
                >
                  {{ node.success === null ? "Running" : node.success ? "Success" : "Failure" }}
                </span>
              </p>
              <p v-if="node.success === false" class="failure">Error: {{ node.error }}</p>
              <el-button
                v-if="node.success === false && node.error"
                size="small"
                type="primary"
                class="fix-with-ai-btn"
                :loading="aiStore.isStreaming"
                @click.stop="onFixWithAi(node)"
              >
                <span class="ai-icon">✨</span>
                Fix with AI
              </el-button>
            </div>
          </el-card>
        </el-timeline-item>
      </el-timeline>
    </div>
  </el-card>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import { format } from "date-fns";
import { InfoFilled } from "@element-plus/icons-vue";
import { useNodeStore } from "../../../stores/column-store";
import { useAiStore } from "../../../stores/ai-store";
import { useEditorStore } from "../../../stores/editor-store";
import { useDrawerStore } from "../../../stores/drawer-store";

interface RunNode {
  node_id: number;
  node_name?: string;
  error?: string;
  success?: boolean;
}

const nodeStore = useNodeStore();
const aiStore = useAiStore();
const editorStore = useEditorStore();
const drawerStore = useDrawerStore();
const runInformation = computed(() => nodeStore.currentRunResult);
const selectedNode = ref<Element | null>(null);

const openFlowSettings = () => editorStore.requestOpenFlowSettings();

// Flow status. The backend leaves `success` null while the flow is running
// and sets `is_running`, so we never render a running flow as "Failed". A
// flow that never ran reports `run_type: "init"` — render that as "Not run
// yet" instead of "Failed", and skip the meaningless 0/0 node count.
const hasRun = computed(() => {
  const info = runInformation.value;
  return !!info && info.run_type !== "init";
});

const runStatusText = computed(() => {
  const info = runInformation.value;
  if (!info) return "";
  if (info.is_running) return "Running";
  if (!hasRun.value) return "No results yet";
  return info.success ? "Succeeded" : "Failed";
});

const runStatusClass = computed(() => ({
  running: !!runInformation.value?.is_running,
  success: !runInformation.value?.is_running && runInformation.value?.success === true,
  failure: !runInformation.value?.is_running && runInformation.value?.success === false,
}));

// Performance mode skips per-step example data, so after a successful run we
// nudge the user toward Development mode to inspect step-by-step data.
const perfNoticeDismissed = ref(false);
const showPerfNotice = computed(
  () =>
    !perfNoticeDismissed.value &&
    runInformation.value?.success === true &&
    runInformation.value?.execution_mode === "Performance",
);

// Reset the dismissal whenever a new run starts so the notice can reappear.
watch(
  () => runInformation.value?.is_running,
  (isRunning) => {
    if (isRunning) perfNoticeDismissed.value = false;
  },
);

const onFixWithAi = (node: RunNode) => {
  // — open the AI drawer and stream a server-built explanation +
  // suggested fix. The backend assembles the schema-grounded prompt
  // from ``flow_id`` + ``node_id`` so we don't have to ship the upstream
  // schema across the wire ourselves.
  void aiStore.explainRunFailure(
    nodeStore.flow_id,
    node.node_id,
    node.error ?? "",
    node.node_name ?? `node-${node.node_id}`,
  );
};

const formatTimestamp = (timestamp: number) => {
  return format(new Date(timestamp * 1000), "yyyy-MM-dd HH:mm:ss");
};

const calculateColor = (success: boolean | undefined) => {
  if (success === null) return "var(--color-info)";
  return success ? "var(--color-success)" : "var(--color-danger)";
};
const formatRunTime = (runTimeMs: number, startTimestamp: number, isRunning: boolean) => {
  let ms = runTimeMs;
  if (isRunning && startTimestamp > 0) {
    ms = Date.now() - startTimestamp * 1000;
  }
  if (ms < 0) return "Not started";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const totalSeconds = ms / 1000;
  if (totalSeconds < 60) return `${Math.round(totalSeconds)} seconds`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = Math.round(totalSeconds % 60);
  return seconds > 0 ? `${minutes} minutes, ${seconds} seconds` : `${minutes} minutes`;
};

const navigateToNode = (nodeId: string) => {
  console.log(nodeId, "nodeId");
  if (selectedNode.value) {
    if (selectedNode.value?.classList.contains("selected")) {
      selectedNode.value.classList.remove("selected");
    }
  }

  const elementId = `#${nodeId}`;
  const nodeComponent = document.querySelector(elementId);
  drawerStore.selectNodeForPreview(Number(nodeId.slice(5)));
  if (nodeComponent) {
    if (!nodeComponent.classList.contains("selected")) {
      nodeComponent.classList.add("selected");
    }

    const button = nodeComponent.querySelector(".el-button");

    if (button) {
      let event = new MouseEvent("click", {
        bubbles: true,
        cancelable: true,
        view: window,
      });
      event.preventDefault();
      button.dispatchEvent(event);
      selectedNode.value = nodeComponent;
    }
  }
};
</script>

<style scoped>
.hide-results-button {
  margin-bottom: 10px;
}
.flow-summary {
  margin-left: 10px;
  font-weight: bold;
  color: var(--color-text-primary);
}
.perf-mode-notice {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin-top: 12px;
  padding: 10px 12px;
  border: 1px solid var(--color-info, #909399);
  border-radius: 8px;
  background-color: var(--color-info-light-9, rgba(144, 147, 153, 0.1));
  font-size: 13px;
  line-height: 1.4;
  color: var(--color-text-primary);
}
.perf-mode-notice__icon {
  flex-shrink: 0;
  font-size: 16px;
  margin-top: 1px;
  color: var(--color-text-secondary);
}
.perf-mode-notice__text {
  flex: 1;
}
.perf-mode-notice__link {
  padding: 0;
  border: none;
  background: none;
  font: inherit;
  font-weight: 600;
  color: var(--color-accent, var(--el-color-primary));
  cursor: pointer;
  text-decoration: underline;
  text-underline-offset: 2px;
}
.perf-mode-notice__link:hover {
  opacity: 0.8;
}
.perf-mode-notice__dismiss {
  flex-shrink: 0;
  border: none;
  background: transparent;
  font-size: 16px;
  line-height: 1;
  cursor: pointer;
  color: var(--color-text-secondary);
  padding: 0 2px;
}
.perf-mode-notice__dismiss:hover {
  color: var(--color-text-primary);
}
.node-card {
  padding: 15px;
}
.node-details p {
  margin: 5px 0;
}
.success {
  color: var(--color-success);
}
.failure {
  color: var(--color-danger);
}
.fix-with-ai-btn {
  margin-top: 6px;
}
.ai-icon {
  margin-right: 4px;
}
.running {
  animation: pulse 1.5s infinite;
}

@keyframes pulse {
  0% {
    opacity: 1;
  }
  50% {
    opacity: 0.4;
  }
  100% {
    opacity: 1;
  }
}

.node-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.node-title {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.node-description {
  margin: 0;
  font-size: 14px;
  color: var(--color-text-secondary);
  line-height: 1.4;
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-box-orient: vertical;
}
</style>
