<!-- CustomNode.vue -->
<template>
  <div v-bind="$attrs">
    <div
      class="custom-node-header"
      data="description_display"
      @contextmenu="onTitleClick"
      @click.stop
    >
      <div>
        <div v-if="!editMode" class="description-display" :style="descriptionTextStyle" @click.stop>
          <div class="edit-icon" title="Edit description" @click.stop="toggleEditMode(true)">
            <svg
              width="12"
              height="12"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
            </svg>
          </div>
          <pre class="description-text">{{ descriptionSummary }}</pre>
          <span v-if="isTruncated" class="truncated-indicator" title="Click to see full description"
            >...</span
          >
        </div>
        <div
          v-else
          :id="props.data.id.toLocaleString()"
          class="custom-node-header"
          :style="overlayStyle"
          data="description_input"
          @click.stop
        >
          <textarea
            :id="props.data.id.toLocaleString()"
            v-model="description"
            class="description-input"
            data="description_input"
            @blur="toggleEditMode(false)"
            @click.stop
          ></textarea>
        </div>
      </div>
    </div>
    <div ref="nodeEl" class="custom-node" @contextmenu.prevent="showContextMenu">
      <!-- Use GenericNode if nodeTemplate exists, otherwise use the component directly -->
      <generic-node
        v-if="data.nodeTemplate"
        :node-id="data.id"
        :node-data="{ ...data.nodeTemplate, id: data.id, label: data.label }"
      />
      <component :is="data.component" v-else-if="data.component" :node-id="data.id" />

      <!-- Handles are always rendered -->
      <div
        v-for="(input, index) in data.inputs"
        :key="input.id"
        class="handle-input"
        :style="getHandleStyle(index, data.inputs.length)"
      >
        <Handle :id="input.id" type="target" :position="input.position" />
      </div>
      <div
        v-for="(output, index) in data.outputs"
        :key="output.id"
        class="handle-output"
        :style="getHandleStyle(index, data.outputs.length)"
      >
        <Handle :id="output.id" type="source" :position="output.position" />
      </div>

      <!-- Teleport Context Menu to body -->
      <Teleport v-if="showMenu" to="body">
        <div ref="menuEl" class="context-menu" :style="contextMenuStyle">
          <div class="context-menu-item" @click="runNode">
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <polygon points="5 3 19 12 5 21 5 3"></polygon>
            </svg>
            <span>{{ isRunning ? 'Running...' : 'Run Now' }}</span>
          </div>
          <div class="context-menu-item" @click="toggleCache">
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"></path>
              <path d="M3 3v5h5"></path>
              <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"></path>
              <path d="M16 21h5v-5"></path>
            </svg>
            <span>{{ isCached ? 'Disable Cache' : 'Enable Cache' }}</span>
          </div>
          <div class="context-menu-divider"></div>
          <div class="context-menu-item" @click="copyNode">
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
            </svg>
            <span>Copy</span>
          </div>
          <div class="context-menu-item context-menu-item-danger" @click="deleteNode">
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path d="M3 6h18"></path>
              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"></path>
              <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
            </svg>
            <span>Delete</span>
          </div>
        </div>
      </Teleport>
    </div>
  </div>
</template>

<script setup lang="ts">
import { Handle } from "@vue-flow/core";
import { computed, ref, onMounted, nextTick, watch, onUnmounted } from "vue";
import { useNodeStore } from "../../stores/column-store";
import { useFlowStore } from "../../stores/flow-store";
import { VueFlowStore } from "@vue-flow/core";
import { NodeCopyValue } from "../../views/DesignerView/types";
import { toSnakeCase } from "../../views/DesignerView/utils";
import { useFlowExecution } from "../../composables/useFlowExecution";
import GenericNode from "./GenericNode.vue";
import type { NodeTemplate } from "../../types";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const nodeEl = ref<HTMLElement | null>(null);
const menuEl = ref<HTMLElement | null>(null);

// Use the flow execution composable with persistent polling
const { triggerNodeFetch, isPollingActive } = useFlowExecution(
  flowStore.flowId,
  { interval: 2000, enabled: true },
  {
    persistPolling: true,
    pollingKey: `node_wrapper_${flowStore.flowId}`,
  },
);

const mouseX = ref<number>(0);
const mouseY = ref<number>(0);
const editMode = ref<boolean>(false);
const showMenu = ref<boolean>(false);
const contextMenuX = ref<number>(0);
const contextMenuY = ref<number>(0);
const isCached = ref<boolean>(false);
const isRunning = ref<boolean>(false);

const CHAR_LIMIT = 100;

// Define the data structure
interface NodeData {
  id: number;
  label: string;
  component?: any; // Made optional since we might use nodeTemplate instead
  inputs: Array<{
    id: string;
    position: any;
  }>;
  outputs: Array<{
    id: string;
    position: any;
  }>;
  nodeTemplate?: NodeTemplate; // Optional NodeTemplate data
  nodeItem?: string; // Optional node item name for backward compatibility
}

const props = defineProps({
  data: {
    type: Object as () => NodeData,
    required: true,
  },
});

const onTitleClick = (event: MouseEvent) => {
  toggleEditMode(true);
  mouseX.value = event.clientX;
  mouseY.value = event.clientY;
};

const showContextMenu = async (event: MouseEvent) => {
  event.preventDefault();
  event.stopPropagation();

  if (editMode.value) {
    toggleEditMode(false);
  }

  contextMenuX.value = event.clientX;
  contextMenuY.value = event.clientY;
  showMenu.value = true;

  // Load the current cache state
  try {
    const nodeData = await nodeStore.getNodeData(props.data.id, true);
    if (nodeData && nodeData.setting_input) {
      isCached.value = nodeData.setting_input.cache_results || false;
    }
  } catch (error) {
    console.error("Error loading node data:", error);
  }

  setTimeout(() => {
    window.addEventListener("click", handleClickOutsideMenu);
  }, 0);

  nextTick(() => {
    updateMenuPosition();
  });
};

const updateMenuPosition = () => {
  if (!menuEl.value) return;

  const menuRect = menuEl.value.getBoundingClientRect();
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;

  let left = contextMenuX.value;
  let top = contextMenuY.value;

  if (left + menuRect.width > viewportWidth - 10) {
    left = viewportWidth - menuRect.width - 10;
  }

  if (top + menuRect.height > viewportHeight - 10) {
    top = viewportHeight - menuRect.height - 10;
  }

  contextMenuX.value = left;
  contextMenuY.value = top;
};

const handleClickOutsideMenu = (event: MouseEvent) => {
  if (menuEl.value && !menuEl.value.contains(event.target as Node)) {
    closeContextMenu();
  }
};

const closeContextMenu = () => {
  showMenu.value = false;
  window.removeEventListener("click", handleClickOutsideMenu);
};

const copyNode = () => {
  const nodeCopyValue: NodeCopyValue = {
    nodeIdToCopyFrom: props.data.id,
    type: props.data.nodeTemplate?.item || props.data.component?.__name || "unknown",
    label: props.data.label,
    description: description.value,
    numberOfInputs: props.data.inputs.length,
    numberOfOutputs: props.data.outputs.length,
    typeSnakeCase:
      props.data.nodeTemplate?.item || toSnakeCase(props.data.component?.__name || "unknown"),
    flowIdToCopyFrom: nodeStore.flow_id,
    multi: props.data.nodeTemplate?.multi,
    nodeTemplate: props.data.nodeTemplate,
  };
  localStorage.setItem("copiedNode", JSON.stringify(nodeCopyValue));
  localStorage.removeItem("copiedMultiNodes");
  closeContextMenu();
};

const deleteNode = () => {
  if (nodeStore.vueFlowInstance) {
    const vueFlow: VueFlowStore = nodeStore.vueFlowInstance;
    vueFlow.removeNodes(props.data.id.toLocaleString(), true);
  }
  closeContextMenu();
};

const runNode = async () => {
  closeContextMenu();

  if (isPollingActive(`node_${props.data.id}`)) {
    return;
  }

  isRunning.value = true;
  try {
    await triggerNodeFetch(props.data.id);

    const checkInterval = setInterval(() => {
      if (!isPollingActive(`node_${props.data.id}`)) {
        clearInterval(checkInterval);
        isRunning.value = false;
      }
    }, 1000);

    setTimeout(() => {
      clearInterval(checkInterval);
      isRunning.value = false;
    }, 60000);
  } catch (error) {
    console.error("Error running node:", error);
    isRunning.value = false;
  }
};

const toggleCache = async () => {
  try {
    const nodeData = await nodeStore.getNodeData(props.data.id, false);
    if (nodeData && nodeData.setting_input) {
      const newCacheValue = !nodeData.setting_input.cache_results;
      nodeData.setting_input.cache_results = newCacheValue;
      isCached.value = newCacheValue;
      await nodeStore.updateSettingsDirectly(nodeData.setting_input);
    }
  } catch (error) {
    console.error("Error toggling cache:", error);
  }
  closeContextMenu();
};

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutsideMenu);
  window.removeEventListener("resize", updateMenuPosition);
  window.removeEventListener("keydown", handleKeyDown);
});

const contextMenuStyle = computed(() => {
  return {
    position: "fixed" as const,
    zIndex: 10000,
    top: `${contextMenuY.value}px`,
    left: `${contextMenuX.value}px`,
  };
});

const descriptionTextStyle = computed(() => {
  const textLength = description.value.length;
  let minWidth = "200px";

  if (textLength < 20) {
    minWidth = "100px";
  } else if (textLength < 30) {
    minWidth = "150px";
  }
  return {
    minWidth: minWidth,
  };
});

const handleKeyDown = (event: KeyboardEvent) => {
  // Normalize key to lowercase to handle Caps Lock being on
  const key = event.key.toLowerCase();
  if ((event.metaKey || event.ctrlKey) && key === "c") {
    // Check if text is selected - if so, let browser handle copy natively
    const selection = window.getSelection();
    const hasTextSelected = selection && selection.toString().trim().length > 0;
    if (hasTextSelected) {
      return; // Let browser handle text copying
    }

    const isNodeSelected = nodeStore.node_id === props.data.id;
    const target = event.target as HTMLElement;
    const isTargetNodeButton = target.classList.contains("node-button");
    if (isNodeSelected && isTargetNodeButton) {
      copyNode();
      event.preventDefault();
    }
  }
};

const handleClickOutside = (event: MouseEvent) => {
  const target = event.target as HTMLElement;
  const target_data = target.getAttribute("data");

  if (
    (target_data == "description_display" || target_data == "description_input") &&
    target.id == props.data.id.toLocaleString()
  ) {
    return;
  } else if (editMode.value) {
    toggleEditMode(false);
  }
};

const toggleEditMode = (state: boolean) => {
  editMode.value = state;
  if (state) {
    window.addEventListener("click", handleClickOutside);
  }
  if (!state) {
    window.removeEventListener("click", handleClickOutside);
    nodeStore.setNodeDescription(props.data.id, description.value);
  }
};

const description = ref<string>("");

const getNodeDescription = async () => {
  description.value = await nodeStore.getNodeDescription(props.data.id);
};

const overlayStyle = computed(() => {
  const overlayWidth = 400;
  const overlayHeight = 200;
  const buffer = 100;

  let left = mouseX.value + buffer;
  let top = mouseY.value + buffer;

  if (left + overlayWidth > window.innerWidth) {
    left -= overlayWidth + 2 * buffer;
  }

  if (top + overlayHeight > window.innerHeight) {
    top -= overlayHeight + 2 * buffer;
  }

  left = Math.max(left, buffer);
  top = Math.max(top, buffer);

  return {
    top: `${top}px`,
    left: `${left}px`,
  };
});

const isTruncated = computed(() => {
  try {
    return description.value.length > CHAR_LIMIT;
  } catch (error) {
    return false;
  }
});

const descriptionSummary = computed(() => {
  if (!description.value) {
    return `${props.data.id}: ${props.data.label}`;
  }

  if (isTruncated.value) {
    const truncatePoint = description.value.lastIndexOf(" ", CHAR_LIMIT);
    const endPoint = truncatePoint > 0 ? truncatePoint : CHAR_LIMIT;
    return description.value.substring(0, endPoint);
  }

  return description.value;
});

function getHandleStyle(index: number, total: number) {
  const topMargin = 30;
  const bottomMargin = 25;
  if (total === 1) {
    return {
      top: "55%",
      transform: "translateY(-55%)",
    };
  } else {
    const spacing = (100 - topMargin - bottomMargin) / (total - 1);
    return {
      top: `${topMargin + spacing * index}%`,
    };
  }
}

onMounted(async () => {
  await nextTick();
  await getNodeDescription();

  window.addEventListener("resize", () => {
    if (showMenu.value) {
      updateMenuPosition();
    }
  });

  window.addEventListener("keydown", handleKeyDown);

  watch(
    () => {
      const flowId = nodeStore.flow_id;
      const nodeId = props.data.id;
      return nodeStore.nodeDescriptions[flowId]?.[nodeId];
    },
    (newDescription) => {
      if (newDescription !== undefined) {
        description.value = newDescription;
      }
    },
  );
});
</script>

<style scoped>
.custom-node {
  border-radius: 4px;
  padding: 1px;
  background-color: var(--color-background-primary);
  display: flex;
  flex-direction: column;
  align-items: center;
  position: relative;
}

.selected {
  border: 2px solid #409eff;
}

.custom-node-header {
  font-weight: 100;
  font-size: small;
  width: 20px;
  white-space: nowrap;
  overflow: visible;
  text-overflow: ellipsis;
  font-family: var(--font-family-base);
}

.description-display {
  position: relative;
  white-space: normal;
  min-width: 100px;
  max-width: 300px;
  width: auto;
  padding: 2px 4px;
  cursor: pointer;
  background-color: var(--color-background-secondary);
  font-family: var(--font-family-base);
  display: flex;
  align-items: flex-start;
  gap: 4px;
  border-radius: 4px;
  color: var(--color-text-primary);
}

.edit-icon {
  opacity: 0;
  transition: opacity 0.2s;
  color: var(--color-accent);
  cursor: pointer;
  display: flex;
  align-items: center;
  padding: 2px;
}

.description-display:hover .edit-icon {
  opacity: 1;
}

.edit-icon:hover {
  color: var(--color-accent-hover);
}

.description-text {
  margin: 0;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-family: var(--font-family-base);
  font-size: var(--font-size-xs);
}

.edit-overlay {
  position: fixed;
  z-index: 1000;
  background: var(--color-background-primary);
  border-radius: 4px;
  box-shadow: var(--shadow-lg);
}

.description-input {
  width: 200px;
  height: 75px;
  resize: both;
  padding: 4px;
  border: 1px solid var(--color-accent);
  border-radius: 4px;
  font-size: small;
  font-family: var(--font-family-base);
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}

.handle-input {
  position: absolute;
  left: -8px;
}

.handle-output {
  position: absolute;
  right: -8px;
}

.context-menu {
  position: fixed;
  z-index: var(--z-index-canvas-context-menu, 100002);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  box-shadow: var(--shadow-lg);
  padding: 4px 0;
  min-width: 120px;
  font-family: var(--font-family-base);
}

.context-menu-item {
  padding: 8px 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-size: 13px;
  transition: background-color 0.2s;
  font-family: var(--font-family-base);
  color: var(--color-text-primary);
}

.context-menu-item:hover {
  background-color: var(--color-background-hover);
}

.context-menu-item svg {
  color: var(--color-text-secondary);
}

.context-menu-item span {
  font-family: var(--font-family-base);
}

.context-menu-divider {
  height: 1px;
  background-color: var(--color-border-primary);
  margin: 4px 0;
}

.context-menu-item-danger {
  color: #dc3545;
}

.context-menu-item-danger:hover {
  background-color: rgba(220, 53, 69, 0.1);
}

.context-menu-item-danger svg {
  color: #dc3545;
}
</style>
