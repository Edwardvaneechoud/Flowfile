<template>
  <div v-bind="$attrs">
    <div class="custom-node-header" data="description_display" @contextmenu="onTitleClick">
      <div>
        <div v-if="!editMode" class="description-display" :style="descriptionTextStyle">
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
        >
          <textarea
            :id="props.data.id.toLocaleString()"
            v-model="description"
            class="description-input"
            data="description_input"
            @blur="toggleEditMode(false)"
          ></textarea>
        </div>
      </div>
    </div>
    <div class="custom-node" ref="nodeEl" @contextmenu.prevent="showContextMenu">
      <component :is="data.component" :node-id="data.id" />
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
      <Teleport to="body" v-if="showMenu">
        <div class="context-menu" :style="contextMenuStyle" ref="menuEl">
          <div class="context-menu-item" @click="copyNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
            </svg>
            <span>Copy</span>
          </div>
          <div class="context-menu-item" @click="deleteNode">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"></path>
              <rect x="8" y="2" width="8" height="4" rx="1" ry="1"></rect>
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
import { computed, ref, defineProps, onMounted, nextTick, watch, onUnmounted } from "vue";
import { useNodeStore } from "../../../../stores/column-store";
import { VueFlowStore } from '@vue-flow/core';
import { NodeCopyInput, NodeCopyValue } from './types'
import {toSnakeCase} from './utils'


const nodeStore = useNodeStore();
const nodeEl = ref<HTMLElement | null>(null);
const menuEl = ref<HTMLElement | null>(null);

const mouseX = ref<number>(0);
const mouseY = ref<number>(0);
const editMode = ref<boolean>(false);
const showMenu = ref<boolean>(false);
const contextMenuX = ref<number>(0);
const contextMenuY = ref<number>(0);

const CHAR_LIMIT = 100;

const onTitleClick = (event: MouseEvent) => {
  toggleEditMode(true);
  mouseX.value = event.clientX;
  mouseY.value = event.clientY;
};

const showContextMenu = (event: MouseEvent) => {
  event.preventDefault();
  event.stopPropagation();
  
  // Close any existing edit mode
  if (editMode.value) {
    toggleEditMode(false);
  }
  
  // Store the click position
  contextMenuX.value = event.clientX;
  contextMenuY.value = event.clientY;
  
  // Show the menu
  showMenu.value = true;
  
  // Setup click outside handler after a brief delay
  setTimeout(() => {
    window.addEventListener('click', handleClickOutsideMenu);
  }, 0);
  
  // Update position after the menu is rendered
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
  
  // Adjust if menu would go off-screen
  if (left + menuRect.width > viewportWidth - 10) {
    left = viewportWidth - menuRect.width - 10;
  }
  
  if (top + menuRect.height > viewportHeight - 10) {
    top = viewportHeight - menuRect.height - 10;
  }
  
  // Update position
  contextMenuX.value = left;
  contextMenuY.value = top;
};

const handleClickOutsideMenu = (event: MouseEvent) => {
  // Close the menu if click is outside
  if (menuEl.value && !menuEl.value.contains(event.target as Node)) {
    closeContextMenu();
  }
};

const closeContextMenu = () => {
  showMenu.value = false;
  window.removeEventListener('click', handleClickOutsideMenu);
};



const copyNode = () => {
  // Store the node data in localStorage
  const nodeCopyValue: NodeCopyValue = {
    id: props.data.id,
    type: props.data.component.__name || 'unknown',
    label: props.data.label,
    description: description.value,
    numberOfInputs: props.data.inputs.length,
    numberOfOutputs: props.data.outputs.length,
    typeSnakeCase: toSnakeCase(props.data.component.__name || 'unknown')
  };
  localStorage.setItem('copiedNode', JSON.stringify(nodeCopyValue));
  
  console.log('Node copied:', nodeCopyValue);
  closeContextMenu();
};

const deleteNode = () => {
  // Implementation will be added in the next iteration
  console.log('Paste node functionality will be implemented in the next step');
  if (nodeStore.vueFlowInstance) {
    let vueFlow: VueFlowStore = nodeStore.vueFlowInstance
    vueFlow.removeNodes(props.data.id.toLocaleString(), true)
  }
  
  closeContextMenu();
}


onUnmounted(() => {
  window.removeEventListener('click', handleClickOutsideMenu);
  window.removeEventListener('resize', updateMenuPosition);
});

const contextMenuStyle = computed(() => {
  return {
    position: 'fixed' as const,
    zIndex: 10000,
    top: `${contextMenuY.value}px`,
    left: `${contextMenuX.value}px`,
  };
});

const descriptionTextStyle = computed(() => {
  const textLength = description.value.length;
  let minWidth = "200px"; // default

  if (textLength < 20) {
    minWidth = "100px";
  } else if (textLength < 30) {
    minWidth = "150px";
  }
  return {
    minWidth: minWidth,
  };
});

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
  return description.value.length > CHAR_LIMIT;
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

const props = defineProps({
  data: {
    type: Object,
    required: true,
  },
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
  
  // Listen for window resize to update context menu position if it's open
  window.addEventListener('resize', () => {
    if (showMenu.value) {
      updateMenuPosition();
    }
  });

  watch(
    () => {
      const flowId = nodeStore.flow_id; // Get the current flow ID
      const nodeId = props.data.id; // Get the node ID

      // Access the nested description
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
  background-color: white;
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
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.description-display {
  position: relative;
  white-space: normal;
  min-width: 100px; /* Default minimum width */
  max-width: 300px;
  width: auto; /* Allow dynamic width */
  padding: 2px 4px;
  cursor: pointer;
  background-color: rgba(185, 185, 185, 0.117);
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
  display: flex;
  align-items: flex-start;
  gap: 4px;
  border-radius: 4px;
}

.edit-icon {
  opacity: 0;
  transition: opacity 0.2s;
  color: #0f275f;
  cursor: pointer;
  display: flex;
  align-items: center;
  padding: 2px;
}

.description-display:hover .edit-icon {
  opacity: 1;
}

.edit-icon:hover {
  color: #051233;
}

.description-text {
  margin: 0;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.edit-overlay {
  position: fixed;
  z-index: 1000;
  background: white;
  border-radius: 4px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.description-input {
  width: 200px;
  height: 75px;
  resize: both;
  padding: 4px;
  border: 1px solid #0f275f;
  border-radius: 4px;
  font-size: small;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
  background-color: white;
}

.handle-input {
  position: absolute;
  left: -8px;
}

.handle-output {
  position: absolute;
  right: -8px;
}

/* Context Menu Styles */
.context-menu {
  position: fixed;
  z-index: 10000;
  background-color: white;
  border-radius: 4px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  padding: 4px 0;
  min-width: 120px;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.context-menu-item {
  padding: 8px 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-size: 13px;
  transition: background-color 0.2s;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.context-menu-item:hover {
  background-color: #f5f5f5;
}

.context-menu-item svg {
  color: #555;
}

.context-menu-item span {
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}
</style>