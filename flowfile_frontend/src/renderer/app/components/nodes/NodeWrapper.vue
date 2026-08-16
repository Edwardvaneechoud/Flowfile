<!-- CustomNode.vue -->
<template>
  <div v-bind="$attrs">
    <div class="custom-node-header" data="description_display" @dblclick="onTitleClick" @click.stop>
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
    <!-- Right-click bubbles to VueFlow's node handler → the canvas ContextMenu
         (Canvas.vue @node-context-menu). This component no longer owns a menu. -->
    <div ref="nodeEl" class="custom-node">
      <generic-node
        v-if="data.nodeTemplate"
        :node-id="data.id"
        :node-data="{ ...data.nodeTemplate, id: data.id, label: data.label }"
      />
      <component :is="data.component" v-else-if="data.component" :node-id="data.id" />

      <!-- Artifact badges (published/consumed indicators) -->
      <ArtifactBadge :node-id="data.id" />

      <!-- Handles are always rendered -->
      <div
        v-for="(input, index) in sideInputs"
        :key="input.id"
        class="handle-input"
        :style="getHandleStyle(index, sideInputs.length)"
      >
        <span v-if="input.label && sideInputs.length > 1" class="handle-label handle-label--input">
          {{ input.label }}
        </span>
        <!-- The title lives on the handle, not the label: .handle-label is
             pointer-events:none, so a title there would never surface. Handle
             drops fallthrough attrs, hence the directive. -->
        <Handle
          :id="input.id"
          v-native-title="input.title"
          type="target"
          :position="input.position"
        />
      </div>
      <!-- Fixed parameter-data handle (run_flow): bottom-center, subdued until hovered -->
      <div
        v-if="parameterInput"
        :key="parameterInput.id"
        class="handle-input--parameter"
        :title="parameterInput.title"
      >
        <Handle :id="parameterInput.id" type="target" :position="parameterInput.position" />
      </div>
      <div
        v-for="(output, index) in data.outputs"
        :key="output.id"
        class="handle-output"
        :style="getHandleStyle(index, data.outputs.length)"
      >
        <Handle
          :id="output.id"
          v-native-title="output.title"
          type="source"
          :position="output.position"
        />
        <span
          v-if="output.label && data.outputs.length > 1"
          class="handle-label handle-label--output"
        >
          {{ output.label }}
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// TODO(refactor): Plan to extract:
//   - NodeDescriptionEditor.vue (~lines 11-48)
//   - NodeHandles.vue: handle rendering loops (~lines 64-89)
// (The per-node context menu moved to the canvas ContextMenu —
//  Canvas.vue @node-context-menu + composables/useContextMenu.)
import { Handle } from "@vue-flow/core";
import { computed, ref, onMounted, nextTick, watch, onUnmounted } from "vue";
import { useNodeStore } from "../../stores/column-store";
import GenericNode from "./GenericNode.vue";
import ArtifactBadge from "./ArtifactBadge.vue";
import type { NodeTemplate, NodeHandle } from "../../types";

const nodeStore = useNodeStore();
const nodeEl = ref<HTMLElement | null>(null);

const mouseX = ref<number>(0);
const mouseY = ref<number>(0);
const editMode = ref<boolean>(false);

const CHAR_LIMIT = 100;

interface NodeData {
  id: number;
  label: string;
  component?: ReturnType<(typeof import("vue"))["defineComponent"]>;
  nodeReference?: string;
  inputs: NodeHandle[];
  outputs: NodeHandle[];
  nodeTemplate?: NodeTemplate;
  nodeItem?: string;
}

const props = defineProps({
  data: {
    type: Object as () => NodeData,
    required: true,
  },
});

// VueFlow's <Handle> does not inherit fallthrough attributes, so the native
// tooltip has to be written onto its root element directly.
function applyNativeTitle(el: HTMLElement, value?: string) {
  if (value) el.setAttribute("title", value);
  else el.removeAttribute("title");
}

const vNativeTitle = {
  mounted: (el: HTMLElement, binding: { value?: string }) => applyNativeTitle(el, binding.value),
  updated: (el: HTMLElement, binding: { value?: string }) => applyNativeTitle(el, binding.value),
};

// The parameter-data handle (run_flow) renders bottom-center; only real data
// inputs share the left edge spacing.
const sideInputs = computed(() => props.data.inputs.filter((input) => input.kind !== "parameter"));
const parameterInput = computed(() =>
  props.data.inputs.find((input) => input.kind === "parameter"),
);

const onTitleClick = (event: MouseEvent) => {
  toggleEditMode(true);
  mouseX.value = event.clientX;
  mouseY.value = event.clientY;
};

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

// Baseline captured on entering edit mode: leaving without a change must not
// POST — an untouched auto-generated description would otherwise be pinned as
// a user description and stop regenerating.
let descriptionAtEditStart = "";

const toggleEditMode = (state: boolean) => {
  if (state === editMode.value) return;
  editMode.value = state;
  if (state) {
    descriptionAtEditStart = description.value;
    window.addEventListener("click", handleClickOutside);
  } else {
    window.removeEventListener("click", handleClickOutside);
    if (description.value !== descriptionAtEditStart) {
      nodeStore.setNodeDescription(props.data.id, description.value);
    }
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

// Registered at setup level (not inside onMounted's async body) so Vue ties it
// to the component's effect scope and stops it on unmount.
watch(
  () => {
    const flowId = nodeStore.flow_id;
    const nodeId = props.data.id;
    return nodeStore.nodeDescriptions[flowId]?.[nodeId];
  },
  (newEntry) => {
    if (newEntry !== undefined) {
      description.value = newEntry.description;
    }
  },
);

onMounted(async () => {
  await nextTick();
  await getNodeDescription();
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
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

/* The letters sit over the node icon, so they carry their own chip — the icons
   are saturated gradient circles and bare grey text on one is unreadable.
   Both colours are literals because the card underneath (.node-button) is a
   theme-independent #dedede; a themed chip would invert against it in dark mode. */
.handle-label {
  position: absolute;
  font-size: 0.6rem;
  font-weight: 600;
  line-height: 1;
  color: #4a4a4a;
  background-color: #dedede;
  border-radius: 3px;
  padding: 1px 2px;
  white-space: nowrap;
  pointer-events: none;
  top: 50%;
  transform: translateY(-50%);
}

.handle-label--input {
  left: 13px;
}

/* Parameter handles (input-0 on dynamic-input nodes): square amber marker. */
.handle-input--parameter {
  position: absolute;
  bottom: -8px;
  left: 50%;
  transform: translateX(-50%);
}

.handle-input--parameter :deep(.vue-flow__handle) {
  background-color: var(--color-info, #909399);
  border-color: var(--color-info, #909399);
  border-radius: 2px;
  opacity: 0.55;
  transition:
    opacity 0.15s ease,
    transform 0.15s ease,
    background-color 0.15s ease;
}

.handle-input--parameter:hover :deep(.vue-flow__handle) {
  opacity: 1;
  transform: scale(1.3);
  background-color: var(--color-primary, #409eff);
  border-color: var(--color-primary, #409eff);
}

.handle-label--output {
  right: 13px;
}
</style>
