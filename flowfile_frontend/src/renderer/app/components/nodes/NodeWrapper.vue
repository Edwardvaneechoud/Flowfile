<!-- CustomNode.vue -->
<template>
  <div v-bind="$attrs">
    <div class="custom-node-header" data="description_display" @dblclick="onTitleClick" @click.stop>
      <div
        v-if="!editMode"
        class="description-display"
        :class="{ 'description-display--placeholder': !description }"
        @click.stop
      >
        <pre
          ref="descriptionTextEl"
          class="description-text"
        ><template v-for="(segment, index) in displaySegments" :key="index">{{ segment }}<wbr v-if="index < displaySegments.length - 1" /></template></pre>
        <button
          type="button"
          class="edit-icon"
          aria-label="Edit description"
          title="Edit description"
          @click.stop="toggleEditMode(true)"
        >
          <svg
            width="12"
            height="12"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
            aria-hidden="true"
          >
            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
          </svg>
        </button>
        <div v-if="isTruncated" class="description-tooltip" role="tooltip">{{ description }}</div>
      </div>
      <div
        v-else
        :id="props.data.id.toLocaleString()"
        class="description-editor"
        data="description_input"
        @click.stop
      >
        <textarea
          :id="props.data.id.toLocaleString()"
          ref="descriptionInputEl"
          v-model="description"
          class="description-input"
          data="description_input"
          rows="3"
          placeholder="Describe what this node does"
          @blur="toggleEditMode(false)"
          @keydown.esc.prevent="cancelEdit"
          @click.stop
        ></textarea>
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

const editMode = ref<boolean>(false);
const descriptionTextEl = ref<HTMLElement | null>(null);
const descriptionInputEl = ref<HTMLTextAreaElement | null>(null);

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

const onTitleClick = () => {
  toggleEditMode(true);
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
    nextTick(() => descriptionInputEl.value?.focus());
  } else {
    window.removeEventListener("click", handleClickOutside);
    if (description.value !== descriptionAtEditStart) {
      nodeStore.setNodeDescription(props.data.id, description.value);
    }
  }
};

const cancelEdit = () => {
  description.value = descriptionAtEditStart;
  toggleEditMode(false);
};

const description = ref<string>("");

const getNodeDescription = async () => {
  description.value = await nodeStore.getNodeDescription(props.data.id);
};

const displayText = computed(() => description.value || `${props.data.id}: ${props.data.label}`);

// A <wbr> follows every dot so dotted identifiers (schema.table) wrap at the
// dot instead of overflowing the bubble or breaking mid-token.
const displaySegments = computed(() => {
  const parts = displayText.value.split(".");
  return parts.map((part, index) => (index < parts.length - 1 ? `${part}.` : part));
});

// The text is CSS line-clamped; measure whether the clamp actually cut
// anything so the full-text tooltip only appears when there is more to see.
const isTruncated = ref(false);
const measureTruncation = async () => {
  await nextTick();
  const el = descriptionTextEl.value;
  isTruncated.value = el !== null && el.scrollHeight > el.clientHeight + 1;
};

watch([displayText, editMode], measureTruncation);

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
  await measureTruncation();
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

/* Kept narrow so the bubble never widens the node box VueFlow measures;
   the bubble overflows the header on purpose. */
.custom-node-header {
  width: 20px;
  overflow: visible;
  font-family: var(--font-family-base);
}

.description-display {
  position: relative;
  display: flex;
  align-items: flex-start;
  gap: 2px;
  width: max-content;
  min-width: 0;
  max-width: 240px;
  padding: 2px 2px 2px 6px;
  border-radius: var(--border-radius-md);
  background-color: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition:
    background-color 0.15s ease,
    color 0.15s ease;
}

.description-display:hover {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.description-display--placeholder .description-text {
  color: var(--color-text-tertiary);
}

.description-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 3;
  overflow: hidden;
  flex: 1 1 auto;
  min-width: 0;
  margin: 0;
  font-family: var(--font-family-base);
  font-size: var(--font-size-xs);
  font-weight: 500;
  line-height: 1.35;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.edit-icon {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  padding: 0;
  border: 0;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-tertiary);
  opacity: 0;
  cursor: pointer;
  transition:
    opacity 0.15s ease,
    background-color 0.15s ease;
}

.description-display:hover .edit-icon,
.edit-icon:focus-visible {
  opacity: 1;
}

.edit-icon:hover {
  color: var(--color-accent);
}

.description-tooltip {
  position: absolute;
  left: 0;
  bottom: calc(100% + 6px);
  z-index: 10;
  width: max-content;
  max-width: 320px;
  padding: 6px 10px;
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
  box-shadow: var(--shadow-md);
  font-size: var(--font-size-xs);
  line-height: 1.4;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  pointer-events: none;
  visibility: hidden;
  opacity: 0;
  transition: opacity 0.15s ease;
}

.description-display:hover .description-tooltip {
  visibility: visible;
  opacity: 1;
}

.description-input {
  display: block;
  width: 240px;
  min-height: 64px;
  padding: 6px 8px;
  resize: both;
  border: 1px solid var(--color-accent);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
  box-shadow: var(--shadow-sm);
  font-family: var(--font-family-base);
  font-size: var(--font-size-xs);
  line-height: 1.4;
  outline: none;
}

.description-input:focus {
  box-shadow: 0 0 0 3px var(--color-focus-ring-accent);
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
