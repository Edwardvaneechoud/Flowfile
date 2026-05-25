<script setup lang="ts">
// Visual group container: a labeled, auto-fitting box drawn behind its member nodes.
// Organizational only — it has no effect on execution. Rename/recolor/collapse persist
// via FlowApi.updateGroup; ungroup/collapse are delegated to the useNodeGroups composable.
import { Handle, Position, useVueFlow } from "@vue-flow/core";
import { computed, nextTick, ref } from "vue";

import { FlowApi } from "../../api";
import {
  GROUP_SOURCE_HANDLE,
  GROUP_TARGET_HANDLE,
  useNodeGroups,
} from "../../composables/useNodeGroups";
import { useFlowStore } from "../../stores/flow-store";
import type { GroupColor, GroupNodeData } from "../../types/flow.types";

const props = defineProps<{
  id: string;
  data: GroupNodeData;
  selected?: boolean;
}>();

const flowStore = useFlowStore();
const { updateNodeData } = useVueFlow();
const { ungroupNodes, setGroupCollapsed } = useNodeGroups();

const GROUP_COLORS: GroupColor[] = ["slate", "blue", "green", "amber", "rose", "violet", "cyan"];
// Header/border tint per color token. Body uses the same hue at low alpha.
const COLOR_HEX: Record<GroupColor, string> = {
  slate: "#64748b",
  blue: "#3b82f6",
  green: "#22c55e",
  amber: "#f59e0b",
  rose: "#f43f5e",
  violet: "#8b5cf6",
  cyan: "#06b6d4",
};

const activeColor = computed<GroupColor>(() => props.data.color ?? "slate");
const accent = computed(() => COLOR_HEX[activeColor.value]);

const editing = ref(false);
const labelDraft = ref("");
const labelInput = ref<HTMLInputElement | null>(null);
const showPalette = ref(false);

const groupId = computed(() => props.data.id);

async function persist(update: Parameters<typeof FlowApi.updateGroup>[2]): Promise<void> {
  if (flowStore.flowId === null) return;
  const response = await FlowApi.updateGroup(flowStore.flowId, groupId.value, update);
  flowStore.updateHistoryState(response.history);
}

function startEditing(): void {
  labelDraft.value = props.data.label;
  editing.value = true;
  nextTick(() => labelInput.value?.focus());
}

async function commitLabel(): Promise<void> {
  if (!editing.value) return;
  editing.value = false;
  const name = labelDraft.value.trim();
  if (!name || name === props.data.label) return;
  updateNodeData(props.id, { label: name });
  await persist({ name });
}

async function pickColor(color: GroupColor): Promise<void> {
  showPalette.value = false;
  if (color === props.data.color) return;
  updateNodeData(props.id, { color });
  await persist({ color });
}

async function onUngroup(): Promise<void> {
  await ungroupNodes(groupId.value);
}

async function onToggleCollapse(): Promise<void> {
  await setGroupCollapsed(groupId.value, !props.data.collapsed);
}
</script>

<template>
  <div
    class="group-node"
    :class="{ selected, collapsed: data.collapsed }"
    :style="{ '--group-accent': accent, borderColor: accent }"
  >
    <!-- When collapsed the members are hidden, so the pill exposes its own in/out handles
         that boundary "proxy" edges attach to (see useNodeGroups.addGroupProxyEdges). -->
    <template v-if="data.collapsed">
      <Handle
        :id="GROUP_TARGET_HANDLE"
        type="target"
        :position="Position.Left"
        class="group-handle"
      />
      <Handle
        :id="GROUP_SOURCE_HANDLE"
        type="source"
        :position="Position.Right"
        class="group-handle"
      />
    </template>
    <div class="group-header" :style="{ backgroundColor: accent }">
      <button
        class="group-collapse"
        :title="data.collapsed ? 'Expand group' : 'Collapse group'"
        @click.stop="onToggleCollapse"
      >
        {{ data.collapsed ? "▸" : "▾" }}
      </button>
      <button
        class="group-color-dot"
        title="Change color"
        @click.stop="showPalette = !showPalette"
      ></button>

      <input
        v-if="editing"
        ref="labelInput"
        v-model="labelDraft"
        class="group-label-input"
        @keyup.enter="commitLabel"
        @blur="commitLabel"
        @click.stop
      />
      <span v-else class="group-label" :title="data.label" @dblclick.stop="startEditing">
        {{ data.label }}
      </span>

      <button class="group-ungroup" title="Ungroup" @click.stop="onUngroup">✕</button>

      <div v-if="showPalette" class="group-palette" @click.stop>
        <button
          v-for="color in GROUP_COLORS"
          :key="color"
          class="group-swatch"
          :style="{ backgroundColor: COLOR_HEX[color] }"
          :title="color"
          @click="pickColor(color)"
        ></button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.group-node {
  width: 100%;
  height: 100%;
  border: 1.5px solid var(--group-accent, #64748b);
  border-radius: 10px;
  /* Tint the body lightly so edges/nodes stay readable through it. */
  background-color: color-mix(in srgb, var(--group-accent, #64748b) 7%, transparent);
  box-sizing: border-box;
}
.group-node.selected {
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--group-accent, #64748b) 60%, transparent);
}
.group-header {
  display: flex;
  align-items: center;
  gap: 6px;
  height: 30px;
  padding: 0 8px;
  border-radius: 8px 8px 0 0;
  color: #fff;
  font-size: 12px;
  font-weight: 600;
  position: relative;
}
/* A collapsed group is just the header pill — fill it and round all corners. */
.group-node.collapsed .group-header {
  height: 100%;
  border-radius: 8px;
}
.group-label {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
}
.group-label-input {
  flex: 1;
  border: none;
  border-radius: 3px;
  padding: 1px 4px;
  font-size: 12px;
  font-weight: 600;
}
.group-collapse {
  border: none;
  background: transparent;
  color: #fff;
  cursor: pointer;
  font-size: 16px;
  line-height: 1;
  width: 22px;
  height: 22px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0;
  border-radius: 4px;
  flex: 0 0 auto;
}
.group-collapse:hover {
  background: rgba(255, 255, 255, 0.25);
}
.group-color-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  border: 1.5px solid rgba(255, 255, 255, 0.8);
  background: transparent;
  cursor: pointer;
  padding: 0;
  flex: 0 0 auto;
}
.group-ungroup {
  border: none;
  background: transparent;
  color: #fff;
  cursor: pointer;
  font-size: 12px;
  line-height: 1;
  padding: 2px 4px;
  border-radius: 3px;
  flex: 0 0 auto;
}
.group-ungroup:hover {
  background: rgba(255, 255, 255, 0.25);
}
.group-palette {
  position: absolute;
  top: 28px;
  left: 8px;
  display: flex;
  gap: 4px;
  padding: 5px;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  z-index: 10;
}
.group-swatch {
  width: 16px;
  height: 16px;
  border-radius: 50%;
  border: 1px solid rgba(0, 0, 0, 0.1);
  cursor: pointer;
  padding: 0;
}
/* In/out connection points on the collapsed pill (proxy edges attach here). */
.group-handle {
  width: 9px;
  height: 9px;
  background: var(--group-accent, #64748b);
  border: 1.5px solid #fff;
}
</style>
