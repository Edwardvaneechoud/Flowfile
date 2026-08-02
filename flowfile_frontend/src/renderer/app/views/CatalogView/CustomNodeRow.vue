<template>
  <div class="node-row" :data-testid="`catalog-node-${node.file_name}`" @click="emit('open')">
    <img :src="iconUrl" alt="" class="node-row__icon" />
    <span class="node-row__name">{{ node.node_name || node.file_name }}</span>

    <span v-if="node.environment === 'kernel'" class="status-badge status-badge--warning">
      Kernel
    </span>
    <span
      v-if="node.environment === 'kernel' && readiness"
      class="status-badge readiness-chip"
      :class="readinessChipClass"
      :title="readinessTitle"
    >
      {{ readinessLabel }}
    </span>
    <el-tooltip
      v-if="community"
      :content="`Installed from the community registry · v${community.version}`"
      placement="top"
    >
      <button class="status-badge status-badge--success community-chip" @click.stop="openSource">
        Community <i class="fa-solid fa-arrow-up-right-from-square" />
      </button>
    </el-tooltip>
    <el-tooltip v-if="node.source !== 'default'" :content="node.source" placement="top">
      <span class="status-badge status-badge--info source-chip">
        <i class="fa-solid fa-folder" /> {{ node.source_label }}
      </span>
    </el-tooltip>
    <el-tooltip v-if="node.error" :content="node.error" placement="top">
      <span class="node-row__broken"><i class="fa-solid fa-triangle-exclamation" /></span>
    </el-tooltip>

    <span class="node-row__intro">{{ node.intro || "No description" }}</span>

    <div class="node-row__actions" @click.stop>
      <button class="btn btn-sm btn-secondary" @click="emit('open')">
        <i class="fa-solid fa-pen" /> Open
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
// One row of the catalog's Custom Nodes tab. Own component because
// useNodeIconUrl is a watchEffect composable and cannot be called in a v-for.
// Shares .node-row* styling with the designer browser (_node-rows.css).
import { computed } from "vue";
import { ElTooltip } from "element-plus";
import { useNodeIconUrl } from "../../composables/useCustomNodeIcon";
import { communityNodeSourceUrl, type InstallReceipt } from "../../api/communityNodes";
import type { CatalogCustomNode } from "../../api/nodeDesigner";
import type { KernelMatchBatchSummary } from "../../types";
import { desktop } from "../../../lib/desktop";

const props = defineProps<{
  node: CatalogCustomNode;
  community?: InstallReceipt | null;
  readiness?: KernelMatchBatchSummary | null;
}>();

const emit = defineEmits<{ (e: "open"): void }>();

const iconUrl = useNodeIconUrl(() => props.node.node_icon);

const readinessChipClass = computed(() => {
  if (props.readiness?.level === "full") return "status-badge--success";
  if (props.readiness?.level === "partial") return "status-badge--warning";
  return "readiness-chip--none";
});

const readinessLabel = computed(() => {
  if (props.readiness?.level === "full") return "✓ kernel ready";
  if (props.readiness?.level === "partial") return "kernel needs packages";
  return "no matching kernel";
});

const readinessTitle = computed(() => {
  const name = props.readiness?.best_kernel_name;
  if (props.readiness?.level === "full") return `Kernel "${name}" has all required packages`;
  if (props.readiness?.level === "partial")
    return `Kernel "${name}" is missing some of the required packages`;
  return "No kernel has this node's required packages yet";
});

function openSource() {
  if (props.community) void desktop.openExternal(communityNodeSourceUrl(props.community.node_id));
}
</script>

<style scoped>
.readiness-chip {
  white-space: nowrap;
}
.readiness-chip--none {
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
}
.source-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  cursor: default;
}
</style>
