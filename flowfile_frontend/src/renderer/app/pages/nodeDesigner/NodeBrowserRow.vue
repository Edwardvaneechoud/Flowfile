<template>
  <div
    class="node-row"
    :class="{ 'node-row--current': isCurrent }"
    :data-testid="`node-card-${node.file_name}`"
    @click="emit('edit', node.file_name)"
  >
    <img :src="iconUrl" alt="" class="node-row__icon" />
    <span class="node-row__name">{{ node.node_name || node.file_name }}</span>

    <span v-if="isCurrent" class="status-badge status-badge--info">Editing</span>
    <span v-if="node.environment === 'kernel'" class="status-badge status-badge--warning">
      Kernel
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
    <span v-if="node.error" class="node-row__broken" title="Failed to load">
      <i class="fa-solid fa-triangle-exclamation" />
    </span>

    <span class="node-row__intro">{{ node.intro || "No description" }}</span>

    <div class="node-row__actions" @click.stop>
      <button
        class="btn btn-sm"
        title="Edit"
        :data-testid="`node-edit-${node.file_name}`"
        @click="emit('edit', node.file_name)"
      >
        <i class="fa-solid fa-pen" /> Edit
      </button>
      <el-tooltip content="Duplicate" placement="top">
        <button class="btn btn-sm btn-icon" @click="emit('duplicate', node.file_name)">
          <i class="fa-solid fa-copy" />
        </button>
      </el-tooltip>
      <el-tooltip content="View code" placement="top">
        <button class="btn btn-sm btn-icon" @click="emit('view', node.file_name)">
          <i class="fa-solid fa-code" />
        </button>
      </el-tooltip>
      <el-tooltip content="Delete" placement="top">
        <button class="btn btn-sm btn-icon btn-danger" @click="emit('delete', node.file_name)">
          <i class="fa-solid fa-trash" />
        </button>
      </el-tooltip>
    </div>
  </div>
</template>

<script setup lang="ts">
// One row of the custom-node browser. Its own component because useNodeIconUrl
// is a watchEffect composable and cannot be called inside a v-for.
import { ElTooltip } from "element-plus";
import { useNodeIconUrl } from "../../composables/useCustomNodeIcon";
import { communityNodeSourceUrl, type InstallReceipt } from "../../api/communityNodes";
import { desktop } from "../../../lib/desktop";
import type { CustomNodeInfo } from "./types";

const props = defineProps<{
  node: CustomNodeInfo;
  isCurrent: boolean;
  community?: InstallReceipt | null;
}>();

const emit = defineEmits<{
  (e: "edit", fileName: string): void;
  (e: "duplicate", fileName: string): void;
  (e: "view", fileName: string): void;
  (e: "delete", fileName: string): void;
}>();

const iconUrl = useNodeIconUrl(() => props.node.node_icon);

function openSource() {
  if (props.community) void desktop.openExternal(communityNodeSourceUrl(props.community.node_id));
}
</script>

<!-- Row layout is shared with the catalog's Custom Nodes tab: styles/components/_node-rows.css -->
