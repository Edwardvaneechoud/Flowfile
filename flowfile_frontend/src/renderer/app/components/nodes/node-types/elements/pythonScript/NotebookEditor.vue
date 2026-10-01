<template>
  <Notebook
    :cells="cells"
    :executor="executor"
    :owner-id="ownerId"
    :input-names="inputNames"
    :upstream-columns="upstreamColumns"
    @update:cells="(next) => emit('update:cells', next)"
  />
</template>

<script lang="ts" setup>
/** The Python Script node's notebook: the shared `<Notebook>` running on the selected kernel. */
import { computed } from "vue";
import type { NotebookCell } from "../../../../../types/node.types";
import Notebook from "../../../../notebook/Notebook.vue";
import { ownerIdForNode } from "../../../../notebook/editorViews";
import { createKernelExecutor } from "../../../../notebook/notebookExecutor";
import type { UpstreamColumn } from "./useUpstreamColumns";

interface Props {
  cells: NotebookCell[];
  kernelId: string | null;
  flowId: number;
  nodeId: number;
  dependingOnIds: number[];
  inputNames?: string[];
  upstreamColumns?: UpstreamColumn[];
}

const props = withDefaults(defineProps<Props>(), {
  inputNames: () => [],
  upstreamColumns: () => [],
});
const emit = defineEmits<{
  (e: "update:cells", cells: NotebookCell[]): void;
}>();

const ownerId = computed(() => ownerIdForNode(props.flowId, props.nodeId));
const executor = createKernelExecutor({
  getKernelId: () => props.kernelId,
  getFlowId: () => props.flowId,
  getNodeId: () => props.nodeId,
});
</script>
