<template>
  <el-dialog
    :model-value="modelValue"
    title="Create kernel"
    width="640px"
    align-center
    append-to-body
    class="create-kernel-dialog"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-alert
      v-if="dockerDown"
      type="warning"
      :closable="false"
      show-icon
      title="Docker is not running"
      description="Kernels require Docker. Start Docker, then reopen this dialog."
    />
    <template v-else>
      <el-alert
        v-if="suggestion && suggestion.flavour_image_available === false"
        type="info"
        :closable="false"
        show-icon
        class="flavour-hint"
        :title="`The suggested ${suggestion.config.image_flavour} image isn't installed yet`"
        description="Another flavour was selected below — you can install the suggested image from the Kernel Manager."
      />
      <KernelCreateForm
        :flavour-info="flavourInfo"
        :image-statuses="imageStatuses"
        :initial="seedSnapshot"
        :on-create="handleCreate"
      />
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ElMessage } from "element-plus";
import { computed, ref, watch } from "vue";

import { KernelApi } from "@/api/kernel.api";
import { useKernelResources } from "@/composables/useKernelResources";
import type { KernelConfig, KernelInfo, KernelSuggestion } from "@/types";

import KernelCreateForm from "./KernelCreateForm.vue";
import { mergePackages } from "./kernelMatch";

const props = withDefaults(
  defineProps<{
    modelValue: boolean;
    suggestion?: KernelSuggestion | null;
    autoStart?: boolean;
  }>(),
  { suggestion: null, autoStart: true },
);

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "created", kernel: KernelInfo): void;
}>();

const { dockerStatus, flavourInfo, imageStatuses, ensureLoaded } = useKernelResources();

const seedSnapshot = ref<Partial<KernelConfig> | null>(null);

function buildSeed(): Partial<KernelConfig> | null {
  const s = props.suggestion;
  if (!s) return null;
  // If the suggested flavour image may be missing, the form will downgrade
  // the flavour — flavour-covered deps must then ride along as packages.
  if (s.flavour_image_available !== true && s.covered_by_flavour.length) {
    return { ...s.config, packages: mergePackages(s.config.packages, s.covered_by_flavour) };
  }
  return { ...s.config };
}

// @open never fires for a dialog mounted already-open — watch the model instead.
// The seed is snapshotted per open so background match refreshes can't clobber edits.
watch(
  () => props.modelValue,
  (open) => {
    if (open) {
      seedSnapshot.value = buildSeed();
      void ensureLoaded();
    }
  },
  { immediate: true },
);

const dockerDown = computed(() => dockerStatus.value !== null && !dockerStatus.value.available);

async function handleCreate(config: KernelConfig): Promise<void> {
  let created: KernelInfo;
  try {
    created = await KernelApi.create(config);
  } catch (error) {
    ElMessage.error((error as Error).message || "Failed to create kernel");
    throw error; // form keeps its input
  }
  let result = created;
  if (props.autoStart) {
    try {
      result = await KernelApi.start(created.id);
    } catch (error) {
      // The kernel exists and should be selected regardless of a start failure.
      ElMessage.warning(
        `Kernel created but failed to start: ${(error as Error).message}. ` +
          "Start it from the Kernel Manager.",
      );
    }
  }
  emit("created", result);
  emit("update:modelValue", false);
}
</script>

<style scoped>
.flavour-hint {
  margin-bottom: 12px;
}
</style>
