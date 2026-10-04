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
        v-if="pendingForSeed && !creationInFlight"
        type="info"
        :closable="false"
        show-icon
        class="flavour-hint"
        :title="`Kernel &quot;${pendingForSeed.name}&quot; is already being created`"
        description="You'll get a notification when it's ready — pick a different kernel ID to create another one."
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
import { computed, ref, watch } from "vue";

import { useKernelCreationTracker } from "@/composables/useKernelCreationTracker";
import { useKernelResources } from "@/composables/useKernelResources";
import type { KernelConfig, KernelInfo, KernelSuggestion } from "@/types";

import KernelCreateForm from "./KernelCreateForm.vue";

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
const { createKernel, pendingCreations } = useKernelCreationTracker();

const seedSnapshot = ref<Partial<KernelConfig> | null>(null);
const creationInFlight = ref(false);

// A remounted dialog can be seeded with an id whose creation is still in flight.
const pendingForSeed = computed(() => {
  const id = seedSnapshot.value?.id;
  if (!id) return null;
  return pendingCreations.value.find((p) => p.id === id) ?? null;
});

function buildSeed(): Partial<KernelConfig> | null {
  const s = props.suggestion;
  if (!s) return null;
  return { ...s.config };
}

// @open never fires for a dialog mounted already-open — watch the model instead.
// The seed is snapshotted per open (never mid-create) so nothing clobbers edits.
watch(
  () => props.modelValue,
  (open) => {
    if (open) {
      if (!creationInFlight.value) seedSnapshot.value = buildSeed();
      void ensureLoaded();
    }
  },
  { immediate: true },
);

const dockerDown = computed(() => dockerStatus.value !== null && !dockerStatus.value.available);

async function handleCreate(config: KernelConfig): Promise<void> {
  creationInFlight.value = true;
  try {
    // Tracker owns outcome notifications; rejections propagate so the form keeps its input.
    const result = await createKernel(config, { autoStart: props.autoStart });
    emit("created", result);
    emit("update:modelValue", false);
  } finally {
    creationInFlight.value = false;
  }
}
</script>

<style scoped>
.flavour-hint {
  margin-bottom: 12px;
}
</style>
