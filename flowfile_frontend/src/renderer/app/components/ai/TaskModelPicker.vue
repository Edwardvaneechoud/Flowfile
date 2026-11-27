<template>
  <p v-if="line" class="task-model" :data-testid="`task-model-${surface}`">
    <span class="task-model__line">{{ line }}</span>
    <span class="task-model__sep">·</span>
    <el-popover
      v-model:visible="open"
      trigger="click"
      placement="bottom-start"
      :width="320"
      popper-class="task-model-popover"
    >
      <template #reference>
        <a href="#" class="task-model__change" @click.prevent>Change</a>
      </template>
      <div class="task-model__body" :data-testid="`task-model-${surface}-popover`">
        <p class="task-model__title">Model for {{ label }}</p>
        <label class="task-model__label" :for="`task-model-provider-${surface}`">Provider</label>
        <el-select
          :id="`task-model-provider-${surface}`"
          size="default"
          placeholder="Same as main"
          popper-class="model-select-dropdown"
          :teleported="false"
          :model-value="pick?.provider ?? ''"
          @change="(value: string) => setTaskProvider(surface, value)"
        >
          <el-option value="" label="Same as main" />
          <el-option
            v-for="opt in providerOptions"
            :key="opt.value"
            :value="opt.value"
            :label="opt.label"
            :disabled="opt.disabled"
          />
        </el-select>
        <p v-if="isUnconfigured(pick?.provider ?? null)" class="task-model__warn">
          This provider has no API key, so these calls will fail.
        </p>
        <template v-if="pick">
          <label class="task-model__label" :for="`task-model-model-${surface}`">Model</label>
          <el-select
            :id="`task-model-model-${surface}`"
            size="default"
            placeholder="Provider default"
            popper-class="model-select-dropdown"
            :teleported="false"
            :model-value="pick.model ?? ''"
            @change="(value: string) => setTaskModel(surface, value)"
          >
            <el-option value="" label="Provider default" />
            <el-option v-for="m in taskModels(surface)" :key="m" :value="m" :label="m" />
          </el-select>
        </template>
        <a href="#" class="task-model__all" @click.prevent="openSettings">All AI settings</a>
      </div>
    </el-popover>
  </p>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { useTaskModels } from "../../composables/useTaskModels";

const props = defineProps<{ surface: string; label: string }>();

const router = useRouter();
const {
  aiStore,
  providerOptions,
  isUnconfigured,
  ownPick,
  taskModels,
  setTaskProvider,
  setTaskModel,
  describe,
} = useTaskModels();
const open = ref(false);

const pick = computed(() => ownPick(props.surface));
const line = computed(() =>
  !aiStore.aiDisabled && aiStore.hasConfiguredProvider ? describe(props.surface) : null,
);

function openSettings() {
  open.value = false;
  void router.push({ name: "ai", query: { tab: "assistant" } });
}

onMounted(() => {
  if (!aiStore.providers.length && !aiStore.providersLoading) void aiStore.loadProviders();
});
</script>

<style scoped>
.task-model {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-1);
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.task-model__sep {
  color: var(--color-text-muted);
}

.task-model__change {
  font-size: var(--font-size-xs);
}

.task-model__body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.task-model__title {
  margin: 0;
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-sm);
}

.task-model__label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.task-model__warn {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-warning-dark);
}

.task-model__all {
  align-self: flex-start;
  font-size: var(--font-size-xs);
}
</style>
