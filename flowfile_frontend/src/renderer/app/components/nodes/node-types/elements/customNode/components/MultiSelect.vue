<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-select
      :model-value="modelValue"
      multiple
      filterable
      placeholder="Select one or more options"
      style="width: 100%"
      size="small"
      :loading="loading"
      loading-text="Loading options…"
      @update:model-value="$emit('update:modelValue', $event)"
    >
      <el-option
        v-for="item in options"
        :key="Array.isArray(item) ? item[0] : item"
        :label="Array.isArray(item) ? item[1] : item"
        :value="Array.isArray(item) ? item[0] : item"
      />
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { computed, PropType } from "vue";
import type { ArtifactOption, GlobalArtifactOption, MultiSelectComponent } from "../interface";
import { buildArtifactOptions } from "./artifactFilter";

const props = defineProps({
  schema: {
    type: Object as PropType<MultiSelectComponent>,
    required: true,
  },
  modelValue: {
    type: Array as PropType<string[]>,
    default: () => [],
  },
  incomingColumns: {
    type: Array as PropType<string[]>,
    default: () => [],
  },
  availableArtifacts: {
    type: Array as PropType<ArtifactOption[]>,
    default: () => [],
  },
  globalArtifacts: {
    type: Array as PropType<GlobalArtifactOption[]>,
    default: () => [],
  },
  loading: {
    type: Boolean,
    default: false,
  },
});

defineEmits(["update:modelValue"]);

// Value is always the bare artifact name; scope decides source and label.
const options = computed(() => {
  const opts = props.schema.options;
  if (Array.isArray(opts)) {
    return opts;
  }
  if (opts?.__type__ === "IncomingColumns") {
    return props.incomingColumns;
  }
  if (opts?.__type__ === "AvailableArtifacts") {
    return buildArtifactOptions(
      opts.scope,
      props.availableArtifacts,
      props.globalArtifacts,
      opts.type_filter,
    );
  }
  return [];
});
</script>
