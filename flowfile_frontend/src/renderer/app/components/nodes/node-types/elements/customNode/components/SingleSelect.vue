<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-select
      :model-value="modelValue"
      filterable
      placeholder="Select an option"
      style="width: 100%"
      size="small"
      :loading="loading"
      loading-text="Loading options…"
      @update:model-value="$emit('update:modelValue', $event)"
    >
      <el-option
        v-for="item in options"
        :key="optionKey(item)"
        :label="optionLabel(item)"
        :value="optionValue(item)"
      />
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { computed, PropType } from "vue";
import type { ArtifactOption, GlobalArtifactOption, SingleSelectComponent } from "../interface";
import {
  buildArtifactOptions,
  optionKey,
  optionLabel,
  optionValue,
  type SelectOptionItem,
} from "./artifactFilter";

const props = defineProps({
  schema: {
    type: Object as PropType<SingleSelectComponent>,
    required: true,
  },
  modelValue: {
    type: [String, Number, Object],
    default: null,
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

// Artifact values are bare names or namespace-qualified refs; scope decides the source.
const options = computed((): SelectOptionItem[] => {
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
