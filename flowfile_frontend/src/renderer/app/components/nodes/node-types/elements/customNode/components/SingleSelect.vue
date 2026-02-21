// 📄 SingleSelect.vue

<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-select
      :model-value="modelValue"
      filterable
      placeholder="Select an option"
      style="width: 100%"
      size="large"
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
// No changes are needed in the <script> section
import { computed, PropType } from "vue";
import type { SingleSelectComponent } from "../interface";

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
    type: Array as PropType<string[]>,
    default: () => [],
  },
});

defineEmits(["update:modelValue"]);

const options = computed(() => {
  if (Array.isArray(props.schema.options)) {
    return props.schema.options;
  }
  if (props.schema.options?.__type__ === "IncomingColumns") {
    return props.incomingColumns;
  }
  if (props.schema.options?.__type__ === "AvailableArtifacts") {
    return props.availableArtifacts;
  }
  return [];
});
</script>
