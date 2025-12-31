<template>
  <div class="parquet-table-settings"></div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import { OutputParquetTable } from "../../../baseNode/nodeInput";

const props = defineProps({
  modelValue: {
    type: Object as () => OutputParquetTable,
    required: true,
  },
});

defineEmits(["update:modelValue"]);
const localParquetTable = ref(props.modelValue);

watch(
  () => props.modelValue,
  (newVal) => {
    localParquetTable.value = newVal;
  },
  { deep: true },
);
</script>

<style scoped>
.parquet-table-settings button:hover {
  background-color: #e4e4e4;
}
</style>
