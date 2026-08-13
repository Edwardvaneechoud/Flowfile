<template>
  <div class="excel-table-settings">
    Sheet name:
    <el-input v-model="localExcelTable.sheet_name" size="small" class="m-2" @input="updateParent" />
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import { OutputExcelTable } from "../../../baseNode/nodeInput";

const props = defineProps({
  modelValue: {
    type: Object as () => OutputExcelTable,
    required: true,
  },
});

const emit = defineEmits(["update:modelValue"]);
const localExcelTable = ref(props.modelValue);

const updateParent = () => {
  emit("update:modelValue", localExcelTable.value);
};

watch(
  () => props.modelValue,
  (newVal) => {
    localExcelTable.value = newVal;
  },
  { deep: true },
);
</script>
