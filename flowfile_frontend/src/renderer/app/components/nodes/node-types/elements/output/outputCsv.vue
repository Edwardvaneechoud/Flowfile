<template>
  <div class="csv-table-settings">
    <div class="input-group">
      <label for="delimiter">File delimiter:</label>
      <el-select
        v-model="localCsvTable.delimiter"
        placeholder="Select delimiter"
        size="small"
        style="max-width: 200px"
        @change="updateParent"
      >
        <el-option
          v-for="option in csv_settings.delimiter_options"
          :key="option"
          :label="option"
          :value="option"
        />
      </el-select>
    </div>
    <div class="input-group">
      <label for="encoding">File encoding:</label>
      <el-select
        v-model="localCsvTable.encoding"
        placeholder="Select encoding"
        size="small"
        style="max-width: 200px"
        @change="updateParent"
      >
        <el-option
          v-for="option in csv_settings.encoding_options"
          :key="option"
          :label="option"
          :value="option"
        />
      </el-select>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import { ElSelect, ElOption } from "element-plus";
import "element-plus/dist/index.css";
import { OutputCsvTable } from "../../../baseNode/nodeInput";

const props = defineProps({
  modelValue: {
    type: Object as () => OutputCsvTable,
    required: true,
  },
});

const emit = defineEmits(["update:modelValue"]);
const localCsvTable = ref(props.modelValue);

const csv_settings = {
  delimiter_options: [",", ";", "|", "tab"],
  encoding_options: ["utf-8", "ISO-8859-1", "ASCII"],
};

const updateParent = () => {
  emit("update:modelValue", localCsvTable.value);
};

watch(
  () => props.modelValue,
  (newVal) => {
    localCsvTable.value = newVal;
  },
  { deep: true },
);
</script>

<style scoped>
@import "element-plus/dist/index.css";

.csv-table-settings {
  width: 100%;
  max-width: 600px;
  margin: 20px auto;
  background: var(--color-background-primary);
  border-radius: 8px;
  padding: 20px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.input-group {
  margin-bottom: 20px;
}

.input-group label {
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
  color: var(--color-text-primary);
}

.el-select {
  width: 100%;
}
</style>
