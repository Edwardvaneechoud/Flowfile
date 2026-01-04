<template>
  <div class="csv-table-settings">
    <!-- Row for Has Headers -->
    <div class="row">
      <label for="has-headers">Has Headers:</label>
      <el-checkbox v-model="localCsvTable.has_headers" size="large" @change="updateParent" />
    </div>

    <!-- Row for Delimiter -->
    <div class="row">
      <label for="delimiter">Delimiter:</label>
      <el-select
        v-model="localCsvTable.delimiter"
        placeholder="Select delimiter"
        clearable
        size="small"
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

    <!-- Row for Encoding -->
    <div class="row">
      <label for="encoding">Encoding:</label>
      <el-select
        v-model="localCsvTable.encoding"
        placeholder="Select encoding"
        clearable
        size="small"
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

    <!-- Row for Quote Character -->
    <div class="row">
      <label for="quote-char">Quote Character:</label>
      <el-select
        v-model="localCsvTable.quote_char"
        placeholder="Select quote character"
        clearable
        size="small"
        @change="updateParent"
      >
        <el-option
          v-for="option in csv_settings.quote_char"
          :key="option"
          :label="option"
          :value="option"
        />
      </el-select>
    </div>

    <!-- Row for Row Delimiter -->
    <div class="row">
      <label for="row-delimiter">New Line Delimiter:</label>
      <el-select
        v-model="localCsvTable.row_delimiter"
        placeholder="Select new line delimiter"
        clearable
        size="small"
        @change="updateParent"
      >
        <el-option
          v-for="option in csv_settings.row_delimiter"
          :key="option"
          :label="option"
          :value="option"
        />
      </el-select>
    </div>

    <!-- Row for Schema Infer Length -->
    <div class="row">
      <label for="infer-schema-length">Schema Infer Length:</label>
      <el-slider
        v-model="localCsvTable.infer_schema_length"
        :step="1000"
        :max="100000"
        :min="0"
        show-stops
        size="small"
        @change="updateParent"
      />
    </div>

    <!-- Row for Truncate Long Lines -->
    <div class="row">
      <label for="truncate-long-lines">Truncate Long Lines:</label>
      <el-checkbox
        v-model="localCsvTable.truncate_ragged_lines"
        size="large"
        @change="updateParent"
      />
    </div>

    <!-- Row for Ignore Errors -->
    <div class="row">
      <label for="ignore-errors">Ignore Errors:</label>
      <el-checkbox v-model="localCsvTable.ignore_errors" size="large" @change="updateParent" />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import { InputCsvTable } from "../../../baseNode/nodeInput";

const props = defineProps<{
  modelValue: InputCsvTable;
}>();

const emit = defineEmits(["update:modelValue"]);
const localCsvTable = ref({ ...props.modelValue });
const updateParent = () => {
  emit("update:modelValue", localCsvTable.value);
};

const csv_settings = {
  delimiter_options: [",", ";", "|", "tab"],
  encoding_options: ["utf-8", "ISO-8859-1", "ASCII"],
  row_delimiter: ["\\n", "\\r\\n", "\\r"],
  quote_char: ['"', "'", "auto"],
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
* {
  box-sizing: border-box;
}

.csv-table-settings {
  background: var(--color-background-primary);
  border-radius: 6px;
  padding: 16px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  margin: 20px auto;
  max-width: 600px;
  overflow-x: hidden; /* Prevent horizontal scroll */
}

.row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
  gap: 20px;
  max-width: 100%; /* Prevent overflow */
}

label {
  font-weight: 500;
  color: var(--color-text-primary);
  font-size: 14px;
  flex: 1;
  max-width: 100%;
}

.el-select,
.el-slider,
.el-checkbox {
  flex: 2;
  max-width: 100%; /* Prevent overflow */
}

.el-slider {
  padding-top: 10px;
}

.el-checkbox {
  display: flex;
  align-items: center;
}

@media (max-width: 600px) {
  .row {
    flex-direction: column;
    align-items: flex-start;
  }

  .el-select,
  .el-slider,
  .el-checkbox {
    width: 100%;
  }
}
</style>
