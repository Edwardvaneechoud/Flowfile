<template>
  <div class="excel-table-settings">
    <div class="mandatory-section">
      <label for="sheet-name">Sheet Name:</label>
      <el-input
        id="sheet-name"
        v-model="localExcelTable.sheet_name"
        type="text"
        required
        size="small"
        @input="updateParent"
      />
      <hr v-if="showOptionalSettings" class="section-divider" />
    </div>
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
const showOptionalSettings = ref(false);

const updateParent = () => {
  console.log(localExcelTable.value);
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

<style scoped>
.excel-table-settings {
  width: 100%;
  max-width: 600px; /* Set a maximum width for better layout */
  margin: auto; /* Center align the form */
}

.excel-table-settings label {
  display: block;
  margin-bottom: 10px;
  color: #333;
  font-size: 16px;
  font-weight: 500;
}

.excel-table-settings input[type="text"],
.excel-table-settings input[type="number"] {
  width: 100%;
  padding: 10px 15px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 16px;
  color: #333;
  background-color: #f5f5f5;
  transition: background-color 0.3s ease;
}

.excel-table-settings input[type="text"]:hover,
.excel-table-settings input[type="number"]:hover {
  background-color: #e4e4e4;
}

.excel-table-settings .checkbox-container {
  display: flex;
  align-items: center;
  margin-bottom: 10px;
}

.excel-table-settings .checkbox-label {
  margin-left: 10px;
  font-size: 16px;
}

.excel-table-settings input[type="checkbox"] {
  accent-color: #333; /* Customize the checkbox color */
}

.excel-table-settings button {
  padding: 10px 15px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 16px;
  font-weight: 500;
  color: #333;
  background-color: #f5f5f5;
  cursor: pointer;
  transition: background-color 0.3s ease;
  margin-top: 20px; /* Additional spacing above the button */
}

.excel-table-settings button:hover {
  background-color: #e4e4e4;
}

.optional-section {
  margin-top: 20px;
}

.row-inputs {
  display: flex;
  justify-content: space-between;
  margin-bottom: 20px;
}

.input-group {
  display: flex;
  flex-direction: column;
  flex-basis: 48%; /* Adjusts the width of each group */
}

.input-group label {
  margin-bottom: 5px;
}

.input-group input[type="number"] {
  padding: 10px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background-color: #f5f5f5;
}

.section-divider {
  border: none;
  height: 1px;
  background-color: #999999; /* Color of the divider */
  margin: 20px 0; /* Spacing around the divider */
}

.checkbox-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.checkbox-group {
  display: flex;
  align-items: center;
  margin-bottom: 20px;
}

.checkbox-group label {
  margin-left: 10px;
  font-size: 16px;
  font-weight: 500;
}

.checkbox-group input[type="checkbox"] {
  accent-color: #333; /* Customize the checkbox color */
}
</style>
