<template>
  <div v-if="isLoaded">
    <div class="table">
      <div v-if="localExcelTable" class="selectors">
        <!-- Sheet Name Dropdown -->
        <div class="row">
          <el-row>
            <div class="input-wrapper">
              <label>Sheet Name</label>
              <drop-down
                v-model="localExcelTable.sheet_name"
                placeholder="Select or type sheet name"
                :column-options="sheetNames"
                :is-loading="!sheetNamesLoaded"
              />
              <span v-if="showWarning" class="warning-sign">⚠️</span>
            </div>
          </el-row>
        </div>

        <!-- Checkboxes Group  -->
        <div class="row">
          <el-checkbox v-model="localExcelTable.has_headers" label="Has headers" size="large" />
          <el-checkbox
            v-model="localExcelTable.type_inference"
            label="Type inference"
            size="large"
          />
        </div>

        <hr class="section-divider" />

        <div class="button-container">
          <button class="toggle-button" @click="toggleOptionalSettings">
            {{ showOptionalSettings ? "Hide" : "Show" }} Optional Settings
          </button>
        </div>

        <div v-if="showOptionalSettings" class="optional-section">
          <hr class="section-divider" />

          <!-- Table Sizes Title -->
          <div class="table-sizes">Table sizes</div>

          <!-- Start and End Row Inputs -->
          <div class="row">
            <div class="input-wrapper">
              <label for="start-row">Start Row</label>
              <input
                id="start-row"
                v-model.number="localExcelTable.start_row"
                type="number"
                class="compact-input"
              />
            </div>
            <div class="input-wrapper">
              <label for="end-row">End Row</label>
              <input
                id="end-row"
                v-model.number="localExcelTable.end_row"
                type="number"
                class="compact-input"
              />
            </div>
          </div>

          <hr class="section-divider" />

          <!-- Start and End Column Inputs -->
          <div class="row">
            <div class="input-wrapper">
              <label for="start-column">Start Column</label>
              <input
                id="start-column"
                v-model.number="localExcelTable.start_column"
                type="number"
                class="compact-input"
              />
            </div>
            <div class="input-wrapper">
              <label for="end-column">End Column</label>
              <input
                id="end-column"
                v-model.number="localExcelTable.end_column"
                type="number"
                class="compact-input"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, watch, onMounted } from "vue";
import { InputExcelTable } from "../../../baseNode/nodeInput";
import dropDown from "../../../baseNode/page_objects/dropDown.vue";
import { getXlsxSheetNamesForPath } from "./utils";
import { CodeLoader } from "vue-content-loader";

const props = defineProps<{
  modelValue: InputExcelTable;
  path: string;
}>();

const isLoaded = ref(false);
const emit = defineEmits(["update:modelValue"]);
const localExcelTable = ref({ ...props.modelValue });

const showOptionalSettings = ref(false);
const sheetNames = ref<string[]>([]);
const sheetNamesLoaded = ref(false);

const getSheetNames = async () => {
  sheetNames.value = await getXlsxSheetNamesForPath(props.path);
  sheetNamesLoaded.value = true;
};

const toggleOptionalSettings = () => {
  showOptionalSettings.value = !showOptionalSettings.value;
};

// FIX 1: Handle undefined sheet_name
const showWarning = computed(() => {
  if (!sheetNamesLoaded.value || !localExcelTable.value.sheet_name) {
    return false;
  }
  return !sheetNames.value.includes(localExcelTable.value.sheet_name);
});

onMounted(() => {
  if (props.path) {
    getSheetNames();
  }
  isLoaded.value = true;
});

watch(
  () => localExcelTable.value,
  (newValue) => {
    emit("update:modelValue", { ...newValue });
  },
  { deep: true },
);
</script>

<style scoped>
.selectors {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.input-wrapper {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
}

label {
  font-weight: 500;
  color: var(--color-text-primary);
  font-size: 14px;
}

input {
  padding: 6px;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 14px;
  width: 99%;
}

.row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.compact-input {
  width: 96%;
  padding: 6px;
  font-size: 14px;
  border: 1px solid #ccc;
  border-radius: 4px;
}

.button-container {
  display: flex;
  justify-content: center;
  margin: 16px 0;
}

.optional-section {
  margin-top: 20px;
}

.section-divider {
  margin: 16px 0;
  border: none;
  border-top: 1px solid #ddd;
}

.table-sizes {
  font-weight: bold;
  margin-bottom: 10px;
}

.warning-sign {
  color: #e74c3c;
  font-size: 16px;
  margin-left: 8px;
}

@media (max-width: 600px) {
  .row {
    flex-direction: column;
  }
}
</style>
