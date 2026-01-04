<template>
  <div>
    <p class="label">Output field</p>
    <div class="select-wrapper">
      <input
        v-model="selectedValue"
        type="text"
        class="select-box"
        @focus="showOptions = true"
        @blur="hideOptions"
      />
      <ul v-if="showOptions" class="options-list">
        <li
          v-for="option in filteredOptions"
          :key="option"
          @mousedown.prevent="selectOption(option)"
        >
          {{ option }}
        </li>
      </ul>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { useNodeStore } from "../../../../stores/column-store";
import { onMounted, ref, Ref, computed, defineExpose, defineProps } from "vue";

const props = defineProps({
  selectedValue: {
    type: String,
    default: "NewField",
  },
});

const selectedValue: Ref<string> = ref("");
const column_options: Ref<string[]> = ref([]);
const showOptions: Ref<boolean> = ref(false);
const nodeStore = useNodeStore();

const filteredOptions = computed(() => {
  if (selectedValue.value) {
    return column_options.value.filter((option) =>
      option.toLowerCase().includes(selectedValue.value.toLowerCase()),
    );
  }
  return column_options.value;
});

const hideOptions = () => {
  setTimeout(() => {
    showOptions.value = false;
  }, 200);
};

const selectOption = (option: string) => {
  selectedValue.value = option;
  showOptions.value = false;
};

onMounted(() => {
  if (nodeStore.nodeData?.main_input?.columns) {
    column_options.value = nodeStore.nodeData.main_input.columns;
  }
  selectedValue.value = props.selectedValue;
});

defineExpose({ selectedValue });
</script>

<style scoped>
.label {
  font-weight: bold;
  margin-bottom: 8px;
  color: var(--color-text-primary);
}

.select-wrapper {
  position: relative;
}

.select-box {
  width: 100%;
  padding: 10px 12px;
  font-size: 14px;
  line-height: 1.4;
  border: 1px solid #ddd;
  border-radius: 4px;
  outline: none;
  transition:
    border-color 0.2s,
    box-shadow 0.2s;
}

.select-box:focus {
  border-color: #3498db;
  box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
}

.options-list {
  position: absolute;
  top: 100%;
  left: 0;
  width: 100%;
  border: 1px solid #ddd;
  border-top: none;
  border-radius: 0 0 4px 4px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  max-height: 200px;
  overflow-y: auto;
  list-style: none;
  margin: 0;
  padding: 0;
  background: var(--color-background-primary);
  z-index: 1050;
}

.options-list li {
  padding: 10px 12px;
  cursor: pointer;
  font-size: 14px;
  color: var(--color-text-primary);
  transition: background-color 0.2s;
}

.options-list li:not(:last-child) {
  border-bottom: 1px solid #eee;
}

.options-list li:hover {
  background-color: #f0f0f0;
}
</style>
../../../stores/column-store
