<template>
  <div>
    <p class="label">Output field</p>
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
        @click="selectOption(option)"
      >
        {{ option }}
      </li>
    </ul>
  </div>
</template>

<script lang="ts" setup>
import { useNodeStore } from "../../../../stores/column-store";
import { onMounted, ref, Ref, computed, defineExpose } from "vue";
const selectedValue: Ref<string> = ref("NewField");
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
  if (nodeStore.nodeData?.right_input?.columns) {
    column_options.value = nodeStore.nodeData.right_input.columns;
  }
});

defineExpose({ selectedValue });
</script>

<style scoped>
.label {
  font-weight: bold;
  margin-bottom: 8px;
}

.select-wrapper {
  position: relative;
}

.select-box {
  width: 100%; /* Full width to fit container */
  padding: 6px 10px; /* Reduced padding */
  font-size: 14px; /* Smaller font size */
  line-height: 1.4; /* Adjust line height for better text alignment */
  border: 1px solid #e0e0e0; /* Lighter border color */
  border-radius: 4px; /* Slightly rounded corners for a softer look */
  box-shadow: none; /* Remove shadow for a flatter style */
  outline: none; /* Remove the default focus outline */
  transition:
    border-color 0.2s,
    box-shadow 0.2s; /* Smooth transition for focus */
}

.select-box:focus {
  border-color: #a0a0a0; /* Darker border on focus for better visibility */
  box-shadow: 0 0 0 2px rgba(130, 130, 130, 0.2); /* Subtle glow effect when focused */
}

.options-list {
  position: absolute; /* Positioned absolutely to overlay content below */
  width: 100%; /* Match width with the select box */
  border: 1px solid #eee; /* Lighter border for the list */
  border-top: none; /* Remove top border to merge with select box */
  border-radius: 0 0 4px 4px; /* Rounded corners at the bottom only */
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.03); /* Even lighter shadow */
  max-height: 200px;
  overflow-y: auto;
  list-style: none;
  margin: 0;
  padding: 0;
  background: #fff; /* Maintain light background */
  z-index: 10; /* Ensure dropdown is above other content */
}

.options-list li {
  padding: 8px 12px; /* Slightly larger horizontal padding */
  cursor: pointer;
  font-size: 15px; /* Reduced font size but not too small */
  color: #555; /* Even softer color */
  line-height: 1.5; /* Improved readability */
  transition: background-color 0.2s; /* Smooth transition for hover effect */
}

.options-list li:not(:last-child) {
  border-bottom: 1px solid #f7f7f7; /* Lighter separators */
}

.options-list li:hover {
  background-color: #f7f7f7; /* Light hover background */
}
</style>
../../../stores/column-store
