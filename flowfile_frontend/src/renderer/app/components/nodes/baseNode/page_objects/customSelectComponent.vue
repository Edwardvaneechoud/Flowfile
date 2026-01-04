<template>
  <div class="select-component" @click="toggleOptions">
    <div class="select-box">
      {{ selectedOption ? selectedOption.label : placeholder }}
      <span class="caret"></span>
    </div>
    <ul v-show="showOptions" class="options-list">
      <li v-for="option in options" :key="option.value" @click="selectOption(option)">
        {{ option.label }}
      </li>
    </ul>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, defineProps, defineEmits, PropType } from "vue";

interface Option {
  value: string;
  label: string;
}

const emit = defineEmits(["update:modelValue"]);

const props = defineProps({
  options: {
    type: Array as PropType<Option[]>,
    required: true,
  },
  modelValue: {
    type: String,
    default: "",
  },
  placeholder: {
    type: String,
    default: "Select an option",
  },
});

const showOptions = ref(false);
const selectedOption = ref<Option | null>(null);

// Watch for external modelValue changes to update the selected option
watch(
  () => props.modelValue,
  (newVal) => {
    selectedOption.value = props.options.find((option) => option.value === newVal) || null;
  },
);

const toggleOptions = () => {
  showOptions.value = !showOptions.value;
};

const selectOption = (option: Option) => {
  selectedOption.value = option;
  emit("update:modelValue", option.value);
  showOptions.value = false;
};
</script>

<style scoped>
.select-component {
  position: relative;
  font-family: sans-serif;
}

.select-box {
  width: 100%;
  padding: 8px;
  font-size: 16px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  box-shadow: var(--shadow-xs);
  cursor: pointer;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.caret {
  border-top: 5px solid var(--color-text-secondary);
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  height: 0;
  width: 0;
}

.options-list {
  position: absolute;
  width: 100%;
  border: 1px solid var(--color-border-primary);
  border-top: none;
  border-radius: 0 0 4px 4px;
  box-shadow: var(--shadow-md);
  background: var(--color-background-primary);
  max-height: 200px;
  overflow-y: auto;
  list-style: none;
  padding: 0;
  margin: 0;
  z-index: 10;
  display: none;
}

.options-list li {
  padding: 8px;
  cursor: pointer;
  color: var(--color-text-primary);
}

.options-list li:hover {
  background-color: var(--color-background-hover);
}

/* When options are visible */
.select-component .options-list {
  display: block;
}
</style>
