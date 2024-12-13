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
  border: 1px solid #ccc;
  border-radius: 4px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  cursor: pointer;
  background-color: white;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.caret {
  border-top: 5px solid #333;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  height: 0;
  width: 0;
}

.options-list {
  position: absolute;
  width: 100%;
  border: 1px solid #ccc;
  border-top: none; /* Seamless transition from select box to options */
  border-radius: 0 0 4px 4px;
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  background: white;
  max-height: 200px;
  overflow-y: auto;
  list-style: none;
  padding: 0;
  margin: 0;
  z-index: 10;
  display: none; /* Hide by default */
}

.options-list li {
  padding: 8px;
  cursor: pointer;
}

.options-list li:hover {
  background-color: #f0f0f0;
}

/* When options are visible */
.select-component .options-list {
  display: block;
}
</style>
