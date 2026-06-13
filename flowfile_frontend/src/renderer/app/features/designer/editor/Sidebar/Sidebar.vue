<template>
  <div class="radio-menu">
    <label
      v-for="(option, index) in options"
      :key="index"
      class="radio-option"
      :class="{ selected: selectedOption === option.value }"
      @click="onToggle(option.value)"
    >
      <PopOver :content="option.text">
        <i :class="option.icon || defaultIcon" class="icon"></i>
      </PopOver>
    </label>
  </div>
</template>

<script setup lang="ts">
import { ref, defineProps, defineEmits, watch } from "vue";
import PopOver from "../PopOver.vue";

interface Option {
  text: string;
  icon: string;
  value: string;
}

const props = defineProps({
  options: {
    type: Array as () => Option[],
    required: true,
  },
  modelValue: {
    type: String,
    default: "",
  },
  defaultIcon: {
    type: String,
    default: "fas fa-circle",
  },
});

const emits = defineEmits(["update:modelValue"]);

const selectedOption = ref(props.modelValue);

const onToggle = (value: string) => {
  emits("update:modelValue", value);
};

watch(
  () => props.modelValue,
  (newValue) => {
    selectedOption.value = newValue;
  },
);
</script>

<style scoped>
.radio-menu {
  display: flex;
  flex-direction: row;
  align-items: start;
}

.radio-option {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 5px 8px;
  margin-right: 4px;
  cursor: pointer;
  color: var(--color-text-tertiary);
  border: 1px solid transparent;
  transition:
    background-color 0.15s,
    color 0.15s,
    border-color 0.15s;
  border-radius: 5px;
}

.radio-option.selected {
  background-color: var(--color-accent-subtle);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.radio-option:not(.selected):hover {
  background-color: var(--color-background-hover);
  color: var(--color-text-secondary);
}

.icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 16px; /* Adjust the size as needed */
  height: 16px; /* Adjust the size as needed */
  z-index: 1;
}
</style>
