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
import PopOver from "../PopOver.vue"; // Adjust the path accordingly

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
    default: "fas fa-circle", // Default Font Awesome icon
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
  padding: 5px;
  cursor: pointer;
  transition: background-color 0.3s;
  border-radius: 5px;
}

.radio-option.selected {
  background-color: #e0f7fa; /* Change this color as needed */
}

.radio-option:hover {
  background-color: #b2ebf2; /* Change this color as needed */
}

.icon {
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 5px;
  width: 16px; /* Adjust the size as needed */
  height: 16px; /* Adjust the size as needed */
  z-index: 1;
}
</style>
