<template>
  <div v-if="isLoaded" ref="dropdownRef" class="dropdown-container">
    <label :for="uniqueId" class="sr-only">{{ label }}</label>
    <div class="input-wrapper">
      <input
        :id="uniqueId"
        v-model="inputValue"
        type="text"
        class="select-box"
        :class="{ 'has-error': hasError && !isLoading }"
        :placeholder="isLoading ? 'Loading...' : placeholder"
        :aria-expanded="showOptions"
        :aria-controls="`${uniqueId}-listbox`"
        :aria-activedescendant="activeDescendant"
        :disabled="isLoading"
        role="combobox"
        @focus="onFocus"
        @input="onInput"
        @blur="onBlur"
        @keydown="onKeyDown"
      />
      <div v-if="isLoading" class="spinner-small"></div>
    </div>
    <ul
      v-if="showOptions && !isLoading && Array.isArray(column_options)"
      :id="`${uniqueId}-listbox`"
      class="options-list"
      role="listbox"
    >
      <li
        v-for="(option, index) in displayedOptions"
        :id="`${uniqueId}-option-${index}`"
        :key="option"
        class="option-item"
        :class="{ 'is-active': index === activeIndex }"
        role="option"
        :aria-selected="inputValue === option"
        @mouseenter="activeIndex = index"
        @mousedown.prevent="selectOption(option)"
      >
        {{ option }}
      </li>
      <li v-if="displayedOptions.length === 0" class="no-options">
        No options found
      </li>
    </ul>
  </div>
</template>

<script lang="ts" setup>
import {
  defineProps,
  defineEmits,
  ref,
  computed,
  watch,
  onMounted,
  onUnmounted,
} from "vue";

const props = defineProps({
  modelValue: {
    type: String,
    default: "",
  },
  columnOptions: {
    type: Array as () => string[],
    required: true,
    default: () => [],
  },
  placeholder: {
    type: String,
    default: "Select an option",
  },
  label: {
    type: String,
    default: "Dropdown",
  },
  allowOther: {
    type: Boolean,
    default: true,
  },
  isLoading: {
    type: Boolean,
    default: false
  }
});

const emits = defineEmits(["update:modelValue", "error"]);

const isLoaded = ref(false);
const inputValue = ref(props.modelValue);
const selectedValue = ref(props.modelValue);
const showOptions = ref(false);
const activeIndex = ref(-1);
const hasError = ref(false);
const dropdownRef = ref<HTMLElement | null>(null);
const uniqueId = `dropdown-${Math.random().toString(36).substr(2, 9)}`;
let inputTimeout: number | null = null;

const column_options = computed(() => props.columnOptions || []);

const hasTyped = ref(false);

const displayedOptions = computed(() => {
  if (!Array.isArray(column_options.value)) return [];
  if (!hasTyped.value) return column_options.value;
  return column_options.value.filter((option) =>
    option.toLowerCase().includes(inputValue.value.toLowerCase())
  );
});

const onFocus = () => {
  showOptions.value = true;
  hasTyped.value = false;
};

const onInput = () => {
  showOptions.value = true;
  hasError.value = false;
  activeIndex.value = -1;
  hasTyped.value = true;

  if (inputTimeout) clearTimeout(inputTimeout);
  if (props.allowOther) {
    inputTimeout = window.setTimeout(() => {
      doUpdate();
    }, 500);
  }
};

const filteredOptions = computed(() => {
  if (!Array.isArray(column_options.value)) return [];
  return column_options.value.filter((option) =>
    option.toLowerCase().includes(inputValue.value.toLowerCase()),
  );
});

const activeDescendant = computed(() =>
  activeIndex.value >= 0
    ? `${uniqueId}-option-${activeIndex.value}`
    : undefined,
);

const selectOption = (option: string) => {
  inputValue.value = option;
  selectedValue.value = option;
  showOptions.value = false;
  hasError.value = false;
  emits("update:modelValue", option);
};

const doUpdate = () => {
  if (!props.allowOther && !column_options.value.includes(inputValue.value)) {
    hasError.value = true;
    emits("error", "Invalid option selected");
    inputValue.value = selectedValue.value || "";
  } else {
    hasError.value = false;
    selectedValue.value = inputValue.value;
    emits("update:modelValue", inputValue.value);
  }
};

const onBlur = () => {
  setTimeout(() => {
    showOptions.value = false;
    doUpdate();
  }, 200);
};

const onKeyDown = (event: KeyboardEvent) => {
  switch (event.key) {
    case "ArrowDown":
      event.preventDefault();
      if (!showOptions.value) {
        showOptions.value = true;
      }
      activeIndex.value = Math.min(
        activeIndex.value + 1,
        filteredOptions.value.length - 1,
      );
      break;
    case "ArrowUp":
      event.preventDefault();
      activeIndex.value = Math.max(activeIndex.value - 1, 0);
      break;
    case "Enter":
      if (activeIndex.value >= 0 && filteredOptions.value[activeIndex.value]) {
        event.preventDefault();
        selectOption(filteredOptions.value[activeIndex.value]);
      }
      break;
    case "Escape":
      event.preventDefault();
      showOptions.value = false;
      break;
  }
};

const handleClickOutside = (event: MouseEvent) => {
  if (dropdownRef.value && !dropdownRef.value.contains(event.target as Node)) {
    showOptions.value = false;
    doUpdate();
  }
};

onMounted(() => {
  document.addEventListener("click", handleClickOutside);
  isLoaded.value = true;
});

onUnmounted(() => {
  document.removeEventListener("click", handleClickOutside);
  if (inputTimeout) {
    clearTimeout(inputTimeout);
  }
  isLoaded.value = false;
});

watch(
  () => props.modelValue,
  (newValue) => {
    inputValue.value = newValue || "";
    selectedValue.value = newValue || "";
  },
);
</script>

<style scoped>
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  border: 0;
}

.dropdown-container {
  position: relative;
}

.input-wrapper {
  position: relative;
}

.select-box {
  width: 100%;
  box-sizing: border-box; /* Add this */
  padding: 8px 12px; 
  font-size: 14px;
  line-height: 1.4;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  box-shadow: none;
  outline: none;
  transition:
    border-color 0.2s,
    box-shadow 0.2s;
}

.options-list {
  position: absolute;
  top: 100%;
  left: 0;
  width: 100%;
  border: 1px solid #eee;
  border-top: none;
  border-radius: 0 0 4px 4px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.03);
  max-height: 200px;
  overflow-y: auto;
  list-style: none;
  margin: 0;
  padding: 0;
  background: #fff;
  z-index: 1000;
}

.option-item {
  padding: 8px 12px;
  cursor: pointer;
  font-size: 15px;
  color: #555;
  line-height: 1.5;
  transition: background-color 0.2s;
}

.option-item:not(:last-child) {
  border-bottom: 1px solid #f7f7f7;
}

.option-item:hover,
.option-item.is-active {
  background-color: #f7f7f7;
}

.no-options {
  padding: 8px 12px;
  font-size: 15px;
  color: #555;
  line-height: 1.5;
}

.spinner-small {
  width: 16px;
  height: 16px;
  border: 2px solid #f3f3f3;
  border-top: 2px solid #3498db;
  border-radius: 50%;
  animation: spin 1s linear infinite;
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
}

@keyframes spin {
  0% { transform: translateY(-50%) rotate(0deg); }
  100% { transform: translateY(-50%) rotate(360deg); }
}

.select-box:disabled {
  background-color: #f9f9f9;
  cursor: not-allowed;
}
</style>