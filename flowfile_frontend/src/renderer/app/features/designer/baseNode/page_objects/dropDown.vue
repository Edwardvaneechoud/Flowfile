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
      <div class="icon-container">
        <div v-if="isLoading" class="spinner"></div>
        <svg v-else class="dropdown-icon" viewBox="0 0 20 20" fill="currentColor">
          <path
            fill-rule="evenodd"
            d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z"
            clip-rule="evenodd"
          />
        </svg>
      </div>
    </div>
    <transition name="fade">
      <div
        v-if="showOptions && !isLoading && Array.isArray(column_options)"
        class="options-container"
      >
        <ul
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
          <li v-if="displayedOptions.length === 0" class="no-options">No options found</li>
        </ul>
      </div>
    </transition>
  </div>
</template>

<script lang="ts" setup>
import { defineProps, defineEmits, ref, computed, watch, onMounted, onUnmounted, nextTick } from "vue";

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
    default: false,
  },
});

const emits = defineEmits(["update:modelValue", "error", "update:value"]);
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
    option.toLowerCase().includes(inputValue.value.toLowerCase()),
  );
});

const onFocus = () => {
  showOptions.value = true;
  hasTyped.value = false;
  
  // Position the dropdown properly on focus
  nextTick(() => {
    positionDropdown();
  });
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
    }, 300); // Reduced from 500ms to 300ms for more responsive feel
  }
  
  // Position the dropdown when filtering
  nextTick(() => {
    positionDropdown();
  });
};

const filteredOptions = computed(() => {
  if (!Array.isArray(column_options.value)) return [];
  return column_options.value.filter((option) =>
    option.toLowerCase().includes(inputValue.value.toLowerCase()),
  );
});

const activeDescendant = computed(() =>
  activeIndex.value >= 0 ? `${uniqueId}-option-${activeIndex.value}` : undefined,
);

const selectOption = (option: string) => {
  inputValue.value = option;
  selectedValue.value = option;
  showOptions.value = false;
  hasError.value = false;
  emits("update:modelValue", option);
  emits("update:value", option); // Add this line to support both events
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
    emits("update:value", inputValue.value); // Add this line to support both events
  }
};

const onBlur = () => {
  setTimeout(() => {
    showOptions.value = false;
    doUpdate();
  }, 150); // Reduced from 200ms to 150ms for more responsive feel
};

const onKeyDown = (event: KeyboardEvent) => {
  switch (event.key) {
    case "ArrowDown":
      event.preventDefault();
      if (!showOptions.value) {
        showOptions.value = true;
        nextTick(() => {
          positionDropdown();
        });
      }
      activeIndex.value = Math.min(activeIndex.value + 1, filteredOptions.value.length - 1);
      scrollActiveOptionIntoView();
      break;
    case "ArrowUp":
      event.preventDefault();
      activeIndex.value = Math.max(activeIndex.value - 1, 0);
      scrollActiveOptionIntoView();
      break;
    case "Enter":
      if (activeIndex.value >= 0 && filteredOptions.value[activeIndex.value]) {
        event.preventDefault();
        selectOption(filteredOptions.value[activeIndex.value]);
      } else if (showOptions.value) {
        showOptions.value = false;
        doUpdate();
      } else {
        showOptions.value = true;
        nextTick(() => {
          positionDropdown();
        });
      }
      break;
    case "Escape":
      event.preventDefault();
      showOptions.value = false;
      break;
  }
};

// Function to scroll active option into view
const scrollActiveOptionIntoView = () => {
  nextTick(() => {
    const activeElement = document.getElementById(`${uniqueId}-option-${activeIndex.value}`);
    if (activeElement) {
      activeElement.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  });
};

// Function to position the dropdown
const positionDropdown = () => {
  const inputEl = dropdownRef.value?.querySelector('.select-box');
  const dropdownEl = dropdownRef.value?.querySelector('.options-container');
  
  if (inputEl && dropdownEl) {
    const inputRect = inputEl.getBoundingClientRect();
    const dropdownEl = dropdownRef.value?.querySelector('.options-container') as HTMLElement;
    
    if (dropdownEl) {
      dropdownEl.style.width = `${inputRect.width}px`;
      dropdownEl.style.top = `${inputRect.bottom}px`;
      dropdownEl.style.left = `${inputRect.left}px`;
      
      // Check if dropdown would go offscreen at the bottom
      const dropdownHeight = dropdownEl.offsetHeight;
      const viewportHeight = window.innerHeight;
      const spaceBelow = viewportHeight - inputRect.bottom;
      
      if (dropdownHeight > spaceBelow) {
        // Position above the input if there's not enough space below
        dropdownEl.style.top = `${inputRect.top - dropdownHeight}px`;
      }
    }
  }
};

const handleClickOutside = (event: MouseEvent) => {
  if (dropdownRef.value && !dropdownRef.value.contains(event.target as Node)) {
    showOptions.value = false;
    doUpdate();
  }
};

const handleScroll = () => {
  if (showOptions.value) {
    positionDropdown();
  }
};

onMounted(() => {
  document.addEventListener('click', handleClickOutside);
  window.addEventListener('scroll', handleScroll, true);
  window.addEventListener('resize', positionDropdown);
  isLoaded.value = true;
});

onUnmounted(() => {
  document.removeEventListener('click', handleClickOutside);
  window.removeEventListener('scroll', handleScroll, true);
  window.removeEventListener('resize', positionDropdown);
  
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

watch(
  () => showOptions.value,
  (isOpen) => {
    if (isOpen) {
      nextTick(() => {
        positionDropdown();
      });
    }
  }
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
  width: 100%;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}

.input-wrapper {
  position: relative;
  display: flex;
  align-items: center;
}

.select-box {
  width: 100%;
  box-sizing: border-box;
  padding: 8px 30px 8px 12px;
  font-size: 14px;
  line-height: 1.4;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
  outline: none;
  transition: all 0.2s ease;
  background-color: white;
  color: #333;
}

.select-box:focus {
  border-color: #3182ce;
  box-shadow: 0 0 0 3px rgba(49, 130, 206, 0.15);
}

.select-box.has-error {
  border-color: #e53e3e;
  box-shadow: 0 0 0 3px rgba(229, 62, 62, 0.15);
}

.select-box:disabled {
  background-color: #f9f9f9;
  cursor: not-allowed;
  opacity: 0.7;
}

.icon-container {
  position: absolute;
  right: 10px;
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
  display: flex;
  align-items: center;
  justify-content: center;
}

.dropdown-icon {
  width: 16px;
  height: 16px;
  color: #718096;
  transition: transform 0.2s ease;
}

.options-container {
  position: fixed;
  z-index: 9999;
  width: 100%;
  margin-top: 4px;
}

.options-list {
  background: white;
  border-radius: 6px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  max-height: 220px;
  overflow-y: auto;
  list-style: none;
  margin: 0;
  padding: 4px 0;
  border: 1px solid #e2e8f0;
  scrollbar-width: thin;
}

.options-list::-webkit-scrollbar {
  width: 6px;
}

.options-list::-webkit-scrollbar-track {
  background: #f7fafc;
}

.options-list::-webkit-scrollbar-thumb {
  background-color: #cbd5e0;
  border-radius: 3px;
}

.option-item {
  padding: 8px 12px;
  cursor: pointer;
  font-size: 14px;
  color: #4a5568;
  line-height: 1.5;
  transition: all 0.15s ease;
  border-left: 2px solid transparent;
}

.option-item:hover, 
.option-item.is-active {
  background-color: #ebf8ff;
  color: #3182ce;
  border-left-color: #3182ce;
}

.no-options {
  padding: 10px 12px;
  font-size: 14px;
  color: #718096;
  text-align: center;
  font-style: italic;
}

.spinner {
  width: 16px;
  height: 16px;
  border: 2px solid rgba(49, 130, 206, 0.2);
  border-top: 2px solid #3182ce;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

/* Fade transition for dropdown */
.fade-enter-active, .fade-leave-active {
  transition: opacity 0.15s, transform 0.15s;
}

.fade-enter-from, .fade-leave-to {
  opacity: 0;
  transform: translateY(-5px);
}
</style>