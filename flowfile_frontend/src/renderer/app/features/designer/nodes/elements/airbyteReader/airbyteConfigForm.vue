<template>
  <div class="form-container">
    <div class="form-grid">
      <div v-for="(item, index) in computedSchema" :key="index" class="form-item-wrapper">
        <!-- Handle array fields -->
        <div v-if="item.type === 'array'" class="single-item">
          <div
            class="compact-header"
            @mouseover="showPopover(item.description ?? '', $event)"
            @mouseleave="hidePopover"
          >
            {{ item.title || item.key }}
            <span v-if="item.required" class="tag">*</span>
          </div>

          <div class="array-input-section">
            <div class="input-with-button">
              <input
                v-model="newArrayValue[item.key]"
                type="text"
                class="minimal-input"
                :placeholder="`Add new ${item.title || item.key}`"
                @keyup.enter="addArrayValue(item)"
              />
              <button class="add-btn" type="button" @click="addArrayValue(item)">Add</button>
            </div>

            <div class="items-container">
              <div
                v-for="(value, valueIndex) in getArrayValues(item)"
                :key="valueIndex"
                class="item-box"
              >
                {{ value }}
                <span class="remove-btn" @click="removeArrayValue(item, valueIndex)">x</span>
              </div>
            </div>
          </div>
        </div>

        <!-- Handle oneOf fields (like credentials) -->
        <div v-else-if="item.oneOf" class="collapsible-section">
          <div
            class="compact-header"
            @mouseover="showPopover(item.description ?? '', $event)"
            @mouseleave="hidePopover"
          >
            {{ item.title || item.key }}
            <span v-if="item.required" class="tag">*</span>
          </div>

          <drop-down-generic
            v-model="selectedValues[index]"
            :option-list="item.oneOf.map((opt: OneOfOption) => opt.title)"
            :allow-other="false"
            style="width: 100%; margin-bottom: 8px"
            @change="(value: string) => updateSelectedOption(item, value, index)"
          />

          <div v-if="item.selectedOption !== undefined" class="nested-content">
            <div
              v-for="(property, propKey) in item.oneOf[item.selectedOption].properties"
              :key="propKey"
              class="nested-item"
            >
              <template v-if="propKey !== 'auth_type'">
                <div
                  class="compact-header"
                  @mouseover="showPopover(property.description, $event)"
                  @mouseleave="hidePopover"
                >
                  {{ property.title || propKey }}
                  <span v-if="isRequired(item.oneOf[item.selectedOption], propKey)" class="tag"
                    >*</span
                  >
                </div>
                <input
                  v-model="(item.input_value as Record<string, any>)[propKey]"
                  :type="property.airbyte_secret ? 'password' : 'text'"
                  class="minimal-input"
                  :placeholder="property.title || propKey"
                />
              </template>
            </div>
          </div>
        </div>

        <!-- Handle regular fields -->
        <div v-else-if="!item.properties || item.properties.length === 0" class="single-item">
          <div
            class="compact-header"
            @mouseover="showPopover(item.description ?? '', $event)"
            @mouseleave="hidePopover"
          >
            {{ item.title || item.key }}
            <span v-if="item.required" class="tag">*</span>
          </div>
          <input
            v-model="item.input_value"
            :type="item.airbyte_secret ? 'password' : 'text'"
            class="minimal-input"
            :placeholder="item.title || item.key"
          />
        </div>

        <!-- Handle nested properties -->
        <div v-else class="collapsible-section">
          <button class="minimal-header" :class="{ 'is-open': item.isOpen }" @click="toggle(index)">
            <span class="minimal-chevron">{{ item.isOpen ? "−" : "+" }}</span>
            {{ item.title }}
            <span v-if="item.required" class="tag">*</span>
          </button>

          <div
            v-if="item.isOpen && item.properties && item.properties.length > 0"
            class="nested-content"
          >
            <div
              v-for="(property, propIndex) in item.properties"
              :key="propIndex"
              class="nested-item"
            >
              <div
                class="compact-header"
                @mouseover="showPopover(property.description, $event)"
                @mouseleave="hidePopover"
              >
                {{ property.key }}
                <span v-if="property.required" class="tag">*</span>
              </div>
              <input
                v-model="property.input_value"
                :type="property.airbyte_secret ? 'password' : 'text'"
                class="minimal-input"
                :placeholder="property.key"
              />
            </div>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="popover.show"
      class="minimal-popover"
      :style="{ top: popover.y + 'px', left: popover.x + 'px' }"
    >
      {{ popover.content }}
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick } from "vue";
import { Field, OneOfOption } from "./types";
import DropDownGeneric from "../../../baseNode/page_objects/dropDownGeneric.vue";
import SettingsSection from "../../../components/node/SettingsSection.vue";

const props = defineProps<{
  parsedConfig: Field[];
}>();

interface Popover {
  show: boolean;
  content: string;
  x: number;
  y: number;
}

const popover = ref<Popover>({
  show: false,
  content: "",
  x: 0,
  y: 0,
});

const localConfig = ref([...props.parsedConfig]);
const selectedValues = ref<string[]>(new Array(props.parsedConfig.length).fill(""));
const newArrayValue = ref<Record<string, string>>({});

function isStringArray(value: any): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

// Array handling functions
const getArrayValues = (item: Field): string[] => {
  if (!item.input_value) {
    item.input_value = [];
  }
  if (!isStringArray(item.input_value)) {
    item.input_value = [] as string[];
  }
  return item.input_value as string[];
};

const addArrayValue = (item: Field) => {
  const value = newArrayValue.value[item.key];
  if (!value?.trim()) return;

  if (!item.input_value || !isStringArray(item.input_value)) {
    item.input_value = [];
  }

  // Don't add duplicates
  const currentArray = item.input_value as string[];
  if (!currentArray.includes(value)) {
    currentArray.push(value);
    newArrayValue.value[item.key] = "";
  }
};

const removeArrayValue = (item: Field, index: number) => {
  if (isStringArray(item.input_value)) {
    item.input_value.splice(index, 1);
  }
};

// Initialize selected values from existing configuration
onMounted(() => {
  props.parsedConfig.forEach((item, index) => {
    if (item.oneOf && typeof item.selectedOption === "number" && item.selectedOption >= 0) {
      selectedValues.value[index] = item.oneOf[item.selectedOption].title;
    }
  });
});

const showPopover = (content: string, event: MouseEvent) => {
  if (!content) return;

  popover.value = {
    show: true,
    content,
    x: event.clientX + 10,
    y: event.clientY + 10,
  };
};

const hidePopover = () => {
  popover.value.show = false;
};

const toggle = (index: number) => {
  localConfig.value[index].isOpen = !localConfig.value[index].isOpen;
};

const isRequired = (schema: any, fieldName: string) => {
  return schema.required?.includes(fieldName) || false;
};

const updateSelectedOption = (item: Field, selectedValue: string, index: number) => {
  if (!item.oneOf) return;

  const optionIndex = item.oneOf.findIndex((opt) => opt.title === selectedValue);
  if (optionIndex === -1) return;

  selectedValues.value[index] = selectedValue;

  const localItem = localConfig.value[index];
  if (!localItem || !localItem.oneOf) return;

  localItem.selectedOption = optionIndex;
  const selectedOption = localItem.oneOf[optionIndex];
  const previousValue = localItem.input_value as Record<string, any>;

  const newInputValue: Record<string, any> = {};

  if (selectedOption.properties) {
    Object.entries(selectedOption.properties).forEach(([key, prop]) => {
      if (key === "auth_type") {
        newInputValue[key] = prop.const;
      } else if (previousValue && typeof previousValue === "object" && key in previousValue) {
        newInputValue[key] = previousValue[key];
      } else {
        newInputValue[key] = prop.input_value ?? prop.default ?? "";
      }
    });
  }

  localItem.input_value = newInputValue as Field["input_value"];
};

const computedSchema = computed(() => {
  return props.parsedConfig.map((item) => {
    if (item.oneOf) {
      return {
        ...item,
        selectedOption: item.selectedOption,
        oneOf: item.oneOf.map((option) => ({
          ...option,
          properties: option.properties
            ? Object.entries(option.properties).reduce(
                (acc, [key, value]) => {
                  acc[key] = { ...value }; // Simply preserve all properties exactly as they are
                  return acc;
                },
                {} as Record<string, any>,
              )
            : {},
        })),
      };
    }
    return item;
  });
});

defineExpose({
  localConfig,
});
</script>

<style scoped>
/* Array input styles */
.array-input-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.input-with-button {
  display: flex;
  gap: 8px;
  align-items: center;
}

.items-container {
  display: flex;
  flex-wrap: wrap;
  gap: 10px; /* Space between items */
}

.item-box {
  display: flex;
  align-items: center;
  padding: 5px 10px;
  background-color: #f0f0f0;
  border-radius: 4px;
  font-size: 12px;
  position: relative;
}

.remove-btn {
  margin-left: 8px;
  cursor: pointer;
  color: #100f0f72;
  font-weight: bold;
}

.add-btn {
  padding: 4px 12px;
  background: #7878ff5b;
  border: none;
  border-radius: 3px;
  cursor: pointer;
  font-size: 12px;
  transition: background-color 0.2s;
  white-space: nowrap;
}

.add-btn:hover {
  background: #6363ff5b;
}

/* Your existing styles */
.form-container {
  font-family: -apple-system, BlinkMacSystemFont, sans-serif;
  font-size: 13px;
  color: #333;
}

.form-grid {
  display: grid;
  gap: 8px;
  padding: 12px;
  background: #fff;
  border: 1px solid #eee;
  border-radius: 4px;
}

.form-item-wrapper {
  margin-bottom: 2px;
}

.single-item {
  margin-bottom: 4px;
}

.compact-header {
  font-size: 12px;
  color: #666;
  margin-bottom: 2px;
  display: flex;
  align-items: center;
  gap: 4px;
}

.minimal-header {
  width: 100%;
  text-align: left;
  padding: 6px 8px;
  background: #f5f5f5;
  border: 1px solid #eee;
  border-radius: 3px;
  font-size: 12px;
  display: flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  transition: background 0.2s;
}

.minimal-header:hover {
  background: #f0f0f0;
}

.minimal-header.is-open {
  border-bottom-left-radius: 0;
  border-bottom-right-radius: 0;
}

.minimal-chevron {
  font-size: 14px;
  color: #999;
  width: 12px;
}

.tag {
  color: #ff4757;
  font-size: 14px;
}

.type-indicator {
  color: #999;
  font-size: 11px;
}

.nested-content {
  padding: 8px;
  border: 1px solid #eee;
  border-top: none;
  background: #fff;
  border-bottom-left-radius: 3px;
  border-bottom-right-radius: 3px;
}

.nested-item {
  margin-bottom: 6px;
}

.nested-item:last-child {
  margin-bottom: 0;
}

.minimal-input {
  box-sizing: border-box; /* Add this to include padding in width calculation */
  width: 100%;
  padding: 4px 8px;
  border: 1px solid #ddd;
  border-radius: 3px;
  font-size: 12px;
  background: #fff;
  transition: border 0.2s;
}

.minimal-input:focus {
  outline: none;
  border-color: #666;
}

.minimal-input::placeholder {
  color: #ccc;
}

.minimal-popover {
  position: fixed;
  background: rgba(0, 0, 0, 0.8);
  color: #fff;
  padding: 4px 8px;
  border-radius: 3px;
  font-size: 12px;
  max-width: 250px;
  z-index: 100;
  pointer-events: none;
}

@media (max-width: 640px) {
  .form-grid {
    padding: 8px;
    gap: 6px;
  }

  .minimal-header {
    padding: 4px 6px;
  }

  .minimal-input {
    padding: 3px 6px;
  }
}
</style>
