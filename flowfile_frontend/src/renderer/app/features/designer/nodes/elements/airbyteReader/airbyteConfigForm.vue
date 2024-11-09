<template>
  <div class="form-container">
    <div class="form-grid">
      <div
        v-for="(item, index) in computedSchema"
        :key="index"
        class="form-item-wrapper"
      >
        <div
          v-if="item.properties && item.properties.length == 0"
          class="single-item"
          @mouseover="showPopover(item.description ?? '', $event)"
          @mouseleave="hidePopover"
        >
          <div class="compact-header">
            {{ item.title || item.key || item.description }}
            <span v-if="item.required" class="tag">*</span>
          </div>
          <input
            v-model="item.input_value"
            :type="item.airbyte_secret ? 'password' : 'text'"
            class="minimal-input"
            :placeholder="item.title || item.key"
          />
        </div>
        <div v-else class="collapsible-section">
          <button
            class="minimal-header"
            :class="{ 'is-open': item.isOpen }"
            @click="toggle(index)"
          >
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
              @mouseover="showPopover(property.description, $event)"
              @mouseleave="hidePopover"
            >
              <div class="compact-header">
                {{ property.key }}
                <span class="type-indicator">({{ property.type }})</span>
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
// Script remains the same
import { ref, computed } from "vue";
import { Field } from "./types";

const props = defineProps<{
  parsedConfig: Field[];
}>();

interface Popover {
  show: boolean;
  content: string;
  x: number;
  y: number;
}

const popover = ref<Popover>({ show: false, content: "", x: 0, y: 0 });
const localConfig = ref([...props.parsedConfig]);

const showPopover = (content: string, event: MouseEvent) => {
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

const computedSchema = computed(() => props.parsedConfig);
</script>

<style scoped>
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
