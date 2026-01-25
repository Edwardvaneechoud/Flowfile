<template>
  <div class="listbox-wrapper">
    <div class="listbox-row">
      <div class="listbox-subtitle">{{ title }}</div>
      <div class="items-container">
        <div v-if="item !== ''" class="item-box">
          {{ item }}
          <span class="remove-btn" @click="emitRemove(item)">x</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
defineProps({
  title: { type: String, required: true },
  item: { type: String, required: true }, // Changed to a single item
});

const emit = defineEmits(["removeItem"]);

/**
 * Emit the remove event when an item is clicked.
 * @param {string} item - The item to remove.
 */
const emitRemove = (item: string) => {
  emit("removeItem", item);
};
</script>

<style scoped>
.items-container {
  display: flex;
  gap: 10px; /* Space between items */
}

.item-box {
  display: flex;
  align-items: center;
  padding: 5px 10px;
  background-color: var(--color-bg-secondary);
  border-radius: 4px;
  font-size: 12px; /* Font size set to 12px */
  position: relative;
}

.remove-btn {
  margin-left: 8px;
  cursor: pointer;
  color: var(--color-text-secondary);
  font-weight: bold;
}
</style>
