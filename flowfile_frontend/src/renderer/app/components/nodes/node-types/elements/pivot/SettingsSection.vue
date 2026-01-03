<template>
  <div class="listbox-wrapper">
    <div class="listbox-row">
      <div class="listbox-subtitle">{{ title }}</div>
      <div class="items-container">
        <div v-for="(item, index) in items" :key="index" class="item-box">
          <div v-if="item !== ''">
            {{ item }}

            <span class="remove-btn" @click="emitRemove(item)">x</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
defineProps({
  title: { type: String, required: true },
  items: { type: Array as () => string[], required: true },
});

const emit = defineEmits(["removeItem"]);

/**
 * Emit the remove event when an item is right-clicked.
 * @param {string} item - The item to remove.
 */
const emitRemove = (item: string) => {
  emit("removeItem", item);
};
</script>
<style scoped>
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
  font-size: 12px; /* Font size set to 12px */
  position: relative;
}

.remove-btn {
  margin-left: 8px;
  cursor: pointer;
  color: #100f0f72;
  font-weight: bold;
}
</style>
