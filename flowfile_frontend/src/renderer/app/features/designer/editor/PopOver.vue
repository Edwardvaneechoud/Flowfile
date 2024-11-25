<template>
  <div class="popover-container">
    <div
      class="popover-reference"
      @mouseenter="showPopover"
      @mouseleave="hidePopover"
    >
      <slot></slot>
    </div>
    <div v-if="visible" :style="popoverStyle" class="popover">
      <h3 v-if="props.title !== ''">{{ props.title }}</h3>
      <p class="content" v-html="props.content"></p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";

const visible = ref(false);

const props = defineProps({
  content: {
    type: String,
    required: true,
  },
  title: {
    type: String,
    default: "",
  },
});

const popoverStyle = ref({
  top: "0px",
  left: "0px",
});

const showPopover = (event: MouseEvent) => {
  visible.value = true;
  updatePosition(event);
};

const hidePopover = () => {
  visible.value = false;
};

const updatePosition = (event: MouseEvent) => {
  const { clientX, clientY } = event;
  popoverStyle.value = {
    top: `${clientY + 10}px`,
    left: `${clientX + 10}px`,
  };
};
</script>

<style scoped>
.popover-container {
  position: relative;
}

.popover-reference {
  cursor: pointer;
}

.popover {
  position: fixed;
  padding: 10px;
  background-color: #fff;
  border: 0.5px solid #ccc;
  border-radius: 4px;
  z-index: 10000;
}

.popover h3 {
  margin: 0 0 2px;
  font-size: 16px;
}

.popover p {
  margin: 0;
}

.content {
  white-space: pre-wrap;
}
</style>
