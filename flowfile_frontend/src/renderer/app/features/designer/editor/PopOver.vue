<template>
  <div class="popover-container">
    <!-- Attach a ref to the reference element -->
    <div
      ref="referenceEl"
      class="popover-reference"
      @mouseenter="showPopover"
      @mouseleave="hidePopover"
    >
      <slot></slot>
    </div>
    <!-- Attach a ref to the popover itself -->
    <div
      v-if="visible"
      ref="popoverEl"
      :style="popoverStyle"
      class="popover"
      :class="{ 'popover--left': props.placement === 'left' }"
    >
      <h3 v-if="props.title !== ''">{{ props.title }}</h3>
      <p class="content" v-html="props.content"></p>
    </div>
  </div>
</template>
<script setup lang="ts">
import { ref, nextTick } from "vue";

const visible = ref(false);
const referenceEl = ref<HTMLElement | null>(null);
const popoverEl = ref<HTMLElement | null>(null);

const props = defineProps({
  content: {
    type: String,
    required: true,
  },
  title: {
    type: String,
    default: "",
  },
  placement: {
    type: String as () => "top" | "bottom" | "left" | "right",
    default: "top",
  },
  minWidth: {
    type: Number,
    default: 100,
  },
});

const popoverStyle = ref({
  top: "0px",
  left: "0px",
});

const showPopover = () => {
  visible.value = true;
  nextTick(() => {
    updatePosition();
  });
};

const hidePopover = () => {
  visible.value = false;
};

const updatePosition = () => {
  if (!referenceEl.value || !popoverEl.value) return;

  const referenceRect = referenceEl.value.getBoundingClientRect();
  const popoverRect = popoverEl.value.getBoundingClientRect();
  const offset = 20; // Increased offset for a lower popover position

  let top = 0;
  let left = 0;

  switch (props.placement) {
    case "top":
      top = referenceRect.top - popoverRect.height - offset;
      left = referenceRect.left + (referenceRect.width / 2) - (popoverRect.width / 2);
      break;
    case "bottom":
      top = referenceRect.bottom + offset;
      left = referenceRect.left + (referenceRect.width / 2) - (popoverRect.width / 2);
      break;
    case "left":
      top = referenceRect.top + (referenceRect.height / 2) - (popoverRect.height / 2);
      left = referenceRect.left - popoverRect.width - offset;
      break;
    case "right":
      top = referenceRect.top + (referenceRect.height / 2) - (popoverRect.height / 2);
      left = referenceRect.right + offset;
      break;
  }

  popoverStyle.value = { top: `${top}px`, left: `${left}px` };
};
</script>

<style scoped>
.popover-container {
  position: relative;
  display: inline-block;
}

.popover {
  position: fixed;
  padding: 10px;
  background-color: #fff;
  border: 0.5px solid #ccc;
  border-radius: 4px;
  z-index: 10000;
  min-width: 100px; /* You can also bind this dynamically if needed */
}

.popover--left {
  min-width: 200px;
}

.popover-reference {
  cursor: pointer;
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
