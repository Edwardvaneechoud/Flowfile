<template>
  <div class="popover-container">
    <div class="popover-reference" @mouseenter="showPopover" @mouseleave="hidePopover">
      <slot></slot>
    </div>
    <div v-if="visible" :style="popoverStyle" class="popover" :class="{ 'popover--left': placement === 'left' }">
      <h3 v-if="title !== ''">{{ title }}</h3>
      <p class="content" v-html="content"></p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, nextTick, computed } from "vue";

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
  placement: {
    type: String as () => "top" | "bottom" | "left" | "right",
    default: "top",
  },
  minWidth: {
    type: Number,
    default: 100 // Default min-width
  }
});

const { content, title, placement, minWidth } = props;


const popoverStyle = ref({
  top: "0px",
  left: "0px",
});

const showPopover = (event: MouseEvent) => {
  visible.value = true;
  nextTick(() => {
    updatePosition(event);
  });
};

const hidePopover = () => {
  visible.value = false;
};

const updatePosition = (event: MouseEvent) => {
  const referenceRect = (event.target as HTMLElement).getBoundingClientRect();
  const popover = (event.currentTarget as HTMLElement).querySelector('.popover') as HTMLElement;
  const popoverRect = popover?.getBoundingClientRect();


  let top = "0px";
  let left = "0px";
    const offset = 10; // Distance between popover and reference element

  switch (placement) {
    case "top":
      top = `${referenceRect.top - (popoverRect?.height ?? 0) - offset}px`;
      left = `${referenceRect.left + referenceRect.width / 2 - (popoverRect?.width ?? 0) / 2}px`;
      break;
    case "bottom":
      top = `${referenceRect.bottom + offset}px`;
      left = `${referenceRect.left + referenceRect.width / 2 - (popoverRect?.width ?? 0) / 2}px`;
      break;
      case "left":
        top = `${referenceRect.top + referenceRect.height / 2 - (popoverRect?.height ?? 0) / 2}px`;

        // Calculate left with an offset to prevent being at exactly 0
        const leftOffset = 200; // Adjust as needed
        left = `${referenceRect.left - (popoverRect?.width ?? 0) - offset - leftOffset}px`;
      break;
    case "right":
      top = `${referenceRect.top + referenceRect.height / 2 - (popoverRect?.height ?? 0) / 2}px`;
      left = `${referenceRect.right + offset}px`;
      break;
  }

  popoverStyle.value = { top, left };
};
</script>

<style scoped>
.popover-container {
  position: relative;
  display: inline-block; /* Ensure container takes up space */
}

.popover {
  position: fixed;
  padding: 10px;
  background-color: #fff;
  border: 0.5px solid #ccc;
  border-radius: 4px;
  z-index: 10000;
  min-width: v-bind(minWidth+"px");/* Added min-width */
}

.popover--left {
    min-width: 200px; /* Specific width when positioned left */
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
