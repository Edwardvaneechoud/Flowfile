<template>
  <div class="popover-container">
    <div class="popover-reference" @mouseenter="showPopover" @mouseleave="hidePopover">
      <slot></slot>
    </div>
    <div
      v-if="visible"
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
  const popover = (event.currentTarget as HTMLElement).querySelector(".popover") as HTMLElement;
  const popoverRect = popover?.getBoundingClientRect();

  let top = "0px";
  let left = "0px";
  const offset = 10;

  // Removed lexical declaration in case block
  const referenceTop = referenceRect.top;
  const referenceBottom = referenceRect.bottom;
  const referenceLeft = referenceRect.left;
  const referenceRight = referenceRect.right;
  const referenceWidth = referenceRect.width;
  const referenceHeight = referenceRect.height;
  const popoverHeight = popoverRect?.height ?? 0;
  const popoverWidth = popoverRect?.width ?? 0;

  switch (props.placement) {
    case "top":
      top = `${referenceTop - popoverHeight - offset}px`;
      left = `${referenceLeft + referenceWidth / 2 - popoverWidth / 2}px`;
      break;
    case "bottom":
      top = `${referenceBottom + offset}px`;
      left = `${referenceLeft + referenceWidth / 2 - popoverWidth / 2}px`;
      break;
    case "left":
      top = `${referenceTop + referenceHeight / 2 - popoverHeight / 2}px`;
      left = `${referenceLeft - popoverWidth - offset}px`;
      break;
    case "right":
      top = `${referenceTop + referenceHeight / 2 - popoverHeight / 2}px`;
      left = `${referenceRight + offset}px`;
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
  min-width: v-bind(minWidth + "px"); /* Added min-width */
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
