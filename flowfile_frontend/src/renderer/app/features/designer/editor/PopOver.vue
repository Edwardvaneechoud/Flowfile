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

    <!-- The portal target for the popover -->
    <Teleport v-if="visible" to="body">
      <!-- Attach a ref to the popover itself -->
      <div
        ref="popoverEl"
        :style="popoverStyle"
        class="popover"
        :class="{ 'popover--left': props.placement === 'left' }"
      >
        <h3 v-if="props.title !== ''">{{ props.title }}</h3>
        <p class="content" v-html="props.content"></p>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, nextTick, onMounted } from "vue";

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
  zIndex: {
    type: Number,
    default: 9999,
  },
});

const popoverStyle = ref({
  top: "0px",
  left: "0px",
  zIndex: props.zIndex.toString(),
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
      left = referenceRect.left + referenceRect.width / 2 - popoverRect.width / 2;
      break;
    case "bottom":
      top = referenceRect.bottom + offset;
      left = referenceRect.left + referenceRect.width / 2 - popoverRect.width / 2;
      break;
    case "left":
      top = referenceRect.top + referenceRect.height / 2 - popoverRect.height / 2;
      left = referenceRect.left - popoverRect.width - offset;
      break;
    case "right":
      top = referenceRect.top + referenceRect.height / 2 - popoverRect.height / 2;
      left = referenceRect.right + offset;
      break;
  }

  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;

  // Adjust horizontal position if needed
  if (left < 10) left = 10;
  if (left + popoverRect.width > viewportWidth - 10) {
    left = viewportWidth - popoverRect.width - 10;
  }

  // Adjust vertical position if needed
  if (top < 10) top = 10;
  if (top + popoverRect.height > viewportHeight - 10) {
    top = viewportHeight - popoverRect.height - 10;
  }

  popoverStyle.value = {
    top: `${top}px`,
    left: `${left}px`,
    zIndex: props.zIndex.toString(),
  };
};

// Listen for window resize to update popover position
onMounted(() => {
  window.addEventListener("resize", () => {
    if (visible.value) {
      updatePosition();
    }
  });
});
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
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  min-width: v-bind('props.minWidth + "px"');
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
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.popover p {
  margin: 0;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.content {
  white-space: pre-wrap;
}
</style>
