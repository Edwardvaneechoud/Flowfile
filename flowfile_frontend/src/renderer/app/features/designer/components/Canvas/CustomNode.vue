<template>
  <div v-bind="$attrs">
    <div
      class="custom-node-header"
      data="description_display"
      @contextmenu="onTitleClick"
    >
      <div>
        <div v-if="!editMode">
          {{ descriptionSummary }}
        </div>
        <div
          v-else
          :id="props.data.id.toLocaleString()"
          class="custom-node-header"
          :style="overlayStyle"
          data="description_input"
        >
          <textarea
            :id="props.data.id.toLocaleString()"
            v-model="description"
            class="description-input"
            data="description_input"
            @blur="toggleEditMode(false)"
          ></textarea>
        </div>
      </div>
    </div>
    <div class="custom-node">
      <component :is="data.component" :node-id="data.id" />
      <div
        v-for="(input, index) in data.inputs"
        :key="input.id"
        class="handle-input"
        :style="getHandleStyle(index, data.inputs.length)"
      >
        <Handle :id="input.id" type="target" :position="input.position" />
      </div>
      <div
        v-for="(output, index) in data.outputs"
        :key="output.id"
        class="handle-output"
        :style="getHandleStyle(index, data.outputs.length)"
      >
        <Handle :id="output.id" type="source" :position="output.position" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { Handle } from "@vue-flow/core";
import { computed, ref, defineProps, onMounted, nextTick } from "vue";
import { useNodeStore } from "../../../../stores/column-store";

const nodeStore = useNodeStore();
const mouseX = ref<number>(0);
const mouseY = ref<number>(0);
const editMode = ref<boolean>(false);

const onTitleClick = (event: MouseEvent) => {
  console.log("Double clicked");
  console.log(event.clientX, event.clientY);
  toggleEditMode(true);
  mouseX.value = event.clientX;
  mouseY.value = event.clientY;
};

const handleClickOutside = (event: MouseEvent) => {
  const target = event.target as HTMLElement;
  const target_data = target.getAttribute("data");
  console.log(event);
  if (
    (target_data == "description_display" ||
      target_data == "description_input") &&
    target.id == props.data.id.toLocaleString()
  ) {
    return;
  } else if (editMode.value) {
    toggleEditMode(false);
  }
};

const toggleEditMode = (state: boolean) => {
  editMode.value = state;
  if (state) {
    window.addEventListener("click", handleClickOutside);
  }
  if (!state) {
    window.removeEventListener("click", handleClickOutside);
    nodeStore.setNodeDescription(props.data.id, description.value);
  }
};

const description = ref<string>("");

const getNodeDescription = async () => {
  description.value = await nodeStore.getNodeDescription(props.data.id);
};

const overlayStyle = computed(() => {
  const overlayWidth = 400; // Overlay width
  const overlayHeight = 200; // Overlay height
  const buffer = 100; // A small buffer distance from the cursor to the overlay

  let left = mouseX.value + buffer;
  let top = mouseY.value + buffer;

  // Ensuring the overlay doesn't go off the right edge of the viewport
  if (left + overlayWidth > window.innerWidth) {
    left -= overlayWidth + 2 * buffer; // Move it to the left of the cursor if it goes off the right
  }

  // Ensuring the overlay doesn't go off the bottom edge of the viewport
  if (top + overlayHeight > window.innerHeight) {
    top -= overlayHeight + 2 * buffer; // Move it above the cursor if it goes off the bottom
  }

  // Adjust if overlay goes off the left or top edge of the viewport (rare due to cursor positioning)
  left = Math.max(left, buffer); // Ensure it doesn't go off-screen to the left
  top = Math.max(top, buffer); // Ensure it doesn't go off-screen to the top

  return {
    top: `${top}px`,
    left: `${left}px`,
  };
});

const descriptionShort = computed(() => {
  return description.value.length > 12
    ? description.value.slice(0, 12) + "..."
    : description.value;
});

const descriptionSummary = computed(() => {
  return descriptionShort.value || `${props.data.id}: ${props.data.label}`;
});

const props = defineProps({
  data: {
    type: Object,
    required: true,
  },
});

function getHandleStyle(index: number, total: number) {
  const topMargin = 30; // Increase this value to have more margin from the top
  const bottomMargin = 25; // Adjust bottom margin if needed
  if (total === 1) {
    return {
      top: "55%",
      transform: "translateY(-55%)",
    };
  } else {
    const spacing = (100 - topMargin - bottomMargin) / (total - 1); // Calculate spacing
    return {
      top: `${topMargin + spacing * index}%`, // Apply top margin and spacing
    };
  }
}

onMounted(async () => {
  await nextTick();
  await getNodeDescription();
});
</script>

<style scoped>
.custom-node {
  border-radius: 4px;
  padding: 1px;
  background-color: white;
  display: flex;
  flex-direction: column;
  align-items: center;
  position: relative;
}

.selected {
  border: 2px solid #409eff;
}

.custom-node-header {
  font-weight: 100;
  font-size: small;
  width: 20px;
  white-space: nowrap; /* Prevents text from wrapping to the next line */
  overflow: visible; /* Ensures that overflowing text is hidden */
  text-overflow: ellipsis; /* Adds ellipsis (...) to the overflowing text */
}

.handle-input {
  position: absolute;
  left: -8px; /* Position the handles on the left */
}

.handle-output {
  position: absolute;
  right: -8px; /* Position the handles on the right */
}
</style>
