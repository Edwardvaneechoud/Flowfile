<template>
  <div class="component-wrapper">
    <div class="status-indicator" :class="nodeResult?.statusIndicator">
      <span class="tooltip-text">{{ tooltipContent }}</span>
    </div>
    <button :class="['node-button', { selected: isSelected }]" @click="onClick">
      <img :src="getImageUrl(props.imageSrc)" :alt="props.title" width="50" />
    </button>
  </div>
  <div
    v-if="editMode"
    :id="props.nodeId.toLocaleString()"
    class="overlay"
    :style="overlayStyle"
    data="description_input"
  >
    <div class="overlay-content">
      <p
        :id="props.nodeId.toLocaleString()"
        class="overlay-prompt"
        data="description_input"
      >
        Provide a description for the node:
      </p>
      <textarea
        :id="props.nodeId.toLocaleString()"
        v-model="description"
        class="description-input"
        data="description_input"
        @blur="toggleEditMode(false)"
      ></textarea>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  defineProps,
  defineEmits,
  computed,
  onMounted,
  ref,
  nextTick,
  watch,
  onUnmounted,
} from "vue";
import { getImageUrl } from "../utils";
import { useNodeStore } from "../../../stores/column-store";
const description = ref<string>("");
const mouseX = ref(0);
const mouseY = ref(0);
const editMode = ref(false); // Add this line
const nodeStore = useNodeStore();
const props = defineProps({
  nodeId: { type: Number, required: true },
  imageSrc: { type: String, required: true },
  title: { type: String, required: true },
});

const isSelected = computed(() => {
  return nodeStore.node_id == props.nodeId;
});

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

const handleClickOutside = (event: MouseEvent) => {
  const target = event.target as HTMLElement;
  const target_data = target.getAttribute("data");
  if (
    (target_data == "description_display" ||
      target_data == "description_input") &&
    target.id == props.nodeId.toLocaleString()
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
    nodeStore.setNodeDescription(props.nodeId, description.value);
  }
};

interface ResultOutput {
  success: boolean;
  statusIndicator: "success" | "failure" | "unkown" | "warning";
  error?: string;
  hasRun: boolean;
}
const nodeResult = computed<ResultOutput | undefined>(() => {
  const nodeResult = nodeStore.runNodeResultMap.get(props.nodeId);
  const nodeValidation = nodeStore.getNodeValidation(props.nodeId);

  if (nodeResult && nodeValidation) {
    // Case 1: nodeResult is success, nodeValidation is not success, and validation is after result -> warning (orange)
    if (
      nodeResult.success &&
      !nodeValidation.isValid &&
      nodeValidation.validationTime > nodeResult.start_timestamp
    ) {
      return {
        success: nodeResult.success,
        statusIndicator: "warning", // Orange -> warning
        error: nodeValidation.error,
        hasRun: true,
      };
    }
    // Case 2: nodeResult and nodeValidation both success -> success (green)
    if (nodeResult.success && nodeValidation.isValid) {
      return {
        success: true,
        statusIndicator: "success", // Green -> success
        error: nodeResult.error || nodeValidation.error,
        hasRun: true,
      };
    }
    // Case 3: nodeResult fail, nodeValidation success, validation after result -> unkown (white)
    if (
      !nodeResult.success &&
      nodeValidation.isValid &&
      nodeValidation.validationTime > nodeResult.start_timestamp
    ) {
      return {
        success: nodeResult.success,
        statusIndicator: "unkown", // White -> unkown
        error: nodeResult.error || nodeValidation.error,
        hasRun: true,
      };
    }
    // Case 4: nodeResult fail, nodeValidation empty or fail -> failure (red)
    if (
      !nodeResult.success &&
      (!nodeValidation.isValid || !nodeValidation.validationTime)
    ) {
      return {
        success: false,
        statusIndicator: "failure", // Red -> failure
        error: nodeResult.error || nodeValidation.error,
        hasRun: true,
      };
    }
  }
  // Case 5: nodeResult empty, nodeValidation fail -> warning (orange)
  if (!nodeResult && nodeValidation && !nodeValidation.isValid) {
    return {
      success: false,
      statusIndicator: "warning", // Orange -> warning
      error: nodeValidation.error,
      hasRun: false,
    };
  }
  // Case 6: nodeResult empty, nodeValidation success -> unkown (white)
  if (!nodeResult && nodeValidation && nodeValidation.isValid) {
    return {
      success: true,
      statusIndicator: "unkown", // White -> unkown
      error: nodeValidation.error,
      hasRun: false,
    };
  }
  return undefined; // Default case
});

const tooltipContent = computed(() => {
  switch (nodeResult.value?.statusIndicator) {
    case "success":
      return "Operation successful";
    case "failure":
      return (
        "Operation failed: \n" +
        (nodeResult.value?.error || "No error message available")
      );
    case "warning":
      return (
        "Operation warning: \n" +
        (nodeResult.value?.error || "No warning message available")
      );
    case "unkown":
    default:
      return "Status unknown";
  }
});

const getNodeDescription = async () => {
  description.value = await nodeStore.getNodeDescription(props.nodeId);
};

const emits = defineEmits(["click"]);

const onClick = () => {
  emits("click");
};

onMounted(() => {
  watch(
    () => props.nodeId,
    async (newVal) => {
      if (newVal !== -1) {
        // Assuming -1 is an uninitialized state
        await nextTick();
        getNodeDescription();
      }
    },
  );
  //window.addEventListener('click', handleClickOutside)
});

onUnmounted(() => {
  //window.removeEventListener('click', handleClickOutside)
});
</script>

<style scoped>
.status-indicator {
  position: relative;
  display: flex; /* Use flex here if needed */
  align-items: center; /* Align the indicator vertically */
  margin-right: 8px; /* Adjust based on your layout */
}

.status-indicator::before {
  content: "";
  display: block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

.status-indicator.success::before {
  background-color: #4caf50;
}

.status-indicator.failure::before {
  background-color: #f44336;
}

.status-indicator.warning::before {
  background-color: #f09f5dd1;
}

.status-indicator.unknown::before {
  background-color: #ffffff;
}

.tooltip-text {
  visibility: hidden;
  width: 120px;
  background-color: black;
  color: #fff;
  text-align: center;
  border-radius: 6px;
  padding: 5px 0;
  position: absolute;
  z-index: 1;
  bottom: 100%;
  left: 50%;
  margin-left: -60px;
  opacity: 0;
  transition: opacity 0.3s;
}

.status-indicator:hover .tooltip-text {
  visibility: visible;
  opacity: 1;
}

.description-input:hover,
.description-input:focus {
  background-color: #e7e7e7; /* Slightly darker on hover/focus */
  box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.2); /* More pronounced shadow on hover/focus */
}
.component-wrapper {
  position: relative; /* This makes the absolute positioning of the child relative to this container */
  max-width: 60px;
  overflow: visible; /* Allows children to visually overflow */
}

.description-display {
  padding: 8px;
  width: 200px !important;
  max-height: 8px !important;
  background-color: #ffffff; /* Light grey background */
  border-radius: 4px; /* Rounded corners for a modern look */
  cursor: pointer; /* Indicates the text can be clicked */
}

.overlay {
  position: fixed; /* This is key for viewport-level positioning */
  width: 200px; /* Or whatever width you prefer */
  height: 200px; /* Or whatever height you prefer */
  left: 50%; /* Center horizontally */
  top: 50%; /* Center vertically */
  transform: translate(-50%, -50%); /* Adjust based on its own dimensions */
  z-index: 1000; /* High enough to float above everything else */
  /* Your existing styles for background, padding, etc. */
}

.node-button {
  background-color: #ffffff;
  border-radius: 10px;
  border-width: 0.5px;
}

.overlay-content {
  padding: 20px;
  border-radius: 10px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
  align-items: stretch;
}

.overlay-prompt {
  margin-bottom: 10px;
  color: #333;
  font-size: 16px;
}

.description-input {
  margin-bottom: 10px;
  border: 1px solid #ccc;
  border-radius: 4px;
  padding: 10px;
  font-size: 14px;
  height: 100px; /* Adjust based on your needs */
}

.selected {
  border: 2px solid #828282;
}
</style>
