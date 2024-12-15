<template>
  <div v-bind="$attrs">
    <div class="custom-node-header" data="description_display" @contextmenu="onTitleClick">
      <div>
        <div v-if="!editMode" class="description-display" :style="descriptionTextStyle">
          <div class="edit-icon" title="Edit description" @click.stop="toggleEditMode(true)">
            <svg
              width="12"
              height="12"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
            </svg>
          </div>
          <pre class="description-text">{{ descriptionSummary }}</pre>
          <span v-if="isTruncated" class="truncated-indicator" title="Click to see full description"
            >...</span
          >
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
import { computed, ref, defineProps, onMounted, nextTick, watch } from "vue";
import { useNodeStore } from "../../../../stores/column-store";

const nodeStore = useNodeStore();
const mouseX = ref<number>(0);
const mouseY = ref<number>(0);
const editMode = ref<boolean>(false);

const CHAR_LIMIT = 100;

const onTitleClick = (event: MouseEvent) => {
  console.log("Double clicked");
  console.log(event.clientX, event.clientY);
  toggleEditMode(true);
  mouseX.value = event.clientX;
  mouseY.value = event.clientY;
};

const descriptionTextStyle = computed(() => {
  const textLength = description.value.length;
  let minWidth = '200px'; // default

  if (textLength < 20) {
    minWidth = '100px';
  } else if (textLength < 30) {
    minWidth = '150px';
  }
  return {
    minWidth: minWidth,
  };
});

const handleClickOutside = (event: MouseEvent) => {
  const target = event.target as HTMLElement;
  const target_data = target.getAttribute("data");
  console.log(event);
  if (
    (target_data == "description_display" || target_data == "description_input") &&
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
  const overlayWidth = 400;
  const overlayHeight = 200;
  const buffer = 100;

  let left = mouseX.value + buffer;
  let top = mouseY.value + buffer;

  if (left + overlayWidth > window.innerWidth) {
    left -= overlayWidth + 2 * buffer;
  }

  if (top + overlayHeight > window.innerHeight) {
    top -= overlayHeight + 2 * buffer;
  }

  left = Math.max(left, buffer);
  top = Math.max(top, buffer);

  return {
    top: `${top}px`,
    left: `${left}px`,
  };
});

const isTruncated = computed(() => {
  return description.value.length > CHAR_LIMIT;
});

const descriptionSummary = computed(() => {
  if (!description.value) {
    return `${props.data.id}: ${props.data.label}`;
  }

  if (isTruncated.value) {
    const truncatePoint = description.value.lastIndexOf(" ", CHAR_LIMIT);
    const endPoint = truncatePoint > 0 ? truncatePoint : CHAR_LIMIT;
    return description.value.substring(0, endPoint);
  }

  return description.value;
});

const props = defineProps({
  data: {
    type: Object,
    required: true,
  },
});


function getHandleStyle(index: number, total: number) {
  const topMargin = 30;
  const bottomMargin = 25;
  if (total === 1) {
    return {
      top: "55%",
      transform: "translateY(-55%)",
    };
  } else {
    const spacing = (100 - topMargin - bottomMargin) / (total - 1);
    return {
      top: `${topMargin + spacing * index}%`,
    };
  }
}

onMounted(async () => {
  await nextTick();
  await getNodeDescription();

watch(
  () => nodeStore.nodeDescriptions[props.data.id],
  (newDescription) => {
    if (newDescription !== undefined) {
      description.value = newDescription;
    }
  }
);


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
  white-space: nowrap;
  overflow: visible;
  text-overflow: ellipsis;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}


.description-display {
  position: relative;
  white-space: normal;
  min-width: 100px; /* Default minimum width */
  max-width: 300px;
  width: auto; /* Allow dynamic width */
  padding: 2px 4px;
  cursor: pointer;
  background-color: rgba(185, 185, 185, 0.117);
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
  display: flex;
  align-items: flex-start;
  gap: 4px;
  border-radius: 4px; 
}

.edit-icon {
  opacity: 0;
  transition: opacity 0.2s;
  color: #0f275f;
  cursor: pointer;
  display: flex;
  align-items: center;
  padding: 2px;
}

.description-display:hover .edit-icon {
  opacity: 1;
}

.edit-icon:hover {
  color: #051233;
}

.description-text {
  margin: 0;
  white-space: pre-wrap;
  word-wrap: break-word;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
}

.edit-overlay {
  position: fixed;
  z-index: 1000;
  background: white;
  border-radius: 4px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.description-input {
  width: 200px;
  height: 75px;
  resize: both;
  padding: 4px;
  border: 1px solid #0f275f;
  border-radius: 4px;
  font-size: small;
  font-family: "Roboto", "Source Sans Pro", Avenir, Helvetica, Arial, sans-serif;
  background-color: white;
}

.handle-input {
  position: absolute;
  left: -8px;
}

.handle-output {
  position: absolute;
  right: -8px;
}
</style>
