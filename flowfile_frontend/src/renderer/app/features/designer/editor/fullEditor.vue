<template>
  <div class="container">
    <div
      v-if="showSideBar"
      class="options-container"
      :style="{ width: treeNodeWidth }"
    >
      <sidebar v-model="optionSelection" :options="radioOptions" />
      <div class="divider" />
      <div class="selector">
        <column-selector
          v-if="optionSelection === 'fields'"
          @value-selected="handleNodeSelected"
        />
        <func-selector
          v-else
          ref="func-selector"
          @value-selected="handleNodeSelected"
        />
      </div>
    </div>
    <div class="resizer" @mousedown="initResize"></div>
    <div ref="editorWrapper" class="editor-wrapper">
      <sql-editor
        ref="sqlEditor"
        class="prism-editor-ref"
        :editor-string="code"
        @update-editor-string="handleCodeChange"
      />
    </div>
  </div>
  <instant-func-results
    ref="instantFuncResultsRef"
    :node-id="nodeStore.node_id"
  />
</template>

<script lang="ts" setup>
import {
  ref,
  Ref,
  defineExpose,
  defineProps,
  watch,
  onMounted,
  nextTick,
  computed,
} from "vue";
import ColumnSelector from "./ColumnSelector/columnsSelector.vue";
import Sidebar from "./Sidebar/Sidebar.vue";
import SqlEditor from "./SqlEditor.vue";
import { useNodeStore } from "../../../stores/column-store";
import InstantFuncResults from "./instantFuncResults.vue";
import debounce from "lodash/debounce";
import FuncSelector from "./FuncSelector/FuncSelector.vue";

const optionSelection = ref("");
const nodeStore = useNodeStore();

const radioOptions = [
  { value: "fields", text: "Fields", icon: "fa fa-columns" },
  { value: "functions", text: "Functions", icon: "fas fa-atom" },
];

const props = defineProps({
  editorString: { type: String, required: true },
});

const startX = ref(0);
const startWidth = ref(0);
const treeNodeWidth = ref("200px");

const instantFuncResultsRef = ref<Ref<typeof InstantFuncResults> | null>(null);
const code = ref(props.editorString);
nodeStore.setInputCode(props.editorString);

const sqlEditor = ref<typeof SqlEditor | null>(null);
const showTools: Ref<boolean> = ref(true);
const showHideOptions = () => {
  showTools.value = !showTools.value;
};

const showSideBar = computed(
  () => parseInt(treeNodeWidth.value.replace("px", "")) > 50,
);

const handleCodeChange = (newCode: string) => {
  code.value = newCode;
  nodeStore.setInputCode(newCode);
};

const resizeWidth = (event: MouseEvent) => {
  const deltaX = event.clientX - startX.value;
  const newWidth = startWidth.value + deltaX;
  treeNodeWidth.value = Math.min(newWidth, 300) + "px";
};

watch(
  code,
  debounce((newCode: string) => {
    if (instantFuncResultsRef.value) {
      instantFuncResultsRef.value.getInstantFuncResults(newCode);
    }
  }, 1500),
);

defineExpose({ showHideOptions, sqlEditor, showTools });
const handleNodeSelected = (nodeLabel: string) => {
  sqlEditor.value?.insertTextAtCursor(nodeLabel);
};

onMounted(async () => {
  await nextTick();
  if (instantFuncResultsRef.value) {
    instantFuncResultsRef.value.getInstantFuncResults(props.editorString);
  }
});

const initResize = (event: MouseEvent) => {
  startX.value = event.clientX;
  startWidth.value = parseInt(treeNodeWidth.value.replace("px", ""));
  document.addEventListener("mousemove", resizeWidth);
  document.addEventListener("mouseup", stopResize);
};

const stopResize = () => {
  document.removeEventListener("mousemove", resizeWidth);
  document.removeEventListener("mouseup", stopResize);
};
</script>

<style scoped>
.selector {
  overflow-y: scroll;
  max-height: 300px;
}

.container {
  display: flex;
  border: 1px solid #ccc;
  border-radius: 5px;
  overflow: hidden;
  height: 100%; /* Make it take full height */
  cursor: auto;
}

.options-container {
  flex-shrink: 0; /* Prevent shrinking */
  min-width: 50px; /* Minimum width */
  max-height: 300px; /* Maximum height */
  padding-left: 5px;
  padding-right: 5px;
  z-index: 1;
  overflow-y: auto; /* Make it scrollable */
}

.resizer {
  width: 5px;
  cursor: ew-resize; /* East-west resize cursor */
  background-color: #ddd;
  border-right: 0.5px solid #ccc;
  flex-shrink: 0; /* Prevent shrinking */
}

.editor-wrapper {
  flex-grow: 1; /* Take up the remaining space */
  flex-direction: column;
  overflow: hidden; /* Hide overflow */
}

.prism-editor-ref {
  flex: 1;
  padding: 1px;
  min-height: 0px; /* Important to make it respect the parent's height */
}

.error-box-wrapper {
  overflow-y: auto;
  border-top: 1px solid #ccc;
}

.divider {
  border-top: 1px solid #ccc;
  padding-bottom: 10px;
}
</style>
