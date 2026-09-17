<template>
  <div v-if="dataLoaded && nodeFormula" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeFormula"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div v-if="nodeStore.is_loaded">
        <div class="formula-body">
          <editor-side-rail
            v-model:collapsed="railCollapsed"
            :table-schema="railSchema"
            :parameters="parameters"
            :collapsible="true"
            @value-selected="handleRailInsert"
          />
          <div class="formula-entries" @dragend="handleDragEnd">
            <formula-entry-row
              v-for="(entry, index) in entries"
              :key="entryUid(entry)"
              :ref="(el: any) => setRowRef(entryUid(entry), el)"
              :entry="entry"
              :index="index"
              :count="entries.length"
              :columns="columnsFor(index)"
              :data-types="dataTypes"
              :parameters="parameters"
              :issue="issues[index] ?? null"
              :duplicate-warning="duplicates.includes(index)"
              :active="index === activeIndex"
              :dragging="index === draggingIndex"
              :drop-before="index === dropIndex"
              :drop-after="dropIndex === entries.length && index === entries.length - 1"
              :collapsed="collapsedUids.has(entryUid(entry))"
              :autofocus="entries.length === 1"
              :editor-height="entries.length === 1 ? '250px' : 'auto'"
              @focus="focusRow(index)"
              @update-name="updateName(index, $event)"
              @update-data-type="updateDataType(index, $event)"
              @update-expression="updateExpression(index, $event)"
              @remove="removeEntry(index)"
              @move-up="moveEntry(index, 'up')"
              @move-down="moveEntry(index, 'down')"
              @toggle-collapsed="toggleCollapsed(index)"
              @expand="focusRow(index)"
              @dragstart="handleDragStart(index, $event)"
              @dragover.prevent="handleDragOver(index, $event)"
              @drop="handleDrop"
            />
            <div class="formula-footer">
              <button type="button" class="formula-quiet-button formula-add-row" @click="addEntry">
                + Add formula
              </button>
              <button
                v-if="entries.length > 1"
                type="button"
                class="formula-quiet-button formula-collapse-toggle"
                @click="allCollapsed ? expandAll() : collapseAll()"
              >
                {{ allCollapsed ? "Expand all" : "Collapse all" }}
              </button>
            </div>
          </div>
        </div>
        <instant-func-results
          ref="instantResultsRef"
          :node-id="nodeId"
          :fetcher="fetchInstantResult"
        />
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, nextTick, ref, watch } from "vue";
import { CodeLoader } from "vue-content-loader";
import debounce from "lodash/debounce";
import { useNodeStore } from "../../../../../stores/node-store";
import { useFlowStore } from "../../../../../stores/flow-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { NodeApi } from "../../../../../api/node.api";
import EditorSideRail from "../../../../../features/designer/editor/EditorSideRail.vue";
import InstantFuncResults from "../../../../../features/designer/editor/instantFuncResults.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import FormulaEntryRow from "./FormulaEntryRow.vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import type { FlowParameter } from "../../../../../types/flow.types";
import type {
  FormulaChainIssue,
  FormulaInput,
  InstantFuncResult,
  NodeFormula,
} from "../../../../../types";
import {
  AUTO_COLLAPSE_THRESHOLD,
  accumulatedColumnsAt,
  collapsedUidsFor,
  createFormulaInput,
  createFormulaNode,
  duplicateOutputPositions,
  entryUid,
  moveEntryByCommand,
  moveEntryTo,
  normalizeNodeFormula,
  railFoldPreference,
  resolveRailCollapsed,
  toChainEntries,
  toggledUid,
  withoutUid,
  toSavePayload,
  type FormulaColumn,
} from "./formula";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const dataLoaded = ref(false);
const nodeFormula = ref<NodeFormula | null>(null);
const nodeData = ref<null | NodeData>(null);
const nodeId = ref(0);

const dataTypes = [...nodeStore.getDataTypes(), "Auto"];
const parameters = computed<FlowParameter[]>(() => flowStore.parameters);

const activeIndex = ref(0);
const draggingIndex = ref<number | null>(null);
const dropIndex = ref<number | null>(null);
const issues = ref<(FormulaChainIssue | null)[]>([]);
/** UI-only, keyed by row uid: never saved, never persisted across drawer opens. */
const collapsedUids = ref<ReadonlySet<string>>(new Set());
const railCollapsed = ref(false);

/** Data types the chain validator resolved, keyed by column name. */
const resolvedTypes = ref<Record<string, string>>({});

const rowRefs = new Map<string, InstanceType<typeof FormulaEntryRow>>();
const instantResultsRef = ref<InstanceType<typeof InstantFuncResults> | null>(null);

const entries = computed(() => nodeFormula.value?.functions ?? []);

const baseColumns = computed<FormulaColumn[]>(() =>
  (nodeData.value?.main_input?.table_schema ?? []).map((column) => ({
    name: column.name,
    data_type: column.data_type,
  })),
);

const duplicates = computed(() => duplicateOutputPositions(entries.value));

// Names come from the local accumulation so autocomplete is instant; the
// validator only fills in the types it managed to resolve.
const columnsFor = (index: number): FormulaColumn[] =>
  accumulatedColumnsAt(baseColumns.value, entries.value, index).map((column) => ({
    name: column.name,
    data_type: column.data_type || (resolvedTypes.value[column.name] ?? ""),
  }));

const railSchema = computed(() => columnsFor(activeIndex.value));

const applyRailDefault = () => {
  railCollapsed.value = resolveRailCollapsed(railFoldPreference.value, entries.value.length);
};

watch(railCollapsed, (value) => {
  if (value !== resolveRailCollapsed(railFoldPreference.value, entries.value.length)) {
    railFoldPreference.value = value;
  }
});
watch(() => entries.value.length, applyRailDefault);

const allCollapsed = computed(
  () =>
    entries.value.length > 0 && entries.value.every((e) => collapsedUids.value.has(entryUid(e))),
);

const expandRow = async (index: number) => {
  const entry = entries.value[index];
  if (!entry) return;
  const uid = entryUid(entry);
  if (!collapsedUids.value.has(uid)) return;
  collapsedUids.value = withoutUid(collapsedUids.value, uid);
  await nextTick();
  rowRefs.get(uid)?.refreshEditor();
};

/** Focusing a row always expands it, so the rail and the editor can never target a hidden row. */
const focusRow = async (index: number) => {
  activeIndex.value = index;
  await expandRow(index);
};

const toggleCollapsed = (index: number) => {
  const entry = entries.value[index];
  if (!entry) return;
  const uid = entryUid(entry);
  collapsedUids.value = toggledUid(collapsedUids.value, uid);
  if (!collapsedUids.value.has(uid)) void focusRow(index);
};

const collapseAll = () => {
  collapsedUids.value = collapsedUidsFor(entries.value);
};

const expandAll = async () => {
  collapsedUids.value = new Set();
  await nextTick();
  for (const entry of entries.value) rowRefs.get(entryUid(entry))?.refreshEditor();
};

const setRowRef = (uid: string, el: InstanceType<typeof FormulaEntryRow> | null) => {
  if (el) rowRefs.set(uid, el);
  else rowRefs.delete(uid);
};

const handleRailInsert = async (label: string) => {
  const entry = entries.value[activeIndex.value];
  if (!entry) return;
  await expandRow(activeIndex.value);
  rowRefs.get(entryUid(entry))?.insertTextAtCursor(label);
};

const updateName = (index: number, name: string) => {
  entries.value[index].field.name = name;
};

const updateDataType = (index: number, dataType: string) => {
  entries.value[index].field.data_type = dataType;
};

const updateExpression = (index: number, expression: string) => {
  entries.value[index].function = expression;
};

const commitEntries = (next: FormulaInput[]) => {
  if (nodeFormula.value) nodeFormula.value.functions = next;
};

const addEntry = () => {
  if (!nodeFormula.value) return;
  nodeFormula.value.functions = [...entries.value, createFormulaInput()];
  void focusRow(entries.value.length - 1);
};

const removeEntry = (index: number) => {
  if (!nodeFormula.value || entries.value.length <= 1) return;
  nodeFormula.value.functions = entries.value.filter((_, i) => i !== index);
  activeIndex.value = Math.min(activeIndex.value, entries.value.length - 1);
};

/** A reorder is never blocked: a forward reference it creates surfaces as a row issue. */
const moveEntry = (index: number, command: "up" | "down") => {
  const moved = entries.value[index];
  commitEntries(moveEntryByCommand(entries.value, index, command));
  activeIndex.value = entries.value.indexOf(moved);
};

const handleDragStart = (index: number, event: DragEvent) => {
  draggingIndex.value = index;
  event.dataTransfer?.setData("text", "");
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
};

const handleDragOver = (index: number, event: DragEvent) => {
  if (draggingIndex.value === null) return;
  if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
  const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
  dropIndex.value = event.clientY < rect.top + rect.height / 2 ? index : index + 1;
};

const handleDrop = () => {
  if (draggingIndex.value !== null && dropIndex.value !== null) {
    const moved = entries.value[draggingIndex.value];
    commitEntries(moveEntryTo(entries.value, draggingIndex.value, dropIndex.value));
    activeIndex.value = entries.value.indexOf(moved);
  }
  handleDragEnd();
};

const handleDragEnd = () => {
  draggingIndex.value = null;
  dropIndex.value = null;
};

const fetchInstantResult = (): Promise<InstantFuncResult> =>
  NodeApi.getFormulaChainInstantResult(
    Number(nodeStore.flow_id),
    nodeId.value,
    toChainEntries(entries.value),
    activeIndex.value,
  );

const refreshInstantResult = () => {
  const entry = entries.value[activeIndex.value];
  instantResultsRef.value?.getInstantFuncResults(entry?.function ?? "", Number(nodeStore.flow_id));
};

let chainCheckSeq = 0;
const runChainCheck = async () => {
  const seq = ++chainCheckSeq;
  const count = entries.value.length;
  try {
    const result = await NodeApi.checkFormulaChain(
      Number(nodeStore.flow_id),
      nodeId.value,
      toChainEntries(entries.value),
    );
    if (seq !== chainCheckSeq) return;
    issues.value = result.entries.map((entry) => entry.issue ?? null);
    const types: Record<string, string> = {};
    for (const column of result.base_columns) types[column.name] = column.data_type;
    for (const entry of result.entries) {
      for (const column of entry.columns) types[column.name] = column.data_type;
    }
    resolvedTypes.value = types;
  } catch {
    // An older core has no chain validator; the drawer stays usable without it.
    if (seq === chainCheckSeq) issues.value = new Array(count).fill(null);
  }
};

const scheduleChainCheck = debounce(() => void runChainCheck(), 400);
const scheduleInstantResult = debounce(refreshInstantResult, 1500);

watch(
  entries,
  () => {
    scheduleChainCheck();
    scheduleInstantResult();
  },
  { deep: true },
);
watch(activeIndex, refreshInstantResult);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFormula,
  onBeforeSave: () => {
    if (!nodeFormula.value) return false;
    toSavePayload(nodeFormula.value);
    return true;
  },
});

const loadNodeData = async (id: number) => {
  nodeId.value = id;
  nodeData.value = await nodeStore.getNodeData(id, false);
  if (nodeData.value && nodeData.value.setting_input && nodeData.value.setting_input.is_setup) {
    nodeFormula.value = normalizeNodeFormula(nodeData.value.setting_input as NodeFormula);
  } else {
    nodeFormula.value = createFormulaNode(
      Number(nodeStore.flow_id),
      Number(nodeStore.node_id ?? id),
    );
    nodeFormula.value.depending_on_id = nodeData.value?.main_input?.node_id;
  }
  activeIndex.value = 0;
  issues.value = [];
  applyRailDefault();
  // A long chain opens folded down to the row in focus; a short one opens flat.
  collapsedUids.value =
    entries.value.length >= AUTO_COLLAPSE_THRESHOLD
      ? collapsedUidsFor(entries.value, entryUid(entries.value[0]))
      : new Set();
  dataLoaded.value = true;
  await nextTick();
  void runChainCheck();
  refreshInstantResult();
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.formula-body {
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: 5px;
  overflow: hidden;
  background-color: var(--color-background-primary);
}

.formula-entries {
  flex-grow: 1;
  min-width: 0;
}

.formula-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 32px;
  margin-top: 8px;
  border-top: 1px solid var(--color-border-primary);
  padding: 0 12px;
}

.formula-quiet-button {
  padding: 2px 4px;
  font-size: 0.75rem;
  color: var(--color-text-tertiary);
  background: transparent;
  border: none;
  border-radius: 3px;
  cursor: pointer;
}

.formula-quiet-button:hover {
  color: var(--color-text-primary);
  background: var(--color-background-tertiary);
}
</style>
