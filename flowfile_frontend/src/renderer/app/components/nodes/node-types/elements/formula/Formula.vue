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
              :overwrites="overwrites.get(index) ?? null"
              :active="index === activeIndex"
              :dragging="index === draggingIndex"
              :drop-before="index === dropIndex"
              :drop-after="dropIndex === entries.length && index === entries.length - 1"
              :collapsed="collapsedUids.has(entryUid(entry))"
              :autofocus="entries.length === 1"
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
              <div class="formula-footer-tools">
                <button
                  v-if="issueRows.length"
                  type="button"
                  class="formula-quiet-button formula-issue-jump"
                  @click="jumpToIssue"
                >
                  {{ issueSummary }}
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
        </div>
        <instant-func-results
          ref="instantResultsRef"
          :node-id="nodeId"
          :fetcher="fetchInstantResult"
          :label="entries[activeIndex]?.field.name"
        />
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts">
// Session-scoped on purpose: the settings drawer remounts this component on every
// node switch, so an instance ref would forget the user's fold choice each time.
let railFoldPreference: boolean | null = null;
</script>

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
import type { FormulaChainIssue, InstantFuncResult, NodeFormula } from "../../../../../types";
import { applyReorder, moveItems } from "../../../baseNode/selectComponents/columnSelection";
import {
  AUTO_COLLAPSE_THRESHOLD,
  accumulatedColumnsAt,
  createFormulaInput,
  createFormulaNode,
  overwrittenOutputs,
  entryUid,
  normalizeNodeFormula,
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
const collapsedUids = ref(new Set<string>());
const railCollapsed = ref(false);

const rowRefs = new Map<string, InstanceType<typeof FormulaEntryRow>>();

const instantResultsRef = ref<InstanceType<typeof InstantFuncResults> | null>(null);

const entries = computed(() => nodeFormula.value?.functions ?? []);

const baseColumns = computed<FormulaColumn[]>(() =>
  (nodeData.value?.main_input?.table_schema ?? []).map((column) => ({
    name: column.name,
    data_type: column.data_type,
  })),
);

const overwrites = computed(() => overwrittenOutputs(entries.value));

const columnsFor = (index: number): FormulaColumn[] =>
  accumulatedColumnsAt(baseColumns.value, entries.value, index);

const railSchema = computed(() => columnsFor(activeIndex.value));

/** Rows with an error, in order; the footer offers a jump through them. Overwrites don't count. */
const issueRows = computed(() =>
  entries.value.flatMap((_, index) => {
    const issue = issues.value[index];
    return issue && issue.kind !== "duplicate" ? [index] : [];
  }),
);

const issueSummary = computed(() => {
  const count = issueRows.value.length;
  return count === 1 ? "1 error · jump to it" : `${count} errors · jump to next`;
});

watch(railCollapsed, (value) => {
  railFoldPreference = value;
});

const allCollapsed = computed(
  () =>
    entries.value.length > 0 && entries.value.every((e) => collapsedUids.value.has(entryUid(e))),
);

const expandRow = async (index: number) => {
  const entry = entries.value[index];
  if (!entry || !collapsedUids.value.delete(entryUid(entry))) return;
  await nextTick();
  rowRefs.get(entryUid(entry))?.refreshEditor();
};

/** Focusing a row always expands it, so the rail and the editor can never target a hidden row. */
const focusRow = async (index: number) => {
  activeIndex.value = index;
  await expandRow(index);
};

const jumpToIssue = async () => {
  const rows = issueRows.value;
  const index = rows.find((row) => row > activeIndex.value) ?? rows[0];
  if (index === undefined) return;
  await focusRow(index);
  rowRefs.get(entryUid(entries.value[index]))?.scrollIntoView();
};

const toggleCollapsed = (index: number) => {
  const entry = entries.value[index];
  if (!entry) return;
  const uid = entryUid(entry);
  if (collapsedUids.value.has(uid)) void focusRow(index);
  else collapsedUids.value.add(uid);
};

const collapseAll = () => {
  for (const entry of entries.value) collapsedUids.value.add(entryUid(entry));
};

const expandAll = async () => {
  collapsedUids.value.clear();
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
  if (!nodeFormula.value) return;
  const moved = entries.value[index];
  nodeFormula.value.functions = applyReorder(entries.value, [index], command).items;
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
  if (nodeFormula.value && draggingIndex.value !== null && dropIndex.value !== null) {
    const moved = entries.value[draggingIndex.value];
    nodeFormula.value.functions = moveItems(
      entries.value,
      [draggingIndex.value],
      dropIndex.value,
    ).items;
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
    entries.value,
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
      entries.value,
    );
    if (seq !== chainCheckSeq) return;
    issues.value = result.entries.map((entry) => entry.issue ?? null);
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
  railCollapsed.value = railFoldPreference ?? false;
  // A long chain opens folded down to the row in focus; a short one opens flat.
  collapsedUids.value.clear();
  if (entries.value.length >= AUTO_COLLAPSE_THRESHOLD) {
    for (const entry of entries.value.slice(1)) collapsedUids.value.add(entryUid(entry));
  }
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
  display: flex;
  flex-direction: column;
  min-height: 300px;
  flex-grow: 1;
  min-width: 0;
}

.formula-footer {
  flex-shrink: 0;
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

.formula-footer-tools {
  display: flex;
  align-items: center;
  gap: 4px;
}

.formula-issue-jump,
.formula-issue-jump:hover {
  color: var(--color-danger);
}
</style>
