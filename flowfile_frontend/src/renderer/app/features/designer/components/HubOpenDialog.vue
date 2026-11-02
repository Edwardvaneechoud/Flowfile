<template>
  <el-dialog
    v-model="isVisible"
    title="Open from hub"
    width="min(820px, calc(100vw - 32px))"
    :close-on-click-modal="false"
    custom-class="high-z-index-dialog"
  >
    <div class="hub-open-body">
      <div class="hub-open-pane">
        <label class="pane-label">Namespace</label>
        <!-- Mounted per open, so the tree is fetched afresh each time. -->
        <HubNamespaceTree
          v-if="isVisible"
          :model-value="selectedNamespaceId"
          data-testid="hub-open-namespaces"
          @update:model-value="selectNamespace"
        />
      </div>
      <div class="hub-open-pane">
        <label class="pane-label">Flows</label>
        <p v-if="loadingFlows" class="hub-dialog-status">Loading…</p>
        <p v-else-if="selectedNamespaceId === null" class="hub-dialog-status">
          Select a namespace to browse its flows.
        </p>
        <p v-else-if="!flows.length" class="hub-dialog-status">No flows in this namespace.</p>
        <ul v-else class="hub-open-list" data-testid="hub-open-flows">
          <li
            v-for="flow in flows"
            :key="flow.flowUuid"
            class="hub-open-row"
            :class="{ selected: selectedFlow?.flowUuid === flow.flowUuid }"
            @click="selectFlow(flow)"
            @dblclick="open()"
          >
            <span class="material-icons">description</span>
            <span class="hub-open-row-name">{{ flow.name }}</span>
            <span class="hub-open-row-meta">{{ latestVersionLabel(flow.latestVersion) }}</span>
          </li>
        </ul>
      </div>
      <div class="hub-open-pane">
        <label class="pane-label">Version</label>
        <p v-if="loadingVersions" class="hub-dialog-status">Loading…</p>
        <p v-else-if="!selectedFlow" class="hub-dialog-status">Select a flow.</p>
        <p v-else-if="!versions.length" class="hub-dialog-status">
          This flow has no published version yet.
        </p>
        <ul v-else class="hub-open-list" data-testid="hub-open-versions">
          <li
            v-for="version in versions"
            :key="version.version"
            class="hub-open-row hub-open-row--version"
            :class="{ selected: selectedVersion === version.version }"
            @click="selectedVersion = version.version"
            @dblclick="open()"
          >
            <span class="hub-open-row-name">v{{ version.version }}</span>
            <span class="hub-open-row-meta">{{ authorLabel(version) }}</span>
            <span class="hub-open-row-meta">{{ publishedAt(version) }}</span>
            <span v-if="version.message" class="hub-open-row-message">{{ version.message }}</span>
          </li>
        </ul>
      </div>
    </div>
    <p v-if="error" class="hub-dialog-error" data-testid="hub-open-error">{{ error }}</p>

    <template #footer>
      <div class="dialog-footer">
        <el-button size="small" :disabled="busy" @click="isVisible = false">Cancel</el-button>
        <el-button
          type="primary"
          size="small"
          :loading="busy"
          :disabled="!canOpen"
          data-testid="hub-open"
          @click="open()"
        >
          Open
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessageBox } from "element-plus";
import {
  listHubFlowVersions,
  listHubFlows,
  openFromHub,
  refusalOf,
  type HubFlow,
  type HubFlowVersion,
} from "../../../api/hubFlows.api";
import { detailOf } from "../../../utils/apiDetail";
import { authorLabel, latestVersionLabel, refusalMessage } from "../hubFlows";
import HubNamespaceTree from "./HubNamespaceTree.vue";

const props = defineProps<{ visible: boolean }>();

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
  (e: "open-flow", payload: { message: string; flowPath: string }): void;
}>();

const isVisible = ref(props.visible);
const flows = ref<HubFlow[]>([]);
const versions = ref<HubFlowVersion[]>([]);
const selectedNamespaceId = ref<number | null>(null);
const selectedFlow = ref<HubFlow | null>(null);
const selectedVersion = ref<number | null>(null);
const loadingFlows = ref(false);
const loadingVersions = ref(false);
const busy = ref(false);
const error = ref("");

const canOpen = computed(() => !!selectedFlow.value && versions.value.length > 0 && !busy.value);

watch(
  () => props.visible,
  (v) => {
    isVisible.value = v;
    if (v) reset();
  },
);

watch(isVisible, (v) => {
  if (v !== props.visible) emit("update:visible", v);
});

const publishedAt = (version: HubFlowVersion) => new Date(version.publishedAt).toLocaleString();

function reset() {
  error.value = "";
  flows.value = [];
  versions.value = [];
  selectedNamespaceId.value = null;
  selectedFlow.value = null;
  selectedVersion.value = null;
}

async function selectNamespace(id: number | null) {
  selectedNamespaceId.value = id;
  if (id === null) {
    flows.value = [];
    return;
  }
  await loadFlows(id);
}

async function loadFlows(namespaceId: number) {
  error.value = "";
  selectedFlow.value = null;
  versions.value = [];
  loadingFlows.value = true;
  try {
    flows.value = await listHubFlows(namespaceId);
  } catch (err) {
    flows.value = [];
    error.value = detailOf(err, "Couldn't list the hub's flows.");
  } finally {
    loadingFlows.value = false;
  }
}

async function selectFlow(flow: HubFlow) {
  selectedFlow.value = flow;
  selectedVersion.value = null;
  versions.value = [];
  error.value = "";
  loadingVersions.value = true;
  try {
    versions.value = await listHubFlowVersions(flow.flowUuid);
    selectedVersion.value = versions.value[0]?.version ?? null;
  } catch (err) {
    error.value = detailOf(err, "Couldn't list the flow's versions.");
  } finally {
    loadingVersions.value = false;
  }
}

async function open(replace = false) {
  if (!selectedFlow.value || selectedVersion.value === null) return;
  busy.value = true;
  error.value = "";
  const flowUuid = selectedFlow.value.flowUuid;
  const version = selectedVersion.value;
  try {
    const opened = await openFromHub({ flowUuid, version, replace });
    emit("open-flow", { message: "Flow opened from hub", flowPath: opened.flowPath });
    isVisible.value = false;
  } catch (err) {
    const refusal = refusalOf(err);
    if (refusal?.kind === "local_copy_exists" && !replace) {
      busy.value = false;
      await confirmReplace(refusal.flowPath, version);
      return;
    }
    error.value = refusal ? refusalMessage(refusal) : detailOf(err, "Couldn't open the flow.");
  } finally {
    busy.value = false;
  }
}

async function confirmReplace(flowPath: string, version: number) {
  try {
    await ElMessageBox.confirm(
      `A local copy of this flow exists at ${flowPath}. Replace it with v${version} from the hub? Edits you have not published are lost.`,
      "Replace the local copy?",
      { type: "warning", confirmButtonText: "Replace", cancelButtonText: "Cancel" },
    );
  } catch {
    return;
  }
  await open(true);
}
</script>

<style scoped src="./hubDialogs.css"></style>
