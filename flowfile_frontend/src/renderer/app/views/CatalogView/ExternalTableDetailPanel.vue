<template>
  <div class="external-table-detail" data-testid="external-table-detail">
    <button class="back-btn" @click="emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>

    <div class="detail-header">
      <div class="header-title">
        <i class="fa-solid fa-table header-icon"></i>
        <h2>{{ name }}</h2>
        <ExternalCatalogBadge v-if="catalog?.external" :external="catalog.external" />
      </div>
      <p class="reference mono">{{ reference }}</p>
    </div>

    <div v-if="catalogStore.externalTableError" class="overview-error" role="alert">
      <span>{{ catalogStore.externalTableError }}</span>
      <button class="btn btn-secondary btn-sm" @click="load">Retry</button>
    </div>
    <div v-else-if="!detail" class="loading-state">Asking {{ catalogName }} for this table…</div>

    <template v-else>
      <div class="meta-grid">
        <div class="meta-card">
          <span class="meta-label">Format</span>
          <span class="meta-value">Iceberg, version {{ detail.format_version }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Location</span>
          <span class="meta-value mono">{{ detail.location }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Partitioned by</span>
          <span class="meta-value">{{
            detail.partitioned_by.join(", ") || "Not partitioned"
          }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Last changed</span>
          <span class="meta-value" data-testid="external-last-changed">{{ lastChanged }}</span>
          <span v-if="lastCommit" class="meta-sub" data-testid="external-last-commit">{{
            lastCommit
          }}</span>
          <span v-if="snapshots" class="meta-sub" data-testid="external-snapshots">{{
            snapshots
          }}</span>
        </div>
      </div>

      <div class="lineage" data-testid="external-lineage">
        <div class="lineage-row">
          <span class="meta-label">Read by</span>
          <span v-if="readBy.length === 0" class="lineage-none" data-testid="external-read-by">
            No flow here reads it yet.
          </span>
          <span v-else class="lineage-flows" data-testid="external-read-by">
            <button
              v-for="flow in readBy"
              :key="flow.id"
              class="flow-link"
              @click="emit('navigateToFlow', flow.id)"
            >
              {{ flow.name }}
            </button>
          </span>
        </div>
        <div class="lineage-row">
          <span class="meta-label">Written by</span>
          <span v-if="!writtenBy" class="lineage-none" data-testid="external-written-by">
            No flow here has written it.
          </span>
          <span v-else class="lineage-flows" data-testid="external-written-by">
            <button class="flow-link" @click="emit('navigateToFlow', writtenBy?.id)">
              {{ writtenBy.name }}
            </button>
          </span>
        </div>
      </div>

      <p class="kept-line" data-testid="external-kept-line">
        Kept by {{ catalogName }}. Flowfile shows its columns from that catalog's own description;
        rows are read when a flow runs.
      </p>

      <div class="section">
        <h3>Columns</h3>
        <table class="styled-table" data-testid="external-columns">
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="column in detail.columns" :key="column.name">
              <td class="col-name">{{ column.name }}</td>
              <td class="col-type mono">{{ column.dtype }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="section">
        <div class="snippet-header">
          <h3>Read it in a flow</h3>
          <button
            class="btn btn-secondary btn-sm"
            data-testid="external-copy-snippet"
            @click="copy"
          >
            <i class="fa-solid fa-copy"></i> Copy
          </button>
        </div>
        <pre class="snippet" data-testid="external-snippet">{{ snippet }}</pre>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, watch } from "vue";
import { ElMessage } from "element-plus";
import type { NamespaceTree } from "../../types";
import { useCatalogStore } from "../../stores/catalog-store";
import { copyTextEverywhere } from "../../utils/clipboardUtils";
import ExternalCatalogBadge from "./ExternalCatalogBadge.vue";
import { changedLines, readSnippet, snapshotsLine } from "./externalCatalog";

const props = defineProps<{
  catalog: NamespaceTree | null;
  catalogId: number;
  schemaId: number;
  name: string;
}>();
const emit = defineEmits(["close", "navigateToFlow"]);

const catalogStore = useCatalogStore();
const detail = computed(() => catalogStore.externalTable);
const catalogName = computed(() => props.catalog?.name ?? "the other catalog");
const schemaName = computed(
  () => props.catalog?.children.find((child) => child.id === props.schemaId)?.name ?? "",
);
const reference = computed(
  () => detail.value?.reference ?? `${catalogName.value}.${schemaName.value}.${props.name}`,
);
const snippet = computed(() => readSnippet(catalogName.value, schemaName.value, props.name));
const timeCard = computed(() =>
  changedLines(
    detail.value?.updated_at,
    detail.value?.last_commit_at,
    detail.value?.snapshot_count,
  ),
);
const lastChanged = computed(() => timeCard.value.changed);
const lastCommit = computed(() => timeCard.value.commit);
const snapshots = computed(() => snapshotsLine(detail.value?.snapshot_count));
const readBy = computed(() => detail.value?.read_by_flows ?? []);
const writtenBy = computed(() => detail.value?.written_by ?? null);

function load() {
  void catalogStore.loadExternalTable(props.catalogId, props.schemaId, props.name);
}

async function copy() {
  const done = await copyTextEverywhere(snippet.value);
  if (done) ElMessage.success("Copied");
}

watch(() => [props.catalogId, props.schemaId, props.name], load);
onMounted(load);
</script>

<style scoped>
.external-table-detail {
  max-width: 1000px;
  margin: 0 auto;
}

.detail-header {
  margin-bottom: var(--spacing-5);
}

.header-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.header-icon {
  font-size: 20px;
  color: var(--el-color-success, #67c23a);
}

.header-title h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
}

.reference {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.kept-line {
  margin: var(--spacing-4) 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.meta-sub {
  display: block;
  margin-top: var(--spacing-1);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.lineage {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
}

.lineage-row {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-3);
}

.lineage-row .meta-label {
  min-width: 90px;
}

.lineage-none {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.lineage-flows {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2);
}

.flow-link {
  padding: 0;
  border: none;
  background: none;
  font-size: var(--font-size-sm);
  color: var(--color-primary);
  cursor: pointer;
}

.flow-link:hover {
  text-decoration: underline;
}

.col-name {
  font-weight: var(--font-weight-medium);
}

.snippet-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.snippet {
  margin: var(--spacing-2) 0 0;
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md);
  background: var(--color-background-tertiary);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
