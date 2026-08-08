<template>
  <el-dialog
    :model-value="modelValue"
    title="Prune versions"
    width="460px"
    append-to-body
    @update:model-value="emit('update:modelValue', $event)"
    @open="reset"
  >
    <div class="dialog-warning">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <span>
        Pruning permanently deletes the data files of the older versions of
        <strong>{{ artifactName }}</strong
        >. The newest version is always kept. Run a dry run first.
      </span>
    </div>
    <div class="dialog-field">
      <label class="dialog-label">Versions to keep</label>
      <el-input-number v-model="keep" :min="1" size="small" />
      <p class="dialog-hint">
        This model has {{ totalVersions }} version{{ totalVersions === 1 ? "" : "s" }}.
        <span v-if="localEstimate !== null">
          Keeping {{ keep }} would delete {{ localEstimate.count }} ({{
            formatSize(localEstimate.freedBytes)
          }}).
        </span>
      </p>
    </div>
    <div class="dialog-field">
      <el-checkbox v-model="dryRun" size="small">
        Dry run (list versions only, delete nothing)
      </el-checkbox>
    </div>
    <div v-if="result" class="dialog-result">
      <i class="fa-solid fa-circle-check"></i>
      {{ result.deleted > 0 ? "Deleted" : "Would delete" }}
      {{ resultCount }} version{{ resultCount === 1 ? "" : "s" }} ({{
        formatSize(result.freed_bytes)
      }}).
    </div>
    <template #footer>
      <el-button size="small" @click="emit('update:modelValue', false)">Close</el-button>
      <el-button size="small" :type="dryRun ? 'primary' : 'danger'" :loading="running" @click="run">
        {{ dryRun ? "Run dry run" : "Delete versions" }}
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import type { ArtifactPruneResult, ArtifactVersionInfo } from "../../types";
import { useCatalogStore } from "../../stores/catalog-store";
import { formatSize } from "./catalog-formatters";
import { pruneCandidates } from "./artifactVersions";

const props = defineProps<{
  modelValue: boolean;
  artifactName: string;
  totalVersions: number;
  versions: ArtifactVersionInfo[];
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "pruned"): void;
}>();

const store = useCatalogStore();

const keep = ref(5);
const dryRun = ref(true);
const running = ref(false);
const result = ref<ArtifactPruneResult | null>(null);

function reset() {
  keep.value = 5;
  dryRun.value = true;
  result.value = null;
}

// Only meaningful while the loaded page covers the whole history; otherwise the
// server dry run is the only honest preview.
const localEstimate = computed((): { count: number; freedBytes: number } | null => {
  if (props.versions.length < props.totalVersions) return null;
  const { toDelete, freedBytes } = pruneCandidates(props.versions, keep.value);
  return { count: toDelete.length, freedBytes };
});

const resultCount = computed((): number =>
  result.value ? result.value.deleted || result.value.would_delete.length : 0,
);

async function run() {
  running.value = true;
  try {
    result.value = await store.pruneArtifactVersions(keep.value, dryRun.value);
    if (!dryRun.value) {
      ElMessage.success(`Deleted ${result.value.deleted} versions`);
      emit("pruned");
    }
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Prune failed");
  } finally {
    running.value = false;
  }
}
</script>

<style scoped>
.dialog-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-bottom: 12px;
}

.dialog-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}

.dialog-hint {
  margin: 0;
  font-size: 11px;
  color: var(--color-text-tertiary);
}

.dialog-warning {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  margin-bottom: 12px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 4px;
  font-size: 12px;
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.dialog-warning i {
  color: var(--color-warning, #f59e0b);
  margin-top: 2px;
}

.dialog-result {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--color-success, #22c55e);
  margin-top: 4px;
}
</style>
