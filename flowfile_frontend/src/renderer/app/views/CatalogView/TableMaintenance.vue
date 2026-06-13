<template>
  <span class="maintenance-actions">
    <button class="action-btn-lg" :disabled="!table.file_exists" @click="openOptimize">
      <i class="fa-solid fa-compress"></i>
      Optimize
    </button>
    <button class="action-btn-lg" :disabled="!table.file_exists" @click="openVacuum">
      <i class="fa-solid fa-broom"></i>
      Vacuum
    </button>

    <!-- Optimize dialog -->
    <el-dialog v-model="showOptimize" title="Optimize table" width="460px" append-to-body>
      <p class="dialog-help">
        Compacts small Delta files into larger ones. Optionally Z-order by columns to co-locate
        related data for faster filtered reads.
      </p>
      <div class="dialog-field">
        <label class="dialog-label">Z-order columns (optional)</label>
        <el-select
          v-model="zOrderColumns"
          multiple
          filterable
          clearable
          size="small"
          placeholder="Compact only (no Z-order)"
        >
          <el-option v-for="col in columnNames" :key="col" :label="col" :value="col" />
        </el-select>
      </div>
      <div v-if="optimizeResult" class="dialog-result">
        <i class="fa-solid fa-circle-check"></i>
        Done. New size: {{ formatSize(optimizeResult.size_bytes) }}.
        <span v-if="optimizeFilesAdded !== null"> Files written: {{ optimizeFilesAdded }}.</span>
      </div>
      <template #footer>
        <el-button size="small" @click="showOptimize = false">Close</el-button>
        <el-button size="small" type="primary" :loading="optimizing" @click="runOptimize">
          Run optimize
        </el-button>
      </template>
    </el-dialog>

    <!-- Vacuum dialog -->
    <el-dialog v-model="showVacuum" title="Vacuum table" width="460px" append-to-body>
      <div class="dialog-warning">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>
          Vacuum permanently deletes data files from old versions. Time-travel reads
          (<code>delta_version</code>) of vacuumed versions will no longer work. Run a dry run
          first.
        </span>
      </div>
      <div class="dialog-field">
        <label class="dialog-label">Retention (hours)</label>
        <el-input-number v-model="retentionHours" :min="0" :step="24" size="small" />
        <p v-if="retentionHours < 168" class="dialog-hint">
          Below Delta's 168h (7-day) minimum — the retention guard is relaxed for this run.
        </p>
      </div>
      <div class="dialog-field">
        <el-checkbox v-model="dryRun" size="small">
          Dry run (list files only, delete nothing)
        </el-checkbox>
      </div>
      <div v-if="vacuumResult" class="dialog-result">
        <i class="fa-solid fa-circle-check"></i>
        {{ vacuumResult.dry_run ? "Would remove" : "Removed" }}
        {{ vacuumResult.file_count }} file{{ vacuumResult.file_count !== 1 ? "s" : "" }}.
        <span v-if="!vacuumResult.dry_run"
          >New size: {{ formatSize(vacuumResult.size_bytes) }}.</span
        >
      </div>
      <template #footer>
        <el-button size="small" @click="showVacuum = false">Close</el-button>
        <el-button
          size="small"
          :type="dryRun ? 'primary' : 'danger'"
          :loading="vacuuming"
          @click="runVacuum"
        >
          {{ dryRun ? "Run dry run" : "Run vacuum" }}
        </el-button>
      </template>
    </el-dialog>
  </span>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { ElMessage } from "element-plus";
import type { CatalogTable, OptimizeTableResponse, VacuumTableResponse } from "../../types";
import { useCatalogStore } from "../../stores/catalog-store";
import { formatSize } from "./catalog-formatters";

const props = defineProps<{ table: CatalogTable }>();
const store = useCatalogStore();

const columnNames = computed(() => props.table.schema_columns.map((c) => c.name));

const showOptimize = ref(false);
const zOrderColumns = ref<string[]>([]);
const optimizing = ref(false);
const optimizeResult = ref<OptimizeTableResponse | null>(null);

const optimizeFilesAdded = computed(() => {
  const m = optimizeResult.value?.metrics as Record<string, unknown> | undefined;
  const v = m?.numFilesAdded ?? m?.num_files_added;
  return typeof v === "number" ? v : null;
});

function openOptimize() {
  optimizeResult.value = null;
  zOrderColumns.value = [];
  showOptimize.value = true;
}

async function runOptimize() {
  optimizing.value = true;
  try {
    optimizeResult.value = await store.optimizeTable(
      props.table.id,
      zOrderColumns.value.length ? zOrderColumns.value : null,
    );
    ElMessage.success("Table optimized");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Optimize failed");
  } finally {
    optimizing.value = false;
  }
}

const showVacuum = ref(false);
const retentionHours = ref(168);
const dryRun = ref(true);
const vacuuming = ref(false);
const vacuumResult = ref<VacuumTableResponse | null>(null);

function openVacuum() {
  vacuumResult.value = null;
  retentionHours.value = 168;
  dryRun.value = true;
  showVacuum.value = true;
}

async function runVacuum() {
  vacuuming.value = true;
  try {
    vacuumResult.value = await store.vacuumTable(
      props.table.id,
      retentionHours.value,
      dryRun.value,
    );
    ElMessage.success(dryRun.value ? "Dry run complete" : "Table vacuumed");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Vacuum failed");
  } finally {
    vacuuming.value = false;
  }
}
</script>

<style scoped>
.maintenance-actions {
  display: contents;
}

/* Match TableDetailPanel's .action-btn-lg (scoped CSS there doesn't reach this child). */
.action-btn-lg {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: transparent;
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.action-btn-lg:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}
.action-btn-lg:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.dialog-help {
  margin: 0 0 12px;
  font-size: 12px;
  color: var(--color-text-secondary);
  line-height: 1.4;
}

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
