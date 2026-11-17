<template>
  <el-dialog
    :model-value="modelValue"
    title="Table settings"
    width="560px"
    append-to-body
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-form label-width="110px" size="small" @submit.prevent>
      <el-form-item label="Source">
        <TileSourcePicker
          :viz-id="draft.viz_id"
          :table-id="draft.table_id"
          @change="onSourceChange"
        />
        <div class="table-hint">
          Shows the rows of this table, or of the chart's data source, with the dashboard filters
          applied.
        </div>
      </el-form-item>
      <el-form-item label="Columns">
        <el-select
          v-model="chosenFields"
          multiple
          filterable
          :disabled="!hasSource"
          :loading="fieldsLoading"
          :placeholder="fieldsError ?? 'All columns'"
          data-testid="table-columns"
        >
          <el-option v-for="f in fields" :key="f.fid" :value="f.fid" :label="f.fid">
            <span class="field-opt">
              <span>{{ f.fid }}</span>
              <span class="field-opt-dtype">{{ semanticLabel(f.semanticType) }}</span>
            </span>
          </el-option>
        </el-select>
        <div class="table-hint">
          {{
            draft.config.columns.length
              ? "Shown in the order picked."
              : "Every column shows. Pick columns to choose their order, labels and number formats."
          }}
        </div>
      </el-form-item>
      <el-form-item v-if="draft.config.columns.length" label="Labels & formats">
        <div class="column-rows">
          <div v-for="col in draft.config.columns" :key="col.field" class="column-row">
            <span class="column-name" :title="col.field">{{ col.field }}</span>
            <el-input v-model="col.label" :placeholder="col.field" maxlength="120" clearable />
            <template v-if="isNumeric(col.field)">
              <el-select
                :model-value="col.decimals ?? AUTO_DECIMALS"
                class="column-decimals"
                title="Decimals"
                @update:model-value="(v: number) => (col.decimals = v === AUTO_DECIMALS ? null : v)"
              >
                <el-option :value="AUTO_DECIMALS" label="Auto" />
                <el-option v-for="d in DECIMAL_CHOICES" :key="d" :value="d" :label="`${d} dp`" />
              </el-select>
              <el-checkbox v-model="col.thousands" title="Thousands separators">1,000</el-checkbox>
            </template>
          </div>
        </div>
      </el-form-item>
      <el-form-item label="Default sort">
        <div class="sort-row">
          <el-select
            v-model="draft.config.sort_by"
            multiple
            filterable
            :disabled="!hasSource"
            placeholder="Source order"
          >
            <el-option v-for="f in fields" :key="f.fid" :value="f.fid" :label="f.fid" />
          </el-select>
          <el-radio-group
            v-model="draft.config.sort_direction"
            :disabled="!draft.config.sort_by.length"
          >
            <el-radio-button value="ascending">Ascending</el-radio-button>
            <el-radio-button value="descending">Descending</el-radio-button>
          </el-radio-group>
        </div>
        <div class="table-hint">Viewers can sort by any column by clicking its header.</div>
      </el-form-item>
      <el-form-item label="Rows per page">
        <el-select v-model="draft.config.page_size" class="page-size">
          <el-option v-for="n in TABLE_PAGE_SIZES" :key="n" :value="n" :label="String(n)" />
        </el-select>
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="emit('update:modelValue', false)">Cancel</el-button>
      <el-button type="primary" :disabled="!hasSource" @click="onSave">Save</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { useDashboardVizCache } from "../../composables/useDashboardVizCache";
import type { TileField } from "../../composables/useDashboardComputation";
import type { DashboardTableColumn, DashboardTableConfig, DashboardTile } from "../../types";
import TileSourcePicker from "./TileSourcePicker.vue";
import { TABLE_PAGE_SIZES, tableConfigOf } from "./tableTile";

const props = defineProps<{
  modelValue: boolean;
  tile: DashboardTile;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (
    e: "save",
    patch: { viz_id: number | null; table_id: number | null; table: DashboardTableConfig },
  ): void;
}>();

// el-select treats null as "no selection", so auto decimals uses a sentinel.
const AUTO_DECIMALS = -1;
const DECIMAL_CHOICES = [0, 1, 2, 3, 4, 5, 6];

const vizCache = useDashboardVizCache();

const draft = reactive<{
  viz_id: number | null;
  table_id: number | null;
  config: DashboardTableConfig;
}>({ viz_id: null, table_id: null, config: tableConfigOf(null) });

const hasSource = computed(() => draft.viz_id != null || draft.table_id != null);

const fields = ref<TileField[]>([]);
const fieldsLoading = ref(false);
const fieldsError = ref<string | null>(null);
let fieldsSeq = 0;

const loadFields = async () => {
  const seq = ++fieldsSeq;
  fields.value = [];
  fieldsError.value = null;
  if (!hasSource.value) return;
  fieldsLoading.value = true;
  try {
    const resp =
      draft.table_id != null
        ? await vizCache.getTableFields(draft.table_id)
        : await vizCache.getFields(draft.viz_id as number);
    if (seq !== fieldsSeq) return;
    fields.value = (resp.fields ?? []).map((f) => ({
      fid: String(f.fid),
      semanticType: f.semanticType,
    }));
  } catch {
    if (seq === fieldsSeq) fieldsError.value = "Could not load columns";
  } finally {
    if (seq === fieldsSeq) fieldsLoading.value = false;
  }
};

const onOpen = () => {
  draft.viz_id = props.tile.viz_id ?? null;
  draft.table_id = props.tile.table_id ?? null;
  const saved = tableConfigOf(props.tile.table);
  draft.config = {
    ...saved,
    columns: saved.columns.map((c) => ({ thousands: true, ...c })),
    sort_by: [...saved.sort_by],
  };
  void loadFields();
};

// Mounted already open (v-if + v-model in one tick), so el-dialog's @open never fires.
watch(
  () => props.modelValue,
  (open) => {
    if (open) onOpen();
  },
  { immediate: true },
);

// Another source has other columns, so the picks start over.
const onSourceChange = (next: { viz_id: number | null; table_id: number | null }) => {
  draft.viz_id = next.viz_id;
  draft.table_id = next.table_id;
  draft.config = { ...draft.config, columns: [], sort_by: [] };
  void loadFields();
};

const chosenFields = computed<string[]>({
  get: () => draft.config.columns.map((c) => c.field),
  set: (picked) => {
    const kept = new Map(draft.config.columns.map((c) => [c.field, c]));
    draft.config.columns = picked.map(
      (field): DashboardTableColumn => kept.get(field) ?? { field, thousands: true },
    );
  },
});

const isNumeric = (field: string) =>
  fields.value.find((f) => f.fid === field)?.semanticType === "quantitative";

const SEMANTIC_LABEL: Record<string, string> = {
  quantitative: "number",
  nominal: "text",
  ordinal: "text",
  temporal: "date",
};
const semanticLabel = (s?: string) => (s ? (SEMANTIC_LABEL[s] ?? s) : "");

const onSave = () => {
  if (!hasSource.value) return;
  emit("save", {
    viz_id: draft.viz_id,
    table_id: draft.table_id,
    table: {
      ...draft.config,
      columns: draft.config.columns.map((c) => ({
        field: c.field,
        label: c.label?.trim() || null,
        decimals: c.decimals ?? null,
        thousands: c.thousands !== false,
      })),
    },
  });
  emit("update:modelValue", false);
};
</script>

<style scoped>
.table-hint {
  width: 100%;
  margin-top: 4px;
  font-size: 11px;
  line-height: 1.4;
  color: var(--el-text-color-secondary);
}
.field-opt {
  display: flex;
  justify-content: space-between;
  gap: 12px;
}
.field-opt-dtype {
  color: var(--el-text-color-secondary);
  font-family: var(--el-font-family-monospace, monospace);
  font-size: 11px;
}
.column-rows {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
  max-height: 220px;
  overflow-y: auto;
}
.column-row {
  display: grid;
  grid-template-columns: 110px minmax(0, 1fr) 84px auto;
  align-items: center;
  gap: 8px;
}
.column-name {
  font-size: 12px;
  color: var(--el-text-color-regular);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.column-decimals {
  width: 84px;
}
.sort-row {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.page-size {
  width: 110px;
}
</style>
