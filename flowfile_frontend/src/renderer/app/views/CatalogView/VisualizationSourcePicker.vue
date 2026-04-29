<template>
  <el-dialog
    v-model="open"
    title="New chart"
    width="520px"
    append-to-body
    destroy-on-close
    :close-on-click-modal="false"
  >
    <div class="picker-body">
      <el-radio-group v-model="mode" size="default" class="picker-mode">
        <el-radio-button value="table">From a catalog table</el-radio-button>
        <el-radio-button value="sql">From SQL query</el-radio-button>
      </el-radio-group>

      <div v-if="mode === 'table'" class="picker-section">
        <label class="picker-label">Catalog → schema → table</label>
        <el-cascader
          v-model="selectedTablePath"
          :options="tableOptions"
          :props="cascaderProps"
          placeholder="Select a table"
          filterable
          clearable
          class="picker-cascader"
        />
        <p v-if="loadingTree" class="picker-hint">Loading catalog…</p>
        <p v-else-if="!tableOptions.length" class="picker-hint">
          No catalog tables yet. Register a flow output, or switch to "From SQL query".
        </p>
        <p v-else class="picker-hint">
          Charts read from the table directly — pick the source you want to chart.
        </p>
      </div>

      <div v-else class="picker-section">
        <label class="picker-label">SQL query</label>
        <el-input
          v-model="sql"
          type="textarea"
          :rows="6"
          placeholder="SELECT region, SUM(revenue) AS revenue FROM orders GROUP BY region"
          class="picker-sql"
        />
        <p class="picker-hint">
          Runs as an ad-hoc query. You can save the chart to a catalog after building it.
        </p>
      </div>
    </div>

    <template #footer>
      <el-button @click="open = false">Cancel</el-button>
      <el-button type="primary" :disabled="!canContinue" @click="onContinue"> Continue </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { NamespaceTree, VizSourceDescriptor } from "../../types";

const props = defineProps<{ modelValue: boolean }>();
const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "picked", source: VizSourceDescriptor): void;
}>();

const open = computed({
  get: () => props.modelValue,
  set: (v) => emit("update:modelValue", v),
});

const store = useCatalogStore();

type Mode = "table" | "sql";
const mode = ref<Mode>("table");
const selectedTablePath = ref<(number | string)[] | null>(null);
const sql = ref("");

interface CascadeOption {
  value: number;
  label: string;
  children?: CascadeOption[];
  disabled?: boolean;
}

// Namespaces use negative ids so they can never collide with positive table
// ids — combined with checkStrictly:false the cascader only commits to a leaf
// (i.e. an actual table).
const toCascade = (node: NamespaceTree): CascadeOption => {
  const childNs = node.children.map(toCascade);
  const tableLeaves: CascadeOption[] = node.tables.map((t) => ({
    value: t.id,
    label: t.name,
  }));
  const children = [...childNs, ...tableLeaves];
  return {
    value: -node.id,
    label: node.name,
    children: children.length ? children : undefined,
    disabled: !children.length,
  };
};

const tableOptions = computed<CascadeOption[]>(() => store.tree.map(toCascade));

// Tracks the in-flight loadTree() triggered when the dialog opens; goes back
// to false on success or failure so the empty-state hint can render.
const loadingTree = ref(false);

const cascaderProps = {
  checkStrictly: false,
  expandTrigger: "hover" as const,
};

watch(open, async (v) => {
  if (!v) return;
  // Reset state every time the dialog opens.
  mode.value = "table";
  selectedTablePath.value = null;
  sql.value = "";
  if (store.tree.length) return;
  loadingTree.value = true;
  try {
    await store.loadTree();
  } catch (err) {
    console.warn("[catalog] tree refresh failed", err);
  } finally {
    loadingTree.value = false;
  }
});

const canContinue = computed(() => {
  if (mode.value === "table") {
    const last = selectedTablePath.value?.[selectedTablePath.value.length - 1];
    return typeof last === "number" && last > 0;
  }
  return sql.value.trim().length > 0;
});

const onContinue = () => {
  if (mode.value === "table") {
    const last = selectedTablePath.value?.[selectedTablePath.value.length - 1];
    if (typeof last !== "number" || last <= 0) return;
    emit("picked", { source_type: "table", table_id: last });
  } else {
    const trimmed = sql.value.trim();
    if (!trimmed) return;
    emit("picked", { source_type: "sql", sql_query: trimmed });
  }
  open.value = false;
};
</script>

<style scoped>
.picker-body {
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.picker-mode {
  align-self: flex-start;
}
.picker-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.picker-label {
  font-size: 12px;
  color: var(--el-text-color-regular);
}
.picker-cascader {
  width: 100%;
}
.picker-sql :deep(.el-textarea__inner) {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 12.5px;
}
.picker-hint {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
