<template>
  <el-dialog
    :model-value="modelValue"
    title="Add a join"
    width="560px"
    append-to-body
    @update:model-value="emit('update:modelValue', $event)"
    @open="reset"
  >
    <p class="convention">
      The <strong>from</strong> side has many rows for each one row on the <strong>to</strong> side.
      For example, many orders belong to one customer: from <code>orders</code>, to
      <code>customers</code>.
    </p>

    <el-form label-position="top" @submit.prevent>
      <el-form-item label="Name">
        <el-input v-model="name" placeholder="orders_to_customers" />
      </el-form-item>
      <div class="sides">
        <el-form-item label="From table (many rows)">
          <el-select
            v-model="fromId"
            filterable
            placeholder="Table"
            @change="pairs = [emptyPair()]"
          >
            <el-option v-for="t in tables" :key="t.id" :label="t.label" :value="t.id" />
          </el-select>
        </el-form-item>
        <el-form-item label="To table (one row)">
          <el-select v-model="toId" filterable placeholder="Table" @change="pairs = [emptyPair()]">
            <el-option v-for="t in tables" :key="t.id" :label="t.label" :value="t.id" />
          </el-select>
        </el-form-item>
      </div>
      <el-form-item label="Matching columns">
        <div v-for="(pair, i) in pairs" :key="i" class="pair">
          <el-select v-model="pair.from" filterable placeholder="From column" :disabled="!fromId">
            <el-option v-for="c in columnsOf(fromId)" :key="c" :label="c" :value="c" />
          </el-select>
          <i class="fa-solid fa-arrow-right pair-arrow"></i>
          <el-select v-model="pair.to" filterable placeholder="To column" :disabled="!toId">
            <el-option v-for="c in columnsOf(toId)" :key="c" :label="c" :value="c" />
          </el-select>
          <button
            v-if="pairs.length > 1"
            type="button"
            class="pair-remove"
            title="Remove"
            @click="pairs.splice(i, 1)"
          >
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <button type="button" class="btn btn-secondary btn-sm" @click="pairs.push(emptyPair())">
          <i class="fa-solid fa-plus"></i> Add column pair
        </button>
      </el-form-item>
      <el-form-item label="Description (optional)">
        <el-input v-model="description" type="textarea" :rows="2" />
      </el-form-item>
    </el-form>

    <p v-if="error" class="form-error" role="alert">{{ error }}</p>

    <template #footer>
      <button class="btn btn-secondary" @click="emit('update:modelValue', false)">Cancel</button>
      <button class="btn btn-primary" :disabled="!valid || saving" @click="save">
        {{ saving ? "Saving…" : "Add join" }}
      </button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import { useCatalogStore } from "../../../stores/catalog-store";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import { errorText } from "./semanticFormat";

const props = defineProps<{ modelValue: boolean; namespaceId: number }>();
const emit = defineEmits<(e: "update:modelValue", value: boolean) => void>();

interface Pair {
  from: string;
  to: string;
}
const emptyPair = (): Pair => ({ from: "", to: "" });

const catalogStore = useCatalogStore();
const store = useSemanticModelStore();

const name = ref("");
const fromId = ref<number | null>(null);
const toId = ref<number | null>(null);
const pairs = ref<Pair[]>([emptyPair()]);
const description = ref("");
const saving = ref(false);
const error = ref("");

const tables = computed(() => {
  const roots = catalogStore.tree.filter(
    (c) => c.id === props.namespaceId || c.children.some((s) => s.id === props.namespaceId),
  );
  return roots.flatMap((catalog) =>
    catalog.children.flatMap((schema) =>
      (schema.tables ?? []).map((t) => ({
        id: t.id,
        label: `${schema.name}.${t.name}`,
        columns: (t.schema_columns ?? []).map((c) => c.name),
      })),
    ),
  );
});

const columnsOf = (id: number | null) => tables.value.find((t) => t.id === id)?.columns ?? [];

const valid = computed(
  () =>
    name.value.trim() !== "" &&
    fromId.value !== null &&
    toId.value !== null &&
    pairs.value.every((p) => p.from && p.to),
);

function reset() {
  name.value = "";
  fromId.value = null;
  toId.value = null;
  pairs.value = [emptyPair()];
  description.value = "";
  error.value = "";
}

async function save() {
  if (!valid.value || fromId.value === null || toId.value === null) return;
  saving.value = true;
  error.value = "";
  try {
    const facts = await store.propose([
      {
        kind: "join",
        name: name.value.trim(),
        from_table_id: fromId.value,
        to_table_id: toId.value,
        from_columns: pairs.value.map((p) => p.from),
        to_columns: pairs.value.map((p) => p.to),
        description: description.value.trim() || null,
      },
    ]);
    if (facts.some((f) => f.status !== "accepted")) {
      ElMessage.info("Proposed. It waits in the Review tab.");
    }
    emit("update:modelValue", false);
  } catch (e) {
    error.value = errorText(e, "Could not add the join");
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped>
.convention {
  margin: 0 0 var(--spacing-4);
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.sides {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-3);
}

.pair {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  width: 100%;
  margin-bottom: var(--spacing-2);
}

.pair-arrow {
  color: var(--color-text-muted);
}

.pair-remove {
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
}

.form-error {
  margin: 0;
  color: var(--color-danger);
  font-size: var(--font-size-sm);
}
</style>
