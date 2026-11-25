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
        <button
          type="button"
          class="swap-sides"
          title="Swap sides"
          aria-label="Swap sides"
          data-testid="semantic-join-swap"
          :disabled="fromId === null && toId === null"
          @click="swapSides"
        >
          <i class="fa-solid fa-right-left"></i>
        </button>
        <el-form-item label="To table (one row)">
          <el-select v-model="toId" filterable placeholder="Table" @change="pairs = [emptyPair()]">
            <el-option v-for="t in tables" :key="t.id" :label="t.label" :value="t.id" />
          </el-select>
        </el-form-item>
      </div>
      <el-form-item label="Matching columns">
        <div v-for="(pair, i) in pairs" :key="i" class="line pair">
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
            class="icon-btn pair-remove"
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

    <div v-if="error" class="error form-error" role="alert">
      <p>{{ error }}</p>
      <button
        v-if="reversed"
        type="button"
        class="btn btn-secondary btn-sm"
        data-testid="semantic-join-swap-retry"
        :disabled="saving"
        @click="swapAndRetry"
      >
        <i class="fa-solid fa-right-left"></i> Swap sides and try again
      </button>
    </div>

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
import { useCatalogStore } from "../../../stores/catalog-store";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import { errorText } from "../../../types/semantic.types";
import { notePending } from "./semanticFormat";

const props = defineProps<{
  modelValue: boolean;
  namespaceId: number;
  /** Said when the join waits for review. */
  pendingNote?: string;
}>();
const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "added", name: string, fromId: number, toId: number): void;
}>();

const emptyPair = () => ({ from: "", to: "" });

const catalogStore = useCatalogStore();
const store = useSemanticModelStore();

const name = ref("");
const fromId = ref<number | null>(null);
const toId = ref<number | null>(null);
const pairs = ref([emptyPair()]);
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

// Core's refusal of a join whose sides are the wrong way round starts with this sentence.
const reversed = computed(() => error.value.includes("wrong way round"));

function swapSides() {
  [fromId.value, toId.value] = [toId.value, fromId.value];
  pairs.value = pairs.value.map((p) => ({ from: p.to, to: p.from }));
  error.value = "";
}

async function swapAndRetry() {
  swapSides();
  await save();
}

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
    emit("added", name.value.trim(), fromId.value, toId.value);
    notePending(facts, props.pendingNote);
    emit("update:modelValue", false);
  } catch (e) {
    error.value = errorText(e, "Could not add the join");
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped src="./semantic.css"></style>

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
  grid-template-columns: 1fr auto 1fr;
  gap: var(--spacing-3);
}

.pair {
  width: 100%;
  margin-bottom: var(--spacing-2);
}

.pair-arrow,
.pair-remove:hover {
  color: var(--color-text-muted);
}

.form-error {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: var(--spacing-2);
}

.form-error p {
  margin: 0;
}

.swap-sides {
  align-self: center;
  margin-top: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
}

.swap-sides:hover:not(:disabled) {
  color: var(--color-primary);
  border-color: var(--color-primary);
}

.swap-sides:disabled {
  cursor: default;
  opacity: 0.5;
}
</style>
