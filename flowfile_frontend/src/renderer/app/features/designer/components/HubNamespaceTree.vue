<template>
  <div class="hub-namespace-tree">
    <p v-if="loading" class="hub-dialog-status">Loading…</p>
    <ul v-else class="hub-open-list">
      <li
        v-if="noneLabel"
        class="hub-open-row hub-tree-row"
        :class="{ selected: selectedId === null }"
        data-testid="hub-namespace-none"
        @click="choose(null)"
      >
        <span class="hub-tree-chevron" aria-hidden="true"></span>
        <span class="material-icons">folder</span>
        <span class="hub-open-row-name">{{ noneLabel }}</span>
      </li>
      <li
        v-for="row in visible"
        :key="row.id"
        class="hub-open-row hub-tree-row"
        :class="{ selected: selectedId === row.id }"
        :style="{ '--tree-depth': row.depth }"
        @click="choose(row.id)"
      >
        <button
          v-if="row.hasChildren !== false"
          type="button"
          class="hub-tree-chevron"
          :aria-label="chevronLabel(row)"
          :aria-expanded="row.expanded"
          @click.stop="expand(row.id)"
        >
          <span class="material-icons">{{ row.expanded ? "expand_more" : "chevron_right" }}</span>
        </button>
        <span v-else class="hub-tree-chevron" aria-hidden="true"></span>
        <span class="material-icons">{{ row.depth > 0 ? "schema" : "folder" }}</span>
        <span class="hub-open-row-name">{{ row.name }}</span>
      </li>
      <li v-if="!visible.length" class="hub-dialog-status">No namespaces on this hub.</li>
    </ul>
    <p v-if="error" class="hub-dialog-error">{{ error }}</p>
  </div>
</template>

<script setup lang="ts">
import { onMounted, watch } from "vue";
import { chevronLabel, useHubNamespaceTree } from "../hubNamespaceTree";

const props = defineProps<{
  /** The selected namespace's id; `null` for none. */
  modelValue: number | null;
  /** When given, a first row with this label stands for `null`. */
  noneLabel?: string;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: number | null): void;
}>();

const { visible, selectedId, loading, error, load, expand, select } = useHubNamespaceTree();

select(props.modelValue);
watch(
  () => props.modelValue,
  (value) => select(value),
);

onMounted(() => void load());

function choose(id: number | null) {
  select(id);
  emit("update:modelValue", id);
}
</script>

<style scoped src="./hubDialogs.css"></style>
