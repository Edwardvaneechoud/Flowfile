<template>
  <div
    v-for="expressionGroup in filteredData"
    :key="expressionGroup.expression_type"
    class="tree-node"
  >
    <div class="tree-header" @click="toggle(expressionGroup)">
      <span class="toggle-icon">{{ isOpen(expressionGroup) ? "▼" : "▶" }}</span>
      <span class="category-name">{{ humanize(expressionGroup.expression_type) }}</span>
    </div>
    <ul v-if="expressionGroup.expressions && isOpen(expressionGroup)" class="tree-subview">
      <li
        v-for="expression in expressionGroup.expressions"
        :key="expression.name"
        class="tree-leaf"
      >
        <pop-over :content="formatDoc(expression.doc)" :title="expression.name">
          <div class="cool-button-container">
            <button class="cool-button" @click="handleButtonClick(expression.name)">
              {{ expression.name }}
            </button>
          </div>
        </pop-over>
      </li>
    </ul>
  </div>
  <div v-if="filteredData.length === 0" class="empty-hint">No matching functions</div>
</template>

<script lang="ts" setup>
import { ref, computed, defineEmits, onMounted, nextTick } from "vue";
import PopOver from "../PopOver.vue";
import { useNodeStore } from "../../../../stores/column-store";

const props = defineProps<{ filterText?: string }>();

const nodeStore = useNodeStore();

const formatDoc = (doc: string | null): string => {
  if (!doc) return "";
  return doc.replace(/\n/g, "<br>");
};

// "date_functions" -> "Date functions"
const humanize = (key: string): string => {
  const s = key.replace(/_/g, " ").trim();
  return s.charAt(0).toUpperCase() + s.slice(1);
};

const emit = defineEmits<{
  (event: "value-selected", payload: string): void;
}>();

interface Expression {
  name: string;
  doc: string | null;
}

interface ExpressionGroup {
  expression_type: string;
  expressions: Expression[];
}

const handleButtonClick = (funcName: string) => {
  emit("value-selected", funcName);
};

const openNodes = ref<Set<string | undefined>>(new Set());

const toggle = (expressionGroup: ExpressionGroup) => {
  const key = expressionGroup.expression_type;
  if (openNodes.value.has(key)) {
    openNodes.value.delete(key);
  } else {
    openNodes.value.add(key);
  }
};

const isFiltering = computed(() => (props.filterText?.trim().length ?? 0) > 0);

// While a filter is active, only matching expressions (and their groups) are
// shown; groups not matching are dropped entirely.
const filteredData = computed<ExpressionGroup[]>(() => {
  const q = props.filterText?.trim().toLowerCase() ?? "";
  if (!q) return apiData.value;
  return apiData.value
    .map((g) => ({
      ...g,
      expressions: g.expressions.filter((e) => e.name.toLowerCase().includes(q)),
    }))
    .filter((g) => g.expressions.length > 0);
});

// Auto-expand every visible group while filtering; otherwise honor manual state.
const isOpen = (expressionGroup: ExpressionGroup) => {
  if (isFiltering.value) return true;
  return openNodes.value.has(expressionGroup.expression_type);
};

const apiData = ref<ExpressionGroup[]>([]);

onMounted(async () => {
  await nextTick();
  apiData.value = await nodeStore.getExpressionsOverview();
});
</script>

<style scoped>
.tree-node {
  color: var(--color-text-primary);
}

.tree-header {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 3px 5px;
  cursor: pointer;
  border-radius: 4px;
  user-select: none;
}

.tree-header:hover {
  background-color: var(--color-background-hover);
}

.toggle-icon {
  font-size: 9px;
  width: 10px;
  color: var(--color-text-tertiary);
}

.category-name {
  font-weight: 600;
  font-size: 12px;
}

.tree-subview {
  list-style-type: none;
  padding-left: 18px;
  margin: 2px 0;
}

.tree-leaf {
  margin-bottom: 2px;
}

.cool-button-container {
  display: flex;
  flex-direction: column;
  align-items: start;
}

.cool-button {
  width: 100%;
  max-width: 200px;
  border: none;
  color: var(--color-text-primary);
  background-color: transparent;
  padding: 3px 5px;
  text-align: left;
  display: inline-block;
  margin: 1px 0;
  cursor: pointer;
  border-radius: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.cool-button:hover {
  background-color: var(--color-background-hover);
}

.empty-hint {
  padding: 6px 5px;
  font-size: 12px;
  font-style: italic;
  color: var(--color-text-muted);
}
</style>
