<template>
  <div v-for="expressionGroup in apiData" :key="expressionGroup.expression_type" class="tree-node">
    <div @click="toggle(expressionGroup)">
      <span>{{ expressionGroup.expression_type }}</span>
      <span v-if="expressionGroup.expressions" class="toggle-icon">
        {{ isOpen(expressionGroup) ? "▼" : "▶" }}
      </span>
    </div>
    <ul v-if="expressionGroup.expressions && isOpen(expressionGroup)" class="tree-subview">
      <li
        v-for="expression in expressionGroup.expressions"
        :key="expression.name"
        class="tree-leaf"
      >
        <div>
          <pop-over :content="formatDoc(expression.doc)" :title="expression.name">
            <div class="cool-button-container">
              <button class="cool-button" @click="handleButtonClick(expression.name)">
                {{ expression.name }}
              </button>
            </div>
          </pop-over>
        </div>
      </li>
    </ul>
  </div>
</template>

<script lang="ts" setup>
import { ref, defineEmits, onMounted, nextTick } from "vue";
import PopOver from "../PopOver.vue";
import { useNodeStore } from "../../../../stores/column-store";

const nodeStore = useNodeStore();

const formatDoc = (doc: string | null): string => {
  if (!doc) return "";
  return doc.replace(/\n/g, "<br>");
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

const isOpen = (expressionGroup: ExpressionGroup) => {
  const key = expressionGroup.expression_type;
  return openNodes.value.has(key);
};

onMounted(async () => {
  await nextTick();
  apiData.value = await nodeStore.getExpressionsOverview();
});

const apiData = ref<ExpressionGroup[]>([
  {
    expression_type: "date_functions",
    expressions: [
      {
        name: "add_days",
        doc: "\n    Add a specified number of days to a date or timestamp.\n\n    Parameters:\n    - s (Any): The date or timestamp to add days to. Can be a Flowfile expression or any other value.\n    - days (int): The number of days to add.\n\n    Returns:\n    - pl.Expr: A Flowfile expression representing the result of adding `days` to `s`.\n\n    Note: If `s` is not a Flowfile expression, it will be converted into one.\n    ",
      },
      {
        name: "add_hours",
        doc: "\n    Add a specified number of hours to a timestamp.\n\n    Parameters:\n    - s (Any): The timestamp to add hours to. Can be a Flowfile expression or any other value.\n    - hours (int): The number of hours to add.\n\n    Returns:\n    - pl.Expr: A Flowfile expression representing the result of adding `hours` to `s`.\n\n    Note: If `s` is not a Flowfile expression, it will be converted into one.\n    ",
      },
      // Add more expressions as needed
    ],
  },
  // Add more expression groups as needed
]);
</script>

<style scoped>
.toggle-icon {
  margin-left: 10px;
  font-weight: bold;
}

.tree-node {
  color: var(--color-text-primary);
}

.tree-subview {
  list-style-type: none;
  padding-left: 20px;
}

.tree-leaf {
  margin-bottom: 5px;
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
  padding: 3px 3px;
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
</style>
