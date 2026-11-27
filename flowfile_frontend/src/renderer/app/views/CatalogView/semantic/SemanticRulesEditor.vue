<template>
  <el-popover trigger="click" placement="bottom-start" :width="320">
    <template #reference>
      <button
        class="rules-edit"
        :class="{ 'is-empty': rules.length === 0 }"
        type="button"
        :aria-label="`Edit data rules of ${column}`"
        data-testid="rules-edit"
      >
        <i class="fa-solid" :class="rules.length === 0 ? 'fa-plus' : 'fa-pen'"></i>
        <span v-if="rules.length === 0">Add rule</span>
      </button>
    </template>
    <div class="rules-editor" data-testid="rules-editor">
      <p class="group-title">Data rules · {{ column }}</p>
      <ul v-if="rules.length > 0" class="rule-list">
        <li v-for="(rule, index) in rules" :key="index" class="rule-line">
          <span class="rule-text">{{ describeRule(rule) }}</span>
          <button
            class="icon-btn icon-btn-danger"
            type="button"
            :aria-label="`Remove rule ${describeRule(rule)}`"
            data-testid="rule-remove"
            @click="remove(index)"
          >
            <i class="fa-solid fa-xmark"></i>
          </button>
        </li>
      </ul>
      <el-select
        v-model="kind"
        size="small"
        placeholder="Add a rule"
        :teleported="false"
        data-testid="rule-kind"
      >
        <el-option v-for="k in kinds" :key="k.kind" :label="k.label" :value="k.kind" />
      </el-select>
      <p v-if="chosen" class="note">{{ chosen.hint }}</p>
      <el-input
        v-for="input in chosen?.fields ?? []"
        :key="`${kind}-${input.key}`"
        v-model="draft[input.key]"
        size="small"
        :aria-label="input.label"
        :placeholder="placeholderOf(input.key, input.placeholder)"
        :data-testid="`rule-${input.key}`"
        @keydown.enter="add"
      >
        <template #prepend>{{ input.label }}</template>
      </el-input>
      <p v-if="problem" class="error">{{ problem }}</p>
      <div class="rule-actions">
        <el-button
          size="small"
          type="primary"
          :disabled="!kind"
          data-testid="rule-add"
          @click="add"
        >
          Add rule
        </el-button>
      </div>
      <p class="note">
        A row without a value passes every rule but Not null; a formula fails a row it has no answer
        for.
      </p>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { DataRule } from "../../../types/semantic.types";
import { buildRule, describeRule, ruleKey, ruleKindsFor } from "./dataRules";

const props = defineProps<{ column: string; dtype: string | null; rules: DataRule[] }>();
/** The column's whole list after the change; an empty list removes its rules. */
const emit = defineEmits<(e: "save", rules: DataRule[]) => void>();

const kind = ref("");
const draft = ref<Record<string, string>>({});
const problem = ref("");

const kinds = computed(() => ruleKindsFor(props.dtype));
const chosen = computed(() => kinds.value.find((k) => k.kind === kind.value));

watch(kind, () => {
  draft.value = {};
  problem.value = "";
});

// The form is kept until the column's rules change, so a rule the server refuses can be corrected.
watch(
  () => JSON.stringify(props.rules),
  () => (kind.value = ""),
);

const placeholderOf = (key: string, fallback: string) =>
  key === "expression" ? `[${props.column}] >= 0` : fallback;

function add() {
  if (!kind.value) return;
  const built = buildRule(kind.value, draft.value, props.dtype);
  if ("error" in built) {
    problem.value = built.error;
    return;
  }
  const taken = props.rules.some(
    (r) => ruleKey(props.column, r) === ruleKey(props.column, built.rule),
  );
  if (taken) {
    problem.value = "This column already has that rule.";
    return;
  }
  emit("save", [...props.rules, built.rule]);
}

const remove = (index: number) =>
  emit(
    "save",
    props.rules.filter((_rule, i) => i !== index),
  );
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.rules-edit {
  display: inline-flex;
  flex: none;
  align-items: center;
  gap: var(--spacing-1);
  height: 20px;
  padding: 0 var(--spacing-2);
  border: 1px solid transparent;
  border-radius: 999px;
  background: transparent;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.rules-edit.is-empty {
  padding-left: 0;
  font-size: var(--font-size-sm);
}

.rules-edit:hover,
.rules-edit:focus-visible {
  border-color: var(--color-border-primary);
  background: var(--color-background-hover);
  color: var(--color-primary);
}

.rules-editor {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.rule-list {
  margin: 0;
  padding: 0;
  list-style: none;
}

.rule-line {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
  padding: var(--spacing-1) 0;
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
}

.rule-text {
  min-width: 0;
  overflow-wrap: anywhere;
}

.rule-actions {
  display: flex;
  justify-content: flex-end;
}
</style>
