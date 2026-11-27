<template>
  <div class="grid" role="table">
    <div class="grid-row grid-head" role="row">
      <span>Column</span>
      <span>Type</span>
      <span>Description</span>
      <span v-for="choice in CHOICES" :key="choice.attribute" class="head-with-info">
        {{ choice.title }}
        <el-tooltip
          :content="choice.help"
          placement="top"
          :show-after="200"
          popper-class="semantic-help-tooltip"
        >
          <span class="head-info" tabindex="0"><i class="fa-solid fa-circle-info"></i></span>
        </el-tooltip>
      </span>
    </div>
    <div v-for="field in table.fields" :key="field.name" class="grid-row" role="row">
      <span class="col-name" :title="field.name">{{ field.name }}</span>
      <span class="col-type">{{ field.dtype ?? "" }}</span>
      <div class="cell cell-stack">
        <div class="cell">
          <SemanticInlineText
            :value="factText(field.facts.description)"
            :disabled="!editable"
            @commit="saveField(field.name, 'description', $event)"
          />
          <SemanticStatusChip :fact="field.facts.description" />
        </div>
        <div class="cell also">
          <SemanticInlineText
            :value="wordsOf(field.facts.synonyms?.value)"
            placeholder="Also called…"
            :disabled="!editable"
            data-testid="column-synonyms"
            @commit="saveField(field.name, 'synonyms', splitWords($event))"
          />
          <SemanticStatusChip :fact="field.facts.synonyms" />
        </div>
      </div>
      <div v-for="choice in CHOICES" :key="choice.attribute" class="cell">
        <el-select
          :model-value="factText(field.facts[choice.attribute]) || undefined"
          size="small"
          placeholder="Not set"
          clearable
          :disabled="!editable"
          :fit-input-width="false"
          @update:model-value="saveField(field.name, choice.attribute, $event)"
        >
          <el-option
            v-for="o in choice.options"
            :key="o.value"
            :label="o.label"
            :value="o.value"
            class="role-option"
          >
            <span class="role-label">{{ o.label }}</span>
            <span class="role-hint">{{ o.hint }}</span>
          </el-option>
        </el-select>
        <SemanticStatusChip :fact="field.facts[choice.attribute]" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { SemanticProposal, SemanticTable } from "../../../types/semantic.types";
import SemanticInlineText from "./SemanticInlineText.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import {
  CONFIDENTIALITY_HELP,
  CONFIDENTIALITY_OPTIONS,
  ROLE_HELP,
  ROLE_OPTIONS,
  factText,
  splitWords,
} from "./semanticFormat";

const props = withDefaults(defineProps<{ table: SemanticTable; editable?: boolean }>(), {
  editable: true,
});
const emit = defineEmits<(e: "propose", proposal: SemanticProposal) => void>();

const wordsOf = (value: unknown): string =>
  Array.isArray(value) ? value.filter((w) => typeof w === "string").join(", ") : "";

const CHOICES = [
  { attribute: "role", title: "Used as", help: ROLE_HELP, options: ROLE_OPTIONS },
  {
    attribute: "confidentiality",
    title: "Confidentiality",
    help: CONFIDENTIALITY_HELP,
    options: CONFIDENTIALITY_OPTIONS,
  },
];

// A cleared value goes as null, which removes the fact; clearing nothing sends nothing.
const saveField = (column: string, attribute: string, value: unknown) => {
  const blank = !value || (value as string | string[]).length === 0;
  const held = props.table.fields.find((f) => f.name === column)?.facts[attribute];
  if (blank && !held) return;
  emit("propose", {
    kind: "field",
    table_id: props.table.table_id,
    column,
    attribute,
    value: blank ? null : value,
  });
};
</script>

<style scoped>
.grid {
  display: flex;
  flex-direction: column;
  overflow-x: auto;
}

.cell.cell-stack {
  flex-direction: column;
  align-items: stretch;
  gap: 0;
}

.also {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.grid-row {
  display: grid;
  grid-template-columns:
    minmax(120px, 1.1fr) minmax(80px, 0.7fr) minmax(200px, 3fr) minmax(150px, 1fr)
    minmax(150px, 1fr);
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-1) 0;
  border-top: 1px solid var(--color-border-light);
  min-width: 760px;
}

.grid-head {
  border-top: none;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.head-with-info {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
}

/* The tooltip's trigger class resets the font, so it sits on a wrapper, not on the icon. */
.head-info {
  display: inline-flex;
  color: var(--color-text-muted);
  cursor: help;
  font-size: var(--font-size-xs);
}

.role-option {
  height: auto;
  line-height: 1.3;
  padding-top: var(--spacing-1);
  padding-bottom: var(--spacing-1);
}

.role-label {
  display: block;
  font-size: var(--font-size-sm);
}

.role-hint {
  display: block;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  white-space: normal;
}

.col-name {
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-sm);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.col-type {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.cell {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-width: 0;
}

.cell > :first-child {
  flex: 1;
  min-width: 0;
}
</style>

<!-- The tooltip is teleported to the body, so its width cannot be set from the scoped block. -->
<style>
.semantic-help-tooltip {
  max-width: 320px;
  line-height: 1.4;
}
</style>
