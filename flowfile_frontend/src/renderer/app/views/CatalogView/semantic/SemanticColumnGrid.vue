<template>
  <div class="grid" role="table">
    <div class="grid-row grid-head" role="row">
      <span>Column</span>
      <span>Type</span>
      <span>Description</span>
      <span class="head-with-info">
        Used as
        <el-tooltip
          :content="ROLE_HELP"
          placement="top"
          :show-after="200"
          popper-class="semantic-help-tooltip"
        >
          <span class="head-info" tabindex="0"><i class="fa-solid fa-circle-info"></i></span>
        </el-tooltip>
      </span>
      <span class="head-with-info">
        Confidentiality
        <el-tooltip
          :content="CONFIDENTIALITY_HELP"
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
      <div class="cell">
        <SemanticInlineText
          :value="factText(field.facts.description)"
          :disabled="!editable"
          @commit="saveField(field.name, 'description', $event)"
        />
        <SemanticStatusChip
          v-if="needsReview(field.facts.description)"
          :status="field.facts.description!.status"
          :fact="field.facts.description"
        />
      </div>
      <div class="cell">
        <el-select
          :model-value="factText(field.facts.role) || undefined"
          size="small"
          placeholder="Not set"
          clearable
          :disabled="!editable"
          :fit-input-width="false"
          @update:model-value="saveField(field.name, 'role', $event)"
        >
          <el-option
            v-for="r in ROLE_OPTIONS"
            :key="r.value"
            :label="r.label"
            :value="r.value"
            class="role-option"
          >
            <span class="role-label">{{ r.label }}</span>
            <span class="role-hint">{{ r.hint }}</span>
          </el-option>
        </el-select>
        <SemanticStatusChip
          v-if="needsReview(field.facts.role)"
          :status="field.facts.role!.status"
          :fact="field.facts.role"
        />
      </div>
      <div class="cell">
        <el-select
          :model-value="factText(field.facts.confidentiality) || undefined"
          size="small"
          placeholder="Not set"
          clearable
          :disabled="!editable"
          :fit-input-width="false"
          @update:model-value="saveField(field.name, 'confidentiality', $event)"
        >
          <el-option
            v-for="c in CONFIDENTIALITY_OPTIONS"
            :key="c.value"
            :label="c.label"
            :value="c.value"
            class="role-option"
          >
            <span class="role-label">{{ c.label }}</span>
            <span class="role-hint">{{ c.hint }}</span>
          </el-option>
        </el-select>
        <SemanticStatusChip
          v-if="needsReview(field.facts.confidentiality)"
          :status="field.facts.confidentiality!.status"
          :fact="field.facts.confidentiality"
        />
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
  needsReview,
} from "./semanticFormat";

const props = withDefaults(defineProps<{ table: SemanticTable; editable?: boolean }>(), {
  editable: true,
});
const emit = defineEmits<(e: "propose", proposal: SemanticProposal) => void>();

// A cleared value goes as null, which removes the fact; clearing nothing sends nothing.
const saveField = (column: string, attribute: string, value: unknown) => {
  const blank = value === undefined || value === null || value === "";
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
