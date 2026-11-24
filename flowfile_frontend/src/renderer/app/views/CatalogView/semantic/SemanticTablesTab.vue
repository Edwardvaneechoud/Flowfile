<template>
  <div class="tables-tab">
    <p v-if="model.tables.length === 0" class="muted">This schema has no tables yet.</p>

    <section v-for="table in model.tables" :key="table.table_id" class="table-card">
      <header class="table-head">
        <button class="collapse-btn" type="button" @click="toggle(table.table_id)">
          <i
            :class="
              isOpen(table.table_id) ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'
            "
          ></i>
        </button>
        <i class="fa-solid fa-table table-icon"></i>
        <h3 class="table-name">{{ table.name }}</h3>
        <SemanticStatusChip
          v-if="table.facts.description"
          :status="table.facts.description.status"
          :fact="table.facts.description"
        />
        <span class="table-count">{{ table.fields.length }} columns</span>
      </header>
      <div class="table-description">
        <SemanticInlineText
          :value="table.description ?? ''"
          placeholder="Describe what one row of this table is"
          multiline
          @commit="saveTable(table, $event)"
        />
      </div>

      <div v-if="isOpen(table.table_id)" class="grid" role="table">
        <div class="grid-row grid-head" role="row">
          <span>Column</span>
          <span>Type</span>
          <span>Description</span>
          <span>Role</span>
          <span>Confidentiality</span>
        </div>
        <div v-for="field in table.fields" :key="field.name" class="grid-row" role="row">
          <span class="col-name" :title="field.name">{{ field.name }}</span>
          <span class="col-type">{{ field.dtype ?? "" }}</span>
          <div class="cell">
            <SemanticInlineText
              :value="factText(field.facts.description)"
              @commit="saveField(table, field.name, 'description', $event)"
            />
            <SemanticStatusChip
              v-if="field.facts.description"
              :status="field.facts.description.status"
              :fact="field.facts.description"
            />
          </div>
          <div class="cell">
            <el-select
              :model-value="factText(field.facts.role) || undefined"
              size="small"
              placeholder="Role"
              clearable
              @update:model-value="saveField(table, field.name, 'role', $event)"
            >
              <el-option v-for="r in COLUMN_ROLES" :key="r" :label="r" :value="r" />
            </el-select>
            <SemanticStatusChip
              v-if="field.facts.role"
              :status="field.facts.role.status"
              :fact="field.facts.role"
            />
          </div>
          <div class="cell">
            <el-select
              :model-value="factText(field.facts.confidentiality) || undefined"
              size="small"
              placeholder="Level"
              @update:model-value="saveField(table, field.name, 'confidentiality', $event)"
            >
              <el-option v-for="c in CONFIDENTIALITY_LEVELS" :key="c" :label="c" :value="c" />
            </el-select>
            <SemanticStatusChip
              v-if="field.facts.confidentiality"
              :status="field.facts.confidentiality.status"
              :fact="field.facts.confidentiality"
            />
          </div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { ElMessage } from "element-plus";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { SemanticModel, SemanticProposal, SemanticTable } from "../../../types/semantic.types";
import SemanticInlineText from "./SemanticInlineText.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { COLUMN_ROLES, CONFIDENTIALITY_LEVELS, errorText, factText } from "./semanticFormat";

defineProps<{ model: SemanticModel }>();

const store = useSemanticModelStore();
const closed = ref(new Set<number>());

const isOpen = (id: number) => !closed.value.has(id);
const toggle = (id: number) => {
  const next = new Set(closed.value);
  if (!next.delete(id)) next.add(id);
  closed.value = next;
};

async function send(proposal: SemanticProposal) {
  try {
    const facts = await store.propose([proposal]);
    if (facts.some((f) => f.status !== "accepted")) {
      ElMessage.info("Proposed. It waits in the Review tab.");
    }
  } catch (e) {
    ElMessage.error(errorText(e, "Could not save the change"));
  }
}

const saveTable = (table: SemanticTable, value: string) =>
  send({ kind: "table", table_id: table.table_id, attribute: "description", value });

const saveField = (table: SemanticTable, column: string, attribute: string, value: unknown) => {
  if (attribute !== "description" && (value === undefined || value === null || value === ""))
    return;
  return send({ kind: "field", table_id: table.table_id, column, attribute, value });
};
</script>

<style scoped>
.tables-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.table-card {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  padding: var(--spacing-3) var(--spacing-4);
}

.table-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.collapse-btn {
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  width: 20px;
}

.table-icon {
  color: var(--color-primary);
}

.table-name {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
}

.table-count {
  margin-left: auto;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.table-description {
  margin: var(--spacing-1) 0 var(--spacing-3) 28px;
}

.grid {
  display: flex;
  flex-direction: column;
}

.grid-row {
  display: grid;
  grid-template-columns:
    minmax(120px, 1.1fr) minmax(80px, 0.7fr) minmax(200px, 3fr) minmax(130px, 1fr)
    minmax(130px, 1fr);
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-1) 0;
  border-top: 1px solid var(--color-border-light);
}

.grid-head {
  border-top: none;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
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

.muted {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}
</style>
