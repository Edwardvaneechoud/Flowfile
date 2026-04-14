<template>
  <el-dialog :model-value="visible" title="Create Schedule" width="480px" @close="$emit('close')">
    <el-form label-position="top" @submit.prevent="handleCreate">
      <el-form-item label="Flow">
        <el-select
          v-model="form.registration_id"
          placeholder="Select a flow"
          filterable
          style="width: 100%"
        >
          <el-option
            v-for="flow in availableFlows"
            :key="flow.id"
            :label="flow.name"
            :value="flow.id"
          />
        </el-select>
      </el-form-item>

      <el-form-item label="Name (optional)">
        <el-input v-model="form.name" :maxlength="100" placeholder="e.g. Nightly sales refresh" />
      </el-form-item>

      <el-form-item label="Description (optional)">
        <el-input
          v-model="form.description"
          type="textarea"
          :rows="2"
          :maxlength="200"
          show-word-limit
          placeholder="e.g. Nightly sales data refresh"
        />
      </el-form-item>

      <el-form-item label="Schedule Type">
        <el-radio-group v-model="form.schedule_type">
          <el-radio value="interval">Interval</el-radio>
          <el-radio value="table_trigger">Table Trigger</el-radio>
          <el-radio value="table_set_trigger">Table Set Trigger</el-radio>
        </el-radio-group>
      </el-form-item>

      <el-form-item v-if="form.schedule_type === 'interval'" label="Run every">
        <div style="display: flex; gap: 8px; align-items: center">
          <el-input-number v-model="intervalMinutes" :min="1" :max="10080" style="width: 120px" />
          <span>minutes</span>
        </div>
      </el-form-item>

      <el-form-item v-if="form.schedule_type === 'table_trigger'" label="Trigger Table">
        <el-select
          v-model="form.trigger_table_id"
          :placeholder="tables.length === 0 ? 'No catalog tables available' : 'Select a table'"
          :disabled="tables.length === 0"
          filterable
          style="width: 100%"
        >
          <el-option-group v-if="readTables.length > 0" label="Read by this flow">
            <el-option
              v-for="table in readTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag v-if="table.table_type === 'virtual'" size="small" type="info" style="margin-left: 8px">virtual</el-tag>
            </el-option>
          </el-option-group>
          <el-option-group
            v-if="otherTables.length > 0"
            :label="readTables.length > 0 ? 'Other tables' : 'All tables'"
          >
            <el-option
              v-for="table in otherTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag v-if="table.table_type === 'virtual'" size="small" type="info" style="margin-left: 8px">virtual</el-tag>
            </el-option>
          </el-option-group>
        </el-select>
        <div v-if="tables.length === 0" class="hint-text">No catalog tables registered yet.</div>
        <div v-else class="hint-text">The flow will run when this table is refreshed.</div>
      </el-form-item>

      <el-form-item v-if="form.schedule_type === 'table_set_trigger'" label="Trigger Tables">
        <el-select
          v-model="form.trigger_table_ids"
          :placeholder="
            tables.length === 0 ? 'No catalog tables available' : 'Select tables (at least 2)'
          "
          :disabled="tables.length === 0"
          filterable
          multiple
          style="width: 100%"
        >
          <el-option-group v-if="readTables.length > 0" label="Read by this flow">
            <el-option
              v-for="table in readTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag v-if="table.table_type === 'virtual'" size="small" type="info" style="margin-left: 8px">virtual</el-tag>
            </el-option>
          </el-option-group>
          <el-option-group
            v-if="otherTables.length > 0"
            :label="readTables.length > 0 ? 'Other tables' : 'All tables'"
          >
            <el-option
              v-for="table in otherTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag v-if="table.table_type === 'virtual'" size="small" type="info" style="margin-left: 8px">virtual</el-tag>
            </el-option>
          </el-option-group>
        </el-select>
        <div v-if="tables.length === 0" class="hint-text">No catalog tables registered yet.</div>
        <div v-else class="hint-text">
          The flow will run when all selected tables have been refreshed.
        </div>
      </el-form-item>
    </el-form>

    <template #footer>
      <el-button @click="$emit('close')">Cancel</el-button>
      <el-button type="primary" :disabled="!isValid" @click="handleCreate"> Create </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import type { CatalogTable, FlowRegistration, FlowScheduleCreate } from "../../types";

const props = defineProps<{
  visible: boolean;
  flows: FlowRegistration[];
  tables: CatalogTable[];
  preselectedFlowId?: number | null;
}>();

const emit = defineEmits(["close", "create"]);

const intervalMinutes = ref(60);

const form = ref<{
  registration_id: number | null;
  schedule_type: "interval" | "table_trigger" | "table_set_trigger";
  trigger_table_id: number | null;
  trigger_table_ids: number[];
  name: string;
  description: string;
}>({
  registration_id: props.preselectedFlowId ?? null,
  schedule_type: "interval",
  trigger_table_id: null,
  trigger_table_ids: [],
  name: "",
  description: "",
});

const availableFlows = computed(() => props.flows.filter((f) => f.file_exists));

const selectedFlow = computed(
  () => props.flows.find((f) => f.id === form.value.registration_id) ?? null,
);

const readTableIds = computed(() => {
  return new Set(selectedFlow.value?.tables_read?.map((t) => t.id) ?? []);
});

const readTables = computed(() => props.tables.filter((t) => readTableIds.value.has(t.id)));

const otherTables = computed(() => props.tables.filter((t) => !readTableIds.value.has(t.id)));

watch(
  () => form.value.registration_id,
  () => {
    form.value.trigger_table_id = null;
    form.value.trigger_table_ids = [];
  },
);

watch(
  () => props.visible,
  (open) => {
    if (open) {
      form.value.registration_id = props.preselectedFlowId ?? null;
      form.value.schedule_type = "interval";
      form.value.trigger_table_id = null;
      form.value.trigger_table_ids = [];
      form.value.name = "";
      form.value.description = "";
      intervalMinutes.value = 60;
    }
  },
);

const isValid = computed(() => {
  if (!form.value.registration_id) return false;
  if (form.value.schedule_type === "interval" && intervalMinutes.value < 1) return false;
  if (form.value.schedule_type === "table_trigger" && !form.value.trigger_table_id) return false;
  if (form.value.schedule_type === "table_set_trigger" && form.value.trigger_table_ids.length < 2)
    return false;
  return true;
});

function handleCreate() {
  if (!isValid.value || !form.value.registration_id) return;

  const body: FlowScheduleCreate = {
    registration_id: form.value.registration_id,
    schedule_type: form.value.schedule_type,
    name: form.value.name.trim() || null,
    description: form.value.description.trim() || null,
  };

  if (form.value.schedule_type === "interval") {
    body.interval_seconds = intervalMinutes.value * 60;
  } else if (form.value.schedule_type === "table_trigger") {
    body.trigger_table_id = form.value.trigger_table_id;
  } else if (form.value.schedule_type === "table_set_trigger") {
    body.trigger_table_ids = form.value.trigger_table_ids;
  }

  emit("create", body);
}
</script>

<style scoped>
.hint-text {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}
</style>
