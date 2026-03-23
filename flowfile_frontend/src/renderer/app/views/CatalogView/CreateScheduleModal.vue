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

      <el-form-item label="Schedule Type">
        <el-radio-group v-model="form.schedule_type">
          <el-radio value="interval">Interval</el-radio>
          <el-radio value="table_trigger">Table Trigger</el-radio>
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
          :placeholder="
            eligibleTables.length === 0 ? 'No catalog tables read by this flow' : 'Select a table'
          "
          :disabled="eligibleTables.length === 0"
          filterable
          style="width: 100%"
        >
          <el-option
            v-for="table in eligibleTables"
            :key="table.id"
            :label="table.name"
            :value="table.id"
          />
        </el-select>
        <div v-if="form.registration_id && eligibleTables.length === 0" class="hint-text">
          This flow does not read any catalog tables.
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
import type { FlowRegistration, FlowScheduleCreate } from "../../types";

const props = defineProps<{
  visible: boolean;
  flows: FlowRegistration[];
  preselectedFlowId?: number | null;
}>();

const emit = defineEmits<{
  close: [];
  create: [schedule: FlowScheduleCreate];
}>();

const intervalMinutes = ref(60);

const form = ref<{
  registration_id: number | null;
  schedule_type: string;
  trigger_table_id: number | null;
}>({
  registration_id: props.preselectedFlowId ?? null,
  schedule_type: "interval",
  trigger_table_id: null,
});

const availableFlows = computed(() => props.flows.filter((f) => f.file_exists));

const selectedFlow = computed(
  () => props.flows.find((f) => f.id === form.value.registration_id) ?? null,
);

const eligibleTables = computed(() => selectedFlow.value?.tables_read ?? []);

watch(
  () => form.value.registration_id,
  () => {
    form.value.trigger_table_id = null;
  },
);

watch(
  () => props.visible,
  (open) => {
    if (open) {
      form.value.registration_id = props.preselectedFlowId ?? null;
      form.value.schedule_type = "interval";
      form.value.trigger_table_id = null;
      intervalMinutes.value = 60;
    }
  },
);

const isValid = computed(() => {
  if (!form.value.registration_id) return false;
  if (form.value.schedule_type === "interval" && intervalMinutes.value < 1) return false;
  if (form.value.schedule_type === "table_trigger" && !form.value.trigger_table_id) return false;
  return true;
});

function handleCreate() {
  if (!isValid.value || !form.value.registration_id) return;

  const body: FlowScheduleCreate = {
    registration_id: form.value.registration_id,
    schedule_type: form.value.schedule_type,
  };

  if (form.value.schedule_type === "interval") {
    body.interval_seconds = intervalMinutes.value * 60;
  } else {
    body.trigger_table_id = form.value.trigger_table_id;
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
