<template>
  <el-dialog
    :model-value="visible"
    title="Create Schedule"
    width="480px"
    @close="$emit('close')"
  >
    <el-form label-position="top" @submit.prevent="handleCreate">
      <el-form-item label="Flow">
        <el-select
          v-model="form.registration_id"
          placeholder="Select a flow"
          filterable
          style="width: 100%"
        >
          <el-option
            v-for="flow in flows"
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

      <el-form-item
        v-if="form.schedule_type === 'interval'"
        label="Run every"
      >
        <div style="display: flex; gap: 8px; align-items: center">
          <el-input-number
            v-model="intervalMinutes"
            :min="1"
            :max="10080"
            style="width: 120px"
          />
          <span>minutes</span>
        </div>
      </el-form-item>

      <el-form-item
        v-if="form.schedule_type === 'table_trigger'"
        label="Trigger Table"
      >
        <el-select
          v-model="form.trigger_table_id"
          placeholder="Select a table"
          filterable
          style="width: 100%"
        >
          <el-option
            v-for="table in tables"
            :key="table.id"
            :label="table.name"
            :value="table.id"
          />
        </el-select>
      </el-form-item>
    </el-form>

    <template #footer>
      <el-button @click="$emit('close')">Cancel</el-button>
      <el-button type="primary" :disabled="!isValid" @click="handleCreate">
        Create
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import type { FlowRegistration, CatalogTable, FlowScheduleCreate } from "../../types";

const props = defineProps<{
  visible: boolean;
  flows: FlowRegistration[];
  tables: CatalogTable[];
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
