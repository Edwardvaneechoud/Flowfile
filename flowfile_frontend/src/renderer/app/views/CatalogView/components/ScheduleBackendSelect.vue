<template>
  <!-- "" is a choice here (the flow's own setting), so only null and undefined are empty. -->
  <el-select
    v-model="backend"
    :empty-values="[null, undefined]"
    style="width: 100%"
    @visible-change="$event && load()"
  >
    <el-option v-for="o in options" :key="o.key" :label="o.label" :value="o.key" />
  </el-select>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { listComputeBackends } from "../../../api/computeBackends.api";
import { scheduleBackendOptions, type BackendRow } from "../../../utils/executionLocation";

const backend = defineModel<string>({ required: true });

// A listing that fails leaves the flow's own setting, the default worker and the value held.
const rows = ref<BackendRow[]>([]);
const load = async () => {
  rows.value = await listComputeBackends().catch(() => []);
};
onMounted(load);
const options = computed(() => scheduleBackendOptions(backend.value, rows.value));
</script>
