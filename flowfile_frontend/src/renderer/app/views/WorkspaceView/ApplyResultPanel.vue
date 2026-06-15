<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-arrow-down-to-bracket" /> Last apply</h3>
      <span class="ws-card-sub">{{ result.project_root }}</span>
    </header>

    <div v-if="countEntries.length" class="ws-chips">
      <span v-for="[key, value] in countEntries" :key="key" class="ws-chip"
        >{{ key }}: {{ value }}</span
      >
    </div>

    <div v-if="result.missing_secrets.length" class="ws-alert ws-alert-warn">
      <i class="fa-solid fa-key" />
      <div>
        <strong>{{ result.missing_secrets.length }} secret value(s) missing.</strong>
        Set the matching <code class="ws-code-inline">FLOWFILE_SECRET_*</code> variables (see below)
        and apply again — connections were created without them.
      </div>
    </div>
    <div v-else class="ws-alert ws-alert-ok">
      <i class="fa-solid fa-circle-check" /><span>All required secrets were resolved.</span>
    </div>

    <div v-if="result.warnings.length" class="ws-warnings">
      <div v-for="(warning, i) in result.warnings" :key="i" class="ws-warning-line">
        ⚠ {{ warning }}
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { WorkspaceApplyResult } from "../../types";

const props = defineProps<{ result: WorkspaceApplyResult }>();
const countEntries = computed(() => Object.entries(props.result.counts));
</script>
