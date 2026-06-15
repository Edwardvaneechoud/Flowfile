<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-key" /> Required secrets</h3>
      <span class="ws-card-sub"
        >{{ secrets.length }} referenced — values are supplied via env, never committed</span
      >
    </header>

    <el-table :data="rows" size="small" class="ws-secret-table">
      <el-table-column prop="name" label="Secret" min-width="160" />
      <el-table-column label="Environment variable" min-width="280">
        <template #default="{ row }">
          <code class="ws-env">{{ row.env }}</code>
          <button class="ws-icon-btn" title="Copy variable name" @click="copy(row.env)">
            <i class="fa-regular fa-copy" />
          </button>
        </template>
      </el-table-column>
      <el-table-column label="Used by" min-width="220">
        <template #default="{ row }">
          <span class="ws-muted">{{ row.required_by.join(", ") || "—" }}</span>
        </template>
      </el-table-column>
    </el-table>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { ElMessage } from "element-plus";
import type { SecretRequirement } from "../../types";
import { workspaceSecretEnvVar } from "../../types/workspace.types";

const props = defineProps<{ secrets: SecretRequirement[] }>();

const rows = computed(() =>
  props.secrets.map((secret) => ({ ...secret, env: workspaceSecretEnvVar(secret.name) })),
);

async function copy(text: string) {
  try {
    await navigator.clipboard.writeText(text);
    ElMessage.success("Copied");
  } catch {
    /* clipboard unavailable — ignore */
  }
}
</script>
