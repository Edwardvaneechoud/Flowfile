<template>
  <div class="ws-empty">
    <div class="ws-empty-card">
      <div class="ws-empty-icon"><i class="fa-solid fa-code-branch" /></div>
      <h2 class="ws-empty-title">Version your Flowfile environment</h2>
      <p class="ws-empty-text">
        Export your flows, connections and schedules into a deterministic, git-ready folder — review
        changes as diffs, roll back with git, and rebuild the whole environment on a fresh machine.
        <strong>Secret values never leave the database</strong> — only
        <code class="ws-code-inline">${secret:NAME}</code> placeholders are written.
      </p>
      <ul class="ws-empty-points">
        <li><i class="fa-solid fa-check" /> Deterministic export — clean, meaningful diffs</li>
        <li>
          <i class="fa-solid fa-check" /> Secret-free — refill values from env /
          <code class="ws-code-inline">.env</code> on apply
        </li>
        <li><i class="fa-solid fa-check" /> Works with any git host or CI</li>
      </ul>
      <el-button type="primary" size="large" :loading="creating" @click="emit('create')">
        <i class="fa-solid fa-plus" />&nbsp; Create project
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{ creating?: boolean }>();
const emit = defineEmits<{ (e: "create"): void }>();
</script>
