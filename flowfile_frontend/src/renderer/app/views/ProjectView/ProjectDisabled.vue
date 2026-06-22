<template>
  <div class="project-disabled">
    <div class="intro-header">
      <div class="intro-icon"><i class="fa-solid fa-clock-rotate-left"></i></div>
      <div>
        <h2 class="page-title">Project Tracking</h2>
        <p class="page-description">Versioned history of your flows, connections and schedules.</p>
      </div>
    </div>

    <div v-if="adminOnly" class="card">
      <div class="card-content">
        <div class="disabled-box">
          <i class="fa-solid fa-lock"></i>
          <div>
            <p><strong>Restricted to administrators</strong></p>
            <p>
              Project tracking is available to administrator accounts only on this server. Ask an
              administrator if you need a versioned project.
            </p>
          </div>
        </div>
      </div>
    </div>

    <div v-else class="card">
      <div class="card-content">
        <div class="disabled-box">
          <i class="fa-solid fa-circle-info"></i>
          <div>
            <p><strong>Project tracking is turned off on this server</strong></p>
            <p>
              An administrator can enable it by setting the environment variable
              <code>FLOWFILE_ENABLE_PROJECTS=true</code> on the Flowfile core service and restarting
              it (for example <code>docker compose up -d</code>).
            </p>
            <p>
              Once enabled, projects are created in your own private area and are available to
              administrator accounts.
            </p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{ adminOnly?: boolean }>();
</script>

<style scoped>
.project-disabled {
  max-width: 760px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

.intro-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-4, 16px);
  margin-bottom: var(--spacing-4, 16px);
}

.intro-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 48px;
  height: 48px;
  flex-shrink: 0;
  border-radius: 50%;
  background-color: var(--color-accent-subtle, #eff6ff);
  color: var(--color-accent, #2563eb);
  font-size: var(--font-size-xl, 20px);
}

.disabled-box {
  display: flex;
  gap: var(--spacing-4, 16px);
  padding: var(--spacing-4, 16px);
  background-color: var(--color-background-muted, #f8fafc);
  border-left: 4px solid var(--color-accent, #2563eb);
  border-radius: var(--border-radius-md, 8px);
}

.disabled-box i {
  color: var(--color-accent, #2563eb);
  font-size: var(--font-size-2xl, 24px);
  margin-top: var(--spacing-1, 4px);
}

.disabled-box p {
  margin: 0 0 var(--spacing-2, 8px);
  font-size: var(--font-size-sm, 14px);
  color: var(--color-text-secondary, #475569);
  line-height: 1.55;
}

.disabled-box p:last-child {
  margin-bottom: 0;
}

.disabled-box p strong {
  color: var(--color-text-primary, #0f172a);
}

.disabled-box code {
  font-family: var(--font-family-mono, monospace);
  background: var(--color-background-primary, #fff);
  padding: 1px 6px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary, #e2e8f0);
  color: var(--color-text-primary, #0f172a);
}
</style>
