<template>
  <div class="project-intro">
    <div class="intro-header">
      <div class="intro-icon"><i class="fa-solid fa-clock-rotate-left"></i></div>
      <div>
        <h2 class="page-title">Project</h2>
        <p class="page-description">
          Keep a versioned history of your flows, connections and schedules — entirely optional.
        </p>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-content">
        <div class="intro-info-box">
          <i class="fa-solid fa-box-archive"></i>
          <div>
            <p><strong>What is a project?</strong></p>
            <p>
              A project turns a folder on your computer into a safe, versioned copy of your work.
              Your flows, connections and schedules are mirrored there automatically as you go. No
              passwords are ever stored in it.
            </p>
          </div>
        </div>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-content">
        <div class="intro-info-box">
          <i class="fa-solid fa-camera"></i>
          <div>
            <p><strong>Why use one?</strong></p>
            <p>
              Whenever you like, <strong>save a version</strong> — a named snapshot you can come
              back to later. It's like a save point for your whole workspace: experiment freely, and
              restore an earlier version if something goes wrong.
            </p>
          </div>
        </div>
      </div>
    </div>

    <div class="intro-cta">
      <button type="button" class="btn btn-primary" @click="emit('create')">
        <i class="fa-solid fa-plus"></i>
        Create a project
      </button>
      <button type="button" class="btn btn-secondary" @click="emit('open')">
        <i class="fa-solid fa-folder-open"></i>
        Open a project
      </button>
    </div>
    <p class="intro-foot">
      Already have a project folder from another computer? Copy or clone it here, then
      <strong>Open</strong> it to rebuild everything.
    </p>
  </div>
</template>

<script setup lang="ts">
const emit = defineEmits<{ (e: "create"): void; (e: "open"): void }>();
</script>

<style scoped>
.project-intro {
  max-width: 760px;
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

.intro-info-box {
  display: flex;
  gap: var(--spacing-4, 16px);
  padding: var(--spacing-4, 16px);
  background-color: var(--color-background-muted, #f8fafc);
  border-left: 4px solid var(--color-accent, #2563eb);
  border-radius: var(--border-radius-md, 8px);
}

.intro-info-box i {
  color: var(--color-accent, #2563eb);
  font-size: var(--font-size-2xl, 24px);
  margin-top: var(--spacing-1, 4px);
}

.intro-info-box p {
  margin: 0 0 var(--spacing-2, 8px);
  font-size: var(--font-size-sm, 14px);
  color: var(--color-text-secondary, #475569);
  line-height: 1.55;
}

.intro-info-box p:last-child {
  margin-bottom: 0;
}

.intro-info-box p strong {
  color: var(--color-text-primary, #0f172a);
}

.intro-cta {
  display: flex;
  align-items: center;
  gap: var(--spacing-3, 12px);
}

.intro-foot {
  margin-top: var(--spacing-3, 12px);
  font-size: var(--font-size-sm, 14px);
  color: var(--color-text-tertiary, #94a3b8);
}
</style>
