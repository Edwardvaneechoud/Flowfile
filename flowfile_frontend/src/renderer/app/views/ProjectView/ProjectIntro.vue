<template>
  <div class="project-intro">
    <!-- Header: same pattern as the active-project view -->
    <header class="intro-header mb-3">
      <div class="intro-header__title-row">
        <h2 class="page-title">Project Tracking</h2>
        <span class="beta-badge">Beta</span>
      </div>
      <p class="page-description">Store and version your complete environment.</p>
    </header>

    <!-- Feature grid: echoes the stats row of the active view -->
    <div class="intro-features">
      <div class="intro-feature">
        <div class="intro-feature__icon"><i class="fa-solid fa-clock-rotate-left"></i></div>
        <div class="intro-feature__body">
          <p class="intro-feature__title">Versioned history</p>
          <p class="intro-feature__text">Every save is a snapshot you can return to.</p>
        </div>
      </div>
      <div class="intro-feature">
        <div class="intro-feature__icon"><i class="fa-solid fa-rotate-left"></i></div>
        <div class="intro-feature__body">
          <p class="intro-feature__title">Restore anytime</p>
          <p class="intro-feature__text">Roll your whole workspace back in one click.</p>
        </div>
      </div>
      <div class="intro-feature">
        <div class="intro-feature__icon"><i class="fa-solid fa-code-branch"></i></div>
        <div class="intro-feature__body">
          <p class="intro-feature__title">Git-backed</p>
          <p class="intro-feature__text">
            A standard Git repo — push it to GitHub to back up or share.
          </p>
        </div>
      </div>
      <div class="intro-feature">
        <div class="intro-feature__icon"><i class="fa-solid fa-shield-halved"></i></div>
        <div class="intro-feature__body">
          <p class="intro-feature__title">Secrets stay safe</p>
          <p class="intro-feature__text">Passwords are never written into the project.</p>
        </div>
      </div>
    </div>

    <!-- Two-column body: get started (left) + what/why (right) -->
    <div class="intro-grid">
      <aside class="intro-side">
        <section class="card get-started">
          <div class="card-content">
            <h3 class="get-started__title">Get started</h3>
            <p class="get-started__text">
              Turn a folder into a versioned, Git-backed copy of your work — entirely optional.
            </p>
            <div class="get-started__actions">
              <button type="button" class="btn btn-primary" @click="emit('create')">
                <i class="fa-solid fa-plus"></i>
                Create a project
              </button>
              <button type="button" class="btn btn-secondary" @click="emit('open')">
                <i class="fa-solid fa-folder-open"></i>
                Open a project
              </button>
            </div>
            <p class="get-started__foot">
              Already have a project folder from another computer? Copy or clone it here, then
              <strong>Open</strong> it to rebuild everything.
            </p>
          </div>
        </section>
      </aside>

      <main class="intro-main">
        <section class="card mb-3">
          <div class="card-header">
            <h3 class="card-title">
              <i class="fa-solid fa-box-archive card-title__icon"></i>
              What is a project?
            </h3>
          </div>
          <div class="card-content">
            <p class="intro-copy">
              A project turns a folder on your computer into a safe, versioned copy of your work.
              Your flows, connections and schedules are mirrored there automatically as you go.
            </p>
          </div>
        </section>

        <section class="card">
          <div class="card-header">
            <h3 class="card-title">
              <i class="fa-solid fa-camera card-title__icon"></i>
              Why use one?
            </h3>
          </div>
          <div class="card-content">
            <p class="intro-copy">
              Whenever you like, <strong>save a version</strong> — a named snapshot you can come
              back to later. It's like a save point for your whole workspace: experiment freely, and
              restore an earlier version if something goes wrong.
            </p>
          </div>
        </section>
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
const emit = defineEmits<{ (e: "create"): void; (e: "open"): void }>();
</script>

<style scoped>
.project-intro {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

.intro-header__title-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.intro-header .page-description {
  margin-top: var(--spacing-1);
}

.beta-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  letter-spacing: 0.04em;
  text-transform: uppercase;
  line-height: 1.4;
}

/* ===== Feature grid (mirrors the active view's pm-stats) ===== */
.intro-features {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-4);
}

.intro-feature {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  transition:
    transform var(--transition-base) var(--transition-timing),
    box-shadow var(--transition-base) var(--transition-timing);
}

.intro-feature:hover {
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.intro-feature__icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  flex-shrink: 0;
  border-radius: var(--border-radius-md);
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  font-size: 16px;
}

.intro-feature__body {
  min-width: 0;
}

.intro-feature__title {
  margin: 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.intro-feature__text {
  margin: 2px 0 0;
  font-size: var(--font-size-sm);
  line-height: 1.45;
  color: var(--color-text-tertiary);
}

/* ===== Two-column body (mirrors the active view's pm-grid) ===== */
.intro-grid {
  display: grid;
  grid-template-columns: 340px 1fr;
  gap: var(--spacing-4);
  align-items: flex-start;
}

@media (max-width: 900px) {
  .intro-grid {
    grid-template-columns: 1fr;
  }
}

.intro-side,
.intro-main {
  min-width: 0;
}

/* ===== Get started card ===== */
.get-started__title {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.get-started__text {
  margin: 0 0 var(--spacing-4);
  font-size: var(--font-size-md);
  line-height: 1.5;
  color: var(--color-text-secondary);
}

.get-started__actions {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.get-started__actions .btn {
  width: 100%;
  justify-content: center;
}

.get-started__foot {
  margin: var(--spacing-4) 0 0;
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  line-height: 1.5;
  color: var(--color-text-tertiary);
}

.get-started__foot strong {
  color: var(--color-text-secondary);
  font-weight: var(--font-weight-medium);
}

/* ===== Card title icon (matches the active view) ===== */
.card-title {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
}

.card-title__icon {
  color: var(--color-accent);
  font-size: var(--font-size-md);
}

.intro-copy {
  margin: 0;
  font-size: var(--font-size-md);
  line-height: 1.55;
  color: var(--color-text-secondary);
}

.intro-copy strong {
  color: var(--color-text-primary);
  font-weight: var(--font-weight-medium);
}
</style>
