<template>
  <Teleport to="body">
    <Transition name="about-fade">
      <div v-if="visible" class="about-overlay" @click.self="close">
        <div class="about-card" role="dialog" aria-modal="true" aria-label="About Flowfile">
          <button class="about-close" title="Close" @click="close">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
          <div class="about-body">
            <img src="/flowfile.png" alt="Flowfile logo" class="about-logo" />
            <h2 class="about-title">Flowfile</h2>
            <span v-if="version" class="about-version">v{{ version }}</span>
            <p class="about-description">
              Flowfile is an open-source visual ETL platform. This browser build runs Polars
              entirely in WebAssembly — ideal for quick, private data transformations with zero
              setup, where your data never leaves your machine.
            </p>
            <div class="about-upgrade">
              <p class="about-upgrade-title">Need more then the lite version can do?</p>
              <p class="about-upgrade-text">
                The full version adds scheduling, database &amp; cloud connections, many more
                transformations, custom Python code integration, and much more.
              </p>
              <a class="about-upgrade-btn" href="https://flowfile.io" target="_blank" rel="noopener">
                <span>Get the full version at flowfile.io</span>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
              </a>
            </div>
            <div class="about-links">
              <a class="about-link" href="https://edwardvaneechoud.github.io/Flowfile/" target="_blank" rel="noopener">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
                <span>Documentation</span>
              </a>
              <a class="about-link" href="https://github.com/edwardvaneechoud/Flowfile" target="_blank" rel="noopener">
                <svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/></svg>
                <span>GitHub</span>
              </a>
              <a class="about-link" href="https://github.com/edwardvaneechoud/Flowfile/discussions" target="_blank" rel="noopener">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
                <span>Discussions</span>
              </a>
            </div>
            <p class="about-license">Released under the MIT License.</p>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
defineProps<{ visible: boolean; version?: string }>()
const emit = defineEmits<{ (e: 'update:visible', v: boolean): void }>()
function close() {
  emit('update:visible', false)
}
</script>

<style scoped>
.about-overlay {
  position: fixed;
  inset: 0;
  z-index: var(--z-index-modal, 1050);
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-overlay, rgba(0, 0, 0, 0.5));
  padding: var(--spacing-4);
}

.about-card {
  position: relative;
  width: 100%;
  max-width: 440px;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-lg);
}

.about-close {
  position: absolute;
  top: var(--spacing-2);
  right: var(--spacing-2);
  display: flex;
  align-items: center;
  justify-content: center;
  width: 30px;
  height: 30px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-md);
  color: var(--color-text-muted);
  cursor: pointer;
}
.about-close:hover { background: var(--color-background-tertiary); color: var(--color-text-primary); }
.about-close svg { width: 16px; height: 16px; }

.about-body {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  gap: var(--spacing-2);
  padding: var(--spacing-6) var(--spacing-5) var(--spacing-5);
}

.about-logo { width: 64px; height: auto; }

.about-title {
  margin: 0;
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.about-version {
  font-size: var(--font-size-xs);
  font-family: var(--font-family-mono);
  color: var(--color-text-tertiary);
  background: var(--color-background-tertiary);
  border-radius: var(--border-radius-full);
  padding: 2px var(--spacing-2);
}

.about-description {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  line-height: var(--line-height-relaxed);
  color: var(--color-text-secondary);
}

.about-upgrade {
  width: 100%;
  margin-top: var(--spacing-4);
  padding: var(--spacing-4);
  background: rgba(8, 145, 178, 0.1);
  border: 1px solid rgba(8, 145, 178, 0.35);
  border-radius: var(--border-radius-lg);
}

.about-upgrade-title {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.about-upgrade-text {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  line-height: var(--line-height-relaxed);
  color: var(--color-text-secondary);
}

.about-upgrade-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-accent);
  border: none;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-inverse);
  text-decoration: none;
  transition: all var(--transition-fast);
}
.about-upgrade-btn:hover { background: var(--color-accent-hover, var(--color-accent)); transform: translateY(-1px); }
.about-upgrade-btn svg { width: 15px; height: 15px; }

.about-links {
  display: flex;
  justify-content: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-3);
  flex-wrap: wrap;
}

.about-link {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1-5) var(--spacing-3);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  text-decoration: none;
  transition: all var(--transition-fast);
}
.about-link:hover { background: var(--color-background-tertiary); border-color: var(--color-accent); color: var(--color-accent); }
.about-link svg { width: 15px; height: 15px; }

.about-license {
  margin: var(--spacing-3) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.about-fade-enter-active, .about-fade-leave-active { transition: opacity var(--transition-base); }
.about-fade-enter-from, .about-fade-leave-to { opacity: 0; }
</style>
