<template>
  <Teleport to="body">
    <Transition name="modal">
      <div v-if="isOpen" class="docs-overlay" @click.self="$emit('close')">
        <div class="docs-modal">
          <!-- Modal Header -->
          <div class="modal-header">
            <div class="modal-brand">
              <img src="/flowfile.png" alt="Flowfile" class="modal-logo" />
              <div>
                <h2 class="modal-title">Flowfile</h2>
                <p class="modal-subtitle">Browser-Based Data Designer</p>
              </div>
            </div>
            <button class="close-btn" @click="$emit('close')" title="Close">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <line x1="18" y1="6" x2="6" y2="18"/>
                <line x1="6" y1="6" x2="18" y2="18"/>
              </svg>
            </button>
          </div>

          <!-- Modal Content -->
          <div class="modal-content">
            <!-- Hero Section -->
            <section class="hero-section">
              <div class="hero-badge">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                </svg>
                <span>100% Browser-Based</span>
              </div>
              <h1 class="hero-title">
                Transform Your Data
                <span class="title-gradient">Without the Cloud</span>
              </h1>
              <p class="hero-description">
                Flowfile brings the power of professional data transformation to your browser.
                Build visual data pipelines, process files locally, and never worry about
                data privacy again. No servers, no uploads, complete control.
              </p>
              <div class="hero-stats">
                <div class="stat">
                  <span class="stat-value">Zero</span>
                  <span class="stat-label">Data Uploads</span>
                </div>
                <div class="stat-divider"></div>
                <div class="stat">
                  <span class="stat-value">100%</span>
                  <span class="stat-label">Privacy</span>
                </div>
                <div class="stat-divider"></div>
                <div class="stat">
                  <span class="stat-value">Instant</span>
                  <span class="stat-label">Processing</span>
                </div>
              </div>
            </section>

            <!-- Features Section -->
            <section class="section">
              <h2 class="section-title">Powerful Features</h2>
              <div class="features-grid">
                <div class="feature-card">
                  <div class="feature-icon feature-icon-blue">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/>
                      <polyline points="14 2 14 8 20 8"/>
                    </svg>
                  </div>
                  <h3>Visual Data Flow</h3>
                  <p>Drag-and-drop interface to build complex data pipelines. Connect nodes to create transformations without writing code.</p>
                </div>
                <div class="feature-card">
                  <div class="feature-icon feature-icon-green">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                      <line x1="3" y1="9" x2="21" y2="9"/>
                      <line x1="9" y1="21" x2="9" y2="9"/>
                    </svg>
                  </div>
                  <h3>CSV & Data Import</h3>
                  <p>Load CSV files directly from your local machine. Drag and drop or paste data directly into the app.</p>
                </div>
                <div class="feature-card">
                  <div class="feature-icon feature-icon-purple">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <polygon points="12 2 2 7 12 12 22 7 12 2"/>
                      <polyline points="2 17 12 22 22 17"/>
                      <polyline points="2 12 12 17 22 12"/>
                    </svg>
                  </div>
                  <h3>Filter & Transform</h3>
                  <p>Apply powerful filters, select columns, sort data, and group by with aggregations.</p>
                </div>
                <div class="feature-card">
                  <div class="feature-icon feature-icon-orange">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <circle cx="12" cy="12" r="10"/>
                      <line x1="2" y1="12" x2="22" y2="12"/>
                      <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
                    </svg>
                  </div>
                  <h3>Join & Merge</h3>
                  <p>Combine multiple datasets with inner, left, right, and outer joins on any columns.</p>
                </div>
                <div class="feature-card">
                  <div class="feature-icon feature-icon-teal">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <polyline points="16 18 22 12 16 6"/>
                      <polyline points="8 6 2 12 8 18"/>
                    </svg>
                  </div>
                  <h3>Custom Code</h3>
                  <p>Write custom Python/Polars expressions for advanced transformations when visual nodes aren't enough.</p>
                </div>
                <div class="feature-card">
                  <div class="feature-icon feature-icon-pink">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
                    </svg>
                  </div>
                  <h3>Complete Privacy</h3>
                  <p>Your data never leaves your browser. All processing happens locally using WebAssembly technology.</p>
                </div>
              </div>
            </section>

            <!-- How It Works Section -->
            <section class="section">
              <h2 class="section-title">How It Works</h2>
              <div class="steps-container">
                <div class="step">
                  <div class="step-number">1</div>
                  <h3>Load Your Data</h3>
                  <p>Import CSV files by dragging and dropping them onto the canvas, or paste data directly. Your files stay on your machine.</p>
                </div>
                <div class="step">
                  <div class="step-number">2</div>
                  <h3>Build Your Pipeline</h3>
                  <p>Add transformation nodes from the toolbar. Connect them to create your data flow. Filter, sort, join, aggregate, and more.</p>
                </div>
                <div class="step">
                  <div class="step-number">3</div>
                  <h3>Preview & Export</h3>
                  <p>See results instantly with live data previews. Export your transformed data when you're satisfied with the results.</p>
                </div>
              </div>
            </section>

            <!-- Technology Section -->
            <section class="section">
              <h2 class="section-title">Built on Modern Technology</h2>
              <div class="tech-grid">
                <div class="tech-card">
                  <div class="tech-logo">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
                    </svg>
                  </div>
                  <h4>Polars</h4>
                  <p>Lightning-fast DataFrame library written in Rust. Process millions of rows in milliseconds.</p>
                </div>
                <div class="tech-card">
                  <div class="tech-logo tech-logo-wasm">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
                    </svg>
                  </div>
                  <h4>WebAssembly</h4>
                  <p>Near-native performance in your browser. No installations, no dependencies, just open and use.</p>
                </div>
                <div class="tech-card">
                  <div class="tech-logo tech-logo-python">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M14.31 2l.41 2.48h2.76l-.49-2.48h-2.68zM8.41 2l.48 2.48H6.13L6.62 2h1.79zm4.29 5.48L12 10l-.7-2.52H8.89l.48 2.52H6.62l.49-2.52H4.62v14h6.07V14h2.62v5.48h6.07v-14h-2.68z"/>
                    </svg>
                  </div>
                  <h4>Pyodide</h4>
                  <p>Full Python runtime in your browser via WASM. Access the entire Python data science ecosystem.</p>
                </div>
                <div class="tech-card">
                  <div class="tech-logo tech-logo-vue">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M12 12.765l5.592-9.437h-3.276L12 7.33l-2.316-4.002H6.408L12 12.765z"/>
                      <path d="M18.461 3.332L12 14.235 5.539 3.332H1.992L12 21l10.008-17.668h-3.539z"/>
                    </svg>
                  </div>
                  <h4>Vue 3</h4>
                  <p>Modern reactive UI framework. Smooth interactions and instant visual feedback.</p>
                </div>
              </div>
            </section>

            <!-- Use Cases Section -->
            <section class="section">
              <h2 class="section-title">Perfect For</h2>
              <div class="use-cases-grid">
                <div class="use-case">
                  <div class="use-case-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                      <circle cx="9" cy="7" r="4"/>
                      <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
                      <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
                    </svg>
                  </div>
                  <h4>Data Analysts</h4>
                  <p>Quick data exploration and transformation without setup complexity</p>
                </div>
                <div class="use-case">
                  <div class="use-case-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
                    </svg>
                  </div>
                  <h4>Security-Conscious Teams</h4>
                  <p>Process sensitive data without cloud uploads or third-party access</p>
                </div>
                <div class="use-case">
                  <div class="use-case-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <rect x="2" y="3" width="20" height="14" rx="2" ry="2"/>
                      <line x1="8" y1="21" x2="16" y2="21"/>
                      <line x1="12" y1="17" x2="12" y2="21"/>
                    </svg>
                  </div>
                  <h4>Remote Workers</h4>
                  <p>Work offline, on any device, without IT dependencies</p>
                </div>
                <div class="use-case">
                  <div class="use-case-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/>
                      <path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>
                    </svg>
                  </div>
                  <h4>Educators & Students</h4>
                  <p>Learn data concepts visually without infrastructure setup</p>
                </div>
              </div>
            </section>

            <!-- Links Section -->
            <section class="section links-section">
              <h2 class="section-title">Learn More</h2>
              <div class="links-grid">
                <a href="https://edwardvaneechoud.github.io/Flowfile/" target="_blank" rel="noopener" class="link-card">
                  <div class="link-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                      <polyline points="14 2 14 8 20 8"/>
                      <line x1="16" y1="13" x2="8" y2="13"/>
                      <line x1="16" y1="17" x2="8" y2="17"/>
                    </svg>
                  </div>
                  <div class="link-content">
                    <h4>Documentation</h4>
                    <p>Read the full documentation for detailed guides and API reference</p>
                  </div>
                  <svg class="link-arrow" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="5" y1="12" x2="19" y2="12"/>
                    <polyline points="12 5 19 12 12 19"/>
                  </svg>
                </a>
                <a href="https://github.com/Edwardvaneechoud/Flowfile" target="_blank" rel="noopener" class="link-card">
                  <div class="link-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                    </svg>
                  </div>
                  <div class="link-content">
                    <h4>GitHub Repository</h4>
                    <p>View the source code, report issues, and contribute to the project</p>
                  </div>
                  <svg class="link-arrow" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="5" y1="12" x2="19" y2="12"/>
                    <polyline points="12 5 19 12 12 19"/>
                  </svg>
                </a>
              </div>
            </section>
          </div>

          <!-- Modal Footer -->
          <div class="modal-footer">
            <button class="start-btn" @click="$emit('close')">
              <span>Start Building</span>
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <line x1="5" y1="12" x2="19" y2="12"/>
                <polyline points="12 5 19 12 12 19"/>
              </svg>
            </button>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
defineProps<{
  isOpen: boolean
}>()

defineEmits<{
  close: []
}>()
</script>

<style scoped>
.docs-overlay {
  position: fixed;
  inset: 0;
  z-index: 9999;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}

.docs-modal {
  background: var(--color-background-primary);
  border-radius: 16px;
  width: 100%;
  max-width: 900px;
  max-height: calc(100vh - 48px);
  display: flex;
  flex-direction: column;
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
  border: 1px solid var(--color-border-primary);
}

/* Modal Header */
.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20px 24px;
  border-bottom: 1px solid var(--color-border-light);
  flex-shrink: 0;
}

.modal-brand {
  display: flex;
  align-items: center;
  gap: 12px;
}

.modal-logo {
  height: 40px;
  width: auto;
}

.modal-title {
  font-size: 20px;
  font-weight: 700;
  margin: 0;
  color: var(--color-text-primary);
}

.modal-subtitle {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 2px 0 0;
}

.close-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border: none;
  border-radius: 8px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all 0.2s;
}

.close-btn:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.close-btn svg {
  width: 18px;
  height: 18px;
}

/* Modal Content */
.modal-content {
  flex: 1;
  overflow-y: auto;
  padding: 32px;
}

/* Hero Section */
.hero-section {
  text-align: center;
  margin-bottom: 48px;
  padding-bottom: 48px;
  border-bottom: 1px solid var(--color-border-light);
}

.hero-badge {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  background: rgba(59, 130, 246, 0.1);
  border: 1px solid rgba(59, 130, 246, 0.2);
  border-radius: 100px;
  font-size: 13px;
  font-weight: 500;
  color: var(--color-accent);
  margin-bottom: 20px;
}

.hero-badge svg {
  width: 14px;
  height: 14px;
}

.hero-title {
  font-size: 36px;
  font-weight: 800;
  line-height: 1.2;
  margin: 0 0 16px;
  letter-spacing: -0.02em;
}

.title-gradient {
  display: block;
  background: linear-gradient(135deg, #3b82f6 0%, #06b6d4 50%, #8b5cf6 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.hero-description {
  font-size: 16px;
  line-height: 1.7;
  color: var(--color-text-secondary);
  margin: 0 auto 32px;
  max-width: 600px;
}

.hero-stats {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 32px;
}

.stat {
  text-align: center;
}

.stat-value {
  display: block;
  font-size: 24px;
  font-weight: 700;
  color: var(--color-text-primary);
}

.stat-label {
  display: block;
  font-size: 13px;
  color: var(--color-text-secondary);
  margin-top: 4px;
}

.stat-divider {
  width: 1px;
  height: 32px;
  background: var(--color-border-light);
}

/* Sections */
.section {
  margin-bottom: 48px;
}

.section:last-child {
  margin-bottom: 0;
}

.section-title {
  font-size: 24px;
  font-weight: 700;
  margin: 0 0 24px;
  text-align: center;
}

/* Features Grid */
.features-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.feature-card {
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: 12px;
  padding: 20px;
  transition: all 0.2s;
}

.feature-card:hover {
  border-color: var(--color-border-focus);
}

.feature-icon {
  width: 40px;
  height: 40px;
  border-radius: 10px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 12px;
}

.feature-icon svg {
  width: 20px;
  height: 20px;
}

.feature-icon-blue { background: rgba(59, 130, 246, 0.1); color: #3b82f6; }
.feature-icon-green { background: rgba(34, 197, 94, 0.1); color: #22c55e; }
.feature-icon-purple { background: rgba(139, 92, 246, 0.1); color: #8b5cf6; }
.feature-icon-orange { background: rgba(249, 115, 22, 0.1); color: #f97316; }
.feature-icon-teal { background: rgba(20, 184, 166, 0.1); color: #14b8a6; }
.feature-icon-pink { background: rgba(236, 72, 153, 0.1); color: #ec4899; }

.feature-card h3 {
  font-size: 15px;
  font-weight: 600;
  margin: 0 0 8px;
}

.feature-card p {
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Steps */
.steps-container {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 24px;
}

.step {
  text-align: center;
}

.step-number {
  width: 48px;
  height: 48px;
  border-radius: 50%;
  background: linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%);
  color: white;
  font-size: 20px;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto 16px;
}

.step h3 {
  font-size: 16px;
  font-weight: 600;
  margin: 0 0 8px;
}

.step p {
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Tech Grid */
.tech-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}

.tech-card {
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: 12px;
  padding: 20px;
  text-align: center;
}

.tech-logo {
  width: 36px;
  height: 36px;
  margin: 0 auto 12px;
  color: #3b82f6;
}

.tech-logo svg {
  width: 100%;
  height: 100%;
}

.tech-logo-wasm { color: #654ff0; }
.tech-logo-python { color: #3776ab; }
.tech-logo-vue { color: #42b883; }

.tech-card h4 {
  font-size: 14px;
  font-weight: 600;
  margin: 0 0 6px;
}

.tech-card p {
  font-size: 12px;
  line-height: 1.4;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Use Cases Grid */
.use-cases-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}

.use-case {
  text-align: center;
  padding: 16px;
}

.use-case-icon {
  width: 48px;
  height: 48px;
  border-radius: 50%;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto 12px;
  color: var(--color-accent);
}

.use-case-icon svg {
  width: 22px;
  height: 22px;
}

.use-case h4 {
  font-size: 14px;
  font-weight: 600;
  margin: 0 0 6px;
}

.use-case p {
  font-size: 12px;
  line-height: 1.4;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Links Section */
.links-section {
  padding-top: 48px;
  border-top: 1px solid var(--color-border-light);
}

.links-grid {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.link-card {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 16px 20px;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: 12px;
  text-decoration: none;
  transition: all 0.2s;
}

.link-card:hover {
  border-color: var(--color-accent);
  background: var(--color-background-hover);
}

.link-card.highlight {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.05) 0%, rgba(6, 182, 212, 0.05) 100%);
  border-color: rgba(59, 130, 246, 0.2);
}

.link-card.highlight:hover {
  border-color: var(--color-accent);
}

.link-icon {
  width: 40px;
  height: 40px;
  border-radius: 10px;
  background: rgba(59, 130, 246, 0.1);
  color: var(--color-accent);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.link-icon svg {
  width: 20px;
  height: 20px;
}

.link-content {
  flex: 1;
}

.link-content h4 {
  font-size: 15px;
  font-weight: 600;
  margin: 0 0 4px;
  color: var(--color-text-primary);
}

.link-content p {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 0;
}

.link-arrow {
  width: 20px;
  height: 20px;
  color: var(--color-text-muted, var(--color-text-secondary));
  flex-shrink: 0;
  transition: transform 0.2s;
}

.link-card:hover .link-arrow {
  transform: translateX(4px);
  color: var(--color-accent);
}

/* Modal Footer */
.modal-footer {
  padding: 20px 24px;
  border-top: 1px solid var(--color-border-light);
  display: flex;
  justify-content: center;
  flex-shrink: 0;
}

.start-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 12px 28px;
  background: var(--color-accent);
  color: white;
  border: none;
  border-radius: 10px;
  font-size: 15px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  box-shadow: 0 4px 14px rgba(59, 130, 246, 0.3);
}

.start-btn:hover {
  background: var(--color-accent-hover, #1d4ed8);
  transform: translateY(-1px);
  box-shadow: 0 6px 20px rgba(59, 130, 246, 0.4);
}

.start-btn svg {
  width: 18px;
  height: 18px;
}

/* Transitions */
.modal-enter-active,
.modal-leave-active {
  transition: all 0.3s ease;
}

.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}

.modal-enter-from .docs-modal,
.modal-leave-to .docs-modal {
  transform: scale(0.95) translateY(10px);
}

/* Responsive */
@media (max-width: 768px) {
  .docs-overlay {
    padding: 16px;
  }

  .modal-content {
    padding: 24px;
  }

  .hero-title {
    font-size: 28px;
  }

  .hero-stats {
    gap: 20px;
  }

  .features-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .steps-container {
    grid-template-columns: 1fr;
    gap: 20px;
  }

  .tech-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .use-cases-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 480px) {
  .features-grid {
    grid-template-columns: 1fr;
  }

  .tech-grid {
    grid-template-columns: 1fr;
  }

  .use-cases-grid {
    grid-template-columns: 1fr;
  }
}
</style>
