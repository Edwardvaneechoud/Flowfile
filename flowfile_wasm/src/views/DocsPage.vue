<template>
  <div class="docs-page">
    <!-- Navigation -->
    <nav class="docs-nav">
      <div class="nav-content">
        <div class="nav-brand">
          <img src="/flowfile.png" alt="Flowfile" class="nav-logo" />
          <span class="nav-title">Flowfile</span>
        </div>
        <div class="nav-links">
          <a href="#features" class="nav-link">Features</a>
          <a href="#how-it-works" class="nav-link">How It Works</a>
          <a href="#technology" class="nav-link">Technology</a>
          <router-link to="/app" class="nav-cta">Launch App</router-link>
        </div>
      </div>
    </nav>

    <!-- Hero Section -->
    <section class="hero">
      <div class="hero-background">
        <div class="hero-gradient"></div>
        <div class="hero-grid"></div>
      </div>
      <div class="hero-content">
        <div class="hero-badge">
          <span class="badge-icon">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
            </svg>
          </span>
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
        <div class="hero-actions">
          <router-link to="/app" class="btn-primary">
            <span>Start Building</span>
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <line x1="5" y1="12" x2="19" y2="12"/>
              <polyline points="12 5 19 12 12 19"/>
            </svg>
          </router-link>
          <a href="#features" class="btn-secondary">
            <span>Learn More</span>
          </a>
        </div>
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
      </div>
    </section>

    <!-- Features Section -->
    <section id="features" class="features">
      <div class="section-header">
        <h2 class="section-title">Powerful Features</h2>
        <p class="section-description">
          Everything you need to transform, analyze, and prepare your data
        </p>
      </div>
      <div class="features-grid">
        <div class="feature-card">
          <div class="feature-icon feature-icon-blue">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/>
              <polyline points="14 2 14 8 20 8"/>
            </svg>
          </div>
          <h3 class="feature-title">Visual Data Flow</h3>
          <p class="feature-description">
            Drag-and-drop interface to build complex data pipelines.
            Connect nodes to create transformations without writing code.
          </p>
        </div>
        <div class="feature-card">
          <div class="feature-icon feature-icon-green">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
              <line x1="3" y1="9" x2="21" y2="9"/>
              <line x1="9" y1="21" x2="9" y2="9"/>
            </svg>
          </div>
          <h3 class="feature-title">CSV & Data Import</h3>
          <p class="feature-description">
            Load CSV files directly from your local machine.
            Drag and drop or paste data directly into the app.
          </p>
        </div>
        <div class="feature-card">
          <div class="feature-icon feature-icon-purple">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="12 2 2 7 12 12 22 7 12 2"/>
              <polyline points="2 17 12 22 22 17"/>
              <polyline points="2 12 12 17 22 12"/>
            </svg>
          </div>
          <h3 class="feature-title">Filter & Transform</h3>
          <p class="feature-description">
            Apply powerful filters, select columns, sort data,
            and group by with aggregations.
          </p>
        </div>
        <div class="feature-card">
          <div class="feature-icon feature-icon-orange">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="10"/>
              <line x1="2" y1="12" x2="22" y2="12"/>
              <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
            </svg>
          </div>
          <h3 class="feature-title">Join & Merge</h3>
          <p class="feature-description">
            Combine multiple datasets with inner, left, right,
            and outer joins on any columns.
          </p>
        </div>
        <div class="feature-card">
          <div class="feature-icon feature-icon-teal">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polyline points="16 18 22 12 16 6"/>
              <polyline points="8 6 2 12 8 18"/>
            </svg>
          </div>
          <h3 class="feature-title">Custom Code</h3>
          <p class="feature-description">
            Write custom Python/Polars expressions for advanced
            transformations when visual nodes aren't enough.
          </p>
        </div>
        <div class="feature-card">
          <div class="feature-icon feature-icon-pink">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
            </svg>
          </div>
          <h3 class="feature-title">Complete Privacy</h3>
          <p class="feature-description">
            Your data never leaves your browser. All processing
            happens locally using WebAssembly technology.
          </p>
        </div>
      </div>
    </section>

    <!-- How It Works Section -->
    <section id="how-it-works" class="how-it-works">
      <div class="section-header">
        <h2 class="section-title">How It Works</h2>
        <p class="section-description">
          Build powerful data pipelines in three simple steps
        </p>
      </div>
      <div class="steps-container">
        <div class="step">
          <div class="step-number">1</div>
          <div class="step-content">
            <h3 class="step-title">Load Your Data</h3>
            <p class="step-description">
              Import CSV files by dragging and dropping them onto the canvas,
              or paste data directly. Your files stay on your machine.
            </p>
          </div>
        </div>
        <div class="step-connector"></div>
        <div class="step">
          <div class="step-number">2</div>
          <div class="step-content">
            <h3 class="step-title">Build Your Pipeline</h3>
            <p class="step-description">
              Add transformation nodes from the toolbar. Connect them to create
              your data flow. Filter, sort, join, aggregate, and more.
            </p>
          </div>
        </div>
        <div class="step-connector"></div>
        <div class="step">
          <div class="step-number">3</div>
          <div class="step-content">
            <h3 class="step-title">Preview & Export</h3>
            <p class="step-description">
              See results instantly with live data previews. Export your
              transformed data when you're satisfied with the results.
            </p>
          </div>
        </div>
      </div>
    </section>

    <!-- Technology Section -->
    <section id="technology" class="technology">
      <div class="section-header">
        <h2 class="section-title">Built on Modern Technology</h2>
        <p class="section-description">
          Enterprise-grade performance powered by cutting-edge web technologies
        </p>
      </div>
      <div class="tech-grid">
        <div class="tech-card">
          <div class="tech-logo">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
            </svg>
          </div>
          <h4 class="tech-name">Polars</h4>
          <p class="tech-description">
            Lightning-fast DataFrame library written in Rust.
            Process millions of rows in milliseconds.
          </p>
        </div>
        <div class="tech-card">
          <div class="tech-logo tech-logo-wasm">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
            </svg>
          </div>
          <h4 class="tech-name">WebAssembly</h4>
          <p class="tech-description">
            Near-native performance in your browser.
            No installations, no dependencies, just open and use.
          </p>
        </div>
        <div class="tech-card">
          <div class="tech-logo tech-logo-python">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
              <path d="M14.31 2l.41 2.48h2.76l-.49-2.48h-2.68zM8.41 2l.48 2.48H6.13L6.62 2h1.79zm4.29 5.48L12 10l-.7-2.52H8.89l.48 2.52H6.62l.49-2.52H4.62v14h6.07V14h2.62v5.48h6.07v-14h-2.68z"/>
            </svg>
          </div>
          <h4 class="tech-name">Pyodide</h4>
          <p class="tech-description">
            Full Python runtime in your browser via WASM.
            Access the entire Python data science ecosystem.
          </p>
        </div>
        <div class="tech-card">
          <div class="tech-logo tech-logo-vue">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 12.765l5.592-9.437h-3.276L12 7.33l-2.316-4.002H6.408L12 12.765z"/>
              <path d="M18.461 3.332L12 14.235 5.539 3.332H1.992L12 21l10.008-17.668h-3.539z"/>
            </svg>
          </div>
          <h4 class="tech-name">Vue 3</h4>
          <p class="tech-description">
            Modern reactive UI framework.
            Smooth interactions and instant visual feedback.
          </p>
        </div>
      </div>
    </section>

    <!-- Use Cases Section -->
    <section class="use-cases">
      <div class="section-header">
        <h2 class="section-title">Perfect For</h2>
        <p class="section-description">
          Flowfile is designed for anyone who works with data
        </p>
      </div>
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

    <!-- CTA Section -->
    <section class="cta">
      <div class="cta-content">
        <h2 class="cta-title">Ready to Transform Your Data?</h2>
        <p class="cta-description">
          No sign-up required. No data leaves your browser. Just start building.
        </p>
        <router-link to="/app" class="cta-button">
          <span>Launch Flowfile</span>
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="5" y1="12" x2="19" y2="12"/>
            <polyline points="12 5 19 12 12 19"/>
          </svg>
        </router-link>
      </div>
    </section>

    <!-- Footer -->
    <footer class="docs-footer">
      <div class="footer-content">
        <div class="footer-brand">
          <img src="/flowfile.png" alt="Flowfile" class="footer-logo" />
          <span>Flowfile</span>
        </div>
        <p class="footer-text">
          Browser-based data transformation powered by WebAssembly
        </p>
      </div>
    </footer>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { useThemeStore } from '../stores/theme-store'

const themeStore = useThemeStore()

onMounted(() => {
  themeStore.initialize()
})
</script>

<style scoped>
.docs-page {
  min-height: 100vh;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}

/* Navigation */
.docs-nav {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  z-index: 100;
  background: rgba(var(--color-background-primary-rgb, 255, 255, 255), 0.9);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--color-border-light);
}

.nav-content {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 24px;
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.nav-brand {
  display: flex;
  align-items: center;
  gap: 10px;
}

.nav-logo {
  height: 32px;
  width: auto;
}

.nav-title {
  font-size: 20px;
  font-weight: 700;
  color: var(--color-text-primary);
}

.nav-links {
  display: flex;
  align-items: center;
  gap: 32px;
}

.nav-link {
  font-size: 14px;
  font-weight: 500;
  color: var(--color-text-secondary);
  text-decoration: none;
  transition: color 0.2s;
}

.nav-link:hover {
  color: var(--color-text-primary);
}

.nav-cta {
  padding: 10px 20px;
  background: var(--color-accent);
  color: white;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 600;
  text-decoration: none;
  transition: all 0.2s;
}

.nav-cta:hover {
  background: var(--color-accent-hover, #1d4ed8);
  transform: translateY(-1px);
}

/* Hero Section */
.hero {
  position: relative;
  padding: 160px 24px 100px;
  overflow: hidden;
}

.hero-background {
  position: absolute;
  inset: 0;
  pointer-events: none;
}

.hero-gradient {
  position: absolute;
  top: -50%;
  left: 50%;
  transform: translateX(-50%);
  width: 150%;
  height: 100%;
  background: radial-gradient(ellipse at center, rgba(59, 130, 246, 0.15) 0%, transparent 70%);
}

.hero-grid {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(59, 130, 246, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(59, 130, 246, 0.03) 1px, transparent 1px);
  background-size: 60px 60px;
  mask-image: radial-gradient(ellipse at center, black 30%, transparent 70%);
}

.hero-content {
  position: relative;
  max-width: 800px;
  margin: 0 auto;
  text-align: center;
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
  margin-bottom: 24px;
}

.badge-icon {
  display: flex;
}

.badge-icon svg {
  width: 14px;
  height: 14px;
}

.hero-title {
  font-size: 56px;
  font-weight: 800;
  line-height: 1.1;
  margin: 0 0 24px;
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
  font-size: 18px;
  line-height: 1.7;
  color: var(--color-text-secondary);
  margin: 0 0 40px;
  max-width: 600px;
  margin-left: auto;
  margin-right: auto;
}

.hero-actions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 16px;
  margin-bottom: 60px;
}

.btn-primary {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 14px 28px;
  background: var(--color-accent);
  color: white;
  border-radius: 10px;
  font-size: 16px;
  font-weight: 600;
  text-decoration: none;
  transition: all 0.2s;
  box-shadow: 0 4px 14px rgba(59, 130, 246, 0.4);
}

.btn-primary:hover {
  background: var(--color-accent-hover, #1d4ed8);
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
}

.btn-primary svg {
  width: 18px;
  height: 18px;
}

.btn-secondary {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 14px 28px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 10px;
  font-size: 16px;
  font-weight: 600;
  text-decoration: none;
  transition: all 0.2s;
}

.btn-secondary:hover {
  background: var(--color-background-hover);
  border-color: var(--color-border-focus);
}

.hero-stats {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 40px;
}

.stat {
  text-align: center;
}

.stat-value {
  display: block;
  font-size: 28px;
  font-weight: 700;
  color: var(--color-text-primary);
}

.stat-label {
  display: block;
  font-size: 14px;
  color: var(--color-text-secondary);
  margin-top: 4px;
}

.stat-divider {
  width: 1px;
  height: 40px;
  background: var(--color-border-light);
}

/* Features Section */
.features {
  padding: 100px 24px;
  background: var(--color-background-secondary);
}

.section-header {
  text-align: center;
  margin-bottom: 60px;
}

.section-title {
  font-size: 40px;
  font-weight: 700;
  margin: 0 0 16px;
  letter-spacing: -0.01em;
}

.section-description {
  font-size: 18px;
  color: var(--color-text-secondary);
  margin: 0;
}

.features-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 24px;
  max-width: 1200px;
  margin: 0 auto;
}

.feature-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 16px;
  padding: 32px;
  transition: all 0.3s;
}

.feature-card:hover {
  border-color: var(--color-border-focus);
  transform: translateY(-4px);
  box-shadow: 0 12px 40px rgba(0, 0, 0, 0.08);
}

.feature-icon {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: 20px;
}

.feature-icon svg {
  width: 24px;
  height: 24px;
}

.feature-icon-blue {
  background: rgba(59, 130, 246, 0.1);
  color: #3b82f6;
}

.feature-icon-green {
  background: rgba(34, 197, 94, 0.1);
  color: #22c55e;
}

.feature-icon-purple {
  background: rgba(139, 92, 246, 0.1);
  color: #8b5cf6;
}

.feature-icon-orange {
  background: rgba(249, 115, 22, 0.1);
  color: #f97316;
}

.feature-icon-teal {
  background: rgba(20, 184, 166, 0.1);
  color: #14b8a6;
}

.feature-icon-pink {
  background: rgba(236, 72, 153, 0.1);
  color: #ec4899;
}

.feature-title {
  font-size: 18px;
  font-weight: 600;
  margin: 0 0 12px;
}

.feature-description {
  font-size: 14px;
  line-height: 1.6;
  color: var(--color-text-secondary);
  margin: 0;
}

/* How It Works Section */
.how-it-works {
  padding: 100px 24px;
}

.steps-container {
  display: flex;
  align-items: flex-start;
  justify-content: center;
  gap: 0;
  max-width: 1000px;
  margin: 0 auto;
}

.step {
  flex: 1;
  text-align: center;
  padding: 0 24px;
}

.step-number {
  width: 56px;
  height: 56px;
  border-radius: 50%;
  background: linear-gradient(135deg, #3b82f6 0%, #06b6d4 100%);
  color: white;
  font-size: 24px;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto 24px;
  box-shadow: 0 4px 14px rgba(59, 130, 246, 0.3);
}

.step-connector {
  width: 80px;
  height: 2px;
  background: linear-gradient(90deg, #3b82f6, #06b6d4);
  margin-top: 28px;
  flex-shrink: 0;
}

.step-title {
  font-size: 20px;
  font-weight: 600;
  margin: 0 0 12px;
}

.step-description {
  font-size: 14px;
  line-height: 1.6;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Technology Section */
.technology {
  padding: 100px 24px;
  background: var(--color-background-secondary);
}

.tech-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 24px;
  max-width: 1000px;
  margin: 0 auto;
}

.tech-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 16px;
  padding: 28px;
  text-align: center;
  transition: all 0.3s;
}

.tech-card:hover {
  border-color: var(--color-border-focus);
  transform: translateY(-2px);
}

.tech-logo {
  width: 48px;
  height: 48px;
  margin: 0 auto 16px;
  color: #3b82f6;
}

.tech-logo svg {
  width: 100%;
  height: 100%;
}

.tech-logo-wasm {
  color: #654ff0;
}

.tech-logo-python {
  color: #3776ab;
}

.tech-logo-vue {
  color: #42b883;
}

.tech-name {
  font-size: 16px;
  font-weight: 600;
  margin: 0 0 8px;
}

.tech-description {
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Use Cases Section */
.use-cases {
  padding: 100px 24px;
}

.use-cases-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 24px;
  max-width: 1000px;
  margin: 0 auto;
}

.use-case {
  text-align: center;
  padding: 24px;
}

.use-case-icon {
  width: 56px;
  height: 56px;
  border-radius: 50%;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto 16px;
  color: var(--color-accent);
}

.use-case-icon svg {
  width: 24px;
  height: 24px;
}

.use-case h4 {
  font-size: 16px;
  font-weight: 600;
  margin: 0 0 8px;
}

.use-case p {
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-secondary);
  margin: 0;
}

/* CTA Section */
.cta {
  padding: 100px 24px;
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.05) 0%, rgba(6, 182, 212, 0.05) 100%);
  border-top: 1px solid var(--color-border-light);
  border-bottom: 1px solid var(--color-border-light);
}

.cta-content {
  text-align: center;
  max-width: 600px;
  margin: 0 auto;
}

.cta-title {
  font-size: 36px;
  font-weight: 700;
  margin: 0 0 16px;
}

.cta-description {
  font-size: 18px;
  color: var(--color-text-secondary);
  margin: 0 0 32px;
}

.cta-button {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  padding: 16px 32px;
  background: var(--color-accent);
  color: white;
  border-radius: 12px;
  font-size: 18px;
  font-weight: 600;
  text-decoration: none;
  transition: all 0.2s;
  box-shadow: 0 4px 14px rgba(59, 130, 246, 0.4);
}

.cta-button:hover {
  background: var(--color-accent-hover, #1d4ed8);
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
}

.cta-button svg {
  width: 20px;
  height: 20px;
}

/* Footer */
.docs-footer {
  padding: 40px 24px;
  background: var(--color-background-primary);
  border-top: 1px solid var(--color-border-light);
}

.footer-content {
  text-align: center;
}

.footer-brand {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 18px;
  font-weight: 600;
  color: var(--color-text-primary);
  margin-bottom: 8px;
}

.footer-logo {
  height: 24px;
  width: auto;
}

.footer-text {
  font-size: 14px;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Responsive */
@media (max-width: 1024px) {
  .features-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .tech-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .use-cases-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 768px) {
  .hero-title {
    font-size: 36px;
  }

  .features-grid {
    grid-template-columns: 1fr;
  }

  .steps-container {
    flex-direction: column;
    gap: 24px;
  }

  .step-connector {
    width: 2px;
    height: 40px;
    margin: 0 auto;
  }

  .tech-grid {
    grid-template-columns: 1fr;
  }

  .use-cases-grid {
    grid-template-columns: 1fr;
  }

  .nav-links {
    gap: 16px;
  }

  .nav-link {
    display: none;
  }
}
</style>
