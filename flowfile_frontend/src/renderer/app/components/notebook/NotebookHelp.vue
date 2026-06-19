<template>
  <Teleport to="body">
    <div class="modal-overlay" @click="$emit('close')">
      <div class="modal-container" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Notebook reference</h3>
          <button class="modal-close" aria-label="Close" @click="$emit('close')">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <section class="api-section">
            <h4>Getting started</h4>
            <p class="section-description">
              Notebooks run Python (and Markdown) on a kernel. Pick a kernel in the toolbar — Python
              cells need one; Markdown renders without. The catalog API is exposed as
              <code>flowfile_ctx</code>.
            </p>
            <div class="api-item">
              <code>Shift + Enter</code>
              <p>Run the cell (render a Markdown cell).</p>
            </div>
            <div class="api-item">
              <code>Cmd / Ctrl + Enter</code>
              <p>Run the cell and move to the next one.</p>
            </div>
            <div class="api-item">
              <code>df</code>
              <p>A bare value on the last line shows its repr (what the object is).</p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.display(df)</code>
              <p>Render a DataFrame as an interactive, sortable table.</p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.explore(df)</code>
              <p>Open the Graphic Walker explorer (data grid + drag-to-chart).</p>
            </div>
          </section>

          <section class="api-section">
            <h4>Read catalog data</h4>
            <p class="section-description">
              Read tables straight from the catalog — no upstream flow needed. These return a Polars
              <code>LazyFrame</code>; call <code>.collect()</code> for a DataFrame.
            </p>
            <div class="api-item">
              <code>flowfile_ctx.list_catalogs()</code>
              <p>
                List top-level catalogs. Navigate with <code>.get_schema()</code> /
                <code>.list_schemas()</code> / <code>.get_table_ref()</code>.
              </p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.get_catalog("Demo").get_schema("market").read_table("fx_rates")</code>
              <p>Read a table by catalog → schema → name.</p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.read_catalog_table("fx_rates", schema="market")</code>
              <p>
                Shortcut read by name. Pass <code>schema=</code> / <code>namespace_id=</code> to
                disambiguate, <code>delta_version=</code> for time travel.
              </p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.list_catalog_tables(schema="market")</code>
              <p>List available tables as <code>TableRef</code> objects.</p>
            </div>
          </section>

          <section class="api-section">
            <h4>Display &amp; explore</h4>
            <p class="section-description">Render results inline in the cell output.</p>
            <div class="api-item">
              <code>flowfile_ctx.display(obj, title?)</code>
              <p>
                Polars frames → interactive table; also matplotlib / plotly figures, PIL images, and
                HTML strings.
              </p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.explore(df)</code>
              <p>Full Graphic Walker explorer (Data + Visualization tabs).</p>
            </div>
          </section>

          <section class="api-section">
            <h4>Write back to the catalog</h4>
            <div class="api-item">
              <code>flowfile_ctx.write_catalog_table(df, "result")</code>
              <p>Write a DataFrame to a catalog table.</p>
            </div>
            <div class="api-item">
              <code
                >flowfile_ctx.get_catalog("Demo").get_schema("market").write_table(df, "result",
                write_mode="overwrite")</code
              >
              <p>
                Write via a schema ref. <code>write_mode=</code> overwrite / append / upsert…,
                <code>merge_keys=</code> for upsert.
              </p>
            </div>
          </section>

          <section class="api-section">
            <h4>Artifacts &amp; logging</h4>
            <div class="api-item">
              <code>flowfile_ctx.publish_global("model", obj)</code>
              <p>Persist a Python object to the catalog (survives across sessions).</p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.get_global("model")</code>
              <p>Retrieve a global artifact.</p>
            </div>
            <div class="api-item">
              <code>flowfile_ctx.log_info("message")</code>
              <p>Send a message to the log viewer (also log_warning / log_error).</p>
            </div>
          </section>

          <section class="api-section">
            <h4>Common patterns</h4>

            <div class="pattern">
              <h5>Read, transform, display</h5>
              <pre><code>import polars as pl

df = flowfile_ctx.get_catalog("Demo").get_schema("market").read_table("fx_rates")
strong = df.filter(pl.col("rate") > 1).collect()
flowfile_ctx.display(strong)</code></pre>
            </div>

            <div class="pattern">
              <h5>Explore a table</h5>
              <pre><code>df = flowfile_ctx.read_catalog_table("fx_rates", schema="market")
flowfile_ctx.explore(df)</code></pre>
            </div>

            <div class="pattern">
              <h5>Write a result back to the catalog</h5>
              <pre><code>import polars as pl

df = flowfile_ctx.read_catalog_table("fx_rates", schema="market").collect()
summary = df.group_by("currency").agg(pl.col("rate").mean())
flowfile_ctx.write_catalog_table(summary, "fx_rate_avg")</code></pre>
            </div>
          </section>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
defineEmits<{
  (e: "close"): void;
}>();
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 200002;
}

.modal-container {
  background-color: var(--el-bg-color);
  border-radius: var(--el-border-radius-base);
  box-shadow: var(--el-box-shadow);
  max-width: 700px;
  width: 90%;
  max-height: 85vh;
  display: flex;
  flex-direction: column;
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem 1.5rem;
  border-bottom: 1px solid var(--el-border-color-lighter);
  flex-shrink: 0;
}

.modal-title {
  margin: 0;
  font-size: 1.1rem;
  font-weight: 600;
}

.modal-close {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 1.1rem;
  color: var(--el-text-color-secondary);
  padding: 0.25rem;
}

.modal-close:hover {
  color: var(--el-text-color-primary);
}

.modal-content {
  padding: 1.5rem;
  overflow-y: auto;
  flex: 1;
}

.api-section {
  margin-bottom: 1.5rem;
}

.api-section:last-child {
  margin-bottom: 0;
}

.api-section h4 {
  font-size: 1rem;
  font-weight: 600;
  margin: 0 0 0.75rem 0;
  color: var(--el-text-color-primary);
}

.section-description {
  font-size: 0.85rem;
  color: var(--el-text-color-secondary);
  margin: 0 0 0.75rem 0;
  line-height: 1.5;
}

.api-item {
  margin-bottom: 0.5rem;
  padding: 0.5rem 0.75rem;
  background-color: var(--el-fill-color-lighter);
  border-radius: 4px;
}

.api-item code {
  font-family: var(--el-font-family, monospace);
  font-size: 0.85rem;
  color: var(--el-color-primary);
  font-weight: 500;
}

.api-item p {
  margin: 0.25rem 0 0 0;
  font-size: 0.8rem;
  color: var(--el-text-color-secondary);
}

.api-item p code {
  font-size: 0.78rem;
}

.pattern {
  margin-bottom: 1rem;
}

.pattern h5 {
  font-size: 0.85rem;
  font-weight: 600;
  margin: 0 0 0.5rem 0;
  color: var(--el-text-color-regular);
}

.pattern pre {
  background-color: #282c34;
  border-radius: 4px;
  padding: 0.75rem 1rem;
  overflow-x: auto;
  margin: 0;
}

.pattern pre code {
  font-family: var(--el-font-family, monospace);
  font-size: 0.8rem;
  color: #abb2bf;
  white-space: pre;
}
</style>
