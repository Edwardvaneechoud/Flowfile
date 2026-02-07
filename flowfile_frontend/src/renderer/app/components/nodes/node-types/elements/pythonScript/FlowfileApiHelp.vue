<template>
  <div class="modal-overlay" @click="$emit('close')">
    <div class="modal-container" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">FlowFile Python API Reference</h3>
        <button class="modal-close" aria-label="Close" @click="$emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <section class="api-section">
          <h4>Data I/O</h4>
          <div class="api-item">
            <code>flowfile.read_input()</code>
            <p>Read the main input as a Polars LazyFrame.</p>
          </div>
          <div class="api-item">
            <code>flowfile.read_input("left")</code>
            <p>Read a named input connection.</p>
          </div>
          <div class="api-item">
            <code>flowfile.read_inputs()</code>
            <p>Read all inputs as a dict of LazyFrame lists (one per connection).</p>
          </div>
          <div class="api-item">
            <code>flowfile.publish_output(df)</code>
            <p>Write the main output DataFrame.</p>
          </div>
          <div class="api-item">
            <code>flowfile.publish_output(df, "secondary")</code>
            <p>Write a named output DataFrame.</p>
          </div>
        </section>

        <section class="api-section">
          <h4>Artifacts</h4>
          <p class="section-description">
            Artifacts are Python objects stored in kernel memory that persist between node
            executions. Use them to share models, encoders, or any Python object between nodes on
            the same kernel.
          </p>
          <div class="api-item">
            <code>flowfile.publish_artifact("model", obj)</code>
            <p>Store a Python object as a named artifact.</p>
          </div>
          <div class="api-item">
            <code>flowfile.read_artifact("model")</code>
            <p>Retrieve a previously published artifact.</p>
          </div>
          <div class="api-item">
            <code>flowfile.delete_artifact("model")</code>
            <p>Remove an artifact from kernel memory.</p>
          </div>
          <div class="api-item">
            <code>flowfile.list_artifacts()</code>
            <p>List all available artifacts in the kernel.</p>
          </div>
        </section>

        <section class="api-section">
          <h4>Display</h4>
          <p class="section-description">
            Render rich objects (matplotlib figures, plotly figures, PIL images, HTML strings) in the
            output panel.
          </p>
          <div class="api-item">
            <code>flowfile.display(obj)</code>
            <p>Display a rich object in the output panel.</p>
          </div>
          <div class="api-item">
            <code>flowfile.display(fig, "My Chart")</code>
            <p>Display with an optional title.</p>
          </div>
        </section>

        <section class="api-section">
          <h4>Global Artifacts</h4>
          <p class="section-description">
            Global artifacts persist across sessions in the catalog. Use them to share models,
            datasets, or any Python object between flows. The flow must be registered in the catalog
            to use global artifacts.
          </p>
          <div class="api-item">
            <code>flowfile.publish_global("model", obj)</code>
            <p>Persist a Python object to the global artifact store.</p>
          </div>
          <div class="api-item">
            <code>flowfile.publish_global("model", obj, description="...", tags=["ml"])</code>
            <p>Publish with optional description, tags, namespace, and format.</p>
          </div>
          <div class="api-item">
            <code>flowfile.get_global("model")</code>
            <p>Retrieve a Python object from the global artifact store.</p>
          </div>
          <div class="api-item">
            <code>flowfile.get_global("model", version=2)</code>
            <p>Retrieve a specific version of a global artifact.</p>
          </div>
          <div class="api-item">
            <code>flowfile.list_global_artifacts()</code>
            <p>List all available global artifacts (with optional namespace/tag filters).</p>
          </div>
          <div class="api-item">
            <code>flowfile.delete_global_artifact("model")</code>
            <p>Delete a global artifact by name (optionally a specific version).</p>
          </div>
        </section>

        <section class="api-section">
          <h4>Logging</h4>
          <p class="section-description">
            Send log messages to the FlowFile log viewer for debugging and monitoring.
          </p>
          <div class="api-item">
            <code>flowfile.log("message")</code>
            <p>Send a log message (default level: INFO).</p>
          </div>
          <div class="api-item">
            <code>flowfile.log_info("message")</code>
            <p>Send an INFO log message.</p>
          </div>
          <div class="api-item">
            <code>flowfile.log_warning("message")</code>
            <p>Send a WARNING log message.</p>
          </div>
          <div class="api-item">
            <code>flowfile.log_error("message")</code>
            <p>Send an ERROR log message.</p>
          </div>
        </section>

        <section class="api-section">
          <h4>Common Patterns</h4>

          <div class="pattern">
            <h5>Basic Transform</h5>
            <pre><code>import polars as pl

df = flowfile.read_input()
df = df.filter(pl.col("age") > 18)
flowfile.publish_output(df)</code></pre>
          </div>

          <div class="pattern">
            <h5>Train a Model</h5>
            <pre><code>import polars as pl
from sklearn.ensemble import RandomForestClassifier

df = flowfile.read_input().collect()
X = df.select(["feature_1", "feature_2"]).to_numpy()
y = df.get_column("target").to_numpy()

model = RandomForestClassifier()
model.fit(X, y)

flowfile.publish_artifact("model", model)
flowfile.publish_output(flowfile.read_input())</code></pre>
          </div>

          <div class="pattern">
            <h5>Apply a Model</h5>
            <pre><code>import polars as pl

model = flowfile.read_artifact("model")
df = flowfile.read_input().collect()

X = df.select(["feature_1", "feature_2"]).to_numpy()
predictions = model.predict(X)

result = df.with_columns(
    pl.Series("prediction", predictions)
)
flowfile.publish_output(result.lazy())</code></pre>
          </div>

          <div class="pattern">
            <h5>Publish a Global Artifact</h5>
            <pre><code>from sklearn.ensemble import RandomForestClassifier

model = flowfile.read_artifact("model")
flowfile.publish_global("rf_model", model,
    description="Trained random forest",
    tags=["ml", "production"])
flowfile.log_info("Model published to catalog")</code></pre>
          </div>

          <div class="pattern">
            <h5>Multiple Inputs</h5>
            <pre><code>import polars as pl

inputs = flowfile.read_inputs()
# inputs is a dict: {"main": [LazyFrame, ...]}
# Each connected input is a separate LazyFrame in the list
df1, df2 = inputs["main"]
combined = pl.concat([df1, df2])
flowfile.publish_output(combined)</code></pre>
          </div>
        </section>
      </div>
    </div>
  </div>
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
  z-index: 2000;
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
