# Tutorial: K-Means on a Kernel

This tutorial builds one custom node that runs **K-Means clustering** over the numeric columns you choose, using scikit-learn inside an [isolated kernel](kernels.md). It's the kernel counterpart to the local-execution [Emoji Generator tutorial](custom-node-tutorial.md) — same shape, but the `process` method reaches for a third-party library the kernel installs for you.

You'll build the **same node two ways**:

1. **Visually, in the [Node Designer](node-designer.md)** — no file to write by hand. This is the primary path here.
2. **As a plain `.py` file** — for version-controlled or programmatic authoring.

The two are interchangeable: a node built in the designer's visual subset saves to a `.py` file, and that file reopens in the designer. The visual build and the file below are the *same node*.

!!! info "What you'll build"
    A **K-Means Clustering** node that:

    - Lands in the **ML** palette group.
    - Lets the user pick numeric feature columns, the number of clusters `k`, an output column name, and whether to standardize features first.
    - Runs scikit-learn in a Docker kernel and adds a cluster-label column to the data.

## Before you start

This node runs its Python in a **kernel** — a Docker container that pip-installs the packages you ask for — so you'll need a running kernel to test it. Open the [Kernel Manager](kernels.md) from the sidebar and start one (Docker must be available). Everything else happens in the Node Designer.

!!! note "Why a kernel here?"
    scikit-learn isn't one of the libraries Flowfile ships with, and a **local** node can only use what's already installed. Running in a kernel lets this node list `scikit-learn` as a dependency and have it installed before it runs.

---

## Build it in the Node Designer

Open **Node Designer** from the sidebar and click **New**.

### 1. Identity

In the left **Node Settings** panel, under **Identity**:

- **Node Name** → `K-Means Clustering`
- **Category** → `ML` (type it or pick it — this becomes the palette group)
- **Node Icon** → pick anything that reads as clustering/ML.

### 2. Execution environment

Open the **Execution** group and choose the **Isolated kernel** card. A **Dependencies (pip)** editor appears — add one tag:

```
scikit-learn
```

The kernel installs these packages before your node runs. (If Docker is unavailable, the card explains why and links to the Kernel Manager — start a kernel there first.)

<details markdown="1" open>
<summary>Execution environment</summary>

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: capture the Execution group with Isolated kernel selected + scikit-learn dependency tag -->
![Execution set to Isolated kernel with a scikit-learn dependency tag](../../assets/images/developers/kmeans-kernel-environment.svg)

</details>

### 3. The settings form

On the **Form** tab, add a group. Give it the display title **Clustering** and the Python attribute name `clustering` (this is what you'll type in the code). Then **Add a control** four times:

| Control | Field name | Configure |
|---------|------------|-----------|
| **Column Selector** | `feature_columns` | Data types → **Numeric**, **Multiple** on, **Required** on |
| **Numeric Input** | `n_clusters` | Label "Number of clusters (k)", default `3`, min `2`, max `20` |
| **Text Input** | `cluster_column` | Label "Cluster column name", default `cluster` |
| **Toggle Switch** | `standardize` | Label "Standardize features first", default on |

<details markdown="1">
<summary>The Clustering group</summary>

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: capture the Form tab with the Clustering group and its four controls -->
![Form tab with the Clustering group and its four controls](../../assets/images/developers/kmeans-settings-form.svg)

</details>

### 4. The process logic

Switch to the **Code** tab. The signature header is fixed — you write the **body only**. The **Form fields** panel on the left lists every control with its accessor path; click one to insert it.

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

cfg = self.settings_schema.clustering
feature_cols = cfg.feature_columns.value     # list[str]
k = int(cfg.n_clusters.value)                # a Numeric Input is always a float
label_col = cfg.cluster_column.value

df = inputs[0].collect()                     # sklearn needs eager data
features = df.select(feature_cols).to_numpy()
if cfg.standardize.value:
    features = StandardScaler().fit_transform(features)

model = KMeans(n_clusters=k, n_init=10, random_state=42)
labels = model.fit_predict(features)

flowfile_ctx.log_info(
    f"Fitted {k} clusters over {len(feature_cols)} features and {df.height} rows"
)
return df.with_columns(pl.Series(label_col, labels))
```

Three things in that body are worth calling out — they're the difference between a kernel node that runs and one that doesn't:

!!! warning "Import third-party libraries *inside* `process`"
    `from sklearn... import ...` lives **inside** `process`, not at the top of the file. Flowfile's core loads your node file to register it in the palette, and core does **not** have the kernel's dependencies installed. A top-level `import sklearn` would make the node load with an error. Importing inside `process` keeps registration clean — the import only runs in the kernel, which has the package.

- **`int(cfg.n_clusters.value)`** — a Numeric Input value is always a `float`; `KMeans` wants an `int`, so cast it.
- **`flowfile_ctx`** — available directly inside a kernel node's `process`. Use it for run feedback (`log_info`), and — see [Extensions](#extensions) — artifacts. (`flowfile_ctx` is *not* available in a `local` node.)

### 5. Test it

On the **Test** tab, pick a kernel from the **Kernel** selector, then paste this sample — nine customers in three obvious groups — and click **Run test**:

```csv
customer_id,age,annual_income_k,spending_score,segment
C001,24,27.5,33,budget
C002,26,30.1,38,budget
C003,23,25.8,31,budget
C004,45,95.6,81,premium
C005,47,101.5,86,premium
C006,44,90.2,79,premium
C007,57,74.0,18,saver
C008,60,68.5,22,saver
C009,58,71.3,15,saver
```

Set the feature columns to `age`, `annual_income_k`, and `spending_score`, and leave **k** at 3. The output grid gains a `cluster` column, and the three groups fall into three clusters that line up with the `segment` label — which the numeric picker leaves alone. You also get the schema, row count, and your `log_info` line in the **Logs** panel — the same execution path a real run uses. Tick **Save test setup with node** to keep the sample and settings in the file.

!!! tip "A bigger sample to test with"
    For a fuller run, the repo ships [`customer_segments.csv`](https://raw.githubusercontent.com/edwardvaneechoud/Flowfile/main/data/templates/customer_segments.csv) — 45 customers across the same three segments (`budget`, `premium`, `saver`). Read it with a **Read data** node and feed it into the K-Means node on the canvas.

<details markdown="1">
<summary>Test run</summary>

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: capture the Test tab dry-run with the cluster column + the log line -->
![Test tab dry-run showing the cluster column and the log line](../../assets/images/developers/kmeans-test-results.svg)

</details>

### 6. Save

Click **Save**. The node loads immediately — no restart — under the **ML** group in the palette, with a small kernel badge marking it as kernel-backed. Drag it onto the canvas, connect numeric input, choose a kernel in its settings drawer, and run.

<details markdown="1">
<summary>On the canvas</summary>

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: capture the K-Means node in the ML group and placed on the canvas with its kernel badge -->
![The K-Means node in the ML palette group and on the canvas](../../assets/images/developers/kmeans-on-canvas.svg)

</details>

---

## The same node as a file

If you prefer to write the node directly — to version-control it, or to skip the UI — here is the complete file. Save it as `~/.flowfile/user_defined_nodes/kmeans_clustering.py`. Because it stays within the visual subset, it **reopens in the Node Designer**: this file and the visual build above are the same node.

```python
import polars as pl

from flowfile import node_designer as nd


class KMeansSettings(nd.NodeSettings):
    clustering: nd.Section = nd.Section(
        title="Clustering",
        description="Group rows into clusters from their numeric features.",
        feature_columns=nd.ColumnSelector(
            label="Feature columns",
            data_types=nd.Types.Numeric,
            multiple=True,
            required=True,
        ),
        n_clusters=nd.NumericInput(
            label="Number of clusters (k)",
            default=3,
            min_value=2,
            max_value=20,
        ),
        cluster_column=nd.TextInput(
            label="Cluster column name",
            default="cluster",
        ),
        standardize=nd.ToggleSwitch(
            label="Standardize features first",
            default=True,
            description="Scale each feature to mean 0 / unit variance before clustering.",
        ),
    )


class KMeansClusteringNode(nd.CustomNodeBase):
    node_name: str = "K-Means Clustering"
    node_category: str = "ML"
    title: str = "K-Means Clustering"
    intro: str = "Group rows into k clusters over the numeric columns you choose."

    environment: str = "kernel"
    dependencies: list[str] = ["scikit-learn"]

    settings_schema: KMeansSettings = KMeansSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        # Third-party imports live inside process: the kernel has them, core does not.
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        cfg = self.settings_schema.clustering
        feature_cols = cfg.feature_columns.value     # list[str]
        k = int(cfg.n_clusters.value)                # a Numeric Input is always a float
        label_col = cfg.cluster_column.value

        df = inputs[0].collect()                     # sklearn needs eager data
        features = df.select(feature_cols).to_numpy()
        if cfg.standardize.value:
            features = StandardScaler().fit_transform(features)

        model = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = model.fit_predict(features)

        flowfile_ctx.log_info(
            f"Fitted {k} clusters over {len(feature_cols)} features and {df.height} rows"
        )
        return df.with_columns(pl.Series(label_col, labels))
```

!!! note "Not auto-tested"
    Unlike the [greeting example](creating-custom-nodes.md#quick-start), this node isn't part of the docs test suite — it needs scikit-learn and a running kernel, which the test environment doesn't provide. Run it in the designer's Test tab instead.

---

## Extensions

Once the base node works, two small additions show off what a kernel node can do. Both use the [`flowfile_ctx` API](kernel-api.md).

**Persist the fitted model** as a flow artifact, so a downstream node can reuse it:

```python
flowfile_ctx.publish_artifact("kmeans_model", model)
```

**Emit the cluster centroids as a second output.** Declare two output names and return a dict (see [Multiple outputs](creating-custom-nodes.md#multiple-outputs)):

```python
number_of_outputs: int = 2
output_names: list[str] = ["clustered", "centroids"]

# in process, instead of a single return:
centroids = pl.DataFrame(model.cluster_centers_, schema=feature_cols)
return {
    "clustered": df.with_columns(pl.Series(label_col, labels)),
    "centroids": centroids,
}
```

---

## Related Documentation

- [Custom Node Tutorial](custom-node-tutorial.md) — the local-execution counterpart (Emoji Generator).
- [Creating Custom Nodes](creating-custom-nodes.md) — the full SDK reference: components, execution environments, multi-output.
- [Node Designer](node-designer.md) — the visual designer, in depth.
- [Kernel Execution](kernels.md) — creating and running Docker kernels.
- [The flowfile_ctx API](kernel-api.md) — logging, artifacts, and catalog access inside kernels.
