---
name: flowfile-custom-node-authoring
description: Turn a plain-English description ("a node that runs on the kernel and does XGBoost predictions", "a node that trims whitespace", "an ML clustering node") into a correct single-file Flowfile custom node `.py` authored with the `node_designer` SDK (`from flowfile import node_designer as nd`, `CustomNodeBase`, `NodeSettings`/`Section`, `ColumnSelector`/`SingleSelect`/`NumericInput`/…). Use when a task says "generate/create/author/write a custom node", "make a node that does X", "a kernel node", "an sklearn/xgboost/lightgbm ML node", "a node with settings for …", when picking `environment="local"` vs `"kernel"` and `dependencies`, when reading settings inside `process()`, or when a generated node must pass the security scanner / bundle validation to install or publish. NOT for adding a built-in node type across core/frontend/frame/wasm — that is `flowfile-node-development`.
---

# Flowfile custom-node authoring

Generate one `.py` file that defines a user custom node via the `node_designer` SDK, from a description of what the node should do. The file is the whole node: the visual Node Designer round-trips it, the worker (or a Docker kernel) executes its `process()`, and the community registry publishes it as-is.

## When NOT to use this skill

- Adding a **built-in** node type (a first-party transform wired across `flowfile_core` settings schema + `FlowGraph` + frontend registry + `flowfile_frame` + wasm) → `flowfile-node-development`. That is four registries agreeing by naming convention; this skill is one self-contained file.
- Publishing / browsing / installing shared nodes, the community registry, the index bot, device-flow PR publishing → see the community-nodes contract in the root `CLAUDE.md` and `flowfile_core/flowfile/community_nodes/`.
- Kernel container lifecycle, image builds, `FLOWFILE_KERNEL_IMAGE_*`, kernel↔core auth → `flowfile-config-and-flags` + `flowfile_core/flowfile_core/kernel/`.
- Debugging why an *existing* node opens in an error state, `KernelRequiredError`, exec-vs-AST scan failures → `flowfile-debugging-playbook`.
- Deep FlowFrame/Expr / generated-code parity → `flowfile-frame-and-codegen`.

All facts below verified in-repo on **2026-07-12**, branch `feature/add-custom-node-store`. Re-verification commands are in §7 — run them before trusting a line number in a stale checkout.

---

## 0. Mental model — one file, one class

```
my_node.py
├─ import polars as pl
├─ from flowfile import node_designer as nd          # the ONLY flowfile import allowed
├─ class MyNodeSettings(nd.NodeSettings):  ...        # optional: the settings UI
└─ class MyNode(nd.CustomNodeBase):                   # exactly ONE such subclass per file
      node_name / metadata fields ...
      settings_schema: MyNodeSettings = MyNodeSettings()
      def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame: ...
```

- **Canonical import:** `from flowfile import node_designer as nd`. The SDK lives in `shared/node_designer/` and is **import-pure** (no `flowfile_core`, DB, or migrations). A node file may import **only** the SDK + stdlib + third-party libraries — any other `flowfile`/`flowfile_core.*` import raises a contract error in worker/dry-run contexts (`shared/node_designer/loading.py:22-26,48-54`).
- **Exactly one** `CustomNodeBase` subclass per file — more raise `NodeLoadError` (`loading.py:146-156`).
- **On disk:** user nodes live in `~/.flowfile/user_defined_nodes/`. Community installs write a flat `<id>.py` there.
- **Where it runs:** in-process/worker by default (`environment="local"`), or in an isolated Docker kernel (`environment="kernel"`). See §3.

---

## 1. The generation recipe (description → node)

Follow these steps in order. Each maps to a section below.

1. **Classify the transform.** From the description extract: how many **inputs** (default 1) and **outputs** (default 1); which knobs the user should be able to **configure** (columns, numbers, choices, toggles, secrets); whether it needs **heavy/third-party libraries** (sklearn, xgboost, lightgbm, statsmodels).
2. **Pick the environment (§3).** Pure-polars / stdlib → `environment="local"` (omit the field). Needs ML libs or isolation → `environment="kernel"` + `dependencies=[...]` (plain PyPI specs only). **Never** set `execution_location` — that is a core-level concept, not an SDK field.
3. **Design `settings_schema` (§2.3–2.5).** One control per knob, grouped into `Section`s. Keep every kwarg a **designer literal** (§4.4).
4. **Write `process(self, *inputs)` (§2.2, §2.6).** Read settings via `self.settings_schema.<section>.<component>.value`; `.collect()` when a library needs eager data; `int(...)`-cast `NumericInput`/`SliderInput` values; for kernel nodes import heavy libs **inside** `process` and never reference SDK types; return a `LazyFrame`/`DataFrame`, or a `dict` keyed by `output_names` for multi-output.
5. **Add metadata + examples (§2.1).** `node_name` (required), plus `node_category`/`title`/`intro`; `author`/`version`/`tags` for publishing. Add `example_inputs` + `example_settings` (both **required to install/publish**; both must be literals).
6. **Self-check against guardrails (§4).** No DENY constructs. Import only the SDK + third-party. One bare `CustomNodeBase` subclass with `node_name` + `process`. PNG icon only for published bundles.
7. **Verify (§6).** Ruff-clean the file; dry-run it via the designer Test tab or `dry_run.py`; place it in `~/.flowfile/user_defined_nodes/`.

When you have a `~/.flowfile/user_defined_nodes/` reachable and the user wants it installed, write the file there directly; otherwise write to the path the user names (or the repo `flowfile-community-nodes/nodes/<id>/node.py` layout when they intend to publish).

---

## 2. The authoring contract

### 2.1 `CustomNodeBase` fields (`shared/node_designer/custom_node.py:401-444`)

A Pydantic model — declare attributes as **annotated fields with defaults**: `node_name: str = "My Node"`.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `node_name` | `str` | **required** | Node name; slugged to the palette `item`. |
| `node_category` | `str` | `"Custom"` | Palette group. |
| `node_icon` | `str` | `"user-defined-icon.png"` | Bare icon filename (PNG for publishing — SVG is forbidden in bundles, §4.3). |
| `settings_schema` | `NodeSettings \| None` | `None` | The settings-UI object (a `NodeSettings` subclass instance). |
| `number_of_inputs` | `int` | `1` | Input ports → `inputs[0..n-1]`. |
| `number_of_outputs` | `int` | `1` | Output ports. |
| `output_names` | `list[str]` | `["main"]` | Keys for multi-output `dict` returns. |
| `environment` | `Literal["local","kernel"]` | `"local"` | Execution environment (§3). |
| `dependencies` | `list[str]` | `[]` | pip specs; auto-installed **only** when `environment="kernel"`. |
| `example_inputs` | `list[dict[str,list]] \| None` | `None` | Dry-run/publish sample data: one `{col: [values]}` per input port. |
| `example_settings` | `dict[str,dict[str,Any]] \| None` | `None` | Dry-run/publish settings: `{section: {component: value}}`. |
| `title` | `str \| None` | `"Custom Node"` | Settings-drawer title. |
| `intro` | `str \| None` | `"A custom node…"` | Settings-drawer intro. |
| `author` / `version` / `tags` | `str\|None` / `str\|None` / `list[str]` | `None` / `None` / `[]` | Publishing metadata. |
| `node_type` | `Literal["input","output","process"]` | `"process"` | Node role. |
| `transform_type` | `Literal["narrow","wide","other"]` | `"wide"` | Transform hint. |

`requires_kernel: bool` and `kernel_id: str|None` are **deprecated** — they map onto `environment="kernel"`. Use `environment`.

### 2.2 `process` signature (`custom_node.py:732-750`)

```python
def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame | pl.DataFrame:
```

- One `pl.LazyFrame` per connected input, in port order (`inputs[0]`, `inputs[1]`, …). `.collect()` inside `process` when you need eager data.
- **Single output:** return a `LazyFrame` or `DataFrame` (the framework normalizes).
- **Multi-output** (`len(output_names) > 1`): return `dict[str, pl.LazyFrame | pl.DataFrame]` keyed by the `output_names`.

### 2.3 Settings container — `NodeSettings` + `Section`

```python
class MyNodeSettings(nd.NodeSettings):
    my_section: nd.Section = nd.Section(
        title="…", description="…", layout="vertical",   # or "horizontal"
        some_field=nd.TextInput(label="…"),
        another=nd.NumericInput(label="…", default=1.0),
    )
```

`Section(title=None, description=None, hidden=False, layout="vertical", **components)` — every keyword after the first four is a component that becomes a field (`ui_components.py:254-317`).

### 2.4 Control catalog (all subclass `FlowfileInComponent`; import as `nd.<Name>`)

| Component | Constructor args (defaults) |
|---|---|
| `TextInput` | `label`, `default=""`, `placeholder=""` |
| `NumericInput` | `label`, `default=None`, `min_value=None`, `max_value=None` |
| `SliderInput` | `label`, `default=None`, `min_value=0`, `max_value=100`, `step=1` |
| `ToggleSwitch` | `label`, `default=False`, `description=None` |
| `SingleSelect` | `label`, `options` **(required)**, `default=None` |
| `MultiSelect` | `label`, `options` **(required)**, `default=[]` |
| `ColumnSelector` | `label`, `required=False`, `multiple=False`, `data_types="ALL"` |
| `ColumnActionInput` | `label`, `actions=[]`, `output_name_template="{column}_{action}"`, `show_group_by=False`, `show_order_by=False`, `data_types="ALL"` |
| `SecretSelector` | `label`, `options=AvailableSecrets`, `required=False`, `description=None`, `name_prefix=None` |

`options=` for the selects is a `list` of bare strings **or** `(value, label)` tuples, **or** a dynamic marker class passed *by class* (not instance):
- `nd.IncomingColumns` — populate from the input frame's columns.
- `nd.AvailableArtifacts` — populate from upstream artifact names.
- `nd.AvailableSecrets` — the default for `SecretSelector`.

### 2.5 Column type filters — `data_types=` (`shared/node_designer/types.py`)

Groups: `nd.Types.Numeric`, `.String`, `.AnyDate`, `.Boolean`, `.Binary`, `.Complex`, `.All`. Specific: `.Int`/`.Int64`/…, `.Float`/`.Float64`, `.Str`, `.Date`, `.Datetime`, `.Bool`, `.List`, `.Struct`, etc. Accepts one value, a list, bare strings (`"Numeric"`, `"Int64"`, `"int"`/`"float"`/`"str"`), or a real `pl.DataType`. `"ALL"` = no filter. Both `data_types=nd.Types.String` and `data_types=["Numeric"]` are valid spellings.

### 2.6 Reading settings inside `process()`

Access: `self.settings_schema.<section_attr>.<component_attr>.value`. Gotchas:

- `NumericInput` / `SliderInput` `.value` is always a **`float`** — cast with `int(...)` when you need an int.
- `ColumnSelector(multiple=True).value` → `list[str]`; `multiple=False` → a single column-name `str`.
- `SecretSelector` → read `.secret_value` (a `SecretStr`; call `.get_secret_value()`), **not** `.value` (which is the secret *name*). **Local-only** — secrets are not available inside kernels yet (§3.3).
- `ColumnActionInput.value` → a `ColumnActionValue` (`.rows` of `ColumnActionRow`, `.group_by_columns`, `.order_by_column`).

---

## 3. Local vs kernel execution

### 3.1 Choosing

- **`environment="local"` (default):** runs in the Flowfile process/worker. Full SDK + polars available; secrets resolve; no Docker. Use for pure-polars/stdlib transforms.
- **`environment="kernel"`:** runs in an isolated Docker kernel. Use when you need heavy libs (sklearn, xgboost, lightgbm, statsmodels) or isolation. Declare `dependencies` (plain PyPI specs) — auto-installed **only** for kernel nodes. A kernel node **must be bound to a kernel** before it runs; the user picks the kernel (and image flavour) in the UI. Unbound → `KernelRequiredError` (`flow_graph.py:1915`).

### 3.2 What is different inside a kernel (the real path)

A kernel node's source **never runs directly**. Core AST-generates a self-contained script (`user_defined/kernel_codegen.py::generate_kernel_script`, invoked from `flow_graph.py:2169`). Consequences you must design around:

- **SDK imports are stripped**; only the node's own third-party imports + `polars`/`json`/`logging`/`sys` survive. **Do not reference SDK types inside `process`.**
- **Only `process` + nested/helper defs are kept**; **all class-level attribute assignments are dropped.** Put every bit of logic inside `process` — nothing computed at class-body scope exists at kernel runtime.
- **Settings are baked as JSON** and rebuilt behind a proxy, so `self.settings_schema.<section>.<component>.value` still works. **Every settings value must be JSON-serializable** or generation fails (`KernelCodegenError`).
- **Inputs arrive as parquet scans** (`pl.scan_parquet` per input); `.collect()` for eager work. Outputs are marshalled back via an injected `flowfile_ctx.publish_output` epilogue.
- **`flowfile_ctx` is an injected runtime global** (undefined in the plain file — that's expected; linters will flag it). Surface includes `log_info(...)` (surfaces in the Test panel), `read_inputs`/`read_first`, and `publish_artifact(name, obj)` / `read_artifact(name)` (cloudpickle-backed) for persisting a trained model across nodes.

### 3.3 Kernel gotchas

- **No Flowfile SDK inside the kernel** — polars + third-party libs + `flowfile_ctx` only.
- **Secrets are not available in kernels yet** — `SecretSelector.secret_value` raises there. Keep secret-using nodes `local`.
- **Cross-call persistence:** `process()` is one script execution. A model trained and used within the same `process()` is just a local variable. To share a model between a train node and a predict node, `flowfile_ctx.publish_artifact(...)` / `read_artifact(...)`.
- **Docker required.**

### 3.4 Libraries in the kernel images (`kernel_runtime/pyproject.toml` + `poetry.lock`)

- **BASE image** — `polars`, `pyarrow`, `numpy`, `cloudpickle`, `joblib`, `deltalake` (+ plumbing). **No ML libs.**
- **ML image** — BASE **+ `scikit-learn` 1.7.2, `xgboost` 2.1.4, `lightgbm` 4.6.0, `statsmodels` 0.14.6, `polars-ds` 0.12.0** (+ pandas transitive). **This is the flavour with xgboost/sklearn.**
- **LITE image** — base packages baked, only polars pinned so user pip installs float.

**An XGBoost/sklearn kernel node works today with no code changes** — provided the bound kernel uses the ML image, or the node declares the lib in `dependencies` for a per-kernel derived build.

### 3.5 ML node shape

1. `environment: str = "kernel"`.
2. `dependencies: list[str] = ["xgboost"]` (only strictly needed if not relying on the ML image).
3. `import xgboost as xgb` at module top (a non-SDK import, lifted verbatim into the kernel script).
4. All config in `settings_schema` (JSON-serializable), read via `.value`.
5. `.collect()` inputs inside `process` — sklearn/xgboost need eager numpy/pandas.
6. Self-contained train+predict → keep the model local. Train node → predict node → `flowfile_ctx.publish_artifact` / `read_artifact`.

---

## 4. Guardrails — pass the scanner and validator

The scanner (`community_nodes/security_scan.py`) is a conservative pre-filter; the community PR review + server-side re-scan are authoritative. Two outcomes: **DENY** (rejects the node) and **FLAG** (surfaces a capability for the consent dialog; never fails validation).

### 4.1 DENY — never emit these (`security_scan.py:15-37`)

- `eval`/`exec`/`compile`; `__import__`/`importlib.import_module`/`importlib.util`.
- `getattr`/`globals()`/`vars()`/`__builtins__` resolving a dangerous builtin, or `getattr` with a **non-constant** name.
- `ctypes`/`cffi`/`_ctypes`; `os.system`/`os.popen*`/`os.exec*`/`os.spawn*`/`posix.*`.
- `subprocess` with `shell=True` **or** non-literal args; `pty`/`os.forkpty`; importing **both** `socket` and `subprocess`.
- `sys._getframe`/`inspect.stack`/`inspect.currentframe`; importing `pip`/`ensurepip`.
- Opaque high-entropy string blobs (>4096 chars, or ≥1024 chars of near-base64); bulk `os.environ` enumeration.

### 4.2 FLAG — allowed, but expect a consent prompt

`network` (socket/requests/httpx/urllib/boto3/…), `subprocess` (literal args), `fs_write`, `fs_read`, `env_read` (`os.getenv`/`sys.argv`), `dynamic_code` (`setattr`/`delattr` with non-constant name), `serialization` (`pickle.load`/`yaml.load` w/o SafeLoader), `secrets` (`SecretSelector`/`.secret_value`). Only trigger these when the node genuinely needs them.

**ML libraries do not flag** — `xgboost`, `sklearn`/`scikit-learn`, `numpy`, `pandas`, `polars` match no trigger set. (Caveat: loading a model with `pickle.load`/`joblib`→pickle flags `serialization`; prefer `flowfile_ctx.read_artifact` for models.)

### 4.3 Bundle validation to install/publish (`community_nodes/validation.py:385-445`)

- **Folder contract:** `node.py` (≤200 KB) + `manifest.json` (≤16 KB) required; optional `icon.png` (≤256 KB, ≤512×512), `README.md`, `screenshots/` (≤5 PNGs). **SVG forbidden anywhere.** Total ≤6 MB.
- **Class shape:** exactly one class inheriting **exactly** `CustomNodeBase` (no extra bases/decorators/keywords); defines `node_name` + `process()`.
- **Examples required:** both `example_inputs` (literal list) and `example_settings` (literal dict), else `EXAMPLES_REQUIRED`.
- **Dependencies:** non-empty `dependencies` requires `environment="kernel"`; each spec is a plain PyPI name + optional version specifier — no URLs, `git+`, or pip flags.
- **Identity:** folder name == `manifest.id` == slug of `node_name`; versions match and strictly increase on update.

### 4.4 The "designer literals" rule (stay visually editable)

Keep all node attributes and settings/component kwargs as **literals** — constants, lists/tuples/dicts of scalars, `nd.Types.*`/`nd.DataType.*`, SDK marker classes. No f-strings, comprehensions, computed values, or `**kwargs` unpacking at that level, or the node degrades to `code_only` (still valid/installable, but the visual Form tab can't edit it). Logic inside `process` is unconstrained (`node_designer/parsing.py::eval_literal`).

---

## 5. Worked examples (verified in-repo)

### 5(a) Settings-driven local transform — the auto-tested docs quick start
`docs/examples/custom_node.py` (runs under the docs test harness).

```python
import polars as pl

from flowfile import node_designer as nd


class GreetingSettings(nd.NodeSettings):
    main_config: nd.Section = nd.Section(
        title="Greeting Configuration",
        description="Configure how to greet each row",
        name_column=nd.ColumnSelector(
            label="Name Column",
            data_types=nd.Types.String,
            required=True,
        ),
        greeting=nd.SingleSelect(
            label="Greeting",
            options=[("formal", "Hello"), ("casual", "Hey")],
            default="casual",
        ),
    )


class GreetingNode(nd.CustomNodeBase):
    node_name: str = "Greeting Generator"
    node_category: str = "Text Processing"
    title: str = "Add greetings"
    intro: str = "Prefix a name column with a greeting."

    settings_schema: GreetingSettings = GreetingSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        lf = inputs[0]
        name_col = self.settings_schema.main_config.name_column.value
        style = self.settings_schema.main_config.greeting.value
        word = "Hello" if style == "formal" else "Hey"
        return lf.with_columns(
            pl.concat_str([pl.lit(f"{word}, "), pl.col(name_col)]).alias("greeting")
        )
```

### 5(b) Kernel ML node — K-means with scikit-learn
`flowfile-community-nodes/nodes/kmeans_clustering/node.py` (real, published). Note `environment="kernel"`, `dependencies`, the import **inside** `process`, `.collect()` for eager numpy, the `int(...)` cast, and `flowfile_ctx.log_info`.

```python
import polars as pl
from flowfile import node_designer as nd


class KmeansClusteringSettings(nd.NodeSettings):
    clustering: nd.Section = nd.Section(
        title="Clustering",
        description="Settings for K-means cluster",
        layout="horizontal",
        feature_columns=nd.ColumnSelector(
            label="Feature Columns", required=True, multiple=True, data_types=["Numeric"],
        ),
        n_clusters=nd.NumericInput(label="Number of clusters", default=3.0, min_value=2.0, max_value=20.0),
        cluster_column=nd.TextInput(label="Cluster column name", default="cluster"),
        standardize=nd.ToggleSwitch(label="Standardize", default=True),
    )


class KmeansClustering(nd.CustomNodeBase):
    node_name: str = "kmeans clustering"
    node_category: str = "ML"
    node_icon: str = "kmeans.PNG"
    title: str = "kmeans clustering"
    intro: str = "Do a kmeans clustering with sklearn"
    author: str = "Edwardvaneechoud"
    version: str = "1.0.1"
    tags: list[str] = ["machine learning", "data-science"]
    environment: str = "kernel"
    dependencies: list[str] = ["scikit-learn"]
    example_inputs: list[dict[str, list]] = [
        {"age": [26, 45, 47], "annual_income_k": [30.3, 95.6, 101.5], "spending_score": [34, 81, 86]},
    ]
    example_settings: dict[str, dict] = {
        "clustering": {
            "feature_columns": ["age", "annual_income_k", "spending_score"],
            "n_clusters": 3, "cluster_column": "cluster", "standardize": True,
        },
    }
    settings_schema: KmeansClusteringSettings = KmeansClusteringSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        cfg = self.settings_schema.clustering
        feature_cols: list[str] = cfg.feature_columns.value
        k = int(cfg.n_clusters.value)                 # NumericInput is always a float
        label_col = cfg.cluster_column.value

        df = inputs[0].collect()                      # sklearn needs eager data
        features = df.select(feature_cols).to_numpy()
        if cfg.standardize.value:
            features = StandardScaler().fit_transform(features)

        model = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = model.fit_predict(features)
        flowfile_ctx.log_info(f"Fitted {k} clusters over {df.height} rows")   # injected at runtime
        return df.with_columns(pl.Series(label_col, labels))
```

### 5(c) Kernel ML node — XGBoost predictions (the user's example, self-contained)

Trains an `XGBRegressor`/`XGBClassifier` on labeled rows and appends a prediction column. Self-contained (no cross-node artifact needed); for a real train→serve split see the note after.

```python
import polars as pl
from flowfile import node_designer as nd
import xgboost as xgb          # non-SDK import — lifted verbatim into the kernel script


class XGBoostPredictSettings(nd.NodeSettings):
    model: nd.Section = nd.Section(
        title="Model",
        description="Train an XGBoost model on this data and predict a target column.",
        feature_columns=nd.ColumnSelector(
            label="Feature columns", required=True, multiple=True, data_types=["Numeric"],
        ),
        target_column=nd.ColumnSelector(
            label="Target column", required=True, data_types=["Numeric"],
        ),
        task=nd.SingleSelect(
            label="Task", options=[("regression", "Regression"), ("classification", "Classification")],
            default="regression",
        ),
        n_estimators=nd.NumericInput(label="Number of trees", default=200.0, min_value=10.0, max_value=2000.0),
        max_depth=nd.NumericInput(label="Max tree depth", default=6.0, min_value=1.0, max_value=20.0),
        prediction_column=nd.TextInput(label="Prediction column name", default="prediction"),
    )


class XGBoostPredict(nd.CustomNodeBase):
    node_name: str = "XGBoost Predict"
    node_category: str = "ML"
    title: str = "XGBoost predictions"
    intro: str = "Train an XGBoost model on the input and append predictions."
    author: str = "your-name"
    version: str = "1.0.0"
    tags: list[str] = ["machine learning", "xgboost", "prediction"]
    environment: str = "kernel"
    dependencies: list[str] = ["xgboost"]
    example_inputs: list[dict[str, list]] = [
        {"x1": [1.0, 2.0, 3.0, 4.0], "x2": [10.0, 8.0, 6.0, 4.0], "y": [12.0, 11.0, 10.0, 9.0]},
    ]
    example_settings: dict[str, dict] = {
        "model": {
            "feature_columns": ["x1", "x2"], "target_column": "y", "task": "regression",
            "n_estimators": 200, "max_depth": 6, "prediction_column": "prediction",
        },
    }
    settings_schema: XGBoostPredictSettings = XGBoostPredictSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.DataFrame:
        cfg = self.settings_schema.model
        feature_cols: list[str] = cfg.feature_columns.value
        target_col: str = cfg.target_column.value
        pred_col: str = cfg.prediction_column.value

        df = inputs[0].collect()                      # xgboost needs eager numpy
        X = df.select(feature_cols).to_numpy()
        y = df.get_column(target_col).to_numpy()

        params = dict(n_estimators=int(cfg.n_estimators.value), max_depth=int(cfg.max_depth.value))
        Model = xgb.XGBClassifier if cfg.task.value == "classification" else xgb.XGBRegressor
        model = Model(**params)
        model.fit(X, y)
        preds = model.predict(X)

        flowfile_ctx.log_info(f"Trained {cfg.task.value} on {len(feature_cols)} features, {df.height} rows")
        return df.with_columns(pl.Series(pred_col, preds))
```

**Train → serve split (advanced).** A *train* node fits the model and calls `flowfile_ctx.publish_artifact("xgb_model", model)`; a separate *predict* node calls `model = flowfile_ctx.read_artifact("xgb_model")` (both kernel nodes). Use a `SingleSelect`/`TextInput` for the artifact name and `nd.AvailableArtifacts` to let the predict node pick from published artifacts.

### 5(d) Multi-output — split rows by a boolean column

```python
import polars as pl
from flowfile import node_designer as nd


class SplitterSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Split",
        split_column=nd.ColumnSelector(label="Split Column", data_types="Boolean", required=True),
    )


class RowSplitter(nd.CustomNodeBase):
    node_name: str = "Row Splitter"
    node_category: str = "Custom"
    number_of_outputs: int = 2
    output_names: list[str] = ["pass", "fail"]
    example_inputs: list[dict[str, list]] = [{"keep": [True, False, True], "v": [1, 2, 3]}]
    example_settings: dict[str, dict] = {"main": {"split_column": "keep"}}
    settings_schema: SplitterSettings = SplitterSettings()

    def process(self, *inputs: pl.LazyFrame) -> dict:
        col = self.settings_schema.main.split_column.value
        return {"pass": inputs[0].filter(pl.col(col)), "fail": inputs[0].filter(~pl.col(col))}
```

**Multi-input:** set `number_of_inputs: int = 2` and read `inputs[0]`, `inputs[1]` — e.g. `pl.concat([inputs[0], inputs[1]], how="diagonal")`.

---

## 6. Verify the generated node

1. **Lint:** `poetry run ruff check <file>` (tests/generated node files are lint-exempt by config, but a clean parse matters). Confirm it imports: the file must `import polars as pl` and `from flowfile import node_designer as nd` and nothing else from flowfile.
2. **Dry-run** without the app: the designer Test tab drives `flowfile_core/flowfile/user_defined/dry_run.py` — it runs `process` against `example_inputs`/`example_settings` (kernel nodes go through the same AST-generated script path). This is the fastest way to confirm the node executes.
3. **Install for real:** write the file to `~/.flowfile/user_defined_nodes/<name>.py`; the registry hot-reloads on save. A broken file stays visible-with-error rather than vanishing.
4. **Publish-readiness:** run the bundle validator / CLI (`python -m flowfile_core.flowfile.community_nodes.cli`) — this is the same code the community repo's CI uses.

Per the repo's working agreement, **do not execute the user's tests or touch his running services** — write the file and describe the verification the user can run.

---

## 7. Provenance and re-verification

Facts verified 2026-07-12 on `feature/add-custom-node-store`. To re-verify against a fresh checkout:

```bash
# SDK export surface (the exact nd.* names)
sed -n '40,73p' shared/node_designer/__init__.py

# Base-class fields, defaults, process() signature
sed -n '393,779p' shared/node_designer/custom_node.py

# Control catalog + Section
sed -n '49,553p' shared/node_designer/ui_components.py

# Type filters
cat shared/node_designer/types.py

# Kernel codegen (what survives into the kernel script) + kernel_ctx surface
sed -n '1,260p' flowfile_core/flowfile_core/flowfile/user_defined/kernel_codegen.py
cat kernel_runtime/kernel_runtime/flowfile_client.py

# Kernel image contents (which flavour has xgboost/sklearn)
sed -n '1,40p' kernel_runtime/pyproject.toml

# Scanner DENY/FLAG rules + bundle validation
sed -n '1,200p' flowfile_core/flowfile_core/flowfile/community_nodes/security_scan.py
sed -n '385,445p' flowfile_core/flowfile_core/flowfile/community_nodes/validation.py

# Real + tested examples
cat docs/examples/custom_node.py
cat flowfile-community-nodes/nodes/kmeans_clustering/node.py
ls flowfile_core/tests/flowfile/node_designer/corpus/
```

User-facing docs to cross-reference: `docs/users/visual-editor/creating-custom-nodes.md`, `node-designer.md`, `custom-node-tutorial.md`, `kmeans-kernel-node.md`, `kernels.md`, `kernel-api.md`, `community-nodes.md`.

A portable, provider-agnostic version of this guidance (a single system prompt to paste into any LLM, no repo access assumed) lives at `prompts/generate-flowfile-node.md`. Keep the two in sync when the SDK changes.
