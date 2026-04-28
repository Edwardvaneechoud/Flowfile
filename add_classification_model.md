# Implementation Prompt — Add Classification Model

Add a classification model to Flowfile's ML stack, end-to-end. The regression
path (linear/ridge/lasso) is already in place; you're adding the first
classification trainer alongside it and a template flow that demos the
train → apply → evaluate loop on classification data.

## Context — read these before starting

- `shared/ml/algorithms.py` — Pydantic specs for hyperparameters +
  `MLAlgorithmSpec` (already supports `task_type="classification"`).
- `shared/ml/trainers.py` — `Trainer` Protocol + registry. The existing
  `_LinearFamilyTrainer` shows the JSON-coefficients pattern.
- `shared/ml/metrics.py` — already dispatches on `task_type`; currently
  only `"regression"` is in `SUPPORTED_TASK_TYPES`. Extend it.
- `flowfile_core/flowfile_core/flowfile/flow_graph.py` —
  `add_train_model` / `add_apply_model` / `add_evaluate_model`. No
  changes needed if your trainer fits the existing strategy pattern.
- `flowfile_worker/flowfile_worker/funcs.py` — `train_model_task` /
  `apply_model_task` already call `trainer.train` / `.apply`
  generically. No worker change needed if you stick to the JSON
  `serialization_format`. If you need joblib (sklearn), wire the joblib
  branch in `apply_model_task` too.
- `data/templates/flows/house_price_regression.yaml` — template-flow
  shape to mirror.
- `data/templates/generate_template_data.py` — `generate_house_prices()`;
  copy this pattern for your classification CSV.
- `flowfile_core/flowfile_core/templates/template_definitions.py` —
  register the new yaml in `_FLOW_YAML_FILENAMES`.
## What to build

### 1. Classification trainer

- Pick the first algorithm: **logistic regression** (binary). If
  `polars_ds` exposes `logistic_reg`, prefer it for parity with the
  existing trainers' pure-polars approach (`serialization_format =
  "json"`, same coefficient-vector serialisation). If not, fall back
  to scikit-learn with `serialization_format = "joblib"` and extend
  `apply_model_task` in `flowfile_worker/funcs.py` to load joblib
  artifacts (currently only json is loaded).
- Add `HyperparamsLogistic` in `shared/ml/algorithms.py`. Sensible
  starter params: `add_bias` (bool), `l2_reg` (float, ≥0).
- Add `LogisticRegressionTrainer` in `shared/ml/trainers.py`:
  - `model_type = "logistic_regression"`
  - `label = "Logistic Regression"`
  - `task_type = "classification"`
  - `output_dtype = "Int64"` (predicted class id; or `"String"` if
    you prefer to round-trip the original labels)
  - `params_class = HyperparamsLogistic`
  - `train(...)` returns serialised model bytes (JSON or joblib)
  - `apply(...)` returns `lf` with the predicted-label column
- Register the trainer in `TRAINER_REGISTRY`.

### 2. Classification metrics

In `shared/ml/metrics.py`:

- Add `"classification"` to `SUPPORTED_TASK_TYPES`.
- Implement `_classification_metrics(lf, actual, predicted)` returning
  long-form `(metric, value)`:
  - `accuracy` = correct / total
  - `precision`, `recall`, `f1` — macro-averaged across classes
  - `n_correct`, `n_total` — raw counts for transparency
- Cast both columns consistently (`Utf8` is safe; integer label columns
  work fine cast to `Utf8` for grouping).
- Drop nulls before aggregating, mirroring `_regression_metrics`.

### 3. Tests

- `shared/tests/test_ml_metrics.py` — add classification cases:
  - perfect predictions → `accuracy=1`, `f1=1`
  - all wrong → `accuracy=0`
  - imbalanced 3-class case with hand-computed expected values
  - nulls dropped before aggregation
- `flowfile_core/tests/flowfile/test_ml_evaluate.py` — add a test that
  runs `evaluate_model` with `task_type="classification"` end-to-end on
  a small `manual_input`.
- `flowfile_core/tests/flowfile/test_ml_train_apply.py` — add a
  schema-callback test for the new `model_type` so the train/apply
  pair works with `logistic_regression`.
- `flowfile_worker/tests/test_train_apply_model.py` — add a worker
  round-trip test (numerical correctness on a small dataset).

### 4. Template flow

- `data/templates/generate_template_data.py` — add
  `generate_customer_churn()` (or similar) — ~500 rows with a known
  logistic ground truth, e.g.:
  - features: `tenure_months`, `monthly_charges`, `support_calls`,
    `has_contract`
  - label: `churned` (0/1) generated from a logistic of the features
    + Gaussian noise
  - append the call to `__main__`. Run the script and commit the CSV.
- `data/templates/customer_churn.csv` — the generated CSV.
- `data/templates/flows/customer_churn_classification.yaml` — mirror
  `house_price_regression.yaml`'s structure:
  - node 1: `read` → CSV
  - node 2: `random_split` → 80/20 (seed=42)
  - node 3: `train_model` → `logistic_regression` on the train slice
  - node 4: `wait_for` → sync barrier on the trainer
  - node 5: `apply_model` → score the test slice
    (`output_column = "predicted_churn"`)
  - node 6: `evaluate_model` → `actual_column="churned"`,
    `predicted_column="predicted_churn"`, `task_type="auto"`,
    `upstream_train_node_id=3`
  - node 7: `explore_data` → optional, like the regression demo
  - update the `_template_meta` block: id, name, description,
    `category=Beginner`, `tags=[ml, classification, train_model,
    apply_model, evaluate_model]`, icon (pick one that exists).
- `flowfile_core/flowfile_core/templates/template_definitions.py` —
  add `"customer_churn_classification.yaml"` to `_FLOW_YAML_FILENAMES`.

## Acceptance criteria

- `poetry run pytest shared/tests flowfile_core/tests/flowfile/test_ml_*
  flowfile_worker/tests/test_train_apply_model.py` — all green.
- `poetry run ruff check .` — clean.
- `cd flowfile_frontend && npx vue-tsc --noEmit` — clean. The existing
  `task_type` union in `node.types.ts` already covers `"classification"`
  via `MLAlgorithmSpec`; you do **not** need to extend
  `EvaluateModelTaskType` — let `"auto"` + the upstream picker resolve
  it.
- Loading the new template flow in the UI runs without errors and
  produces a metrics table ending in `(accuracy, ...)`.

## Do not

- Do not change the train/apply worker contract (staging path,
  `sha256/size_bytes` payload). Trainer-specific logic stays inside
  `shared/ml/trainers.py`.
- Do not introduce sklearn unless you take the joblib path end-to-end
  (`apply_model_task` currently `json.loads` the model file).
- Do not extend `EvaluateModelTaskType` — the auto/regression UI is the
  user-facing surface; classification flows in via the upstream-train
  picker.
- Do not commit `master_key.txt`, `.env`, or large binaries.
- Polars-only for data ops (no pandas).

## Report back

List the files changed, paste the test summary, and call out any
decision you made (algorithm choice, dataset shape, label encoding)
that wasn't pre-specified above.
