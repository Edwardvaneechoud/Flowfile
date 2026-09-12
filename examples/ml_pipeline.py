"""Split, train, apply, and evaluate a churn classifier with the FlowFrame ML API."""

# --8<-- [start:example]
import flowfile as ff

raw = ff.read_csv("data/templates/customer_churn.csv")

# Hold out 20% of rows for evaluation.
train, test = raw.random_split({"train": 80, "test": 20}, seed=42)

# Fit a binary logistic-regression model on the training slice.
trained = train.train_model(
    target="churned",
    features=["tenure_months", "monthly_charges", "support_calls", "has_contract"],
    model_type="logistic_regression",
    params={"l2_reg": 0.1},
)

# Score the held-out rows. wait_for blocks the apply until the model exists.
scored = test.wait_for(trained).apply_model(
    upstream=trained,
    output_column="predicted_churn",
)

# Compare predictions against the true labels — a long (metric, value) table.
metrics = scored.evaluate_model(
    "churned",
    predicted_column="predicted_churn",
    task_type="classification",
    upstream=trained,
)

result = metrics.collect()
# --8<-- [end:example]

assert result.columns == ["metric", "value"]
assert result.schema["metric"] == ff.String
assert result.schema["value"] == ff.Float64

reported = set(result["metric"].to_list())
for metric in ("accuracy", "precision", "recall", "f1", "n_correct", "n_total"):
    assert metric in reported, f"Expected metric '{metric}' in {reported}"

scored_df = scored.collect()
assert "predicted_churn" in scored_df.columns
assert scored_df.height > 0
