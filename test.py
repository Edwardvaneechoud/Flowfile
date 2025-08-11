import flowfile as ff

df = ff.FlowFrame({
    "product": ["Widget", "Gadget", "Tool"],
    "price": [10.50, 25.00, 8.75],
    "quantity": [100, 50, 200],
    "discount": [0.1, 0.15, 0.05]
})

# Using Flowfile formula syntax (Excel-like)
result = df.with_columns(
    flowfile_formulas=[
        "[price] * [quantity]",                    # Simple multiplication
        "[price] * (1 - [discount])",              # With parentheses
        "if [quantity] > 75 then 'High' else 'Low' endif",      # Conditional
        "round([price] * [discount], 2)"
    ],
    output_column_names=["revenue", "discounted_price", "volume_category", "discount_amount"],
    description="Calculate derived metrics"
)

# Mix formulas with regular Polars expressions
result = df.with_columns([
    ff.col("price").round(0).alias("price_rounded")  # Polars style
]).with_columns(
    flowfile_formulas=["[price_rounded] * [quantity]"],  # Formula style
    output_column_names=["estimated_revenue"]
)
