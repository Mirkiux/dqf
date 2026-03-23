# Example 4 — Target variable drift detection

**Source**: [`examples/04_target_drift.py`](../../examples/04_target_drift.py)

This example demonstrates how dqf monitors a binary classification target variable
over time using `ProportionDriftCheck` (sequential two-proportion Z-test).

---

## What it covers

- Declaring a variable with `role=dqf.VariableRole.TARGET` and `dtype=dqf.DataType.BOOLEAN`
- `build_default_resolver(time_field=...)` dispatches `ProportionDriftCheck` automatically
- Simulating a fraud-rate shift starting in month 7
- Inspecting the drift check's `metadata` (min p-value, n_periods, baseline_periods)

---

## Simulated scenario

```
Months 1–6  : ~5% fraud rate  (stable baseline)
Months 7–12 : ~20% fraud rate (sudden shift — concept drift)
```

The check compares each new period against all prior periods using a two-proportion Z-test.
The p-value drops sharply when month 7 is reached, flagging drift.

---

## Key code

```python
dataset = dqf.VariablesDataset(
    ...
    variables=[
        dqf.Variable(
            name="is_fraud",
            dtype=dqf.DataType.BOOLEAN,
            role=dqf.VariableRole.TARGET,   # <-- triggers ProportionDriftCheck
        ),
    ],
)

resolver = dqf.build_default_resolver(time_field="event_date", period="month")
report = dataset.run_validation(resolver, dataset_name="fraud_labels")
```

---

## Expected output

```
Overall status : FAILED

  [PASS] is_fraud   not_null            observed=0
  [FAIL] is_fraud   proportion_drift    observed=...
         min p-value   : <0.05
         n_periods     : 12
         baseline_periods: 6
```
