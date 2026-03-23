# Example 1 — Basic cross-sectional validation

**Source**: [`examples/01_basic_validation.py`](../../examples/01_basic_validation.py)

This example shows the simplest dqf usage: validate a customer features dataset using
`build_default_resolver` with no time dimension.

---

## What it covers

- Wiring up `MockAdapter` with in-memory DataFrames
- Defining a `UniverseDataset` and `VariablesDataset`
- Declaring `Variable` objects with `DataType` and default `FEATURE` role
- Running validation with `build_default_resolver(null_threshold=0.10)`
- Inspecting `ValidationReport`: overall status, `to_dataframe()`, `failed_variables()`

---

## Variables

| Name | DataType | Expected checks |
|---|---|---|
| `age` | `NUMERIC_CONTINUOUS` | `null_rate` |
| `income` | `NUMERIC_CONTINUOUS` | `null_rate` |
| `segment` | `CATEGORICAL` | `null_rate`, `cardinality` |
| `is_premium` | `BOOLEAN` | `null_rate` |

---

## Key code

```python
resolver = dqf.build_default_resolver(
    null_threshold=0.10,          # fail features with > 10% nulls
    max_categorical_cardinality=20,
)

report = dataset.run_validation(resolver, dataset_name="customer_features")

print(f"Overall status : {report.overall_status.value}")
df = report.to_dataframe()
print(df.to_string(index=False))
```

---

## Expected output

```
Overall status : PASSED
Universe size  : 100

 variable_name check_name  passed  observed_value  threshold  ...
           age  null_rate    True             0.0       0.10  ...
        income  null_rate    True             0.0       0.10  ...
       segment  null_rate    True             0.0       0.10  ...
       segment cardinality   True               3         20  ...
    is_premium  null_rate    True             0.0       0.10  ...

All variables passed.
```
