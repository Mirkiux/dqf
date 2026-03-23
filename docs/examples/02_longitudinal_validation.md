# Example 2 — Longitudinal validation

**Source**: [`examples/02_longitudinal_validation.py`](../../examples/02_longitudinal_validation.py)

This example demonstrates trend detection and structural break checks for a
`NUMERIC_CONTINUOUS` feature aggregated by month.

---

## What it covers

- Enabling longitudinal checks via `time_field` in `build_default_resolver`
- Pre-computing the aggregation SQL that `TrendCheck` / `StructuralBreakCheck` will issue
- Pre-seeding `MockAdapter` with the aggregation DataFrame
- Interpreting `trend` and `structural_break` check results

---

## How longitudinal checks work

Longitudinal checks issue an **aggregation SQL query** at runtime to compute period-level statistics. For `MockAdapter`, you must pre-compute and pre-seed this SQL:

```python
from dqf.checks.longitudinal.trend import TrendCheck

_trend_check = TrendCheck(time_field="order_date", period="month")
TREND_SQL = _trend_check.aggregation_sql("basket_size", "order_date", "month").format(
    source=VARIABLES_SQL
)

adapter = dqf.MockAdapter({
    UNIVERSE_SQL: universe_df,
    VARIABLES_SQL: variables_df,
    TREND_SQL: monthly_agg_df,  # served to both TrendCheck and StructuralBreakCheck
})
```

---

## Key code

```python
resolver = dqf.build_default_resolver(
    time_field="order_date",
    period="month",
    null_threshold=0.05,
)

report = dataset.run_validation(resolver, dataset_name="order_features_monthly")
```

---

## Expected output (stable series — no trend, no structural break)

```
Overall status : PASSED

  [PASS] null_rate             observed=0.0
  [PASS] trend                 observed=...
  [PASS] structural_break      observed=...
```
