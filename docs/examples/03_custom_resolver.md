# Example 3 — Custom resolver

**Source**: [`examples/03_custom_resolver.py`](../../examples/03_custom_resolver.py)

This example shows how to extend `build_default_resolver` with project-specific rules
that override the default check suite for specific variables.

---

## What it covers

- Registering custom rules at a higher priority than the defaults
- Using `RangeCheck` to enforce domain-specific value bounds
- Variables not matched by custom rules fall through to the default pipeline

---

## Priority system

Rules are evaluated in descending priority order. The first matching rule wins.
`build_default_resolver` uses priorities 0–30. Register custom rules at priority > 30
(or > 15 for feature variables) to override defaults for specific columns.

```python
resolver = dqf.build_default_resolver(null_threshold=0.05, max_categorical_cardinality=10)

# credit_score: must be in [300, 850] — overrides NUMERIC_CONTINUOUS at priority 15
resolver.register(
    predicate=lambda v: v.name == "credit_score",
    pipeline_factory=lambda: CheckPipeline([
        ("range", RangeCheck(min_value=300, max_value=850, severity=dqf.Severity.FAILURE))
    ]),
    priority=50,
)

# ltv_ratio: must be in [0, 1]
resolver.register(
    predicate=lambda v: v.name == "ltv_ratio",
    pipeline_factory=lambda: CheckPipeline([
        ("range", RangeCheck(min_value=0.0, max_value=1.0, severity=dqf.Severity.FAILURE))
    ]),
    priority=50,
)
# property_type (CATEGORICAL) still uses the default null_rate + cardinality pipeline
```

---

## Expected output

```
Overall status : PASSED

  [PASS] credit_score    range        observed=...
  [PASS] ltv_ratio       range        observed=...
  [PASS] property_type   null_rate    observed=0.0
  [PASS] property_type   cardinality  observed=2
```
