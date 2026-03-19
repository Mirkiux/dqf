# Plan 11 — Concrete Longitudinal Checks

## Goal

Implement time-aware checks that operate on temporal aggregations of the data. Each declares its own `aggregation_sql()` so the framework can push aggregation to the engine before running statistical analysis in pandas.

## Scope

| Check class | File | Description |
|---|---|---|
| `DistributionDriftCheck` | `longitudinal/distribution_drift.py` | Detects shifts in value distribution across time periods (PSI or KL divergence) |
| `StructuralBreakCheck` | `longitudinal/structural_break.py` | Detects structural breaks in a time series of a metric (Chow test / CUSUM) |
| `TrendCheck` | `longitudinal/trend.py` | Detects statistically significant monotonic trends (Mann-Kendall) |
| `SeasonalityCheck` | `longitudinal/seasonality.py` | Detects unexpected changes in seasonal patterns |
| `ConceptDriftCheck` | `longitudinal/concept_drift.py` | For `VariableRole.TARGET`: detects distribution drift in the target variable — signals potential model retraining need |

## Key Design Notes

- Each check's `aggregation_sql()` returns a SQL GROUP BY query that produces a small time-indexed DataFrame (one row per period). The framework executes this via the adapter and passes the result to `check()`.
- Statistical computation (Chow test, Mann-Kendall, PSI) runs in pandas/scipy/numpy — never in SQL.
- `ConceptDriftCheck` is identical in mechanics to `DistributionDriftCheck` but carries semantic meaning about model monitoring; it should be automatically routed to `VariableRole.TARGET` variables by the default resolver (Plan 13).
- `calibrate(reference_data)` is meaningful for `DistributionDriftCheck` and `ConceptDriftCheck` — a reference distribution can be set from a historical baseline period.

## Definition of Done

- [ ] All 5 checks implemented with correct `aggregation_sql()` and `check()` methods
- [ ] Statistical methods use `scipy.stats` or `numpy`; `scipy` added to optional dependencies
- [ ] `dqf/checks/longitudinal/__init__.py` exports all checks
- [ ] Unit tests use pre-built time-series DataFrames (no DB required)
- [ ] Tests cover: stable series (pass), shifted series (fail), insufficient time periods (SKIPPED or ERROR)
- [ ] All tests pass
- [ ] Committed and pushed to `main`
