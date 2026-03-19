# Plan 13 — Default Check Suite Configuration

## Goal

Provide a ready-to-use, pre-configured `CheckSuiteResolver` with sensible default rules and pre-built `CheckPipeline` instances for common variable types. This is the "batteries included" layer — users can adopt defaults out of the box or use them as starting points for customization.

## Scope

| Item | File |
|---|---|
| Default resolver and pipelines | `src/dqf/defaults/suites.py` |
| Public exports | `src/dqf/defaults/__init__.py` |

## Default Rule Set

Rules are evaluated in descending priority:

| Priority | Predicate | Pipeline |
|---|---|---|
| 50 | `role == TARGET` | `ConceptDriftCheck` + `NullRateCheck` |
| 40 | `semantic_dtype == NUMERIC_CONTINUOUS` | `NullRateCheck` + `RangeCheck` + `DistributionDriftCheck` |
| 40 | `semantic_dtype == NUMERIC_DISCRETE` | `NullRateCheck` + `CardinalityCheck` |
| 30 | `semantic_dtype == CATEGORICAL` | `NullRateCheck` + `AllowedValuesCheck` + `CardinalityCheck` |
| 30 | `semantic_dtype == BOOLEAN` | `NullRateCheck` + `AllowedValuesCheck({"allowed_values": [True, False]})` |
| 20 | `semantic_dtype == DATETIME` | `NullRateCheck` + `RangeCheck` |
| 10 | `semantic_dtype == IDENTIFIER` | `NullRateCheck` + `UniquenessCheck({"threshold": 0.0})` |
| 0  | catch-all | `NullRateCheck({"threshold": 0.20})` |

**Factory function**: `default_resolver() -> CheckSuiteResolver` — returns a fresh instance with all default rules registered. Users call this once and optionally call `register()` to add or override rules.

## Definition of Done

- [ ] `default_resolver()` factory function implemented
- [ ] All default pipelines use sensible thresholds for enterprise data contexts
- [ ] `dqf/__init__.py` exports `default_resolver`
- [ ] Documentation example shows how to extend the default resolver
- [ ] Committed and pushed to `main`
