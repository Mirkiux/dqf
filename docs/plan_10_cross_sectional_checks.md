# Plan 10 — Concrete Cross-Sectional Checks

## Goal

Implement the standard library of point-in-time checks. Each is a concrete `BaseCrossSectionalCheck` subclass.

## Scope

| Check class | File | Description |
|---|---|---|
| `NullRateCheck` | `cross_sectional/null_rate.py` | Fails if null rate exceeds `params["threshold"]` |
| `RangeCheck` | `cross_sectional/range_check.py` | Fails if any value falls outside `[params["min"], params["max"]]` |
| `AllowedValuesCheck` | `cross_sectional/allowed_values.py` | Fails if any value is not in `params["allowed_values"]` |
| `CardinalityCheck` | `cross_sectional/cardinality_check.py` | Fails if distinct count exceeds `params["max_cardinality"]` |
| `UniquenessCheck` | `cross_sectional/uniqueness.py` | Fails if duplicate values exceed `params["threshold"]` rate |
| `RegexPatternCheck` | `cross_sectional/regex_pattern.py` | Fails if non-matching values exceed `params["threshold"]` rate |
| `ReferentialIntegrityCheck` | `cross_sectional/referential_integrity.py` | Fails if values not present in a reference set exceed `params["threshold"]` rate |

## Design Notes

- All checks use universe size (`len(data)` of the left-joined dataset) as `population_size` in `CheckResult`
- `NullRateCheck` naturally captures both structural nulls and value nulls from the `__vd_matched__` contract — no special handling needed
- Each check optionally attaches a `figure_factory` that produces a relevant matplotlib figure (e.g. histogram for `RangeCheck`, bar chart for `AllowedValuesCheck`)

## Definition of Done

- [ ] All 7 checks implemented
- [ ] Each check produces a correct `CheckResult` with `population_size = len(data)`
- [ ] `figure_factory` implemented for at least `NullRateCheck` and `RangeCheck`
- [ ] `dqf/checks/cross_sectional/__init__.py` exports all checks
- [ ] Unit tests cover: pass case, fail case, edge cases (all nulls, empty dataset, all values invalid)
- [ ] All tests pass
- [ ] Committed and pushed to `main`
