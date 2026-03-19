# Plan 14 — Integration Tests and Worked Examples

## Goal

End-to-end tests that validate the full execution path from SQL definition to `ValidationReport`, plus worked examples as Jupyter notebooks demonstrating key use cases.

## Scope

| Item | Location |
|---|---|
| End-to-end integration tests | `tests/test_orchestration.py` (extended) |
| Multi-engine setup example | `examples/multi_engine_setup.ipynb` |
| Longitudinal analysis example | `examples/longitudinal_analysis.ipynb` |
| Custom resolver rules example | `examples/custom_resolver_rules.ipynb` |
| Target variable drift example | `examples/target_variable_drift.ipynb` |

## Integration Test Scenarios

All use `MockAdapter` — no external databases required.

1. **Happy path**: universe + variables dataset → full validation → `ValidationReport` with all PASSED
2. **PK uniqueness failure**: universe with duplicate rows → `ValidationResult` failed, report reflects it
3. **Join fan-out failure**: variables dataset with duplicate join keys → `ValidationResult` failed
4. **Variable check failure**: null rate exceeds threshold → variable status FAILED
5. **Mixed pipeline**: same variable has both cross-sectional and longitudinal checks
6. **Target variable drift**: target variable with distribution shift → `ConceptDriftCheck` fails
7. **Multi-engine**: universe on one MockAdapter, variables on another
8. **Exception resilience**: one check raises an exception → variable status ERROR, other variables unaffected

## Definition of Done

- [ ] All 8 integration scenarios implemented and passing
- [ ] All 4 example notebooks execute end-to-end without errors
- [ ] Notebooks use `MockAdapter` so they run without any database connection
- [ ] Committed and pushed to `main`
