# Plan 9 — Validation Orchestration (`run_validation`)

## Goal

Implement `VariablesDataset.run_validation()` — the method that connects all prior plans into a single end-to-end validation execution. This is the most complex orchestration logic in the library.

## Scope

| Item | File |
|---|---|
| `run_validation()` method on `VariablesDataset` | `src/dqf/datasets/variables.py` |
| End-to-end integration tests | `tests/test_orchestration.py` |

## Orchestration Steps

```
run_validation(resolver: CheckSuiteResolver) -> ValidationReport:

1. Materialize: call self.to_pandas() → joined_data (universe-anchored left join)
2. Dataset-level checks:
   a. validate_pk_uniqueness(joined_data)
   b. validate_join_integrity(variables_data, universe_data)
3. Variable resolution: if self.variables is empty, call resolve_variables()
4. Dispatch: resolver.resolve_all(self.variables) → {name: CheckPipeline}
5. For each variable:
   a. Separate cross-sectional vs longitudinal checks from its pipeline
   b. Cross-sectional: run pipeline against joined_data[variable.name column + universe context]
   c. Longitudinal: call check.aggregation_sql() → execute via adapter → run against time-series result
   d. Attach all CheckResults to variable via variable.attach_result()
6. Assemble ValidationReport from dataset-level checks + variable results
7. Return ValidationReport
```

## Key Design Notes

- `run_validation` accepts the resolver as a parameter — `VariablesDataset` owns no dispatch logic
- Cross-sectional and longitudinal checks may be mixed in the same `CheckPipeline`; the orchestrator routes each check to the appropriate execution path
- Universe size (for `CheckResult.population_size`) is `len(universe_data)`
- If a check raises an unexpected exception, the variable's status is set to `ERROR` and execution continues for remaining variables (fail-safe)

## Definition of Done

- [ ] `run_validation()` implements all 6 steps above
- [ ] Universe size correctly propagated to all `CheckResult.population_size` fields
- [ ] Exception in one variable's check does not abort the entire run
- [ ] End-to-end integration tests use `MockAdapter` + concrete checks from Plans 10/11
- [ ] Tests cover: full happy path, dataset-level check failures, variable-level check failures, mixed cross-sectional/longitudinal pipelines
- [ ] All tests pass
- [ ] Committed and pushed to `main`
