# Plan 6 — Check Abstractions and CheckPipeline

## Goal

Implement the abstract base classes for checks and the `CheckPipeline`. No concrete checks yet — this plan establishes the contract that Plans 10 and 11 will implement.

## Scope

| Item | File |
|---|---|
| `BaseCheck`, `BaseCrossSectionalCheck`, `BaseLongitudinalCheck` | `src/dqf/checks/base.py` |
| `CheckPipeline` | `src/dqf/checks/pipeline.py` |
| Public exports | `src/dqf/checks/__init__.py` |
| Update top-level exports | `src/dqf/__init__.py` |
| Unit tests | `tests/test_checks.py` |

## Detailed Specification

### `BaseCheck` (Abstract)
- `name: str` — abstract property
- `severity: Severity` — abstract property
- `params: dict[str, Any]` — business thresholds declared at construction. Each subclass documents what keys it expects. Default: empty dict.
- `calibrate(reference_data: pd.DataFrame) -> None` — optional statistical baseline calibration. Default implementation is a no-op. Named deliberately to distinguish from sklearn's `fit`.
- Abstract: `check(data: pd.DataFrame, variable: Variable) -> CheckResult`

### `BaseCrossSectionalCheck(BaseCheck)` (Abstract)
- No additional interface beyond `BaseCheck`
- Receives the entity-level universe-joined DataFrame in `check()`

### `BaseLongitudinalCheck(BaseCheck)` (Abstract)
- Additional abstract method: `aggregation_sql(variable_name: str, time_field: str, period: str) -> str` — returns the SQL to execute engine-side to produce a time-indexed summary DataFrame
- `check()` receives the time-aggregated summary DataFrame, not entity-level data

### `CheckPipeline`
- `__init__(steps: list[tuple[str, BaseCheck]], stop_on_failure: bool = False)`
- `run(data: pd.DataFrame, variable: Variable) -> list[CheckResult]` — runs all steps in order, collects results, short-circuits if `stop_on_failure=True` and a FAILURE-severity check fails
- `calibrate(reference_data: pd.DataFrame) -> None` — delegates to all steps
- Composite: `CheckPipeline` is itself a `BaseCheck` so it can be nested as a step inside another pipeline
  - `name` property returns `"pipeline"`
  - `severity` property returns `Severity.FAILURE`
  - `check()` delegates to `run()` — returns a single aggregated `CheckResult` (passed if all steps passed, failed otherwise)

## Explicit Test Case Specifications

To exercise the abstractions without concrete checks, tests use two minimal in-module fakes:

```
FakeCheck(name, severity, passed) — always returns a CheckResult with the given passed/severity
FakeLongitudinalCheck(name, severity, passed) — same, plus implements aggregation_sql()
```

### Class `TestBaseCheckAbstract`
1. `test_cannot_instantiate_base_check` — `BaseCheck()` raises `TypeError`
2. `test_cannot_instantiate_base_cross_sectional` — `BaseCrossSectionalCheck()` raises `TypeError`
3. `test_cannot_instantiate_base_longitudinal` — `BaseLongitudinalCheck()` raises `TypeError`

### Class `TestBaseCheckInterface`
4. `test_calibrate_default_is_noop` — calling `calibrate()` on a concrete check raises no error and returns `None`
5. `test_params_default_is_empty_dict` — concrete check with no params arg → `check.params == {}`
6. `test_params_stored` — concrete check constructed with `params={"threshold": 0.05}` → `check.params == {"threshold": 0.05}`

### Class `TestBaseLongitudinalCheckInterface`
7. `test_aggregation_sql_returns_string` — `aggregation_sql("col", "date", "month")` returns a non-empty string
8. `test_longitudinal_is_base_check` — `isinstance(fake_longitudinal, BaseCheck)` is `True`

### Class `TestCheckPipelineRun`
9. `test_empty_pipeline_returns_empty_list` — `run()` on pipeline with no steps returns `[]`
10. `test_single_step_result_collected` — one passing FakeCheck → list of one passing `CheckResult`
11. `test_two_steps_both_collected` — two passing checks → two results in order
12. `test_results_order_matches_steps_order` — results list preserves insertion order of steps
13. `test_all_results_returned_when_no_failure` — all checks pass → all results returned

### Class `TestCheckPipelineStopOnFailure`
14. `test_stop_on_failure_false_continues_after_failure` — FAILURE-severity failed check mid-pipeline, `stop_on_failure=False` → all subsequent checks still run
15. `test_stop_on_failure_true_halts_after_failure` — FAILURE-severity failed check mid-pipeline, `stop_on_failure=True` → checks after it are NOT run (fewer results returned)
16. `test_warning_severity_does_not_stop_pipeline` — WARNING-severity failed check, `stop_on_failure=True` → pipeline continues (only FAILURE severity triggers stop)
17. `test_passing_failure_severity_does_not_stop` — FAILURE-severity check that PASSES → pipeline continues

### Class `TestCheckPipelineCalibrate`
18. `test_calibrate_delegates_to_all_steps` — pipeline with two FakeChecks that track calibrate calls → both called after `pipeline.calibrate(df)`
19. `test_calibrate_passes_same_dataframe` — the exact DataFrame passed to `pipeline.calibrate()` is forwarded to each step

### Class `TestCheckPipelineComposite`
20. `test_pipeline_is_base_check` — `isinstance(pipeline, BaseCheck)` is `True`
21. `test_nested_pipeline_as_step` — inner pipeline with one step nested inside outer pipeline → `outer.run()` returns results including inner's result
22. `test_nested_stop_on_failure_outer_only` — outer has `stop_on_failure=True`; inner pipeline fails → outer stops after inner step

## Definition of Done

- [ ] `BaseCheck`, `BaseCrossSectionalCheck`, `BaseLongitudinalCheck` implemented as abstract classes
- [ ] `CheckPipeline` with Composite support and `stop_on_failure`
- [ ] `src/dqf/__init__.py` exports `BaseCheck`, `BaseCrossSectionalCheck`, `BaseLongitudinalCheck`, `CheckPipeline`
- [ ] `tests/test_checks.py` contains all 22 test cases above
- [ ] All CI jobs pass (lint, typecheck, test 3.10–3.14)
