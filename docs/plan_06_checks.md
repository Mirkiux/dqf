# Plan 6 — Check Abstractions and CheckPipeline

## Goal

Implement the abstract base classes for checks and the `CheckPipeline`. No concrete checks yet — this plan establishes the contract that Plans 10 and 11 will implement.

## Scope

| Item | File |
|---|---|
| `BaseCheck`, `BaseCrossSectionalCheck`, `BaseLongitudinalCheck` | `src/dqf/checks/base.py` |
| `CheckPipeline` | `src/dqf/checks/pipeline.py` |
| Public exports | `src/dqf/checks/__init__.py` |
| Unit tests | `tests/test_checks.py` |

## Detailed Specification

### `BaseCheck` (Abstract)
- `name: str`
- `severity: Severity`
- `params: dict` — business thresholds declared at construction. Each subclass documents what keys it expects.
- `calibrate(reference_data: pd.DataFrame) -> None` — optional statistical baseline calibration. Default implementation is a no-op. Named deliberately to distinguish from sklearn's `fit`.
- Abstract: `check(data: pd.DataFrame, variable: Variable) -> CheckResult`

### `BaseCrossSectionalCheck(BaseCheck)` (Abstract)
- No additional interface beyond `BaseCheck`
- Receives the entity-level universe-joined DataFrame in `check()`

### `BaseLongitudinalCheck(BaseCheck)` (Abstract)
- Additional abstract method: `aggregation_sql(variable_name: str, time_field: str, period: str) -> str` — returns the SQL to execute engine-side to produce a time-indexed summary DataFrame
- `check()` receives the time-aggregated summary DataFrame, not entity-level data

### `CheckPipeline`
- `steps: List[Tuple[str, BaseCheck]]` — named steps
- `stop_on_failure: bool = False` — if True, short-circuits on first FAILURE-severity failed check
- `run(data: pd.DataFrame, variable: Variable) -> List[CheckResult]`
- `calibrate(reference_data: pd.DataFrame) -> None` — delegates to all steps
- Composite: a `CheckPipeline` can itself be a step inside another `CheckPipeline`

## Definition of Done

- [ ] `BaseCheck`, `BaseCrossSectionalCheck`, `BaseLongitudinalCheck` implemented as abstract classes
- [ ] `CheckPipeline` with Composite support and `stop_on_failure`
- [ ] `dqf/__init__.py` exports `BaseCheck`, `BaseCrossSectionalCheck`, `BaseLongitudinalCheck`, `CheckPipeline`
- [ ] Tests cover: `CheckPipeline` ordering, `stop_on_failure` behaviour, Composite nesting, `calibrate` delegation
- [ ] All tests pass
- [ ] Committed and pushed to `main`
