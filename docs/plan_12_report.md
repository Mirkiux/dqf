# Plan 12 — ValidationReport and Rendering

## Goal

Implement the `ValidationReport` — the top-level output of `run_validation` — and its rendering capabilities including flat export, HTML report, and plot materialization.

## Scope

| Item | File |
|---|---|
| `ValidationReport` | `src/dqf/report.py` |
| Unit tests | `tests/test_report.py` |

## Detailed Specification

### `ValidationReport`

**Attributes**:
- `dataset_name: str`
- `run_timestamp: datetime`
- `universe_size: int`
- `dataset_level_checks: List[ValidationResult]`
- `variable_results: Dict[str, List[CheckResult]]` — keyed by variable name
- `variable_statuses: Dict[str, ValidationStatus]` — keyed by variable name
- `overall_status: ValidationStatus` — `FAILED` if any variable or dataset-level check failed

**Methods**:
- `failed_variables() -> List[str]`
- `warned_variables() -> List[str]` — variables with WARNING-severity failures but overall PASSED
- `to_dataframe() -> pd.DataFrame` — flat tabular summary; one row per check result; columns: `variable`, `check_name`, `passed`, `severity`, `rate`, `threshold`, `observed_value`
- `render(output_path: Optional[str] = None) -> str` — materializes all `figure_factory` callables, assembles an HTML report with embedded base64 plots, optionally writes to file, returns the HTML string

## Definition of Done

- [ ] `ValidationReport` implemented per spec
- [ ] `to_dataframe()` produces correct flat structure
- [ ] `render()` produces valid HTML with embedded plots
- [ ] `render()` handles `None` figure factories gracefully
- [ ] `dqf/__init__.py` exports `ValidationReport`
- [ ] Unit tests cover all methods including HTML rendering
- [ ] All tests pass
- [ ] Committed and pushed to `main`
