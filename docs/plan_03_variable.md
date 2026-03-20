# Plan 3 — The Variable Class

## Goal

Implement the `Variable` class — the descriptor that represents a single column, accumulates metadata from builders, and collects `CheckResult` instances over its lifetime.

## Scope

| Item | File |
|---|---|
| `Variable` class | `src/dqf/variable.py` |
| Unit tests | `tests/test_variable.py` |

## Detailed Specification

### `Variable`

A regular mutable dataclass (not frozen — it must accumulate state over its lifetime).

**Attributes**:
- `name: str` — column name
- `dtype: DataType` — semantic type; set initially from storage dtype, may be overridden by `SemanticTypeInferenceBuilder`
- `nullable: bool` — whether null values are expected; defaults to `True`
- `role: VariableRole` — domain role; defaults to `VariableRole.FEATURE`
- `metadata: dict[str, Any]` — open dictionary populated by `MetadataBuilderPipeline`; never validated by the framework; defaults to empty dict
- `status: ValidationStatus` — recomputed on every `attach_result()` call; starts as `ValidationStatus.PENDING`
- `check_results: list[CheckResult]` — ordered list of results attached over the validation lifecycle; defaults to empty list

**Methods**:
- `attach_result(result: CheckResult) -> None` — appends the result and recomputes `status` per the rule below
- `summary() -> dict[str, Any]` — returns a flat dict with keys: `name`, `dtype`, `role`, `status`, `total_checks`, `failed_checks`, `warned_checks`
- `reset() -> None` — clears `check_results` and resets `status` to `ValidationStatus.PENDING`

**Status recomputation rule** (applied inside `attach_result` after appending):
```
if any result has severity=FAILURE and passed=False → FAILED
elif len(check_results) == 0 → PENDING
else → PASSED
```
Note: a `WARNING`-severity result that `passed=False` does NOT set status to FAILED — the variable still reaches PASSED. The `warned_checks` count in `summary()` captures these.

**ERROR status**: set externally by the orchestrator (`variable.status = ValidationStatus.ERROR`) when an exception prevents evaluation. `attach_result` never sets ERROR.

## Test Specification

### `tests/test_variable.py`

#### Construction
- `test_defaults` — construct with only `name` and `dtype`; verify `nullable=True`, `role=FEATURE`, `status=PENDING`, `metadata={}`, `check_results=[]`
- `test_explicit_construction` — construct with all fields explicitly set; verify each field

#### `attach_result` — status transitions
- `test_attach_passing_failure_severity_sets_passed` — attach a FAILURE-severity result with `passed=True`; verify `status=PASSED`
- `test_attach_failing_failure_severity_sets_failed` — attach a FAILURE-severity result with `passed=False`; verify `status=FAILED`
- `test_attach_failing_warning_severity_sets_passed` — attach a WARNING-severity result with `passed=False`; verify `status=PASSED` (warnings do not fail the variable)
- `test_attach_multiple_results_any_failure_fails` — attach one passing FAILURE result then one failing FAILURE result; verify `status=FAILED`
- `test_attach_multiple_results_all_pass` — attach two passing FAILURE-severity results; verify `status=PASSED`
- `test_attach_appends_to_list` — attach two results; verify `len(check_results) == 2` and order is preserved
- `test_status_stays_failed_after_passing_result` — attach failing FAILURE result then passing FAILURE result; verify `status=FAILED` (once failed, stays failed)

#### `summary()`
- `test_summary_shape` — verify all required keys are present: `name`, `dtype`, `role`, `status`, `total_checks`, `failed_checks`, `warned_checks`
- `test_summary_counts_with_no_results` — verify `total_checks=0`, `failed_checks=0`, `warned_checks=0`
- `test_summary_counts_with_mixed_results` — attach: 1 passing FAILURE, 1 failing FAILURE, 1 failing WARNING; verify `total_checks=3`, `failed_checks=1`, `warned_checks=1`

#### `reset()`
- `test_reset_clears_results` — attach results, call `reset()`, verify `check_results==[]`
- `test_reset_sets_status_pending` — attach results (status=PASSED or FAILED), call `reset()`, verify `status=PENDING`
- `test_reset_preserves_metadata` — populate `metadata`, call `reset()`, verify `metadata` is unchanged

#### Mutability
- `test_metadata_is_mutable` — add a key to `metadata` directly; verify it persists
- `test_status_can_be_set_externally` — set `variable.status = ValidationStatus.ERROR` directly; verify it is stored (used by orchestrator for exception cases)

## File Layout After This Plan

```
src/dqf/
├── variable.py       (implemented)
tests/
└── test_variable.py  (implemented)
```

## Definition of Done

- [ ] `Variable` dataclass implemented with all attributes and methods above
- [ ] `attach_result()` correctly recomputes status per the specified rule
- [ ] `summary()` returns the specified dict shape with correct counts
- [ ] `reset()` clears results and status, preserves metadata
- [ ] `dqf/__init__.py` exports `Variable`
- [ ] All 16 test cases in `tests/test_variable.py` implemented and passing
- [ ] `ruff check`, `ruff format --check`, and `mypy` pass locally
- [ ] Committed and pushed to `main`
