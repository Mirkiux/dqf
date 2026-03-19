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

**Attributes**:
- `name: str` — column name
- `dtype: DataType` — semantic type; set initially from storage dtype, may be overridden by `SemanticTypeInferenceBuilder`
- `nullable: bool` — whether null values are expected
- `role: VariableRole` — domain role (`FEATURE`, `TARGET`, `IDENTIFIER`, `AUXILIARY`)
- `metadata: dict` — open dictionary populated by `MetadataBuilderPipeline`; never validated by the framework
- `status: ValidationStatus` — recomputed on every `attach_result()` call; starts as `PENDING`
- `check_results: List[CheckResult]` — ordered list of results attached over the validation lifecycle

**Methods**:
- `attach_result(result: CheckResult) -> None` — appends the result and recomputes `status`:
  - Any `FAILURE`-severity result with `passed=False` → `FAILED`
  - All results pass or only `WARNING`-severity failures → `PASSED`
  - No results yet → `PENDING`
  - Exception during evaluation (set externally) → `ERROR`
- `summary() -> dict` — returns `{name, dtype, role, status, total_checks, failed_checks, warned_checks}`
- `reset() -> None` — clears `check_results` and resets `status` to `PENDING`; allows re-running validation

**Construction**: `Variable` is a regular (mutable) dataclass. Unlike `CheckResult`, it must accumulate state over its lifetime.

**Status recomputation rule** (applied inside `attach_result`):
```
if any result has severity=FAILURE and passed=False → FAILED
elif len(check_results) == 0 → PENDING
else → PASSED
```

## File Layout After This Plan

```
src/dqf/
├── variable.py       (implemented)
tests/
└── test_variable.py  (implemented)
```

## Definition of Done

- [ ] `Variable` dataclass implemented with all attributes and methods above
- [ ] `attach_result()` correctly recomputes status per the rule above
- [ ] `summary()` returns the specified dict shape
- [ ] `reset()` correctly clears state
- [ ] `dqf/__init__.py` exports `Variable`
- [ ] `tests/test_variable.py` covers: construction, `attach_result` with all status transitions, `summary()`, `reset()`
- [ ] All tests pass
- [ ] Committed and pushed to `main`
