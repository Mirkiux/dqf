# Plan 7 — CheckSuiteResolver

## Goal

Implement the `CheckSuiteResolver` — a registry of `(predicate, pipeline_factory, priority)` rules that dispatches the right `CheckPipeline` to each `Variable` based on its metadata. Contains no hardcoded logic.

## Scope

| Item | File |
|---|---|
| `CheckSuiteResolver` | `src/dqf/resolver.py` |
| Update top-level exports | `src/dqf/__init__.py` |
| Unit tests | `tests/test_resolver.py` |

## Detailed Specification

### `CheckSuiteResolver`

**Methods**:
- `register(predicate: Callable[[Variable], bool], pipeline_factory: Callable[[], CheckPipeline], priority: int = 0) -> None` — adds a rule; higher priority is evaluated first
- `resolve(variable: Variable) -> CheckPipeline` — walks rules in priority order; returns the pipeline from the first matching predicate. Raises `ValueError` if no rule matches.
- `resolve_all(variables: list[Variable]) -> dict[str, CheckPipeline]` — bulk resolve; returns `{variable.name: pipeline}`

**Rule evaluation**:
Rules are evaluated in descending priority order. The first predicate that returns `True` wins. Rules with equal priority are evaluated in registration order (stable sort).

## Explicit Test Case Specifications

Tests use tagged `CheckPipeline` instances to distinguish which factory was selected, and a simple `make_variable()` helper.

### Class `TestCheckSuiteResolverRegisterAndResolve`
1. `test_resolve_single_rule_matches` — one rule whose predicate always returns `True` → `resolve()` returns a pipeline without raising
2. `test_resolve_raises_value_error_when_no_match` — one rule whose predicate always returns `False` → `resolve()` raises `ValueError`
3. `test_resolve_error_message_contains_variable_name` — `ValueError` message contains the variable's name
4. `test_resolve_empty_resolver_raises_value_error` — resolver with no rules → `ValueError`
5. `test_resolve_returns_fresh_pipeline_per_call` — factory is called each time; two calls to `resolve()` return different objects (`is not`)

### Class `TestCheckSuiteResolverPriority`
6. `test_higher_priority_wins` — two rules both matching; priority 10 registered after priority 0 → priority-10 pipeline returned
7. `test_lower_priority_is_fallback` — priority-10 rule does NOT match; priority-0 catch-all does → priority-0 pipeline returned
8. `test_equal_priority_preserves_registration_order` — two rules at same priority, both matching; first registered wins
9. `test_priority_ordering_independent_of_registration_order` — register low priority first, then high priority → high priority still wins

### Class `TestCheckSuiteResolverResolveAll`
10. `test_resolve_all_empty_list` — `resolve_all([])` returns `{}`
11. `test_resolve_all_single_variable` — returns dict with one entry keyed by variable name
12. `test_resolve_all_multiple_variables` — three variables → dict with three entries
13. `test_resolve_all_keys_are_variable_names` — all keys in result match `variable.name`
14. `test_resolve_all_raises_if_any_variable_unmatched` — one variable has no matching rule → `ValueError`

### Class `TestCheckSuiteResolverPredicateReceivesVariable`
15. `test_predicate_receives_correct_variable` — predicate captures the variable it received; verify it is the exact object passed to `resolve()`
16. `test_predicate_can_inspect_metadata` — variable with `metadata["key"] = "value"` → predicate inspecting that key routes to the correct pipeline

## Definition of Done

- [ ] `CheckSuiteResolver` implemented with `register`, `resolve`, `resolve_all`
- [ ] Priority ordering correct (higher wins; equal priority preserves registration order)
- [ ] `resolve()` raises `ValueError` with message containing variable name when no rule matches
- [ ] `src/dqf/__init__.py` exports `CheckSuiteResolver`
- [ ] `tests/test_resolver.py` contains all 16 test cases above
- [ ] All CI jobs pass (lint, typecheck, test 3.10–3.14)
