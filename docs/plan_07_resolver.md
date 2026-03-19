# Plan 7 — CheckSuiteResolver

## Goal

Implement the `CheckSuiteResolver` — a registry of `(predicate, pipeline_factory, priority)` rules that dispatches the right `CheckPipeline` to each `Variable` based on its metadata. Contains no hardcoded logic.

## Scope

| Item | File |
|---|---|
| `CheckSuiteResolver` | `src/dqf/resolver.py` |
| Unit tests | `tests/test_resolver.py` |

## Detailed Specification

### `CheckSuiteResolver`

**Attributes**:
- `rules: List[Tuple[int, Callable[[Variable], bool], Callable[[], CheckPipeline]]]` — stored sorted by descending priority

**Methods**:
- `register(predicate: Callable[[Variable], bool], pipeline_factory: Callable[[], CheckPipeline], priority: int = 0) -> None` — adds a rule; higher priority is evaluated first
- `resolve(variable: Variable) -> CheckPipeline` — walks rules in priority order; returns the pipeline from the first matching predicate. Raises `ValueError` if no rule matches.
- `resolve_all(variables: List[Variable]) -> Dict[str, CheckPipeline]` — bulk resolve; returns `{variable.name: pipeline}`

**Rule evaluation**:
Rules are evaluated in descending priority order. The first predicate that returns `True` wins. Rules with equal priority are evaluated in registration order (stable sort).

**Example usage**:
```python
resolver = CheckSuiteResolver()
resolver.register(
    predicate=lambda v: v.metadata.get("semantic_dtype") == DataType.NUMERIC_CONTINUOUS,
    pipeline_factory=lambda: CheckPipeline([("nulls", NullRateCheck({"threshold": 0.05}))]),
    priority=10
)
resolver.register(
    predicate=lambda v: True,  # catch-all
    pipeline_factory=lambda: CheckPipeline([("nulls", NullRateCheck({"threshold": 0.20}))]),
    priority=0
)
```

## Definition of Done

- [ ] `CheckSuiteResolver` implemented with `register`, `resolve`, `resolve_all`
- [ ] Priority ordering is correct (higher wins; equal priority preserves registration order)
- [ ] `resolve()` raises `ValueError` with a descriptive message when no rule matches
- [ ] `dqf/__init__.py` exports `CheckSuiteResolver`
- [ ] Tests cover: priority ordering, catch-all fallback, no-match error, `resolve_all` bulk dispatch
- [ ] All tests pass
- [ ] Committed and pushed to `main`
