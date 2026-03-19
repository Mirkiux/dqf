# Plan 2 — Core Primitives and Value Objects

## Goal

Implement all enumerations and immutable value objects that form the atoms of the dqf framework.
Every subsequent plan depends on these types — nothing else can be built until this plan is complete.

## Scope

### Enumerations

| Class | Module | Purpose |
|---|---|---|
| `DataType` | `dqf/enums.py` | Semantic type of a variable — richer than storage dtype |
| `ValidationStatus` | `dqf/enums.py` | Lifecycle status of a variable or dataset-level check |
| `Severity` | `dqf/enums.py` | Impact level of a test failure |
| `EngineType` | `dqf/enums.py` | Identifies the execution engine behind a DataSourceAdapter |
| `VariableRole` | `dqf/enums.py` | Domain role of a variable in the analytical context |

### Value Objects (immutable dataclasses)

| Class | Module | Purpose |
|---|---|---|
| `TestResult` | `dqf/results.py` | Output of a single test applied to a single variable |
| `ValidationResult` | `dqf/results.py` | Output of a dataset-level invariant check (PK uniqueness, join integrity) |

### Unit Tests

| File | Covers |
|---|---|
| `tests/test_enums.py` | Enum member existence, values, and string representations |
| `tests/test_results.py` | Immutability, field validation, `figure_factory` lazy invocation |

---

## Detailed Specifications

### `DataType` (Enum)

Members:
- `NUMERIC_CONTINUOUS` — measurable quantities with infinite precision (price, temperature, ratio)
- `NUMERIC_DISCRETE` — countable quantities (number of transactions, age in years)
- `CATEGORICAL` — finite set of unordered labels (country, product category)
- `BOOLEAN` — binary flag (is_active, has_churned)
- `DATETIME` — timestamps or dates
- `TEXT` — free-form string with no fixed vocabulary
- `IDENTIFIER` — technical key with no analytical meaning (UUID, surrogate key)

### `ValidationStatus` (Enum)

Members:
- `PENDING` — not yet evaluated
- `PASSED` — all FAILURE-severity tests passed (WARNING-severity failures allowed)
- `FAILED` — at least one FAILURE-severity test did not pass
- `SKIPPED` — evaluation deliberately bypassed (e.g. variable excluded from resolver)
- `ERROR` — an unexpected exception prevented evaluation

### `Severity` (Enum)

Members:
- `WARNING` — test failure is informational; does not set variable status to FAILED
- `FAILURE` — test failure sets variable status to FAILED

### `EngineType` (Enum)

Members:
- `SQLALCHEMY` — any SQLAlchemy-compatible engine (Oracle, PostgreSQL, Snowflake, etc.)
- `DATABRICKS` — Databricks SQL connector
- `SPARK` — native PySpark session
- `MOCK` — in-memory mock for testing

### `VariableRole` (Enum)

Members:
- `FEATURE` — input variable for a model or analysis
- `TARGET` — output variable to be predicted; triggers drift-aware longitudinal tests
- `IDENTIFIER` — carries identity information, not analytical signal
- `AUXILIARY` — contextual variable not directly modelled (date, segment flag)

---

### `TestResult` (frozen dataclass)

All fields are set at construction and cannot be mutated afterwards (`frozen=True`).

Fields:
- `test_name: str` — name of the test that produced this result
- `passed: bool` — whether the test considered the variable to have passed
- `severity: Severity` — impact level if the test failed
- `observed_value: Any` — the raw metric computed (e.g. count of nulls: `5000`)
- `population_size: int` — the universe size used as denominator; never the raw variable dataset size
- `rate: Optional[float]` — `observed_value / population_size` when the metric is a proportion; `None` otherwise
- `threshold: Any` — the value the observed metric was compared against
- `metadata: dict` — open dictionary for test-specific extra context (e.g. column name, period)
- `figure_factory: Optional[Callable[[], Any]]` — zero-argument callable returning a matplotlib Figure; `None` if the test produces no plot. Excluded from equality checks and hashing.

Validation at construction:
- `population_size` must be a positive integer
- `rate`, if provided, must be between 0.0 and 1.0
- `test_name` must be a non-empty string

### `ValidationResult` (frozen dataclass)

Fields:
- `check_name: str` — name of the dataset-level check (e.g. `"pk_uniqueness"`, `"join_integrity"`)
- `passed: bool`
- `details: dict` — open dictionary (e.g. `{"duplicate_count": 3, "duplicate_keys": [...]}`)

---

## File Layout After This Plan

```
dqf/
├── __init__.py        (updated: exports public API symbols)
├── enums.py           (new)
└── results.py         (new)
tests/
├── __init__.py
├── test_enums.py      (new)
└── test_results.py    (new)
```

## Definition of Done

- [ ] All five enums implemented with the members specified above
- [ ] `TestResult` frozen dataclass with field validation at construction
- [ ] `ValidationResult` frozen dataclass
- [ ] `figure_factory` excluded from `__eq__` and `__hash__`
- [ ] `dqf/__init__.py` exports all public symbols
- [ ] `tests/test_enums.py` — 100% branch coverage of enums
- [ ] `tests/test_results.py` — covers: valid construction, invalid `population_size`, invalid `rate`, `figure_factory` lazy call, equality semantics
- [ ] All tests pass locally
- [ ] Committed and pushed to `main`
