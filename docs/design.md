# Data Quality Framework — Architecture & Execution Plan

> **Status**: Design phase complete. Implementation plans approved and sequenced.
> **Last updated**: 2026-03-19
> **GitHub**: `Mirkiux/<repo-name>` (private) — update once repo name is confirmed.

---

## 1. Project Overview

A Python library for data quality validation of analytical datasets. Conceptually modelled after scikit-learn's transformer and pipeline architecture, but oriented entirely toward data quality validation rather than ML transformation.

**Core philosophy**:
- The **universe dataset** defines the population. All quality metrics are measured against it, never against raw variable dataset sizes.
- Everything is a **composable pipeline** — metadata builders, tests, and resolvers all follow the same composable, step-based pattern.
- The framework **executes SQL, it does not generate it**. SQL is the user's responsibility; the framework orchestrates execution and validation.
- **Thresholds are business decisions**, not learned parameters. No sklearn-style `fit` on tests.

---

## 2. Architecture — Seven Layers

### Layer 1 — Core Primitives and Value Objects

All enumerations and immutable value objects. The atoms of the system — everything else depends on them.

**`DataType` (Enum)**
Semantic type, richer than storage dtype:
`NUMERIC_CONTINUOUS`, `NUMERIC_DISCRETE`, `CATEGORICAL`, `BOOLEAN`, `DATETIME`, `TEXT`, `IDENTIFIER`

**`ValidationStatus` (Enum)**
`PENDING`, `PASSED`, `FAILED`, `SKIPPED`, `ERROR`

**`Severity` (Enum)**
`WARNING`, `FAILURE` — controls how pipeline rolls up status.

**`EngineType` (Enum)**
`SQLALCHEMY`, `DATABRICKS`, `SPARK`, `MOCK`

**`VariableRole` (Enum)**
`FEATURE`, `TARGET`, `IDENTIFIER`, `AUXILIARY`

**`TestResult` (Value Object — immutable)**
Produced by a single test applied to a single variable.
- `test_name: str`
- `passed: bool`
- `severity: Severity`
- `observed_value: Any` — raw metric (e.g. count of nulls)
- `population_size: int` — always the universe size, never the raw variable dataset size
- `rate: Optional[float]` — `observed_value / population_size` when applicable
- `threshold: Any` — what the test compared against
- `metadata: dict` — open dictionary for test-specific extra context
- `figure_factory: Optional[Callable[[], Figure]]` — zero-argument callable; plot rendered on demand, never eagerly

**`ValidationResult` (Value Object — immutable)**
Dataset-level check result (PK uniqueness, join integrity):
- `check_name: str`
- `passed: bool`
- `details: dict`

---

### Layer 2 — Variable

Represents a single column: its description, accumulated metadata, and test results. Not a dataframe column — a descriptor that accumulates results over its lifetime.

**`Variable`**
- `name: str`
- `dtype: DataType` — semantic type (may differ from storage dtype)
- `nullable: bool`
- `role: VariableRole`
- `metadata: dict` — open dictionary populated by metadata builders (e.g. `{"semantic_dtype": "NUMERIC_CONTINUOUS", "empirical_null_rate": 0.03, "cardinality": 4}`)
- `status: ValidationStatus` — recomputed each time a result is attached
- `test_results: List[TestResult]`

Methods:
- `attach_result(result: TestResult)` — appends result, recomputes status. A single FAILURE sets status to FAILED; WARNING alone sets to PASSED with warnings.
- `summary() -> dict` — consolidated view across all results.

---

### Layer 3 — DataSourceAdapter

Abstracts SQL execution and DataFrame materialization. Each dataset instance carries its own adapter, enabling multi-engine setups (e.g. UniverseDataset on Databricks, VariablesDataset on Oracle).

**`DataSourceAdapter` (Abstract)**
- `execute(sql: str) -> pd.DataFrame` — executes SQL, returns pandas DataFrame
- `engine_type() -> EngineType`

**Concrete implementations**:
- `SQLAlchemyAdapter(connection_string: str)` — covers Oracle, PostgreSQL, Snowflake, MySQL, and most JDBC engines
- `DatabricksAdapter(host, token, http_path, catalog, schema)`
- `SparkAdapter(spark_session)` — for native PySpark execution
- `MockAdapter(dataframes: dict)` — accepts a dict of `{sql_pattern: pd.DataFrame}` for testing and development

**Key contract**: adapters return pandas DataFrames always. Engine-native aggregations (for longitudinal tests) are pushed down via SQL strings constructed by the framework and executed through this adapter. The framework never imports `pyspark` or any connector directly.

---

### Layer 4 — Metadata Subsystem

Dynamically profiles each column to populate `Variable.metadata`. Metadata feeds the `TestSuiteResolver` dispatch logic — its quality directly determines the quality of test selection.

**`BaseMetadataBuilder` (Abstract)**
- `name: str`
- `profile(series: pd.Series, variable: Variable) -> dict` — inspects the series, returns a flat dict to be merged into `variable.metadata`. Does not mutate the variable directly.

**`MetadataBuilderPipeline`**
- Chains multiple builders sequentially.
- Each builder's output dict is merged into `variable.metadata`.
- Same interface as `BaseMetadataBuilder` (Composite pattern).

**Core concrete builders**:
- `StorageDtypeBuilder` — records the raw pandas/engine dtype as `storage_dtype`
- `NullabilityProfileBuilder` — computes empirical null rate, sets `empirical_null_rate`, `is_nullable`
- `CardinalityBuilder` — counts distinct values, sets `cardinality`, `is_high_cardinality`
- `DistributionShapeBuilder` — for numeric columns: mean, std, skewness, kurtosis, min, max
- `SemanticTypeInferenceBuilder` — attempts coercion to numeric/datetime for varchar columns; sets `semantic_dtype` which may override `storage_dtype` in resolver dispatch

**Engine agnosticism**: builders always receive a `pd.Series`. The caller (i.e. `resolve_variables()` in `VariablesDataset`) materializes the data via the adapter before passing columns to builders. For large datasets, `resolve_variables()` works on a stratified sample — metadata estimation does not require the full dataset.

---

### Layer 5 — Test Abstractions and TestPipeline

**`BaseCrossSectionalTest` (Abstract)**
Point-in-time tests. Operate on the entity-level universe-joined DataFrame.
- `name: str`
- `severity: Severity`
- `params: dict` — business thresholds declared at construction (e.g. `{"threshold": 0.10}`)
- `calibrate(reference_data: pd.DataFrame)` — optional; for statistical baselines (e.g. reference distribution for drift tests). Named deliberately differently from sklearn's `fit`.
- `check(data: pd.DataFrame, variable: Variable) -> TestResult` — executes validation

**`BaseLongitudinalTest` (Abstract)**
Time-aware tests. Require temporal aggregation before execution.
- Everything in `BaseCrossSectionalTest`, plus:
- `aggregation_sql(variable_name: str, time_field: str, period: str) -> str` — returns the SQL aggregation to execute engine-side. The framework calls this to generate a time-indexed summary, brings it back as a small pandas DataFrame, then calls `check()`.
- `check(time_series_data: pd.DataFrame, variable: Variable) -> TestResult` — receives the aggregated time-series, not raw entity-level data

**`TestPipeline`**
- `steps: List[Tuple[str, BaseTest]]` — named steps; supports `BaseCrossSectionalTest` and `BaseLongitudinalTest` mixed in the same pipeline
- `stop_on_failure: bool` — short-circuits on first FAILURE-severity result
- `run(data, variable: Variable) -> List[TestResult]`
- Composite: a `TestPipeline` can itself be a step inside another `TestPipeline`

---

### Layer 6 — TestSuiteResolver

Dispatches the right `TestPipeline` to each `Variable` based on its metadata. Contains no hardcoded logic — all rules are registered externally.

**`TestSuiteResolver`**
- `rules: List[Tuple[Callable[[Variable], bool], Callable[[], TestPipeline], int]]` — ordered list of (predicate, pipeline factory, priority)
- `register(predicate, pipeline_factory, priority=0)` — adds a rule; higher priority evaluated first
- `resolve(variable: Variable) -> TestPipeline` — walks rules in priority order, returns first match
- `resolve_all(variables: List[Variable]) -> Dict[str, TestPipeline]`

Predicates can inspect any field in `Variable` or `variable.metadata`, enabling rich dispatch:
```
resolver.register(
    predicate = lambda v: v.metadata.get("semantic_dtype") == "NUMERIC_CONTINUOUS" and not v.nullable,
    factory   = lambda: TestPipeline([("nulls", NullRateCheck({"threshold": 0.0})), ("range", RangeCheck({"min": 0}))]),
    priority  = 10
)
resolver.register(
    predicate = lambda v: v.role == VariableRole.TARGET,
    factory   = lambda: TestPipeline([("drift", DistributionDriftTest({"period": "month"}))]),
    priority  = 20  # target variables get drift tests regardless of dtype
)
resolver.register(
    predicate = lambda v: True,   # catch-all
    factory   = lambda: TestPipeline([("nulls", NullRateCheck({"threshold": 0.20}))]),
    priority  = 0
)
```

---

### Layer 7 — Dataset Classes

**`UniverseDataset`**
The population anchor. All quality metrics are measured against the universe.
- `sql: str` — SQL statement producing the universe
- `primary_key: List[str]`
- `time_field: Optional[str]` — required for longitudinal tests
- `adapter: DataSourceAdapter`

Methods:
- `to_pandas() -> pd.DataFrame`
- `to_spark()` — delegates to adapter if SparkAdapter
- `validate_pk_uniqueness(data: pd.DataFrame) -> ValidationResult`

**`VariablesDataset`**
- `sql: str` — SQL statement producing the raw variables dataset
- `primary_key: List[str]`
- `universe: UniverseDataset`
- `join_keys: Dict[str, str]` — `{"variables_col": "universe_col"}` mapping
- `variables: List[Variable]` — populated by `resolve_variables()` or manually
- `adapter: DataSourceAdapter` — can be a different engine than universe's adapter

Methods:
- `to_pandas() -> pd.DataFrame` — **always returns a LEFT JOIN from universe to variables**. Universe rows with no match appear with NULL variable values. A framework-managed boolean column `__vd_matched__` flags whether each universe row had a match, enabling downstream tests to distinguish structural nulls (missing from variables dataset) from value nulls.
- `to_spark()`
- `validate_pk_uniqueness(data) -> ValidationResult`
- `validate_join_integrity(data, universe_data) -> ValidationResult` — verifies join does not fan out universe rows
- `resolve_variables(data: pd.DataFrame, builder_pipeline: MetadataBuilderPipeline) -> List[Variable]` — inspects materialized DataFrame, runs each column through the builder pipeline
- `run_validation(resolver: TestSuiteResolver) -> ValidationReport` — main method; see below

**`run_validation` orchestration**:
1. Materialize universe and variables (left join)
2. Run dataset-level invariant checks (PK uniqueness, join integrity)
3. Call `resolve_variables()` if `self.variables` is empty
4. Call `resolver.resolve_all(self.variables)` to get per-variable pipelines
5. For each variable: if pipeline contains only cross-sectional tests → run against the entity-level DataFrame. If pipeline contains longitudinal tests → generate the aggregation SQL via `aggregation_sql()`, execute it through the adapter, run against the returned time-series DataFrame.
6. Attach all `TestResult`s to their respective `Variable` instances
7. Assemble and return a `ValidationReport`

---

### Layer 8 — ValidationReport

The top-level output of `run_validation`.

**`ValidationReport`**
- `dataset_name: str`
- `run_timestamp: datetime`
- `dataset_level_checks: List[ValidationResult]`
- `variable_reports: Dict[str, List[TestResult]]` — keyed by variable name
- `overall_status: ValidationStatus`

Methods:
- `to_dataframe() -> pd.DataFrame` — flat tabular summary suitable for logging or storage
- `failed_variables() -> List[str]`
- `warnings() -> List[TestResult]`
- `render(output_path: Optional[str]) -> str` — materializes all `figure_factory` callables, assembles HTML report, optionally writes to file

---

## 3. Key Design Decisions and Rationale

| Decision | Rationale |
|---|---|
| No `fit` on `BaseTest` | Thresholds are business decisions, not learned. Use `params: dict` at construction. Optional `calibrate()` for statistical baselines only. |
| No `fit` on `BaseMetadataBuilder` | Profilers inspect, they do not learn. Method is `profile(series, variable) -> dict`. |
| Universe as left join anchor | Percentage-based metrics must use universe size as denominator. Raw variable dataset size is misleading (can be much larger or smaller than population). |
| `__vd_matched__` flag column | Distinguishes structural nulls (entity absent from variables dataset) from value nulls (entity present, value is null). Tests can optionally use this for root-cause breakdown. |
| `DataSourceAdapter` per dataset | Enables multi-engine setups (e.g. Databricks for universe, Oracle for variables) without framework-level SQL federation. Default cross-engine join strategy: materialize both to pandas, join in-memory. |
| SQL dialect is user responsibility | Framework executes SQL, does not generate it. The `sql` attribute is always user-authored in the target engine's dialect. |
| Two test families | Cross-sectional tests receive entity-level data; longitudinal tests declare their aggregation SQL and receive a time-indexed summary. Dispatch happens in `run_validation`. |
| `figure_factory` as lazy callable | Plots are only rendered when requested (e.g. at report generation time). Keeps `TestResult` lightweight. |
| `TestSuiteResolver` is external to datasets | Dataset classes have no dispatch logic. The resolver is injected at `run_validation` call time, making dispatch strategies swappable. |
| Metadata drives dispatch | The richer the metadata, the better the resolver's decisions. `SemanticTypeInferenceBuilder` is critical for bronze-layer data where storage dtype (varchar) differs from semantic dtype (numeric). |

---

## 4. Execution Plans — Sequenced

Each plan is independently completable and testable before the next begins.

---

### Plan 1 — Project Scaffold and Tooling
Directory structure, package layout, dependency management (`pyproject.toml`), linting and formatting configuration (`ruff`, `mypy`), CI skeleton (GitHub Actions), test framework setup (`pytest`).

### Plan 2 — Core Primitives and Value Objects
All enumerations (`DataType`, `ValidationStatus`, `Severity`, `EngineType`, `VariableRole`) and immutable value objects (`TestResult`, `ValidationResult`). Full unit test coverage. These are the atoms — everything else depends on them.

### Plan 3 — The Variable Class
`Variable` with metadata dict, status, and test results collection. `attach_result()` with status recomputation logic. `summary()`. Full unit tests.

### Plan 4 — DataSourceAdapter Abstraction and Implementations
Abstract `DataSourceAdapter`. Concrete: `SQLAlchemyAdapter`, `DatabricksAdapter`, `SparkAdapter`, `MockAdapter`. Integration tests using `MockAdapter`.

### Plan 5 — Metadata Subsystem
Abstract `BaseMetadataBuilder`, `MetadataBuilderPipeline`. Core concrete builders: `StorageDtypeBuilder`, `NullabilityProfileBuilder`, `CardinalityBuilder`, `DistributionShapeBuilder`, `SemanticTypeInferenceBuilder`. Unit tests for each builder and the pipeline.

### Plan 6 — Test Abstractions and TestPipeline
`BaseCrossSectionalTest`, `BaseLongitudinalTest` with `params: dict` and optional `calibrate()`. `TestPipeline` with Composite pattern and `stop_on_failure`. No concrete tests yet — just the abstractions and full unit tests.

### Plan 7 — TestSuiteResolver
Registry with predicate-based rule registration, priority ordering, `resolve()` and `resolve_all()`. Unit tests covering priority ordering, catch-all fallback, and edge cases.

### Plan 8 — Dataset Classes and Materialization
`UniverseDataset` and `VariablesDataset`: attributes, `DataSourceAdapter` integration, `to_pandas()` with universe-anchored left join (including `__vd_matched__` flag), PK uniqueness validation, join integrity validation, `resolve_variables()`. Unit and integration tests using `MockAdapter`.

### Plan 9 — Validation Orchestration (`run_validation`)
The main method of `VariablesDataset`: routing variables to cross-sectional vs longitudinal paths, invoking the resolver, dispatching pipeline runs, assembling `ValidationReport`. This plan connects all prior plans. End-to-end integration tests using `MockAdapter`.

### Plan 10 — Concrete Cross-Sectional Tests
Standard library of point-in-time tests: `NullRateCheck`, `RangeCheck`, `AllowedValuesCheck`, `CardinalityCheck`, `UniquenessCheck`, `RegexPatternCheck`, `ReferentialIntegrityCheck`. Unit tests with figure_factory coverage.

### Plan 11 — Concrete Longitudinal Tests
Time-aware tests: `DistributionDriftTest`, `StructuralBreakTest`, `TrendTest`, `SeasonalityCheck`, `ConceptDriftTest` (for target variables). Each declares its `aggregation_sql()`. Unit tests with mock time-series data.

### Plan 12 — ValidationReport and Rendering
Full `ValidationReport` structure, `to_dataframe()`, `render()` with figure materialization and HTML assembly, plot embedding via base64. Unit tests for all output formats.

### Plan 13 — Default Test Suite Configuration
Pre-configured `TestSuiteResolver` with sensible default rules for common variable types. Pre-built pipelines users can adopt out of the box. This is the "batteries included" layer.

### Plan 14 — Integration Tests and Worked Examples
End-to-end tests using `MockAdapter` covering the full path from SQL definition to `ValidationReport`. Worked examples as notebooks: multi-engine setup, longitudinal analysis, custom resolver rules, target variable drift.

### Plan 15 — Packaging and Documentation
API reference documentation (Sphinx or MkDocs), user guide with conceptual explanations, `PyPI` packaging, changelog, contribution guidelines.

---

## 5. Dependency Graph Between Plans

```
1 (scaffold)
└── 2 (primitives)
    └── 3 (Variable)
        ├── 4 (DataSourceAdapter)   ← independent branch
        ├── 5 (Metadata subsystem)
        └── 6 (Test abstractions)
            └── 7 (TestSuiteResolver)
                └── 8 (Dataset classes) ← depends on 4, 5, 6, 7
                    └── 9 (run_validation)
                        ├── 10 (Cross-sectional tests)  ┐
                        ├── 11 (Longitudinal tests)     ├─ parallelizable
                        └── 12 (ValidationReport)       ┘
                            └── 13 (Default suites)
                                └── 14 (Integration tests & examples)
                                    └── 15 (Packaging & docs)
```

**Critical path**: 1 → 2 → 3 → 6 → 7 → 8 → 9 → 12 → 15
Plans 4 and 5 can run in parallel with 6 after Plan 3.
Plans 10, 11, and 13 are parallelizable after Plan 9.
