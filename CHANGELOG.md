# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.1] — 2026-03-24

### Added

- `DatabricksNotebookAdapter` — zero-argument adapter for Databricks notebook environments where `spark` is pre-injected by the runtime. Discovers the session lazily from `sys.modules['__main__']`; raises a clear `RuntimeError` when used outside a notebook context.
- `examples/05_databricks_notebook.ipynb` — end-to-end notebook example using `DatabricksNotebookAdapter` with a lending dataset scenario (feature validation + target drift monitoring).

---

## [0.1.0] — 2026-03-23

Initial release. Full implementation of the composable data quality validation framework.

### Added

#### Core primitives
- `DataType` enum: `NUMERIC_CONTINUOUS`, `NUMERIC_DISCRETE`, `CATEGORICAL`, `BOOLEAN`, `DATETIME`, `TEXT`, `IDENTIFIER`
- `ValidationStatus` enum: `PENDING`, `PASSED`, `FAILED`, `SKIPPED`, `ERROR`
- `Severity` enum: `WARNING`, `FAILURE`
- `VariableRole` enum: `FEATURE`, `TARGET`, `IDENTIFIER`, `AUXILIARY`
- `EngineType` enum: `SQLALCHEMY`, `DATABRICKS`, `SPARK`, `MOCK`
- `CheckResult` immutable value object with `check_name`, `passed`, `severity`, `observed_value`, `population_size`, `rate`, `threshold`, `metadata`, and `figure_factory`
- `ValidationResult` immutable value object for dataset-level checks

#### Variable
- `Variable` descriptor with `name`, `dtype`, `role`, `nullable`, `metadata`, `status`, and `check_results`
- `attach_result()` — appends a `CheckResult` and recomputes status
- `summary()` — flat dict view of validation state
- `reset()` — clears results for re-use

#### Adapters
- `DataSourceAdapter` abstract base class
- `SQLAlchemyAdapter` — PostgreSQL, Snowflake, BigQuery, Oracle, and any SQLAlchemy engine
- `DatabricksAdapter` — Databricks SQL warehouses via `databricks-sql-connector`
- `SparkAdapter` — Apache Spark (local or cluster)
- `MockAdapter` — in-memory SQL-to-DataFrame mapping for tests and examples

#### Metadata subsystem
- `MetadataBuilder` base class
- `NullRateBuilder`, `CardinalityBuilder`, `DataTypeInferenceBuilder` — profile columns before checks run

#### Check infrastructure
- `BaseCheck` abstract base with `run(data, variable, adapter)` contract
- `BaseCrossSectionalCheck` — checks that operate on the materialised snapshot
- `BaseLongitudinalCheck` — checks that issue an aggregation SQL query and analyse time series
- `CheckPipeline` — ordered list of `(name, check)` pairs, sklearn-style

#### Cross-sectional checks
- `NullRateCheck` — null fraction vs threshold
- `NotNullCheck` — zero-tolerance null check
- `CardinalityCheck` — distinct value count vs limit
- `OutlierCheck` — Tukey IQR outlier detection
- `RangeCheck` — value bounds enforcement
- `UniquenessCheck` — duplicate detection
- `AllowedValuesCheck` — membership in an explicit set
- `RegexPatternCheck` — regex conformance
- `ReferentialIntegrityCheck` — foreign-key integrity

#### Longitudinal checks
- `TrendCheck` — monotonic trend via Kendall's tau
- `StructuralBreakCheck` — abrupt level shift via CUSUM
- `ProportionDriftCheck` — sequential two-proportion Z-test for binary targets
- `KSDriftCheck` — sequential Kolmogorov-Smirnov test for continuous distributions
- `ChiSquaredDriftCheck` — sequential chi-squared test for categorical distributions
- `DistributionDriftCheck` — Population Stability Index (PSI)
- `SeasonalityCheck` — seasonal pattern detection
- `ConceptDriftCheck` — sliding-window concept drift detection

#### Resolver
- `CheckSuiteResolver` — priority-ordered registry mapping `Variable` → `CheckPipeline`
- `register(predicate, pipeline_factory, priority)` — add a rule
- `resolve(variable)` / `resolve_all(dataset)` — dispatch

#### Datasets
- `UniverseDataset` — population definition with PK uniqueness validation
- `VariablesDataset` — left-joins to universe; exposes `run_validation(resolver)`
- `MaterialisedData` — cached universe-anchored snapshot

#### Validation orchestration
- `VariablesDataset.run_validation(resolver, dataset_name)` — full end-to-end validation run
- PK uniqueness check, join key uniqueness check, variable profiling, check dispatch, result assembly

#### ValidationReport
- `ValidationReport` — top-level output with `overall_status`, `universe_size`, `variable_results`
- `to_dataframe()` — flat tabular summary (one row per check)
- `failed_variables()` / `warned_variables()` — convenience accessors
- `render()` — HTML report with embedded matplotlib figures

#### Default resolver
- `build_default_resolver(time_field, period, null_threshold, max_categorical_cardinality, max_discrete_cardinality)` — batteries-included resolver covering all variable types
- Type-specific pipeline factories exported individually: `identifier_pipeline`, `target_binary_pipeline`, `target_categorical_pipeline`, `target_continuous_pipeline`, `numeric_continuous_pipeline`, `numeric_discrete_pipeline`, `categorical_pipeline`, `boolean_pipeline`, `catch_all_pipeline`

#### Tests and examples
- 480 unit and integration tests (pytest)
- 4 worked example scripts in `examples/`

[Unreleased]: https://github.com/Mirkiux/dqf/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Mirkiux/dqf/releases/tag/v0.1.0
