# dqf — Data Quality Framework

A composable, pipeline-based Python library for data quality validation of analytical datasets.

`dqf` is designed for data engineers and data scientists working with enterprise data warehouses and lakes. It validates ML features, analytical datasets, and target variables at any scale — connecting directly to Databricks, BigQuery, Snowflake, PostgreSQL, Spark, or any SQLAlchemy-compatible engine.

```
pip install dqf
```

---

## Why dqf?

| Feature | dqf |
|---|---|
| Universe-anchored metrics | All null rates, coverage, and drift statistics are measured against the **universe population**, not the raw dataset row count |
| SQL-first | You write the SQL; dqf executes it and validates results |
| Composable pipelines | Mix and match cross-sectional and longitudinal checks exactly like sklearn pipelines |
| Batteries-included resolver | `build_default_resolver()` dispatches the right checks to each variable type automatically |
| Multi-engine | Databricks, SQLAlchemy (Snowflake, BigQuery, Postgres, …), Spark, or in-memory `MockAdapter` |
| Zero training | Thresholds are explicit business decisions, not learned parameters |

---

## Quick start

### Cross-sectional validation

```python
import pandas as pd
import dqf

adapter = dqf.MockAdapter({
    "SELECT id FROM entities": pd.DataFrame({"id": range(1, 101)}),
    "SELECT id, age, segment FROM features": pd.DataFrame({
        "id": range(1, 101),
        "age": [25 + i % 50 for i in range(100)],
        "segment": ["A" if i % 2 == 0 else "B" for i in range(100)],
    }),
})

universe = dqf.UniverseDataset(
    sql="SELECT id FROM entities",
    primary_key=["id"],
    adapter=adapter,
)

dataset = dqf.VariablesDataset(
    sql="SELECT id, age, segment FROM features",
    primary_key=["id"],
    universe=universe,
    join_keys={"id": "id"},
    adapter=adapter,
    variables=[
        dqf.Variable(name="age", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="segment", dtype=dqf.DataType.CATEGORICAL),
    ],
)

resolver = dqf.build_default_resolver(null_threshold=0.05)
report = dataset.run_validation(resolver, dataset_name="my_features")

print(report.overall_status.value)   # PASSED / FAILED
print(report.to_dataframe())
```

### Longitudinal validation (time series)

Pass `time_field` to `build_default_resolver` to enable trend detection and structural break checks for numeric features, and drift checks for targets:

```python
resolver = dqf.build_default_resolver(
    time_field="event_date",
    period="month",
    null_threshold=0.05,
)
report = dataset.run_validation(resolver, dataset_name="monthly_features")
```

### Custom rules on top of the defaults

```python
from dqf.checks.cross_sectional.range_check import RangeCheck
from dqf.checks.pipeline import CheckPipeline

resolver = dqf.build_default_resolver(null_threshold=0.05)

# Override the default NUMERIC_CONTINUOUS rule for credit_score
resolver.register(
    predicate=lambda v: v.name == "credit_score",
    pipeline_factory=lambda: CheckPipeline([
        ("range", RangeCheck(min_value=300, max_value=850, severity=dqf.Severity.FAILURE))
    ]),
    priority=50,  # higher than the default priority 15
)
```

See the [examples/](examples/) directory for complete runnable scripts.

---

## Core concepts

### Universe dataset

The population against which all metrics are measured. Every quality statistic — null rate, coverage, drift — is expressed as a fraction of the universe size, not the raw dataset row count.

```python
universe = dqf.UniverseDataset(
    sql="SELECT customer_id FROM customers",
    primary_key=["customer_id"],
    adapter=adapter,
)
```

### Variables dataset

The dataset under analysis. Always left-joined to the universe, so missing rows (coverage gaps) are visible as nulls rather than silently excluded.

```python
dataset = dqf.VariablesDataset(
    sql="SELECT customer_id, age, income FROM features",
    primary_key=["customer_id"],
    universe=universe,
    join_keys={"customer_id": "customer_id"},
    adapter=adapter,
    variables=[
        dqf.Variable(name="age", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="income", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
    ],
)
```

### Variable

A descriptor for a single column. Carries semantic type (`DataType`), role (`VariableRole`), and accumulates check results over its lifetime.

```python
dqf.Variable(
    name="is_fraud",
    dtype=dqf.DataType.BOOLEAN,
    role=dqf.VariableRole.TARGET,
)
```

### CheckSuiteResolver

A priority-ordered registry that maps each `Variable` to a `CheckPipeline`. The first matching rule wins.

```python
resolver = dqf.CheckSuiteResolver()
resolver.register(
    predicate=lambda v: v.dtype == dqf.DataType.CATEGORICAL,
    pipeline_factory=lambda: my_categorical_pipeline(),
    priority=10,
)
```

### CheckPipeline

An ordered list of `(name, check)` pairs — analogous to sklearn's `Pipeline`. Steps run sequentially; a `FAILURE`-severity failure marks the variable as failed.

```python
from dqf.checks.pipeline import CheckPipeline
from dqf.checks.cross_sectional.null_rate import NullRateCheck
from dqf.checks.cross_sectional.cardinality_check import CardinalityCheck

pipeline = CheckPipeline([
    ("null_rate", NullRateCheck(threshold=0.05, severity=dqf.Severity.FAILURE)),
    ("cardinality", CardinalityCheck(max_cardinality=20, severity=dqf.Severity.WARNING)),
])
```

### ValidationReport

The top-level output of `run_validation`. Provides per-variable check results, overall status, and HTML rendering.

```python
report.overall_status        # ValidationStatus.PASSED / FAILED
report.universe_size         # int
report.variable_results      # dict[str, list[CheckResult]]
report.to_dataframe()        # flat pandas DataFrame, one row per check
report.failed_variables()    # list of variable names with FAILURE results
report.render()              # HTML string with embedded plots
```

---

## Data types and roles

### `DataType`

| Value | Meaning |
|---|---|
| `NUMERIC_CONTINUOUS` | Real-valued numeric (float, double) |
| `NUMERIC_DISCRETE` | Integer-valued numeric with bounded cardinality |
| `CATEGORICAL` | String or low-cardinality categorical |
| `BOOLEAN` | Binary 0/1 or True/False |
| `DATETIME` | Date or timestamp |
| `TEXT` | Free-form text |
| `IDENTIFIER` | Primary key or join key column |

### `VariableRole`

| Value | Meaning |
|---|---|
| `FEATURE` | Input feature (default) |
| `TARGET` | Prediction target — drift checks applied |
| `IDENTIFIER` | Key column — not-null enforced |
| `AUXILIARY` | Metadata column — not validated |

---

## Built-in checks

### Cross-sectional

| Class | Check name | Description |
|---|---|---|
| `NullRateCheck` | `null_rate` | Fails/warns when null fraction exceeds threshold |
| `NotNullCheck` | `not_null` | Fails when any null is present |
| `CardinalityCheck` | `cardinality` | Warns when distinct value count exceeds limit |
| `OutlierCheck` | `outlier` | Flags outliers via Tukey IQR method |
| `RangeCheck` | `range` | Fails when any value is outside [min, max] |
| `UniquenessCheck` | `uniqueness` | Fails when duplicate values appear |
| `AllowedValuesCheck` | `allowed_values` | Fails when values outside an allowed set appear |
| `RegexPatternCheck` | `regex_pattern` | Fails when values don't match a regex pattern |
| `ReferentialIntegrityCheck` | `referential_integrity` | Fails when values are missing from a reference set |

### Longitudinal (time series)

| Class | Check name | Description |
|---|---|---|
| `TrendCheck` | `trend` | Detects monotonic trend via Kendall's tau |
| `StructuralBreakCheck` | `structural_break` | Detects abrupt level shifts via CUSUM |
| `ProportionDriftCheck` | `proportion_drift` | Sequential Z-test for binary target drift |
| `KSDriftCheck` | `ks_drift` | Sequential KS test for continuous distribution drift |
| `ChiSquaredDriftCheck` | `chisquared_drift` | Sequential chi-squared test for categorical drift |
| `DistributionDriftCheck` | `distribution_drift` | General distribution drift (PSI) |
| `SeasonalityCheck` | `seasonality` | Detects seasonal patterns |
| `ConceptDriftCheck` | `concept_drift` | Detects concept drift using sliding window comparison |

---

## Default resolver — priority table

`build_default_resolver()` registers rules in this priority order:

| Priority | Condition | Pipeline |
|---|---|---|
| 30 | `role == IDENTIFIER` | `not_null` (FAILURE) |
| 25 | `role == TARGET`, `dtype == BOOLEAN` | `not_null` + `proportion_drift` |
| 24 | `role == TARGET`, `dtype in (CATEGORICAL, NUMERIC_DISCRETE)` | `not_null` + `chisquared_drift` |
| 23 | `role == TARGET`, `dtype == NUMERIC_CONTINUOUS` | `not_null` + `ks_drift` |
| 20 | `role == TARGET` (catch-all) | `not_null` |
| 15 | `dtype == NUMERIC_CONTINUOUS` | `null_rate` + `trend` + `structural_break` |
| 10 | `dtype == NUMERIC_DISCRETE` | `null_rate` + `cardinality` + `outlier` (+ `chisquared_drift` if time) |
| 7 | `dtype == CATEGORICAL` | `null_rate` + `cardinality` |
| 5 | `dtype == BOOLEAN` | `null_rate` |
| 0 | catch-all | `null_rate` (WARNING) |

Longitudinal checks are omitted when `time_field=None` (the default).

---

## Adapters

| Adapter | Use case |
|---|---|
| `SQLAlchemyAdapter` | PostgreSQL, Snowflake, BigQuery, Oracle, any SQLAlchemy engine |
| `DatabricksAdapter` | Databricks SQL warehouses |
| `SparkAdapter` | Apache Spark (local or cluster) |
| `MockAdapter` | Unit tests and examples — maps SQL strings to DataFrames |

```python
# SQLAlchemy (e.g. PostgreSQL)
from sqlalchemy import create_engine
adapter = dqf.SQLAlchemyAdapter(create_engine("postgresql+psycopg2://..."))

# Databricks
adapter = dqf.DatabricksAdapter(
    server_hostname="...", http_path="...", access_token="..."
)

# Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
adapter = dqf.SparkAdapter(spark)

# Mock (tests)
adapter = dqf.MockAdapter({"SELECT id FROM t": pd.DataFrame({"id": [1, 2, 3]})})
```

---

## Installation

```
pip install dqf
```

Optional extras:

```
pip install "dqf[spark]"       # PySpark support
pip install "dqf[databricks]"  # Databricks SQL connector
pip install "dqf[docs]"        # MkDocs + mkdocstrings for building the docs
```

Requires Python 3.10+.

---

## Examples

| File | Description |
|---|---|
| [examples/01_basic_validation.py](examples/01_basic_validation.py) | Cross-sectional validation with `build_default_resolver` |
| [examples/02_longitudinal_validation.py](examples/02_longitudinal_validation.py) | Trend + structural break checks with a time dimension |
| [examples/03_custom_resolver.py](examples/03_custom_resolver.py) | Domain-specific range checks alongside the default suite |
| [examples/04_target_drift.py](examples/04_target_drift.py) | Binary target drift detection (fraud rate shift) |

---

## Development

```bash
git clone https://github.com/Mirkiux/dqf
cd dqf
pip install -e ".[dev]"
pytest
ruff check src tests
mypy src
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
