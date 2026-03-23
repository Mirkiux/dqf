# Concepts

This page explains the core ideas behind dqf. Understanding these will help you configure validation correctly and interpret results.

---

## Universe dataset

The **universe** defines the population. Every quality metric — null rate, coverage, drift — is expressed as a fraction of the universe size.

```python
universe = dqf.UniverseDataset(
    sql="SELECT customer_id FROM customers WHERE cohort_month = '2024-01-01'",
    primary_key=["customer_id"],
    adapter=adapter,
)
```

**Why?** If you have 1 000 customers in your universe but only 900 rows in the features table, the missing 100 are coverage gaps. A raw null rate on the 900 rows would hide this. dqf performs a **left join** from the universe to the features table, so the 100 missing rows appear as nulls — and the null rate is 100/1000 = 10%, not 0%.

---

## Variables dataset

The **variables dataset** contains the feature columns (or target variable) to be validated.

```python
dataset = dqf.VariablesDataset(
    sql="SELECT customer_id, age, income, segment FROM customer_features",
    primary_key=["customer_id"],
    universe=universe,
    join_keys={"customer_id": "customer_id"},
    adapter=adapter,
    variables=[
        dqf.Variable(name="age", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="income", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="segment", dtype=dqf.DataType.CATEGORICAL),
    ],
)
```

The `sql` string is a plain SQL query executed by the adapter. dqf does not generate SQL for the raw data — you own that query.

**Materialisation** — when validation runs, dqf executes `universe.sql` and `dataset.sql`, then performs a left join on `join_keys`. The resulting DataFrame (universe-anchored) is passed to every check.

---

## Variable

A `Variable` is a descriptor for a single column. It carries:

- `name` — column name
- `dtype` — semantic type (`DataType`)
- `role` — purpose of the column (`VariableRole`)
- `nullable` — whether nulls are allowed (checked by NotNullCheck when `False`)
- `metadata` — open dict, populated by metadata builders during validation
- `status` — recomputed each time a `CheckResult` is attached

```python
dqf.Variable(
    name="is_fraud",
    dtype=dqf.DataType.BOOLEAN,
    role=dqf.VariableRole.TARGET,
)
```

### DataType

| Value | Meaning |
|---|---|
| `NUMERIC_CONTINUOUS` | Real-valued numeric |
| `NUMERIC_DISCRETE` | Integer-valued numeric with bounded cardinality |
| `CATEGORICAL` | String or low-cardinality category |
| `BOOLEAN` | Binary 0/1 or True/False |
| `DATETIME` | Date or timestamp |
| `TEXT` | Free-form text |
| `IDENTIFIER` | Primary key or join key |

### VariableRole

| Value | Meaning |
|---|---|
| `FEATURE` | Input feature (default) |
| `TARGET` | Prediction target — drift checks applied |
| `IDENTIFIER` | Key column — not-null enforced |
| `AUXILIARY` | Metadata column — not validated |

---

## CheckPipeline

A `CheckPipeline` is an ordered list of `(name, check)` pairs — analogous to sklearn's `Pipeline`.

```python
from dqf.checks.pipeline import CheckPipeline
from dqf.checks.cross_sectional.null_rate import NullRateCheck
from dqf.checks.cross_sectional.cardinality_check import CardinalityCheck

pipeline = CheckPipeline([
    ("null_rate", NullRateCheck(threshold=0.05, severity=dqf.Severity.FAILURE)),
    ("cardinality", CardinalityCheck(max_cardinality=20, severity=dqf.Severity.WARNING)),
])
```

Each step runs in order. A `FAILURE`-severity check that fails marks the variable as `FAILED`. A `WARNING`-severity failure records a warning but does not change overall status.

---

## CheckSuiteResolver

The `CheckSuiteResolver` maps each `Variable` to a `CheckPipeline` via priority-ordered predicate rules. The first matching rule wins.

```python
resolver = dqf.CheckSuiteResolver()

resolver.register(
    predicate=lambda v: v.dtype == dqf.DataType.CATEGORICAL,
    pipeline_factory=lambda: my_categorical_pipeline(),
    priority=10,
)
resolver.register(
    predicate=lambda v: True,  # catch-all
    pipeline_factory=lambda: fallback_pipeline(),
    priority=0,
)
```

`build_default_resolver()` returns a pre-configured resolver covering all common variable types. You can extend it by registering rules at a higher priority:

```python
resolver = dqf.build_default_resolver(null_threshold=0.05)

# Override for a specific column at priority 50 > default 15
resolver.register(
    predicate=lambda v: v.name == "credit_score",
    pipeline_factory=lambda: my_credit_score_pipeline(),
    priority=50,
)
```

---

## Cross-sectional vs longitudinal checks

**Cross-sectional checks** operate on the materialised snapshot — a single DataFrame. They are stateless and instantaneous:

- `NullRateCheck`, `NotNullCheck`, `CardinalityCheck`, `OutlierCheck`, `RangeCheck`, `UniquenessCheck`, `AllowedValuesCheck`, `RegexPatternCheck`, `ReferentialIntegrityCheck`

**Longitudinal checks** operate on a time series. They issue an **aggregation SQL query** (via `aggregation_sql()`) to compute period-level statistics, then analyse the resulting time series:

- `TrendCheck`, `StructuralBreakCheck`, `ProportionDriftCheck`, `KSDriftCheck`, `ChiSquaredDriftCheck`, `DistributionDriftCheck`, `SeasonalityCheck`, `ConceptDriftCheck`

To use longitudinal checks, pass `time_field` to `build_default_resolver()`:

```python
resolver = dqf.build_default_resolver(
    time_field="event_date",
    period="month",
)
```

### MockAdapter and longitudinal checks

When using `MockAdapter`, you must pre-seed the aggregation SQL. The aggregation SQL is computed via:

```python
check = TrendCheck(time_field="event_date", period="month")
agg_sql = check.aggregation_sql("my_column", "event_date", "month").format(
    source="SELECT id, event_date, my_column FROM my_table"
)
# Now register: MockAdapter({..., agg_sql: monthly_agg_df})
```

See [Example 2](examples/02_longitudinal_validation.md) for a complete worked example.

---

## ValidationReport

`run_validation()` returns a `ValidationReport`:

```python
report = dataset.run_validation(resolver, dataset_name="my_features")

report.overall_status          # ValidationStatus.PASSED / FAILED
report.universe_size           # int — size of the universe
report.variable_results        # dict[str, list[CheckResult]]
report.to_dataframe()          # pandas DataFrame, one row per check result
report.failed_variables()      # list[str] — variables with FAILURE results
report.warned_variables()      # list[str] — variables with WARNING results
report.render()                # HTML string with embedded plots
```

### Severity and status

- `FAILURE` — the check failed and this variable is marked `FAILED`. The overall report status becomes `FAILED`.
- `WARNING` — the check failed but the variable is marked `PASSED` (with warnings). The overall status remains `PASSED` unless another variable has a `FAILURE`.
