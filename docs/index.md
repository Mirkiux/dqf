# dqf — Data Quality Framework

A composable, pipeline-based Python library for data quality validation of analytical datasets.

---

## Installation

```bash
pip install dqf
```

Optional extras:

```bash
pip install "dqf[spark]"       # PySpark support
pip install "dqf[databricks]"  # Databricks SQL connector
pip install "dqf[docs]"        # MkDocs + mkdocstrings for building these docs
```

Requires Python 3.10+.

---

## Quick start

```python
import pandas as pd
import dqf

# 1. Wire up an adapter (MockAdapter for tests; SQLAlchemyAdapter / DatabricksAdapter in production)
adapter = dqf.MockAdapter({
    "SELECT id FROM entities": pd.DataFrame({"id": range(1, 101)}),
    "SELECT id, age, segment FROM features": pd.DataFrame({
        "id": range(1, 101),
        "age": [25 + i % 50 for i in range(100)],
        "segment": ["A" if i % 2 == 0 else "B" for i in range(100)],
    }),
})

# 2. Define the universe (population)
universe = dqf.UniverseDataset(
    sql="SELECT id FROM entities",
    primary_key=["id"],
    adapter=adapter,
)

# 3. Define the variables dataset
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

# 4. Run validation
resolver = dqf.build_default_resolver(null_threshold=0.05)
report = dataset.run_validation(resolver, dataset_name="my_features")

# 5. Inspect results
print(report.overall_status.value)  # PASSED / FAILED
print(report.to_dataframe().to_string(index=False))
```

---

## Key concepts

- **[Universe dataset](concepts.md#universe-dataset)** — defines the population. All metrics are measured against it.
- **[Variables dataset](concepts.md#variables-dataset)** — left-joined to the universe so coverage gaps become visible nulls.
- **[CheckPipeline](concepts.md#checkpipeline)** — ordered list of checks, sklearn-style.
- **[CheckSuiteResolver](concepts.md#checksuiteresolver)** — priority-ordered registry that dispatches the right pipeline to each variable.
- **[ValidationReport](concepts.md#validationreport)** — top-level output with per-variable results, overall status, and HTML rendering.

---

## Why dqf?

| | dqf |
|---|---|
| Universe-anchored metrics | Null rates and coverage are fractions of the **universe size**, not the raw dataset row count |
| SQL-first | You write the SQL; dqf executes it and validates results |
| Composable | Mix cross-sectional and longitudinal checks exactly like sklearn pipelines |
| Batteries-included | `build_default_resolver()` dispatches the right checks to each variable type automatically |
| Multi-engine | Databricks, SQLAlchemy, Spark, or in-memory MockAdapter |
| Explicit thresholds | No learned parameters — thresholds are your business decisions |
