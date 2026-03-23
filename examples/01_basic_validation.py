"""Example 1 — Basic cross-sectional validation.

Demonstrates the simplest usage of dqf: validate a dataset of ML features
using the batteries-included resolver with no time dimension.

All external I/O is replaced by MockAdapter so this script runs
without any database connection.
"""

from __future__ import annotations

import pandas as pd

import dqf

# ──────────────────────────────────────────────────────────────────────────────
# 1.  Describe the data
# ──────────────────────────────────────────────────────────────────────────────
#
# In a real project these SQL strings would query your data warehouse.
# MockAdapter maps them directly to in-memory DataFrames so the example
# is self-contained.

UNIVERSE_SQL = "SELECT customer_id FROM customers"
VARIABLES_SQL = "SELECT customer_id, age, income, segment, is_premium FROM features"

universe_df = pd.DataFrame({"customer_id": range(1, 101)})

features_df = pd.DataFrame(
    {
        "customer_id": range(1, 101),
        "age": [25 + i % 50 for i in range(100)],
        "income": [30_000 + i * 500 for i in range(100)],
        "segment": ["A" if i % 3 == 0 else ("B" if i % 3 == 1 else "C") for i in range(100)],
        "is_premium": [i % 4 == 0 for i in range(100)],
    }
)

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Wire up the adapter and datasets
# ──────────────────────────────────────────────────────────────────────────────

adapter = dqf.MockAdapter({UNIVERSE_SQL: universe_df, VARIABLES_SQL: features_df})

universe = dqf.UniverseDataset(
    sql=UNIVERSE_SQL,
    primary_key=["customer_id"],
    adapter=adapter,
)

dataset = dqf.VariablesDataset(
    sql=VARIABLES_SQL,
    primary_key=["customer_id"],
    universe=universe,
    join_keys={"customer_id": "customer_id"},
    adapter=adapter,
    variables=[
        dqf.Variable(name="age", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="income", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="segment", dtype=dqf.DataType.CATEGORICAL),
        dqf.Variable(name="is_premium", dtype=dqf.DataType.BOOLEAN),
    ],
)

# ──────────────────────────────────────────────────────────────────────────────
# 3.  Run validation with the default resolver (no time dimension)
# ──────────────────────────────────────────────────────────────────────────────

resolver = dqf.build_default_resolver(
    null_threshold=0.10,  # fail features with > 10% nulls
    max_categorical_cardinality=20,
)

report = dataset.run_validation(resolver, dataset_name="customer_features")

# ──────────────────────────────────────────────────────────────────────────────
# 4.  Inspect the results
# ──────────────────────────────────────────────────────────────────────────────

print(f"Overall status : {report.overall_status.value}")
print(f"Universe size  : {report.universe_size}")
print()

df = report.to_dataframe()
print(df.to_string(index=False))
print()

if report.failed_variables():
    print("Failed variables :", report.failed_variables())
else:
    print("All variables passed.")
