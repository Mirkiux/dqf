"""Example 2 — Longitudinal validation with a time dimension.

Demonstrates trend detection and structural break checks for a
NUMERIC_CONTINUOUS feature column that is aggregated by month.

MockAdapter serves both the raw variables query and the pre-aggregated
time series that the longitudinal checks will request.
"""

from __future__ import annotations

import pandas as pd

import dqf
from dqf.checks.longitudinal.trend import TrendCheck

# ──────────────────────────────────────────────────────────────────────────────
# 1.  SQL strings
# ──────────────────────────────────────────────────────────────────────────────

UNIVERSE_SQL = "SELECT order_id FROM orders"
VARIABLES_SQL = "SELECT order_id, order_date, basket_size FROM order_features"

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Pre-compute the aggregation SQL that TrendCheck / StructuralBreakCheck
#     will issue at runtime so MockAdapter can serve the right DataFrame.
#
#     Both checks share the same SQL template — only one entry is needed.
# ──────────────────────────────────────────────────────────────────────────────

_trend_check = TrendCheck(time_field="order_date", period="month")
TREND_SQL = _trend_check.aggregation_sql("basket_size", "order_date", "month").format(
    source=VARIABLES_SQL
)

# ──────────────────────────────────────────────────────────────────────────────
# 3.  Build DataFrames
# ──────────────────────────────────────────────────────────────────────────────

universe_df = pd.DataFrame({"order_id": range(1, 1201)})

variables_df = pd.DataFrame(
    {
        "order_id": range(1, 1201),
        "order_date": "placeholder",  # SQL engine truncates this; MockAdapter ignores it
        "basket_size": [42.0 + (i % 5) * 0.5 for i in range(1200)],
    }
)

# Stable basket_size series — no trend, no structural break
monthly_agg_df = pd.DataFrame(
    {
        "period": [f"2024-{m:02d}-01" for m in range(1, 13)],
        "metric": [42.5, 42.7, 42.3, 42.6, 42.4, 42.5, 42.6, 42.3, 42.5, 42.4, 42.6, 42.5],
        "n": [100] * 12,
    }
)

# ──────────────────────────────────────────────────────────────────────────────
# 4.  Wire up adapter and datasets
# ──────────────────────────────────────────────────────────────────────────────

adapter = dqf.MockAdapter(
    {
        UNIVERSE_SQL: universe_df,
        VARIABLES_SQL: variables_df,
        TREND_SQL: monthly_agg_df,  # served to both TrendCheck and StructuralBreakCheck
    }
)

universe = dqf.UniverseDataset(
    sql=UNIVERSE_SQL,
    primary_key=["order_id"],
    adapter=adapter,
)

dataset = dqf.VariablesDataset(
    sql=VARIABLES_SQL,
    primary_key=["order_id"],
    universe=universe,
    join_keys={"order_id": "order_id"},
    adapter=adapter,
    variables=[
        dqf.Variable(name="basket_size", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
    ],
)

# ──────────────────────────────────────────────────────────────────────────────
# 5.  Run validation with time_field enabled
# ──────────────────────────────────────────────────────────────────────────────

resolver = dqf.build_default_resolver(
    time_field="order_date",
    period="month",
    null_threshold=0.05,
)

report = dataset.run_validation(resolver, dataset_name="order_features_monthly")

# ──────────────────────────────────────────────────────────────────────────────
# 6.  Inspect results
# ──────────────────────────────────────────────────────────────────────────────

print(f"Overall status : {report.overall_status.value}")
print()

for check_result in report.variable_results.get("basket_size", []):
    status = "PASS" if check_result.passed else "FAIL"
    print(f"  [{status}] {check_result.check_name:<20} observed={check_result.observed_value}")
