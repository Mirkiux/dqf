"""Example 4 — Target variable drift detection.

Demonstrates how dqf monitors a binary classification target variable
over time using the sequential two-proportion Z-test (ProportionDriftCheck).

The example simulates a stable period followed by a sudden positive-rate
shift — the check should flag drift in the later periods.
"""

from __future__ import annotations

import pandas as pd

import dqf
from dqf.checks.longitudinal.proportion_drift import ProportionDriftCheck

# ──────────────────────────────────────────────────────────────────────────────
# 1.  SQL strings
# ──────────────────────────────────────────────────────────────────────────────

UNIVERSE_SQL = "SELECT transaction_id FROM transactions"
VARIABLES_SQL = "SELECT transaction_id, event_date, is_fraud FROM transaction_labels"

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Pre-compute the aggregation SQL for ProportionDriftCheck
# ──────────────────────────────────────────────────────────────────────────────

_prop_check = ProportionDriftCheck(time_field="event_date", period="month")
PROP_SQL = _prop_check.aggregation_sql("is_fraud", "event_date", "month").format(
    source=VARIABLES_SQL
)

# ──────────────────────────────────────────────────────────────────────────────
# 3.  Simulate a fraud-rate shift starting month 7
#
#     Months 1–6: ~5% fraud rate (stable baseline)
#     Months 7–12: ~20% fraud rate (sudden shift — concept drift)
# ──────────────────────────────────────────────────────────────────────────────

universe_df = pd.DataFrame({"transaction_id": range(1, 1201)})

variables_df = pd.DataFrame(
    {
        "transaction_id": range(1, 1201),
        "event_date": "placeholder",
        "is_fraud": [0] * 1200,  # raw rows; aggregation SQL computes fraud rate
    }
)

# Pre-aggregated monthly fraud stats
monthly_agg_df = pd.DataFrame(
    {
        "period": [f"2024-{m:02d}-01" for m in range(1, 13)],
        # Months 1–6: 5 fraud out of 100; Months 7–12: 20 fraud out of 100
        "positive": [5, 5, 4, 6, 5, 5, 20, 19, 21, 20, 18, 22],
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
        PROP_SQL: monthly_agg_df,
    }
)

universe = dqf.UniverseDataset(
    sql=UNIVERSE_SQL,
    primary_key=["transaction_id"],
    adapter=adapter,
)

dataset = dqf.VariablesDataset(
    sql=VARIABLES_SQL,
    primary_key=["transaction_id"],
    universe=universe,
    join_keys={"transaction_id": "transaction_id"},
    adapter=adapter,
    variables=[
        dqf.Variable(
            name="is_fraud",
            dtype=dqf.DataType.BOOLEAN,
            role=dqf.VariableRole.TARGET,
        ),
    ],
)

# ──────────────────────────────────────────────────────────────────────────────
# 5.  Run validation with time_field enabled
# ──────────────────────────────────────────────────────────────────────────────

resolver = dqf.build_default_resolver(time_field="event_date", period="month")

report = dataset.run_validation(resolver, dataset_name="fraud_labels")

# ──────────────────────────────────────────────────────────────────────────────
# 6.  Inspect results
# ──────────────────────────────────────────────────────────────────────────────

print(f"Overall status : {report.overall_status.value}")
print()

for check_result in report.variable_results.get("is_fraud", []):
    status = "PASS" if check_result.passed else "FAIL"
    print(f"  [{status}] {check_result.check_name:<20} observed={check_result.observed_value}")
    if check_result.check_name == "proportion_drift":
        meta = check_result.metadata
        print(f"         min p-value   : {meta.get('min_p_value')}")
        print(f"         n_periods     : {meta.get('n_periods')}")
        print(f"         baseline_periods: {meta.get('baseline_periods')}")
