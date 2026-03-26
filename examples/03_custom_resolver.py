"""Example 3 — Custom check and metadata resolver rules on top of the defaults.

Demonstrates how to extend both build_default_resolver and
build_default_metadata_resolver with project-specific rules.  Custom rules
registered at a higher priority than the built-in ones take precedence for
matching variables.
"""

from __future__ import annotations

import pandas as pd

import dqf
from dqf.checks.cross_sectional.range_check import RangeCheck
from dqf.checks.pipeline import CheckPipeline
from dqf.metadata.base import MetadataBuilderPipeline
from dqf.metadata.builders.distribution_builder import DistributionShapeBuilder
from dqf.metadata.builders.nullability_builder import NullabilityProfileBuilder

# ──────────────────────────────────────────────────────────────────────────────
# 1.  SQL strings and DataFrames
# ──────────────────────────────────────────────────────────────────────────────

UNIVERSE_SQL = "SELECT loan_id FROM loans"
VARIABLES_SQL = "SELECT loan_id, credit_score, ltv_ratio, property_type FROM loan_features"

universe_df = pd.DataFrame({"loan_id": range(1, 51)})
variables_df = pd.DataFrame(
    {
        "loan_id": range(1, 51),
        "credit_score": [650 + i % 150 for i in range(50)],  # 650–799
        "ltv_ratio": [0.60 + (i % 30) * 0.01 for i in range(50)],  # 0.60–0.89
        "property_type": ["SFR" if i % 2 == 0 else "CONDO" for i in range(50)],
    }
)

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Build the default metadata resolver and add domain-specific profiling
#
#  The default resolver applies dtype-appropriate builders.  Here we override
#  credit_score with a richer pipeline (nullability + full distribution shape)
#  at priority 50, which beats the default NUMERIC_CONTINUOUS rule at 15.
# ──────────────────────────────────────────────────────────────────────────────

metadata_resolver = dqf.build_default_metadata_resolver(
    cardinality=dqf.CardinalityThresholds(high=10)
)

metadata_resolver.register(
    predicate=lambda v: v.name == "credit_score",
    pipeline_factory=lambda: MetadataBuilderPipeline(
        [
            ("nullability", NullabilityProfileBuilder()),
            ("distribution", DistributionShapeBuilder()),
        ]
    ),
    priority=50,
)

# ──────────────────────────────────────────────────────────────────────────────
# 3.  Build the default check resolver and add domain-specific rules
# ──────────────────────────────────────────────────────────────────────────────

resolver = dqf.build_default_resolver(
    null_threshold=0.05, cardinality=dqf.CardinalityThresholds(high=10)
)

# credit_score must be in [300, 850] — override the generic NUMERIC_CONTINUOUS rule
resolver.register(
    predicate=lambda v: v.name == "credit_score",
    pipeline_factory=lambda: CheckPipeline(
        [("range", RangeCheck(min_value=300, max_value=850, severity=dqf.Severity.FAILURE))]
    ),
    priority=50,  # beats default priority 15 for NUMERIC_CONTINUOUS
)

# ltv_ratio must be in [0, 1] — another domain-specific rule
resolver.register(
    predicate=lambda v: v.name == "ltv_ratio",
    pipeline_factory=lambda: CheckPipeline(
        [("range", RangeCheck(min_value=0.0, max_value=1.0, severity=dqf.Severity.FAILURE))]
    ),
    priority=50,
)

# ──────────────────────────────────────────────────────────────────────────────
# 4.  Wire up adapter and datasets
# ──────────────────────────────────────────────────────────────────────────────

adapter = dqf.MockAdapter({UNIVERSE_SQL: universe_df, VARIABLES_SQL: variables_df})

universe = dqf.UniverseDataset(
    sql=UNIVERSE_SQL,
    primary_key=["loan_id"],
    adapter=adapter,
)

dataset = dqf.VariablesDataset(
    sql=VARIABLES_SQL,
    primary_key=["loan_id"],
    universe=universe,
    join_keys={"loan_id": "loan_id"},
    adapter=adapter,
    variables=[
        dqf.Variable(name="credit_score", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="ltv_ratio", dtype=dqf.DataType.NUMERIC_CONTINUOUS),
        dqf.Variable(name="property_type", dtype=dqf.DataType.CATEGORICAL),
    ],
)

# ──────────────────────────────────────────────────────────────────────────────
# 5.  Profile metadata then run validation
# ──────────────────────────────────────────────────────────────────────────────

dataset.resolve_variables(metadata_resolver)

print("-- Variable metadata -------------------------------------------------------")
for var in dataset.variables:
    print(f"  {var.name:<15} {var.metadata}")
print()

report = dataset.run_validation(resolver, dataset_name="loan_features")

# ──────────────────────────────────────────────────────────────────────────────
# 6.  Inspect results — custom rules appear as "range", default as "null_rate"
# ──────────────────────────────────────────────────────────────────────────────

print(f"Overall status : {report.overall_status.value}")
print()

for var_name, results in report.variable_results.items():
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {var_name:<15} {r.check_name:<12} observed={r.observed_value}")
