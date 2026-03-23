"""Default check suite configuration — batteries-included pipelines and resolver."""

from dqf.defaults.suites import (
    boolean_pipeline,
    build_default_resolver,
    catch_all_pipeline,
    categorical_pipeline,
    identifier_pipeline,
    numeric_continuous_pipeline,
    numeric_continuous_pipeline_no_time,
    numeric_discrete_pipeline,
    target_pipeline,
    target_pipeline_no_time,
)

__all__ = [
    "build_default_resolver",
    "identifier_pipeline",
    "target_pipeline",
    "target_pipeline_no_time",
    "numeric_continuous_pipeline",
    "numeric_continuous_pipeline_no_time",
    "numeric_discrete_pipeline",
    "categorical_pipeline",
    "boolean_pipeline",
    "catch_all_pipeline",
]
