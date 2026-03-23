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
    target_binary_pipeline,
    target_binary_pipeline_no_time,
    target_categorical_pipeline,
    target_categorical_pipeline_no_time,
    target_continuous_pipeline,
    target_continuous_pipeline_no_time,
)

__all__ = [
    "build_default_resolver",
    "identifier_pipeline",
    "target_binary_pipeline",
    "target_binary_pipeline_no_time",
    "target_categorical_pipeline",
    "target_categorical_pipeline_no_time",
    "target_continuous_pipeline",
    "target_continuous_pipeline_no_time",
    "numeric_continuous_pipeline",
    "numeric_continuous_pipeline_no_time",
    "numeric_discrete_pipeline",
    "categorical_pipeline",
    "boolean_pipeline",
    "catch_all_pipeline",
]
