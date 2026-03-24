"""Default check suite and metadata pipeline configuration — batteries-included."""

from dqf.defaults.metadata import (
    boolean_metadata_pipeline,
    build_default_metadata_pipeline,
    catch_all_metadata_pipeline,
    categorical_metadata_pipeline,
    identifier_metadata_pipeline,
    numeric_continuous_metadata_pipeline,
    numeric_discrete_metadata_pipeline,
    target_metadata_pipeline,
)
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
    # check suite
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
    # metadata pipelines
    "build_default_metadata_pipeline",
    "identifier_metadata_pipeline",
    "target_metadata_pipeline",
    "numeric_continuous_metadata_pipeline",
    "numeric_discrete_metadata_pipeline",
    "categorical_metadata_pipeline",
    "boolean_metadata_pipeline",
    "catch_all_metadata_pipeline",
]
