"""Default metadata pipeline configuration — batteries-included profiling pipelines.

The :func:`build_default_metadata_pipeline` function selects the right
:class:`~dqf.metadata.base.MetadataBuilderPipeline` for a given variable
based on its role and dtype.  It is the metadata counterpart to
:func:`~dqf.defaults.suites.build_default_resolver`.

Each pipeline factory is also exported individually so users can mix and
match default pipelines with custom ones.

Default dispatch order (mirrors the check-suite priority order)
---------------------------------------------------------------
1.  IDENTIFIER role      — storage_dtype + nullability
2.  TARGET role          — storage_dtype + nullability + semantic_type
3.  NUMERIC_CONTINUOUS   — storage_dtype + nullability + distribution
4.  NUMERIC_DISCRETE     — storage_dtype + nullability + distribution + cardinality
5.  CATEGORICAL          — storage_dtype + nullability + cardinality
6.  BOOLEAN              — storage_dtype + nullability
7.  catch-all            — storage_dtype + nullability + semantic_type

Rationale per dtype
-------------------
- **IDENTIFIER** — presence of nulls is all that matters; distribution and
  cardinality add noise (identifiers are always high-cardinality by design).
- **TARGET** — semantic type inference helps the resolver pick the right drift
  check when the declared dtype is ``FEATURE`` / unknown.
- **NUMERIC_CONTINUOUS** — distributional shape (mean, std, skewness, kurtosis)
  is the primary signal; cardinality is meaningless for a continuous variable.
- **NUMERIC_DISCRETE** — distribution shape *and* cardinality: a spike in
  distinct values is a common sign of dtype mis-classification.
- **CATEGORICAL** — cardinality is the main concern; descriptive stats are
  inapplicable to string columns.
- **BOOLEAN** — storage dtype and null rate only; cardinality is always ≤ 2.
- **catch-all** — semantic inference helps downstream consumers decide what to
  do with an unrecognised variable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dqf.enums import DataType, VariableRole
from dqf.metadata.base import MetadataBuilderPipeline
from dqf.metadata.builders.cardinality_builder import CardinalityBuilder
from dqf.metadata.builders.distribution_builder import DistributionShapeBuilder
from dqf.metadata.builders.dtype_builder import StorageDtypeBuilder
from dqf.metadata.builders.nullability_builder import NullabilityProfileBuilder
from dqf.metadata.builders.semantic_builder import SemanticTypeInferenceBuilder

if TYPE_CHECKING:
    from dqf.variable import Variable

# ──────────────────────────────────────────────────────────────────────────────
# IDENTIFIER pipeline
# ──────────────────────────────────────────────────────────────────────────────


def identifier_metadata_pipeline() -> MetadataBuilderPipeline:
    """Metadata pipeline for ``VariableRole.IDENTIFIER`` variables.

    Profiles storage dtype and nullability only.  Cardinality and distribution
    are intentionally omitted — identifiers are always high-cardinality by
    design, and descriptive stats are not meaningful for key columns.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
        ]
    )


# ──────────────────────────────────────────────────────────────────────────────
# TARGET pipeline
# ──────────────────────────────────────────────────────────────────────────────


def target_metadata_pipeline(low_cardinality_threshold: int = 20) -> MetadataBuilderPipeline:
    """Metadata pipeline for ``VariableRole.TARGET`` variables.

    Profiles storage dtype, nullability, and infers the semantic type.
    Semantic inference is included so that the check resolver can confirm
    the declared dtype at runtime and pick the correct drift check.

    Parameters
    ----------
    low_cardinality_threshold:
        Number of distinct non-null values below which an object series is
        inferred as ``CATEGORICAL`` by :class:`~dqf.metadata.builders.SemanticTypeInferenceBuilder`.
        Default ``20``.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
            ("semantic_type", SemanticTypeInferenceBuilder(low_cardinality_threshold)),
        ]
    )


# ──────────────────────────────────────────────────────────────────────────────
# Feature pipelines — one per dtype
# ──────────────────────────────────────────────────────────────────────────────


def numeric_continuous_metadata_pipeline() -> MetadataBuilderPipeline:
    """Metadata pipeline for ``DataType.NUMERIC_CONTINUOUS`` feature variables.

    Profiles storage dtype, nullability, and full distributional shape
    (mean, std, min, max, skewness, kurtosis).  Cardinality is omitted
    because it is not meaningful for continuous variables.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
            ("distribution", DistributionShapeBuilder()),
        ]
    )


def numeric_discrete_metadata_pipeline(
    high_cardinality_threshold: int = 50,
) -> MetadataBuilderPipeline:
    """Metadata pipeline for ``DataType.NUMERIC_DISCRETE`` feature variables.

    Profiles storage dtype, nullability, distributional shape, and cardinality.
    A high cardinality count on a declared-discrete variable is a common sign
    of dtype mis-classification that should surface early.

    Parameters
    ----------
    high_cardinality_threshold:
        Number of distinct non-null values above which
        :class:`~dqf.metadata.builders.CardinalityBuilder` sets
        ``is_high_cardinality = True``.  Default ``50``.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
            ("distribution", DistributionShapeBuilder()),
            ("cardinality", CardinalityBuilder(high_cardinality_threshold)),
        ]
    )


def categorical_metadata_pipeline(
    high_cardinality_threshold: int = 50,
) -> MetadataBuilderPipeline:
    """Metadata pipeline for ``DataType.CATEGORICAL`` feature variables.

    Profiles storage dtype, nullability, and cardinality.  Distributional
    shape is omitted — descriptive statistics are not applicable to string
    or low-cardinality integer columns.

    Parameters
    ----------
    high_cardinality_threshold:
        Cardinality threshold passed to
        :class:`~dqf.metadata.builders.CardinalityBuilder`.  Default ``50``.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
            ("cardinality", CardinalityBuilder(high_cardinality_threshold)),
        ]
    )


def boolean_metadata_pipeline() -> MetadataBuilderPipeline:
    """Metadata pipeline for ``DataType.BOOLEAN`` feature variables.

    Profiles storage dtype and nullability only.  Cardinality is always ≤ 2
    for boolean columns, so it adds no information.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
        ]
    )


def catch_all_metadata_pipeline(low_cardinality_threshold: int = 20) -> MetadataBuilderPipeline:
    """Fallback metadata pipeline for variables that match no other rule.

    Profiles storage dtype, nullability, and infers the semantic type so that
    downstream consumers have enough information to decide how to handle the
    variable.

    Parameters
    ----------
    low_cardinality_threshold:
        Passed to :class:`~dqf.metadata.builders.SemanticTypeInferenceBuilder`.
        Default ``20``.
    """
    return MetadataBuilderPipeline(
        [
            ("storage_dtype", StorageDtypeBuilder()),
            ("nullability", NullabilityProfileBuilder()),
            ("semantic_type", SemanticTypeInferenceBuilder(low_cardinality_threshold)),
        ]
    )


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline factory
# ──────────────────────────────────────────────────────────────────────────────


def build_default_metadata_pipeline(
    variable: Variable,
    high_cardinality_threshold: int = 50,
    low_cardinality_threshold: int = 20,
) -> MetadataBuilderPipeline:
    """Return the appropriate default metadata pipeline for *variable*.

    Dispatch follows the same role → dtype priority order used by
    :func:`~dqf.defaults.suites.build_default_resolver`:

    1. ``IDENTIFIER`` role → :func:`identifier_metadata_pipeline`
    2. ``TARGET`` role → :func:`target_metadata_pipeline`
    3. ``NUMERIC_CONTINUOUS`` → :func:`numeric_continuous_metadata_pipeline`
    4. ``NUMERIC_DISCRETE`` → :func:`numeric_discrete_metadata_pipeline`
    5. ``CATEGORICAL`` → :func:`categorical_metadata_pipeline`
    6. ``BOOLEAN`` → :func:`boolean_metadata_pipeline`
    7. catch-all → :func:`catch_all_metadata_pipeline`

    Parameters
    ----------
    variable:
        The :class:`~dqf.variable.Variable` to profile.
    high_cardinality_threshold:
        Passed to pipelines that include a
        :class:`~dqf.metadata.builders.CardinalityBuilder`
        (``NUMERIC_DISCRETE``, ``CATEGORICAL``).  Default ``50``.
    low_cardinality_threshold:
        Passed to pipelines that include a
        :class:`~dqf.metadata.builders.SemanticTypeInferenceBuilder`
        (``TARGET``, catch-all).  Default ``20``.

    Returns
    -------
    MetadataBuilderPipeline
        A pipeline ready to call ``.profile(dataset, variable)``.

    Examples
    --------
    ::

        pipeline = build_default_metadata_pipeline(variable)
        pipeline.profile(dataset, variable)
        print(variable.metadata)
    """
    if variable.role == VariableRole.IDENTIFIER:
        return identifier_metadata_pipeline()

    if variable.role == VariableRole.TARGET:
        return target_metadata_pipeline(low_cardinality_threshold)

    if variable.dtype == DataType.NUMERIC_CONTINUOUS:
        return numeric_continuous_metadata_pipeline()

    if variable.dtype == DataType.NUMERIC_DISCRETE:
        return numeric_discrete_metadata_pipeline(high_cardinality_threshold)

    if variable.dtype == DataType.CATEGORICAL:
        return categorical_metadata_pipeline(high_cardinality_threshold)

    if variable.dtype == DataType.BOOLEAN:
        return boolean_metadata_pipeline()

    return catch_all_metadata_pipeline(low_cardinality_threshold)
