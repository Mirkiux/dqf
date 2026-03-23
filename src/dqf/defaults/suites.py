"""Default check suite configuration — batteries-included resolver and pipeline factories.

The :func:`build_default_resolver` function returns a pre-configured
:class:`~dqf.resolver.CheckSuiteResolver` with sensible rules for common
variable types.  It is the recommended starting point for new projects.

Each pipeline factory is also exported individually so users can mix and
match default pipelines with custom ones.

Default priority order
----------------------
30  IDENTIFIER role   — uniqueness only (identifiers must be unique and rarely null)
20  TARGET role        — null rate (FAILURE) + concept drift (WARNING, when time_field given)
15  NUMERIC_CONTINUOUS — null rate (FAILURE) + trend/structural-break (WARNING, when time_field)
10  NUMERIC_DISCRETE   — null rate (FAILURE) + high-cardinality guard (WARNING)
 7  CATEGORICAL        — null rate (FAILURE) + high-cardinality guard (WARNING)
 5  BOOLEAN            — null rate (FAILURE)
 0  catch-all          — null rate (WARNING)
"""

from __future__ import annotations

from dqf.checks.cross_sectional.cardinality_check import CardinalityCheck
from dqf.checks.cross_sectional.null_rate import NullRateCheck
from dqf.checks.cross_sectional.uniqueness import UniquenessCheck
from dqf.checks.longitudinal.concept_drift import ConceptDriftCheck
from dqf.checks.longitudinal.structural_break import StructuralBreakCheck
from dqf.checks.longitudinal.trend import TrendCheck
from dqf.checks.pipeline import CheckPipeline
from dqf.enums import DataType, Severity, VariableRole
from dqf.resolver import CheckSuiteResolver

# ──────────────────────────────────────────────────────────────────────────────
# Pipeline factories
# ──────────────────────────────────────────────────────────────────────────────


def identifier_pipeline() -> CheckPipeline:
    """Pipeline for ``VariableRole.IDENTIFIER`` variables.

    Checks uniqueness (FAILURE).  Null rate is intentionally omitted —
    identifier columns are expected to be non-null by construction and a
    null identifier typically signals a data loading error caught earlier
    in the pipeline.
    """
    return CheckPipeline(
        [
            ("uniqueness", UniquenessCheck(severity=Severity.FAILURE)),
        ]
    )


def target_pipeline(
    time_field: str,
    period: str = "month",
    null_threshold: float = 0.05,
) -> CheckPipeline:
    """Pipeline for ``VariableRole.TARGET`` variables *with* a time dimension.

    Steps:

    1. **null_rate** (FAILURE) — targets should rarely be null; default threshold 5%.
    2. **concept_drift** (WARNING) — distribution shift relative to the first-half
       reference signals potential model retraining need.

    Parameters
    ----------
    time_field:
        Name of the datetime column used to build the aggregation time series.
    period:
        DATE_TRUNC period (e.g. ``"month"``).
    null_threshold:
        Maximum allowed null rate for the target column.  Default ``0.05``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
            (
                "concept_drift",
                ConceptDriftCheck(time_field=time_field, period=period, severity=Severity.WARNING),
            ),
        ]
    )


def target_pipeline_no_time(null_threshold: float = 0.05) -> CheckPipeline:
    """Pipeline for ``VariableRole.TARGET`` variables *without* a time dimension.

    Checks null rate only (FAILURE).

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate for the target column.  Default ``0.05``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
        ]
    )


def numeric_continuous_pipeline(
    time_field: str,
    period: str = "month",
    null_threshold: float = 0.20,
) -> CheckPipeline:
    """Pipeline for ``DataType.NUMERIC_CONTINUOUS`` variables *with* a time dimension.

    Steps:

    1. **null_rate** (FAILURE) — fail when more than *null_threshold* of values are null.
    2. **trend** (WARNING) — significant monotonic trend signals drift over time.
    3. **structural_break** (WARNING) — large CUSUM peak signals an abrupt level shift.

    Parameters
    ----------
    time_field:
        Name of the datetime column used for aggregation.
    period:
        DATE_TRUNC period (e.g. ``"month"``).
    null_threshold:
        Maximum allowed null rate.  Default ``0.20``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
            ("trend", TrendCheck(time_field=time_field, period=period, severity=Severity.WARNING)),
            (
                "structural_break",
                StructuralBreakCheck(
                    time_field=time_field, period=period, severity=Severity.WARNING
                ),
            ),
        ]
    )


def numeric_continuous_pipeline_no_time(null_threshold: float = 0.20) -> CheckPipeline:
    """Pipeline for ``DataType.NUMERIC_CONTINUOUS`` variables *without* a time dimension.

    Checks null rate only (FAILURE).

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.20``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
        ]
    )


def numeric_discrete_pipeline(
    null_threshold: float = 0.20,
    max_cardinality: int = 100,
) -> CheckPipeline:
    """Pipeline for ``DataType.NUMERIC_DISCRETE`` variables.

    Steps:

    1. **null_rate** (FAILURE) — fail when null rate exceeds *null_threshold*.
    2. **cardinality** (WARNING) — warn when distinct values exceed *max_cardinality*,
       which may indicate the column has been mis-classified.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.20``.
    max_cardinality:
        Maximum expected number of distinct values.  Default ``100``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
            (
                "cardinality",
                CardinalityCheck(max_cardinality=max_cardinality, severity=Severity.WARNING),
            ),
        ]
    )


def categorical_pipeline(
    null_threshold: float = 0.20,
    max_cardinality: int = 50,
) -> CheckPipeline:
    """Pipeline for ``DataType.CATEGORICAL`` variables.

    Steps:

    1. **null_rate** (FAILURE) — fail when null rate exceeds *null_threshold*.
    2. **cardinality** (WARNING) — warn when distinct values exceed *max_cardinality*,
       which may signal unexpected category explosion or encoding issues.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.20``.
    max_cardinality:
        Maximum expected number of distinct category values.  Default ``50``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
            (
                "cardinality",
                CardinalityCheck(max_cardinality=max_cardinality, severity=Severity.WARNING),
            ),
        ]
    )


def boolean_pipeline(null_threshold: float = 0.20) -> CheckPipeline:
    """Pipeline for ``DataType.BOOLEAN`` variables.

    Checks null rate (FAILURE).  Cardinality is not checked — boolean columns
    are expected to have at most two distinct values by definition.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.20``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
        ]
    )


def catch_all_pipeline(null_threshold: float = 0.20) -> CheckPipeline:
    """Fallback pipeline for variables that match no other rule.

    Checks null rate as a WARNING (not FAILURE) — the variable type is unknown
    so the threshold is applied gently.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.20``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.WARNING)),
        ]
    )


# ──────────────────────────────────────────────────────────────────────────────
# Resolver factory
# ──────────────────────────────────────────────────────────────────────────────


def build_default_resolver(
    time_field: str | None = None,
    period: str = "month",
    null_threshold: float = 0.20,
    target_null_threshold: float = 0.05,
    max_discrete_cardinality: int = 100,
    max_categorical_cardinality: int = 50,
) -> CheckSuiteResolver:
    """Return a pre-configured :class:`~dqf.resolver.CheckSuiteResolver`.

    This is the recommended starting point for new projects.  All rules have
    sensible defaults; pass keyword arguments to tighten or relax thresholds.

    When *time_field* is ``None`` (the default), longitudinal checks
    (trend, structural break, concept drift) are omitted from all pipelines.
    Pass the name of a datetime column to enable them.

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.  Required for
        longitudinal checks.  ``None`` disables them.
    period:
        DATE_TRUNC period string (e.g. ``"month"``, ``"week"``).
    null_threshold:
        Maximum null rate applied to NUMERIC_CONTINUOUS, NUMERIC_DISCRETE,
        CATEGORICAL, BOOLEAN, and the catch-all rule.  Default ``0.20``.
    target_null_threshold:
        Maximum null rate for TARGET variables.  Default ``0.05`` (stricter).
    max_discrete_cardinality:
        Cardinality warning threshold for NUMERIC_DISCRETE variables.  Default ``100``.
    max_categorical_cardinality:
        Cardinality warning threshold for CATEGORICAL variables.  Default ``50``.

    Returns
    -------
    CheckSuiteResolver
        A resolver with rules registered in priority order.

    Examples
    --------
    Cross-sectional only (no time dimension)::

        resolver = build_default_resolver()

    With longitudinal checks enabled::

        resolver = build_default_resolver(time_field="event_date", period="month")

    Custom thresholds::

        resolver = build_default_resolver(
            time_field="event_date",
            null_threshold=0.10,
            target_null_threshold=0.01,
        )
    """
    resolver = CheckSuiteResolver()

    # Priority 30 — IDENTIFIER role
    resolver.register(
        predicate=lambda v: v.role == VariableRole.IDENTIFIER,
        pipeline_factory=identifier_pipeline,
        priority=30,
    )

    # Priority 20 — TARGET role
    if time_field is not None:
        _tf = time_field
        _p = period
        _tnt = target_null_threshold
        resolver.register(
            predicate=lambda v: v.role == VariableRole.TARGET,
            pipeline_factory=lambda: target_pipeline(_tf, _p, _tnt),
            priority=20,
        )
    else:
        _tnt2 = target_null_threshold
        resolver.register(
            predicate=lambda v: v.role == VariableRole.TARGET,
            pipeline_factory=lambda: target_pipeline_no_time(_tnt2),
            priority=20,
        )

    # Priority 15 — NUMERIC_CONTINUOUS
    if time_field is not None:
        _tf2 = time_field
        _p2 = period
        _nt = null_threshold
        resolver.register(
            predicate=lambda v: v.dtype == DataType.NUMERIC_CONTINUOUS,
            pipeline_factory=lambda: numeric_continuous_pipeline(_tf2, _p2, _nt),
            priority=15,
        )
    else:
        _nt2 = null_threshold
        resolver.register(
            predicate=lambda v: v.dtype == DataType.NUMERIC_CONTINUOUS,
            pipeline_factory=lambda: numeric_continuous_pipeline_no_time(_nt2),
            priority=15,
        )

    # Priority 10 — NUMERIC_DISCRETE
    _nt3 = null_threshold
    _mdc = max_discrete_cardinality
    resolver.register(
        predicate=lambda v: v.dtype == DataType.NUMERIC_DISCRETE,
        pipeline_factory=lambda: numeric_discrete_pipeline(_nt3, _mdc),
        priority=10,
    )

    # Priority 7 — CATEGORICAL
    _nt4 = null_threshold
    _mcc = max_categorical_cardinality
    resolver.register(
        predicate=lambda v: v.dtype == DataType.CATEGORICAL,
        pipeline_factory=lambda: categorical_pipeline(_nt4, _mcc),
        priority=7,
    )

    # Priority 5 — BOOLEAN
    _nt5 = null_threshold
    resolver.register(
        predicate=lambda v: v.dtype == DataType.BOOLEAN,
        pipeline_factory=lambda: boolean_pipeline(_nt5),
        priority=5,
    )

    # Priority 0 — catch-all (WARNING only)
    _nt6 = null_threshold
    resolver.register(
        predicate=lambda v: True,
        pipeline_factory=lambda: catch_all_pipeline(_nt6),
        priority=0,
    )

    return resolver
