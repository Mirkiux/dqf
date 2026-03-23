"""Default check suite configuration — batteries-included resolver and pipeline factories.

The :func:`build_default_resolver` function returns a pre-configured
:class:`~dqf.resolver.CheckSuiteResolver` with sensible rules for common
variable types.  It is the recommended starting point for new projects.

Each pipeline factory is also exported individually so users can mix and
match default pipelines with custom ones.

Default priority order
----------------------
30  IDENTIFIER role                        — not-null check (no nulls allowed)
25  TARGET role + BOOLEAN dtype            — not-null + ProportionDriftCheck (binary Z-test)
24  TARGET role + CATEGORICAL/DISCRETE     — not-null + ChiSquaredDriftCheck
23  TARGET role + NUMERIC_CONTINUOUS       — not-null + KSDriftCheck
20  TARGET role (catch-all)                — not-null only (unknown dtype)
15  NUMERIC_CONTINUOUS  — null rate (FAILURE) + trend + structural break (FAILURE)
10  NUMERIC_DISCRETE   — null rate (FAILURE) + cardinality (WARNING) + outlier (FAILURE)
 7  CATEGORICAL                            — null rate (FAILURE) + cardinality (WARNING)
 5  BOOLEAN                                — null rate (FAILURE)
 0  catch-all                              — null rate (WARNING)

When *time_field* is ``None``, longitudinal checks are omitted:
  - TARGET binary/categorical/continuous pipelines fall back to not-null only
    (continuous also adds OutlierCheck)
  - NUMERIC_CONTINUOUS falls back to null rate only
"""

from __future__ import annotations

from dqf.checks.cross_sectional.cardinality_check import CardinalityCheck
from dqf.checks.cross_sectional.not_null import NotNullCheck
from dqf.checks.cross_sectional.null_rate import NullRateCheck
from dqf.checks.cross_sectional.outlier import OutlierCheck
from dqf.checks.longitudinal.chisquared_drift import ChiSquaredDriftCheck
from dqf.checks.longitudinal.ks_drift import KSDriftCheck
from dqf.checks.longitudinal.proportion_drift import ProportionDriftCheck
from dqf.checks.longitudinal.structural_break import StructuralBreakCheck
from dqf.checks.longitudinal.trend import TrendCheck
from dqf.checks.pipeline import CheckPipeline
from dqf.enums import DataType, Severity, VariableRole
from dqf.resolver import CheckSuiteResolver

# ──────────────────────────────────────────────────────────────────────────────
# IDENTIFIER pipeline
# ──────────────────────────────────────────────────────────────────────────────


def identifier_pipeline() -> CheckPipeline:
    """Pipeline for ``VariableRole.IDENTIFIER`` variables.

    Checks that the column contains no null values (FAILURE).  Composite
    primary-key uniqueness is already enforced at the dataset level by
    :meth:`~dqf.datasets.variables.VariablesDataset.validate_pk_uniqueness`;
    a per-column uniqueness check here would be incorrect for composite keys.
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
        ]
    )


# ──────────────────────────────────────────────────────────────────────────────
# TARGET pipelines — one per semantic type
# ──────────────────────────────────────────────────────────────────────────────


def target_binary_pipeline(time_field: str, period: str = "month") -> CheckPipeline:
    """Pipeline for binary ``TARGET`` variables *with* a time dimension.

    Steps:

    1. **not_null** (FAILURE) — target must never be null.
    2. **proportion_drift** (FAILURE) — sequential Z-test detects shifts in the
       positive-class proportion across periods.
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
            (
                "proportion_drift",
                ProportionDriftCheck(
                    time_field=time_field, period=period, severity=Severity.FAILURE
                ),
            ),
        ]
    )


def target_binary_pipeline_no_time() -> CheckPipeline:
    """Pipeline for binary ``TARGET`` variables *without* a time dimension.

    Checks not-null only (FAILURE).
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
        ]
    )


def target_categorical_pipeline(time_field: str, period: str = "month") -> CheckPipeline:
    """Pipeline for categorical or numeric-discrete ``TARGET`` variables *with* a time dimension.

    Steps:

    1. **not_null** (FAILURE) — target must never be null.
    2. **chisquared_drift** (FAILURE) — sequential chi-squared test detects
       shifts in the category distribution across periods.
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
            (
                "chisquared_drift",
                ChiSquaredDriftCheck(
                    time_field=time_field, period=period, severity=Severity.FAILURE
                ),
            ),
        ]
    )


def target_categorical_pipeline_no_time() -> CheckPipeline:
    """Pipeline for categorical or numeric-discrete ``TARGET`` variables *without* a time dimension.

    Checks not-null only (FAILURE).
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
        ]
    )


def target_continuous_pipeline(time_field: str, period: str = "month") -> CheckPipeline:
    """Pipeline for continuous ``TARGET`` variables *with* a time dimension.

    Steps:

    1. **not_null** (FAILURE) — target must never be null.
    2. **ks_drift** (FAILURE) — sequential KS test detects distributional shifts
       across periods.
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
            (
                "ks_drift",
                KSDriftCheck(time_field=time_field, period=period, severity=Severity.FAILURE),
            ),
        ]
    )


def target_continuous_pipeline_no_time() -> CheckPipeline:
    """Pipeline for continuous ``TARGET`` variables *without* a time dimension.

    Steps:

    1. **not_null** (FAILURE) — target must never be null.
    2. **outlier** (FAILURE) — univariate outlier detection via Tukey's IQR method.
    """
    return CheckPipeline(
        [
            ("not_null", NotNullCheck(severity=Severity.FAILURE)),
            ("outlier", OutlierCheck(severity=Severity.FAILURE)),
        ]
    )


# ──────────────────────────────────────────────────────────────────────────────
# Feature pipelines
# ──────────────────────────────────────────────────────────────────────────────


def numeric_continuous_pipeline(
    time_field: str,
    period: str = "month",
    null_threshold: float = 0.10,
) -> CheckPipeline:
    """Pipeline for ``DataType.NUMERIC_CONTINUOUS`` feature variables *with* a time dimension.

    Steps:

    1. **null_rate** (FAILURE) — fail when more than *null_threshold* of values are null.
    2. **trend** (FAILURE) — significant monotonic trend signals metric drift.
    3. **structural_break** (FAILURE) — CUSUM spike signals an abrupt level shift.

    Parameters
    ----------
    time_field:
        Name of the datetime column used for aggregation.
    period:
        DATE_TRUNC period (e.g. ``"month"``).
    null_threshold:
        Maximum allowed null rate.  Default ``0.10``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
            ("trend", TrendCheck(time_field=time_field, period=period, severity=Severity.FAILURE)),
            (
                "structural_break",
                StructuralBreakCheck(
                    time_field=time_field, period=period, severity=Severity.FAILURE
                ),
            ),
        ]
    )


def numeric_continuous_pipeline_no_time(null_threshold: float = 0.10) -> CheckPipeline:
    """Pipeline for ``DataType.NUMERIC_CONTINUOUS`` feature variables *without* a time dimension.

    Checks null rate (FAILURE).

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.10``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
        ]
    )


def numeric_discrete_pipeline(
    null_threshold: float = 0.10,
    max_cardinality: int = 50,
) -> CheckPipeline:
    """Pipeline for ``DataType.NUMERIC_DISCRETE`` feature variables.

    Steps:

    1. **null_rate** (FAILURE) — fail when null rate exceeds *null_threshold*.
    2. **cardinality** (WARNING) — warn when distinct values exceed *max_cardinality*,
       which may indicate the column has been mis-classified as discrete.
    3. **outlier** (FAILURE) — univariate outlier detection via Tukey's IQR method.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.10``.
    max_cardinality:
        Maximum expected number of distinct values.  Default ``50``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
            (
                "cardinality",
                CardinalityCheck(max_cardinality=max_cardinality, severity=Severity.WARNING),
            ),
            ("outlier", OutlierCheck(severity=Severity.FAILURE)),
        ]
    )


def categorical_pipeline(
    null_threshold: float = 0.10,
    max_cardinality: int = 50,
) -> CheckPipeline:
    """Pipeline for ``DataType.CATEGORICAL`` feature variables.

    Steps:

    1. **null_rate** (FAILURE) — fail when null rate exceeds *null_threshold*.
    2. **cardinality** (WARNING) — warn when distinct values exceed *max_cardinality*,
       which may signal unexpected category explosion or encoding issues.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.10``.
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


def boolean_pipeline(null_threshold: float = 0.10) -> CheckPipeline:
    """Pipeline for ``DataType.BOOLEAN`` feature variables.

    Checks null rate (FAILURE).

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.10``.
    """
    return CheckPipeline(
        [
            ("null_rate", NullRateCheck(threshold=null_threshold, severity=Severity.FAILURE)),
        ]
    )


def catch_all_pipeline(null_threshold: float = 0.10) -> CheckPipeline:
    """Fallback pipeline for variables that match no other rule.

    Checks null rate as a WARNING (not FAILURE) — the variable type is unknown
    so the threshold is applied gently.

    Parameters
    ----------
    null_threshold:
        Maximum allowed null rate.  Default ``0.10``.
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
    null_threshold: float = 0.10,
    max_discrete_cardinality: int = 50,
    max_categorical_cardinality: int = 50,
) -> CheckSuiteResolver:
    """Return a pre-configured :class:`~dqf.resolver.CheckSuiteResolver`.

    This is the recommended starting point for new projects.  All rules have
    sensible defaults; pass keyword arguments to tighten or relax thresholds.

    When *time_field* is ``None`` (the default), longitudinal checks are omitted
    from all pipelines.  Pass the name of a datetime column to enable them.

    TARGET variables are dispatched to type-specific pipelines:

    - **BOOLEAN** → :func:`target_binary_pipeline` (proportion Z-test)
    - **CATEGORICAL / NUMERIC_DISCRETE** → :func:`target_categorical_pipeline` (chi-squared)
    - **NUMERIC_CONTINUOUS** → :func:`target_continuous_pipeline` (KS test)
    - Other dtypes → not-null only (no longitudinal)

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.  Required for
        longitudinal checks.  ``None`` disables them.
    period:
        DATE_TRUNC period string (e.g. ``"month"``, ``"week"``).
    null_threshold:
        Maximum null rate applied to NUMERIC_CONTINUOUS, NUMERIC_DISCRETE,
        CATEGORICAL, BOOLEAN, and the catch-all rule.  Default ``0.10``.
    max_discrete_cardinality:
        Cardinality warning threshold for NUMERIC_DISCRETE variables.  Default ``50``.
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
            null_threshold=0.05,
        )
    """
    resolver = CheckSuiteResolver()

    # Priority 30 — IDENTIFIER role
    resolver.register(
        predicate=lambda v: v.role == VariableRole.IDENTIFIER,
        pipeline_factory=identifier_pipeline,
        priority=30,
    )

    # Priority 25-20 — TARGET role, dispatched by dtype
    if time_field is not None:
        _tf = time_field
        _p = period

        resolver.register(
            predicate=lambda v: v.role == VariableRole.TARGET and v.dtype == DataType.BOOLEAN,
            pipeline_factory=lambda: target_binary_pipeline(_tf, _p),
            priority=25,
        )
        resolver.register(
            predicate=lambda v: (
                v.role == VariableRole.TARGET
                and v.dtype in (DataType.CATEGORICAL, DataType.NUMERIC_DISCRETE)
            ),
            pipeline_factory=lambda: target_categorical_pipeline(_tf, _p),
            priority=24,
        )
        resolver.register(
            predicate=lambda v: (
                v.role == VariableRole.TARGET and v.dtype == DataType.NUMERIC_CONTINUOUS
            ),
            pipeline_factory=lambda: target_continuous_pipeline(_tf, _p),
            priority=23,
        )
    else:
        resolver.register(
            predicate=lambda v: v.role == VariableRole.TARGET and v.dtype == DataType.BOOLEAN,
            pipeline_factory=target_binary_pipeline_no_time,
            priority=25,
        )
        resolver.register(
            predicate=lambda v: (
                v.role == VariableRole.TARGET
                and v.dtype in (DataType.CATEGORICAL, DataType.NUMERIC_DISCRETE)
            ),
            pipeline_factory=target_categorical_pipeline_no_time,
            priority=24,
        )
        resolver.register(
            predicate=lambda v: (
                v.role == VariableRole.TARGET and v.dtype == DataType.NUMERIC_CONTINUOUS
            ),
            pipeline_factory=target_continuous_pipeline_no_time,
            priority=23,
        )

    # Catch-all for TARGET variables with unrecognised dtype
    resolver.register(
        predicate=lambda v: v.role == VariableRole.TARGET,
        pipeline_factory=lambda: CheckPipeline([("not_null", NotNullCheck())]),
        priority=20,
    )

    # Priority 15 — NUMERIC_CONTINUOUS feature
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

    # Priority 10 — NUMERIC_DISCRETE feature
    _nt3 = null_threshold
    _mdc = max_discrete_cardinality
    resolver.register(
        predicate=lambda v: v.dtype == DataType.NUMERIC_DISCRETE,
        pipeline_factory=lambda: numeric_discrete_pipeline(_nt3, _mdc),
        priority=10,
    )

    # Priority 7 — CATEGORICAL feature
    _nt4 = null_threshold
    _mcc = max_categorical_cardinality
    resolver.register(
        predicate=lambda v: v.dtype == DataType.CATEGORICAL,
        pipeline_factory=lambda: categorical_pipeline(_nt4, _mcc),
        priority=7,
    )

    # Priority 5 — BOOLEAN feature
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
