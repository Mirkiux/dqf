"""Tests for build_default_resolver and the individual pipeline factories."""

from __future__ import annotations

import pytest

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
from dqf.config import CardinalityThresholds
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
from dqf.enums import DataType, Severity, VariableRole
from dqf.variable import Variable

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE):
    return Variable(name="col", dtype=dtype, role=role)


def step_checks(pipeline: CheckPipeline) -> list:
    """Return the check instances from pipeline._steps."""
    return [check for _, check in pipeline._steps]


def step_types(pipeline: CheckPipeline) -> list[type]:
    return [type(c) for c in step_checks(pipeline)]


# ──────────────────────────────────────────────────────────────────────────────
# IDENTIFIER pipeline
# ──────────────────────────────────────────────────────────────────────────────


class TestIdentifierPipeline:
    def test_has_not_null_check(self):
        assert step_types(identifier_pipeline()) == [NotNullCheck]

    def test_not_null_is_failure(self):
        assert step_checks(identifier_pipeline())[0].severity == Severity.FAILURE


# ──────────────────────────────────────────────────────────────────────────────
# TARGET pipelines
# ──────────────────────────────────────────────────────────────────────────────


class TestTargetBinaryPipeline:
    def test_has_not_null_and_proportion_drift(self):
        assert step_types(target_binary_pipeline("ts")) == [
            NotNullCheck,
            CardinalityCheck,
            ProportionDriftCheck,
        ]

    def test_proportion_drift_is_failure(self):
        checks = step_checks(target_binary_pipeline("ts"))
        assert checks[1].severity == Severity.FAILURE


class TestTargetBinaryPipelineNoTime:
    def test_has_not_null_only(self):
        assert step_types(target_binary_pipeline_no_time()) == [NotNullCheck, CardinalityCheck]


class TestTargetCategoricalPipeline:
    def test_has_not_null_and_chisquared(self):
        assert step_types(target_categorical_pipeline("ts")) == [
            NotNullCheck,
            CardinalityCheck,
            ChiSquaredDriftCheck,
        ]

    def test_chisquared_is_failure(self):
        checks = step_checks(target_categorical_pipeline("ts"))
        assert checks[1].severity == Severity.FAILURE


class TestTargetCategoricalPipelineNoTime:
    def test_has_not_null_only(self):
        assert step_types(target_categorical_pipeline_no_time()) == [NotNullCheck, CardinalityCheck]


class TestTargetContinuousPipeline:
    def test_has_not_null_and_ks_drift(self):
        assert step_types(target_continuous_pipeline("ts")) == [
            NotNullCheck,
            CardinalityCheck,
            KSDriftCheck,
        ]

    def test_ks_drift_is_failure(self):
        checks = step_checks(target_continuous_pipeline("ts"))
        assert checks[1].severity == Severity.FAILURE


class TestTargetContinuousPipelineNoTime:
    def test_has_not_null_and_outlier(self):
        assert step_types(target_continuous_pipeline_no_time()) == [
            NotNullCheck,
            CardinalityCheck,
            OutlierCheck,
        ]

    def test_outlier_is_failure(self):
        checks = step_checks(target_continuous_pipeline_no_time())
        assert checks[1].severity == Severity.FAILURE


# ──────────────────────────────────────────────────────────────────────────────
# Feature pipelines
# ──────────────────────────────────────────────────────────────────────────────


class TestNumericContinuousPipeline:
    def test_has_null_trend_break(self):
        types = step_types(numeric_continuous_pipeline("ts"))
        assert types == [NullRateCheck, TrendCheck, StructuralBreakCheck]

    def test_all_are_failure(self):
        for check in step_checks(numeric_continuous_pipeline("ts")):
            assert check.severity == Severity.FAILURE

    def test_default_null_threshold_010(self):
        checks = step_checks(numeric_continuous_pipeline("ts"))
        assert checks[0]._threshold == pytest.approx(0.10)


class TestNumericContinuousPipelineNoTime:
    def test_has_null_rate_only(self):
        assert step_types(numeric_continuous_pipeline_no_time()) == [NullRateCheck]

    def test_default_null_threshold_010(self):
        checks = step_checks(numeric_continuous_pipeline_no_time())
        assert checks[0]._threshold == pytest.approx(0.10)


class TestNumericDiscretePipeline:
    def test_has_null_outlier_without_time(self):
        assert step_types(numeric_discrete_pipeline()) == [NullRateCheck, OutlierCheck]

    def test_adds_chisquared_drift_with_time(self):
        assert step_types(numeric_discrete_pipeline(time_field="ts")) == [
            NullRateCheck,
            OutlierCheck,
            ChiSquaredDriftCheck,
        ]

    def test_chisquared_drift_is_failure(self):
        checks = step_checks(numeric_discrete_pipeline(time_field="ts"))
        assert checks[2].severity == Severity.FAILURE

    def test_outlier_is_failure(self):
        checks = step_checks(numeric_discrete_pipeline())
        assert checks[1].severity == Severity.FAILURE

    def test_default_null_threshold_010(self):
        checks = step_checks(numeric_discrete_pipeline())
        assert checks[0]._threshold == pytest.approx(0.10)


class TestCategoricalPipeline:
    def test_has_null_rate_and_cardinality(self):
        assert step_types(categorical_pipeline()) == [NullRateCheck, CardinalityCheck]

    def test_cardinality_is_warning(self):
        assert step_checks(categorical_pipeline())[1].severity == Severity.WARNING

    def test_default_max_cardinality_50(self):
        assert step_checks(categorical_pipeline())[1]._max == 50

    def test_default_null_threshold_010(self):
        checks = step_checks(categorical_pipeline())
        assert checks[0]._threshold == pytest.approx(0.10)


class TestBooleanPipeline:
    def test_has_null_rate_only(self):
        assert step_types(boolean_pipeline()) == [NullRateCheck, CardinalityCheck]

    def test_null_rate_is_failure(self):
        assert step_checks(boolean_pipeline())[0].severity == Severity.FAILURE

    def test_default_null_threshold_010(self):
        assert step_checks(boolean_pipeline())[0]._threshold == pytest.approx(0.10)


class TestCatchAllPipeline:
    def test_has_null_rate_only(self):
        assert step_types(catch_all_pipeline()) == [NullRateCheck]

    def test_null_rate_is_warning(self):
        assert step_checks(catch_all_pipeline())[0].severity == Severity.WARNING

    def test_default_null_threshold_010(self):
        assert step_checks(catch_all_pipeline())[0]._threshold == pytest.approx(0.10)


# ──────────────────────────────────────────────────────────────────────────────
# build_default_resolver — resolver dispatch
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildDefaultResolver:
    def test_returns_resolver(self):
        from dqf.resolver import CheckSuiteResolver

        assert isinstance(build_default_resolver(), CheckSuiteResolver)

    # IDENTIFIER
    def test_identifier_gets_not_null(self):
        v = make_variable(role=VariableRole.IDENTIFIER)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NotNullCheck]

    # TARGET + dtype dispatch
    def test_target_boolean_no_time_gets_not_null(self):
        v = make_variable(dtype=DataType.BOOLEAN, role=VariableRole.TARGET)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck]

    def test_target_boolean_with_time_gets_proportion_drift(self):
        v = make_variable(dtype=DataType.BOOLEAN, role=VariableRole.TARGET)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck, ProportionDriftCheck]

    def test_target_categorical_no_time_gets_not_null(self):
        v = make_variable(dtype=DataType.CATEGORICAL, role=VariableRole.TARGET)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck]

    def test_target_categorical_with_time_gets_chisquared(self):
        v = make_variable(dtype=DataType.CATEGORICAL, role=VariableRole.TARGET)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck, ChiSquaredDriftCheck]

    def test_target_discrete_with_time_gets_chisquared(self):
        v = make_variable(dtype=DataType.NUMERIC_DISCRETE, role=VariableRole.TARGET)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck, ChiSquaredDriftCheck]

    def test_target_continuous_no_time_gets_not_null_and_outlier(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.TARGET)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck, OutlierCheck]

    def test_target_continuous_with_time_gets_ks_drift(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.TARGET)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck, KSDriftCheck]

    def test_target_unknown_dtype_gets_not_null_fallback(self):
        v = make_variable(dtype=DataType.TEXT, role=VariableRole.TARGET)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck]

    # Feature dtype dispatch
    def test_numeric_continuous_no_time(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NullRateCheck]

    def test_numeric_continuous_with_time(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NullRateCheck, TrendCheck, StructuralBreakCheck]

    def test_numeric_discrete_no_time(self):
        v = make_variable(dtype=DataType.NUMERIC_DISCRETE)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NullRateCheck, OutlierCheck]

    def test_numeric_discrete_with_time_gets_chisquared(self):
        v = make_variable(dtype=DataType.NUMERIC_DISCRETE)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NullRateCheck, OutlierCheck, ChiSquaredDriftCheck]

    def test_categorical(self):
        v = make_variable(dtype=DataType.CATEGORICAL)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NullRateCheck, CardinalityCheck]

    def test_boolean(self):
        v = make_variable(dtype=DataType.BOOLEAN)
        pipeline = build_default_resolver().resolve(v)
        assert step_types(pipeline) == [NullRateCheck, CardinalityCheck]

    def test_text_falls_through_to_catch_all(self):
        v = make_variable(dtype=DataType.TEXT)
        pipeline = build_default_resolver().resolve(v)
        checks = step_checks(pipeline)
        assert len(checks) == 1
        assert isinstance(checks[0], NullRateCheck)
        assert checks[0].severity == Severity.WARNING

    # Priority ordering
    def test_identifier_beats_continuous(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.IDENTIFIER)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck]

    def test_target_continuous_beats_numeric_continuous(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.TARGET)
        pipeline = build_default_resolver(time_field="ts").resolve(v)
        assert step_types(pipeline) == [NotNullCheck, CardinalityCheck, KSDriftCheck]

    # Threshold propagation
    def test_null_threshold_propagates_to_continuous(self):
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS)
        pipeline = build_default_resolver(null_threshold=0.05).resolve(v)
        assert step_checks(pipeline)[0]._threshold == pytest.approx(0.05)

    def test_max_categorical_cardinality_propagates(self):
        v = make_variable(dtype=DataType.CATEGORICAL)
        pipeline = build_default_resolver(cardinality=CardinalityThresholds(high=20)).resolve(v)
        assert step_checks(pipeline)[1]._max == 20

    def test_each_resolve_returns_fresh_pipeline(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.BOOLEAN)
        assert resolver.resolve(v) is not resolver.resolve(v)

    # Top-level import
    def test_importable_from_dqf(self):
        from dqf import build_default_resolver as bdr

        assert callable(bdr)
