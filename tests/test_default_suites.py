"""Tests for build_default_resolver and the individual pipeline factories."""

from __future__ import annotations

import pytest

from dqf.checks.cross_sectional.cardinality_check import CardinalityCheck
from dqf.checks.cross_sectional.null_rate import NullRateCheck
from dqf.checks.cross_sectional.uniqueness import UniquenessCheck
from dqf.checks.longitudinal.concept_drift import ConceptDriftCheck
from dqf.checks.longitudinal.structural_break import StructuralBreakCheck
from dqf.checks.longitudinal.trend import TrendCheck
from dqf.checks.pipeline import CheckPipeline
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
# Pipeline factory tests
# ──────────────────────────────────────────────────────────────────────────────


class TestIdentifierPipeline:
    def test_returns_pipeline(self):
        assert isinstance(identifier_pipeline(), CheckPipeline)

    def test_has_uniqueness_check(self):
        assert step_types(identifier_pipeline()) == [UniquenessCheck]

    def test_uniqueness_is_failure(self):
        checks = step_checks(identifier_pipeline())
        assert checks[0].severity == Severity.FAILURE


class TestTargetPipeline:
    def test_has_null_rate_and_concept_drift(self):
        p = target_pipeline("ts", "month")
        assert step_types(p) == [NullRateCheck, ConceptDriftCheck]

    def test_null_rate_is_failure(self):
        checks = step_checks(target_pipeline("ts", "month"))
        null_check = checks[0]
        assert null_check.severity == Severity.FAILURE

    def test_concept_drift_is_warning(self):
        checks = step_checks(target_pipeline("ts", "month"))
        drift_check = checks[1]
        assert drift_check.severity == Severity.WARNING

    def test_custom_null_threshold(self):
        checks = step_checks(target_pipeline("ts", null_threshold=0.01))
        assert checks[0]._threshold == pytest.approx(0.01)


class TestTargetPipelineNoTime:
    def test_has_null_rate_only(self):
        assert step_types(target_pipeline_no_time()) == [NullRateCheck]

    def test_null_rate_is_failure(self):
        checks = step_checks(target_pipeline_no_time())
        assert checks[0].severity == Severity.FAILURE


class TestNumericContinuousPipeline:
    def test_has_null_trend_break(self):
        p = numeric_continuous_pipeline("ts", "month")
        assert step_types(p) == [NullRateCheck, TrendCheck, StructuralBreakCheck]

    def test_null_rate_is_failure(self):
        checks = step_checks(numeric_continuous_pipeline("ts"))
        assert checks[0].severity == Severity.FAILURE

    def test_trend_is_warning(self):
        checks = step_checks(numeric_continuous_pipeline("ts"))
        assert checks[1].severity == Severity.WARNING

    def test_structural_break_is_warning(self):
        checks = step_checks(numeric_continuous_pipeline("ts"))
        assert checks[2].severity == Severity.WARNING


class TestNumericContinuousPipelineNoTime:
    def test_has_null_rate_only(self):
        assert step_types(numeric_continuous_pipeline_no_time()) == [NullRateCheck]

    def test_null_rate_is_failure(self):
        checks = step_checks(numeric_continuous_pipeline_no_time())
        assert checks[0].severity == Severity.FAILURE


class TestNumericDiscretePipeline:
    def test_has_null_rate_and_cardinality(self):
        assert step_types(numeric_discrete_pipeline()) == [NullRateCheck, CardinalityCheck]

    def test_cardinality_is_warning(self):
        checks = step_checks(numeric_discrete_pipeline())
        assert checks[1].severity == Severity.WARNING

    def test_custom_max_cardinality(self):
        checks = step_checks(numeric_discrete_pipeline(max_cardinality=200))
        assert checks[1]._max == 200


class TestCategoricalPipeline:
    def test_has_null_rate_and_cardinality(self):
        assert step_types(categorical_pipeline()) == [NullRateCheck, CardinalityCheck]

    def test_cardinality_is_warning(self):
        checks = step_checks(categorical_pipeline())
        assert checks[1].severity == Severity.WARNING

    def test_default_max_cardinality_50(self):
        checks = step_checks(categorical_pipeline())
        assert checks[1]._max == 50


class TestBooleanPipeline:
    def test_has_null_rate_only(self):
        assert step_types(boolean_pipeline()) == [NullRateCheck]

    def test_null_rate_is_failure(self):
        checks = step_checks(boolean_pipeline())
        assert checks[0].severity == Severity.FAILURE


class TestCatchAllPipeline:
    def test_has_null_rate_only(self):
        assert step_types(catch_all_pipeline()) == [NullRateCheck]

    def test_null_rate_is_warning(self):
        checks = step_checks(catch_all_pipeline())
        assert checks[0].severity == Severity.WARNING


# ──────────────────────────────────────────────────────────────────────────────
# build_default_resolver — resolver structure
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildDefaultResolver:
    def test_returns_resolver(self):
        from dqf.resolver import CheckSuiteResolver

        assert isinstance(build_default_resolver(), CheckSuiteResolver)

    def test_identifier_role_gets_uniqueness(self):
        resolver = build_default_resolver()
        v = make_variable(role=VariableRole.IDENTIFIER)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [UniquenessCheck]

    def test_target_role_no_time_gets_null_rate_only(self):
        resolver = build_default_resolver()
        v = make_variable(role=VariableRole.TARGET)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck]

    def test_target_role_with_time_gets_concept_drift(self):
        resolver = build_default_resolver(time_field="event_date")
        v = make_variable(role=VariableRole.TARGET)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck, ConceptDriftCheck]

    def test_numeric_continuous_no_time_gets_null_rate_only(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck]

    def test_numeric_continuous_with_time_gets_longitudinal(self):
        resolver = build_default_resolver(time_field="ts")
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck, TrendCheck, StructuralBreakCheck]

    def test_numeric_discrete_gets_cardinality(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.NUMERIC_DISCRETE)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck, CardinalityCheck]

    def test_categorical_gets_cardinality(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.CATEGORICAL)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck, CardinalityCheck]

    def test_boolean_gets_null_rate(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.BOOLEAN)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck]

    def test_text_falls_through_to_catch_all(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.TEXT)
        pipeline = resolver.resolve(v)
        checks = step_checks(pipeline)
        assert len(checks) == 1
        assert isinstance(checks[0], NullRateCheck)
        assert checks[0].severity == Severity.WARNING

    def test_catch_all_covers_unknown_types(self):
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.DATETIME)
        pipeline = resolver.resolve(v)
        checks = step_checks(pipeline)
        assert checks[0].severity == Severity.WARNING

    # Priority tests

    def test_identifier_role_beats_any_dtype(self):
        """IDENTIFIER role (priority 30) wins over NUMERIC_CONTINUOUS (priority 15)."""
        resolver = build_default_resolver(time_field="ts")
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.IDENTIFIER)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [UniquenessCheck]

    def test_target_role_beats_numeric_continuous(self):
        """TARGET role (priority 20) wins over NUMERIC_CONTINUOUS (priority 15)."""
        resolver = build_default_resolver(time_field="ts")
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.TARGET)
        pipeline = resolver.resolve(v)
        assert step_types(pipeline) == [NullRateCheck, ConceptDriftCheck]

    # Threshold propagation

    def test_custom_null_threshold_propagates(self):
        resolver = build_default_resolver(null_threshold=0.10)
        v = make_variable(dtype=DataType.NUMERIC_CONTINUOUS)
        pipeline = resolver.resolve(v)
        checks = step_checks(pipeline)
        assert checks[0]._threshold == pytest.approx(0.10)

    def test_custom_target_null_threshold_propagates(self):
        resolver = build_default_resolver(target_null_threshold=0.01)
        v = make_variable(role=VariableRole.TARGET)
        pipeline = resolver.resolve(v)
        checks = step_checks(pipeline)
        assert checks[0]._threshold == pytest.approx(0.01)

    def test_custom_categorical_cardinality(self):
        resolver = build_default_resolver(max_categorical_cardinality=20)
        v = make_variable(dtype=DataType.CATEGORICAL)
        pipeline = resolver.resolve(v)
        checks = step_checks(pipeline)
        assert checks[1]._max == 20

    def test_each_resolve_returns_fresh_pipeline(self):
        """Factory is called per resolve — pipelines are not shared."""
        resolver = build_default_resolver()
        v = make_variable(dtype=DataType.BOOLEAN)
        p1 = resolver.resolve(v)
        p2 = resolver.resolve(v)
        assert p1 is not p2

    # Top-level import

    def test_importable_from_dqf(self):
        from dqf import build_default_resolver as bdr

        assert callable(bdr)
