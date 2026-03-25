"""Tests for BaseCheck abstractions and CheckPipeline (plan 6)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from dqf.adapters.mock_adapter import MockAdapter
from dqf.checks.base import BaseCheck, BaseCrossSectionalCheck, BaseLongitudinalCheck
from dqf.checks.pipeline import CheckPipeline
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.enums import DataType, Severity
from dqf.results import CheckResult
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# In-module fakes
# ---------------------------------------------------------------------------


class FakeCheck(BaseCrossSectionalCheck):
    """Minimal concrete check that always returns a fixed result."""

    def __init__(
        self,
        name: str,
        severity: Severity = Severity.FAILURE,
        passed: bool = True,
        params: dict[str, Any] | None = None,
    ) -> None:
        self._name = name
        self._severity = severity
        self._passed = passed
        self._params = params or {}
        self.calibrate_calls: list[Any] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return self._params

    def calibrate(self, dataset: VariablesDataset) -> None:
        self.calibrate_calls.append(dataset)

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        population_size = len(dataset.universe.materialise())
        return CheckResult(
            check_name=self._name,
            passed=self._passed,
            severity=self._severity,
            observed_value=0,
            population_size=population_size if population_size > 0 else 1,
            threshold=None,
        )


class FakeLongitudinalCheck(BaseLongitudinalCheck):
    """Minimal longitudinal check fake."""

    def __init__(self, name: str = "longitudinal", passed: bool = True) -> None:
        self._name = name
        self._passed = passed

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return Severity.FAILURE

    @staticmethod
    def aggregation_sql(variable_name: str, time_field: str, period: str) -> str:
        return f"SELECT {time_field}, AVG({variable_name}) FROM t GROUP BY {time_field}"

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        return CheckResult(
            check_name=self._name,
            passed=self._passed,
            severity=self.severity,
            observed_value=0,
            population_size=1,
            threshold=None,
        )


def make_variable() -> Variable:
    return Variable(name="x", dtype=DataType.NUMERIC_CONTINUOUS)


_UNIVERSE_SQL = "SELECT * FROM universe"
_VARIABLES_SQL = "SELECT * FROM variables"


def make_dataset(n: int = 3) -> VariablesDataset:
    universe_df = pd.DataFrame({"_uid": range(n)})
    variables_df = pd.DataFrame({"_uid": range(n), "x": range(n)})
    universe = UniverseDataset(
        sql=_UNIVERSE_SQL,
        primary_key=["_uid"],
        adapter=MockAdapter({_UNIVERSE_SQL: universe_df}),
    )
    return VariablesDataset(
        sql=_VARIABLES_SQL,
        primary_key=["_uid"],
        universe=universe,
        join_keys={"_uid": "_uid"},
        adapter=MockAdapter({_VARIABLES_SQL: variables_df}),
    )


# ---------------------------------------------------------------------------
# TestBaseCheckAbstract
# ---------------------------------------------------------------------------


class TestBaseCheckAbstract:
    def test_cannot_instantiate_base_check(self) -> None:
        with pytest.raises(TypeError):
            BaseCheck()  # type: ignore[abstract]

    def test_cannot_instantiate_base_cross_sectional(self) -> None:
        with pytest.raises(TypeError):
            BaseCrossSectionalCheck()  # type: ignore[abstract]

    def test_cannot_instantiate_base_longitudinal(self) -> None:
        with pytest.raises(TypeError):
            BaseLongitudinalCheck()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# TestBaseCheckInterface
# ---------------------------------------------------------------------------


class TestBaseCheckInterface:
    def test_calibrate_default_is_noop(self) -> None:
        check = FakeCheck("c")
        check.calibrate(make_dataset())  # must not raise

    def test_params_default_is_empty_dict(self) -> None:
        check = FakeCheck("c")
        assert check.params == {}

    def test_params_stored(self) -> None:
        check = FakeCheck("c", params={"threshold": 0.05})
        assert check.params == {"threshold": 0.05}


# ---------------------------------------------------------------------------
# TestBaseLongitudinalCheckInterface
# ---------------------------------------------------------------------------


class TestBaseLongitudinalCheckInterface:
    def test_aggregation_sql_returns_string(self) -> None:
        check = FakeLongitudinalCheck()
        sql = check.aggregation_sql("score", "event_date", "month")
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_longitudinal_is_base_check(self) -> None:
        check = FakeLongitudinalCheck()
        assert isinstance(check, BaseCheck)


# ---------------------------------------------------------------------------
# TestCheckPipelineRun
# ---------------------------------------------------------------------------


class TestCheckPipelineRun:
    def test_empty_pipeline_returns_empty_list(self) -> None:
        pipeline = CheckPipeline([])
        results = pipeline.run(make_dataset(), make_variable())
        assert results == []

    def test_single_step_result_collected(self) -> None:
        pipeline = CheckPipeline([("c1", FakeCheck("c1", passed=True))])
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 1
        assert results[0].passed is True

    def test_two_steps_both_collected(self) -> None:
        pipeline = CheckPipeline(
            [("c1", FakeCheck("c1", passed=True)), ("c2", FakeCheck("c2", passed=True))]
        )
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 2

    def test_results_order_matches_steps_order(self) -> None:
        pipeline = CheckPipeline([("first", FakeCheck("first")), ("second", FakeCheck("second"))])
        results = pipeline.run(make_dataset(), make_variable())
        assert results[0].check_name == "first"
        assert results[1].check_name == "second"

    def test_all_results_returned_when_no_failure(self) -> None:
        steps: list[tuple[str, BaseCheck]] = [
            ("c", FakeCheck(f"c{i}", passed=True)) for i in range(5)
        ]
        pipeline = CheckPipeline(steps)
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 5


# ---------------------------------------------------------------------------
# TestCheckPipelineStopOnFailure
# ---------------------------------------------------------------------------


class TestCheckPipelineStopOnFailure:
    def test_stop_on_failure_false_continues_after_failure(self) -> None:
        pipeline = CheckPipeline(
            [
                ("fail", FakeCheck("fail", Severity.FAILURE, passed=False)),
                ("after", FakeCheck("after", passed=True)),
            ],
            stop_on_failure=False,
        )
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 2

    def test_stop_on_failure_true_halts_after_failure(self) -> None:
        pipeline = CheckPipeline(
            [
                ("fail", FakeCheck("fail", Severity.FAILURE, passed=False)),
                ("after", FakeCheck("after", passed=True)),
            ],
            stop_on_failure=True,
        )
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 1
        assert results[0].check_name == "fail"

    def test_warning_severity_does_not_stop_pipeline(self) -> None:
        pipeline = CheckPipeline(
            [
                ("warn", FakeCheck("warn", Severity.WARNING, passed=False)),
                ("after", FakeCheck("after", passed=True)),
            ],
            stop_on_failure=True,
        )
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 2

    def test_passing_failure_severity_does_not_stop(self) -> None:
        pipeline = CheckPipeline(
            [
                ("ok", FakeCheck("ok", Severity.FAILURE, passed=True)),
                ("after", FakeCheck("after", passed=True)),
            ],
            stop_on_failure=True,
        )
        results = pipeline.run(make_dataset(), make_variable())
        assert len(results) == 2


# ---------------------------------------------------------------------------
# TestCheckPipelineCalibrate
# ---------------------------------------------------------------------------


class TestCheckPipelineCalibrate:
    def test_calibrate_delegates_to_all_steps(self) -> None:
        c1 = FakeCheck("c1")
        c2 = FakeCheck("c2")
        pipeline = CheckPipeline([("c1", c1), ("c2", c2)])
        pipeline.calibrate(make_dataset())
        assert len(c1.calibrate_calls) == 1
        assert len(c2.calibrate_calls) == 1

    def test_calibrate_passes_same_dataset(self) -> None:
        c1 = FakeCheck("c1")
        dataset = make_dataset()
        pipeline = CheckPipeline([("c1", c1)])
        pipeline.calibrate(dataset)
        assert c1.calibrate_calls[0] is dataset


# ---------------------------------------------------------------------------
# TestCheckPipelineComposite
# ---------------------------------------------------------------------------


class TestCheckPipelineComposite:
    def test_pipeline_is_base_check(self) -> None:
        pipeline = CheckPipeline([])
        assert isinstance(pipeline, BaseCheck)

    def test_nested_pipeline_as_step(self) -> None:
        inner = CheckPipeline([("inner_c", FakeCheck("inner_c", passed=True))])
        outer = CheckPipeline([("inner", inner), ("outer_c", FakeCheck("outer_c", passed=True))])
        results = outer.run(make_dataset(), make_variable())
        # outer_c result + inner's aggregated result
        assert len(results) == 2

    def test_nested_stop_on_failure_outer_only(self) -> None:
        # inner pipeline has a failing check
        inner = CheckPipeline(
            [("inner_fail", FakeCheck("inner_fail", Severity.FAILURE, passed=False))]
        )
        # outer has stop_on_failure=True; inner's aggregated result fails → outer stops
        outer = CheckPipeline(
            [("inner", inner), ("outer_after", FakeCheck("outer_after", passed=True))],
            stop_on_failure=True,
        )
        results = outer.run(make_dataset(), make_variable())
        assert len(results) == 1
        assert results[0].check_name == "pipeline"
