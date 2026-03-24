"""End-to-end integration tests for VariablesDataset.run_validation (plan 9)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqf.adapters.mock_adapter import MockAdapter
from dqf.checks.base import BaseCrossSectionalCheck
from dqf.checks.pipeline import CheckPipeline
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.enums import DataType, Severity, ValidationStatus
from dqf.metadata.base import BaseMetadataBuilder, MetadataBuilderPipeline
from dqf.metadata.resolver import MetadataResolver
from dqf.report import ValidationReport
from dqf.resolver import CheckSuiteResolver
from dqf.results import CheckResult
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------------

_UNIVERSE_SQL = "SELECT * FROM universe"
_VARIABLES_SQL = "SELECT * FROM variables"

# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------

_UNIVERSE_DF = pd.DataFrame({"entity_id": [1, 2, 3]})
_VARIABLES_DF = pd.DataFrame(
    {
        "entity_id": [1, 2, 3],
        "score": [0.9, 0.5, 0.1],
        "category": ["A", "B", "A"],
    }
)

# ---------------------------------------------------------------------------
# In-module fakes
# ---------------------------------------------------------------------------


class AlwaysPassCheck(BaseCrossSectionalCheck):
    @property
    def name(self) -> str:
        return "always_pass"

    @property
    def severity(self) -> Severity:
        return Severity.FAILURE

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        n = len(dataset.universe.materialise())
        return CheckResult(
            check_name=self.name,
            passed=True,
            severity=self.severity,
            observed_value=0,
            population_size=n,
            threshold=None,
        )


class AlwaysFailCheck(BaseCrossSectionalCheck):
    @property
    def name(self) -> str:
        return "always_fail"

    @property
    def severity(self) -> Severity:
        return Severity.FAILURE

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        n = len(dataset.universe.materialise())
        return CheckResult(
            check_name=self.name,
            passed=False,
            severity=self.severity,
            observed_value=1,
            population_size=n,
            threshold=None,
        )


class AlwaysWarnCheck(BaseCrossSectionalCheck):
    @property
    def name(self) -> str:
        return "always_warn"

    @property
    def severity(self) -> Severity:
        return Severity.WARNING

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        n = len(dataset.universe.materialise())
        return CheckResult(
            check_name=self.name,
            passed=False,
            severity=self.severity,
            observed_value=1,
            population_size=n,
            threshold=None,
        )


class NopBuilder(BaseMetadataBuilder):
    @property
    def name(self) -> str:
        return "nop"

    def profile(self, dataset: VariablesDataset, variable: Variable) -> dict[str, Any]:
        return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_dataset(
    universe_df: pd.DataFrame | None = None,
    variables_df: pd.DataFrame | None = None,
) -> VariablesDataset:
    if universe_df is None:
        universe_df = _UNIVERSE_DF.copy()
    if variables_df is None:
        variables_df = _VARIABLES_DF.copy()
    universe = UniverseDataset(
        sql=_UNIVERSE_SQL,
        primary_key=["entity_id"],
        adapter=MockAdapter({_UNIVERSE_SQL: universe_df}),
    )
    return VariablesDataset(
        sql=_VARIABLES_SQL,
        primary_key=["entity_id"],
        universe=universe,
        join_keys={"entity_id": "entity_id"},
        adapter=MockAdapter({_VARIABLES_SQL: variables_df}),
    )


def make_resolver(check: BaseCrossSectionalCheck | None = None) -> CheckSuiteResolver:
    r = CheckSuiteResolver()
    r.register(lambda v: True, lambda: CheckPipeline([("c", check or AlwaysPassCheck())]))
    return r


# ---------------------------------------------------------------------------
# TestRunValidationReturnType
# ---------------------------------------------------------------------------


class TestRunValidationReturnType:
    def test_returns_validation_report(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        assert isinstance(ds.run_validation(make_resolver()), ValidationReport)

    def test_report_has_run_timestamp(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        assert report.run_timestamp is not None

    def test_report_stores_dataset_name(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(), dataset_name="my_dataset")
        assert report.dataset_name == "my_dataset"

    def test_report_default_dataset_name_empty_string(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        assert report.dataset_name == ""


# ---------------------------------------------------------------------------
# TestRunValidationDatasetLevelChecks
# ---------------------------------------------------------------------------


class TestRunValidationDatasetLevelChecks:
    def test_two_dataset_level_checks_present(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        assert len(report.dataset_level_checks) == 2

    def test_pk_uniqueness_check_passed(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        pk = next(r for r in report.dataset_level_checks if r.check_name == "pk_uniqueness")
        assert pk.passed is True

    def test_pk_uniqueness_check_fails_on_duplicates(self) -> None:
        vars_df = pd.DataFrame({"entity_id": [1, 1, 3], "score": [0.9, 0.8, 0.1]})
        ds = make_dataset(variables_df=vars_df)
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        pk = next(r for r in report.dataset_level_checks if r.check_name == "pk_uniqueness")
        assert pk.passed is False

    def test_join_integrity_check_passed(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        ji = next(r for r in report.dataset_level_checks if r.check_name == "join_integrity")
        assert ji.passed is True

    def test_join_integrity_check_fails_on_fanout(self) -> None:
        fanout_df = pd.DataFrame({"entity_id": [1, 1, 2, 3], "score": [0.9, 0.8, 0.5, 0.1]})
        ds = make_dataset(variables_df=fanout_df)
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        ji = next(r for r in report.dataset_level_checks if r.check_name == "join_integrity")
        assert ji.passed is False

    def test_dataset_level_checks_stored_on_dataset(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        ds.run_validation(make_resolver())
        assert ds.pk_validation is not None
        assert ds.join_validation is not None


# ---------------------------------------------------------------------------
# TestRunValidationVariableReports
# ---------------------------------------------------------------------------


class TestRunValidationVariableReports:
    def test_variable_reports_keyed_by_variable_name(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        assert "score" in report.variable_results

    def test_variable_reports_contain_check_results(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        assert len(report.variable_results["score"]) == 1
        assert report.variable_results["score"][0].check_name == "always_pass"

    def test_multiple_variables_all_present_in_report(self) -> None:
        ds = make_dataset()
        ds.variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS),
            Variable(name="category", dtype=DataType.CATEGORICAL),
        ]
        report = ds.run_validation(make_resolver())
        assert set(report.variable_results.keys()) == {"score", "category"}

    def test_check_results_attached_to_variable_objects(self) -> None:
        ds = make_dataset()
        v = Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)
        ds.variables = [v]
        ds.run_validation(make_resolver())
        assert len(v.check_results) == 1

    def test_multiple_checks_per_variable_all_collected(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        r = CheckSuiteResolver()
        r.register(
            lambda v: True,
            lambda: CheckPipeline([("pass", AlwaysPassCheck()), ("warn", AlwaysWarnCheck())]),
        )
        report = ds.run_validation(r)
        assert len(report.variable_results["score"]) == 2

    def test_empty_variables_produces_empty_report(self) -> None:
        ds = make_dataset()
        ds.variables = []
        report = ds.run_validation(CheckSuiteResolver())
        assert report.variable_results == {}


# ---------------------------------------------------------------------------
# TestRunValidationOverallStatus
# ---------------------------------------------------------------------------


class TestRunValidationOverallStatus:
    def test_passed_when_all_checks_pass(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert report.overall_status == ValidationStatus.PASSED

    def test_failed_when_failure_check_fails(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysFailCheck()))
        assert report.overall_status == ValidationStatus.FAILED

    def test_passed_with_only_warning_failures(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysWarnCheck()))
        assert report.overall_status == ValidationStatus.PASSED

    def test_failed_when_pk_uniqueness_fails(self) -> None:
        vars_df = pd.DataFrame({"entity_id": [1, 1, 3], "score": [0.9, 0.8, 0.1]})
        ds = make_dataset(variables_df=vars_df)
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert report.overall_status == ValidationStatus.FAILED

    def test_failed_when_join_integrity_fails(self) -> None:
        fanout_df = pd.DataFrame({"entity_id": [1, 1, 2, 3], "score": [0.9, 0.8, 0.5, 0.1]})
        ds = make_dataset(variables_df=fanout_df)
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert report.overall_status == ValidationStatus.FAILED

    def test_passed_with_no_variables(self) -> None:
        ds = make_dataset()
        ds.variables = []
        report = ds.run_validation(CheckSuiteResolver())
        assert report.overall_status == ValidationStatus.PASSED


# ---------------------------------------------------------------------------
# TestRunValidationAutoResolveVariables
# ---------------------------------------------------------------------------


class TestRunValidationAutoResolveVariables:
    def _make_nop_resolver(self) -> MetadataResolver:
        resolver = MetadataResolver()
        resolver.register(
            predicate=lambda v: True,
            pipeline_factory=lambda: MetadataBuilderPipeline([("nop", NopBuilder())]),
            priority=0,
        )
        return resolver

    def test_variables_auto_resolved_when_empty(self) -> None:
        ds = make_dataset()
        ds.run_validation(make_resolver(), metadata_resolver=self._make_nop_resolver())
        # universe has entity_id; variables has entity_id + score + category
        # joined result has entity_id + score + category (+ __vd_matched__ excluded)
        assert len(ds.variables) > 0
        assert all(isinstance(v, Variable) for v in ds.variables)

    def test_pre_set_variables_not_overwritten(self) -> None:
        ds = make_dataset()
        pre_set = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        ds.variables = pre_set
        ds.run_validation(make_resolver(), metadata_resolver=self._make_nop_resolver())
        assert len(ds.variables) == 1

    def test_no_metadata_resolver_and_empty_variables_produces_empty_report(self) -> None:
        ds = make_dataset()
        # variables is empty, no metadata_resolver → no variables resolved
        report = ds.run_validation(CheckSuiteResolver())
        assert report.variable_results == {}


# ---------------------------------------------------------------------------
# TestValidationReportHelpers
# ---------------------------------------------------------------------------


class TestValidationReportHelpers:
    def test_failed_variables_returns_failing_names(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysFailCheck()))
        assert report.failed_variables() == ["score"]

    def test_failed_variables_empty_when_all_pass(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert report.failed_variables() == []

    def test_warned_variables_returns_warning_variable_names(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysWarnCheck()))
        warns = report.warned_variables()
        assert warns == ["score"]

    def test_warned_variables_empty_when_no_warning_failures(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert report.warned_variables() == []

    def test_warned_variables_excludes_failure_severity_variables(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver(AlwaysFailCheck()))
        assert report.warned_variables() == []


# ---------------------------------------------------------------------------
# TestRunValidationStateAndCaching
# ---------------------------------------------------------------------------


class TestRunValidationStateAndCaching:
    def test_validation_state_pending_before_run(self) -> None:
        ds = make_dataset()
        assert ds.validation_state == ValidationStatus.PENDING

    def test_validation_report_none_before_run(self) -> None:
        ds = make_dataset()
        assert ds.validation_report is None

    def test_validation_state_passed_after_all_pass(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert ds.validation_state == ValidationStatus.PASSED

    def test_validation_state_failed_after_failure(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        ds.run_validation(make_resolver(AlwaysFailCheck()))
        assert ds.validation_state == ValidationStatus.FAILED

    def test_validation_report_stored_on_dataset(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        report = ds.run_validation(make_resolver())
        assert ds.validation_report is report

    def test_second_run_skipped_when_state_is_passed(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        first = ds.run_validation(make_resolver(AlwaysPassCheck()))
        second = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert second is first  # same cached report returned

    def test_second_run_not_skipped_when_state_is_failed(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        first = ds.run_validation(make_resolver(AlwaysFailCheck()))
        second = ds.run_validation(make_resolver(AlwaysFailCheck()))
        assert second is not first  # re-executed

    def test_force_reruns_even_when_state_is_passed(self) -> None:
        ds = make_dataset()
        ds.variables = [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS)]
        first = ds.run_validation(make_resolver(AlwaysPassCheck()))
        assert ds.validation_state == ValidationStatus.PASSED
        second = ds.run_validation(make_resolver(AlwaysPassCheck()), force=True)
        assert second is not first  # fresh report produced
