"""End-to-end integration tests using build_default_resolver.

These tests exercise the full path from SQL definition → VariablesDataset
→ run_validation → ValidationReport using MockAdapter (no external
dependencies).  They complement the unit tests in test_run_validation.py
by using real check implementations and the batteries-included resolver
rather than hand-rolled stubs.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dqf.adapters.mock_adapter import MockAdapter
from dqf.checks.longitudinal.chisquared_drift import ChiSquaredDriftCheck
from dqf.checks.longitudinal.ks_drift import KSDriftCheck
from dqf.checks.longitudinal.proportion_drift import ProportionDriftCheck
from dqf.checks.longitudinal.trend import TrendCheck
from dqf.checks.pipeline import CheckPipeline
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.defaults.suites import build_default_resolver
from dqf.enums import DataType, Severity, ValidationStatus, VariableRole
from dqf.report import ValidationReport
from dqf.variable import Variable

# ──────────────────────────────────────────────────────────────────────────────
# Shared SQL strings
# ──────────────────────────────────────────────────────────────────────────────

_UNIVERSE_SQL = "SELECT entity_id FROM universe"
_FEATURE_VARS_SQL = "SELECT entity_id, score, category, is_active FROM variables"
_LONGITUDINAL_VARS_SQL = "SELECT entity_id, ts, score FROM variables"
_TARGET_BINARY_SQL = "SELECT entity_id, ts, target FROM variables"
_TARGET_CONTINUOUS_SQL = "SELECT entity_id, ts, score FROM variables_continuous"
_TARGET_CATEGORICAL_SQL = "SELECT entity_id, ts, label FROM variables_cat"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _agg_sql(check: object, variable_name: str, time_field: str, source_sql: str) -> str:
    """Compute the aggregation SQL that a longitudinal check will issue at runtime."""
    assert hasattr(check, "aggregation_sql")
    template = check.aggregation_sql(variable_name, time_field, "month")
    return str(template.format(source=source_sql))


def _make_dataset(
    universe_sql: str,
    vars_sql: str,
    universe_df: pd.DataFrame,
    vars_df: pd.DataFrame,
    variables: list[Variable],
    extra_results: dict[str, pd.DataFrame] | None = None,
) -> VariablesDataset:
    """Build a VariablesDataset backed by a single MockAdapter."""
    results: dict[str, pd.DataFrame] = {universe_sql: universe_df, vars_sql: vars_df}
    if extra_results:
        results.update(extra_results)
    adapter = MockAdapter(results)
    universe = UniverseDataset(sql=universe_sql, primary_key=["entity_id"], adapter=adapter)
    return VariablesDataset(
        sql=vars_sql,
        primary_key=["entity_id"],
        universe=universe,
        join_keys={"entity_id": "entity_id"},
        adapter=adapter,
        variables=variables,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Cross-sectional end-to-end (no time_field)
# ──────────────────────────────────────────────────────────────────────────────


class TestEndToEndCrossSectional:
    """build_default_resolver() with no time_field through a full run_validation."""

    @pytest.fixture()
    def dataset(self) -> VariablesDataset:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5]})
        vars_df = pd.DataFrame(
            {
                "entity_id": [1, 2, 3, 4, 5],
                "score": [0.1, 0.5, 0.9, 0.3, 0.7],
                "category": ["A", "B", "A", "C", "B"],
                "is_active": [True, False, True, True, False],
            }
        )
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE),
            Variable(name="category", dtype=DataType.CATEGORICAL, role=VariableRole.FEATURE),
            Variable(name="is_active", dtype=DataType.BOOLEAN, role=VariableRole.FEATURE),
        ]
        return _make_dataset(_UNIVERSE_SQL, _FEATURE_VARS_SQL, universe_df, vars_df, variables)

    def test_returns_validation_report(self, dataset: VariablesDataset) -> None:
        report = dataset.run_validation(build_default_resolver())
        assert isinstance(report, ValidationReport)

    def test_numeric_continuous_gets_null_rate_check(self, dataset: VariablesDataset) -> None:
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["score"]]
        assert "null_rate" in names

    def test_categorical_gets_null_rate_and_cardinality(self, dataset: VariablesDataset) -> None:
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["category"]]
        assert "null_rate" in names
        assert "cardinality" in names

    def test_boolean_gets_null_rate(self, dataset: VariablesDataset) -> None:
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["is_active"]]
        assert names == ["null_rate", "cardinality"]

    def test_overall_status_passed_on_clean_data(self, dataset: VariablesDataset) -> None:
        report = dataset.run_validation(build_default_resolver())
        assert report.overall_status == ValidationStatus.PASSED

    def test_all_three_variables_present_in_report(self, dataset: VariablesDataset) -> None:
        report = dataset.run_validation(build_default_resolver())
        assert set(report.variable_results) == {"score", "category", "is_active"}

    def test_null_threshold_strict_fails(self) -> None:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5]})
        # 2/5 = 40% nulls in score
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5], "score": [0.1, None, 0.9, None, 0.7]})
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE)
        ]
        dataset = _make_dataset(_UNIVERSE_SQL, _FEATURE_VARS_SQL, universe_df, vars_df, variables)
        report = dataset.run_validation(build_default_resolver(null_threshold=0.10))
        assert report.overall_status == ValidationStatus.FAILED

    def test_null_threshold_lenient_passes(self) -> None:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5]})
        # 2/5 = 40% nulls in score
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5], "score": [0.1, None, 0.9, None, 0.7]})
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE)
        ]
        dataset = _make_dataset(_UNIVERSE_SQL, _FEATURE_VARS_SQL, universe_df, vars_df, variables)
        report = dataset.run_validation(build_default_resolver(null_threshold=0.50))
        assert report.overall_status == ValidationStatus.PASSED


# ──────────────────────────────────────────────────────────────────────────────
# Identifier and target dispatch (cross-sectional)
# ──────────────────────────────────────────────────────────────────────────────


class TestEndToEndIdentifierAndTarget:
    """IDENTIFIER and TARGET roles are dispatched to the right pipelines."""

    def test_identifier_gets_not_null_check(self) -> None:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3], "score": [0.1, 0.5, 0.9]})
        variables = [
            Variable(
                name="entity_id", dtype=DataType.NUMERIC_DISCRETE, role=VariableRole.IDENTIFIER
            ),
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, score FROM variables", universe_df, vars_df, variables
        )
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["entity_id"]]
        assert names == ["not_null"]

    def test_identifier_beats_feature_dtype(self) -> None:
        """IDENTIFIER role at priority 30 beats NUMERIC_CONTINUOUS at priority 15."""
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        variables = [
            Variable(
                name="entity_id",
                dtype=DataType.NUMERIC_CONTINUOUS,
                role=VariableRole.IDENTIFIER,
            ),
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id FROM vars", universe_df, vars_df, variables
        )
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["entity_id"]]
        assert names == ["not_null"]

    def test_target_continuous_no_time_gets_not_null_and_outlier(self) -> None:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
        vars_df = pd.DataFrame(
            {
                "entity_id": range(1, 11),
                "score": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            }
        )
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.TARGET),
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, score FROM variables", universe_df, vars_df, variables
        )
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["score"]]
        assert names == ["not_null", "cardinality", "outlier"]

    def test_target_boolean_no_time_gets_not_null_only(self) -> None:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3], "target": [True, False, True]})
        variables = [
            Variable(name="target", dtype=DataType.BOOLEAN, role=VariableRole.TARGET),
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL,
            "SELECT entity_id, target FROM variables",
            universe_df,
            vars_df,
            variables,
        )
        report = dataset.run_validation(build_default_resolver())
        names = [r.check_name for r in report.variable_results["target"]]
        assert names == ["not_null", "cardinality"]

    def test_identifier_not_null_check_passes_with_no_nulls(self) -> None:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3], "score": [1.0, 2.0, 3.0]})
        variables = [
            Variable(
                name="entity_id", dtype=DataType.NUMERIC_DISCRETE, role=VariableRole.IDENTIFIER
            ),
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, score FROM variables", universe_df, vars_df, variables
        )
        report = dataset.run_validation(build_default_resolver())
        id_result = report.variable_results["entity_id"][0]
        assert id_result.check_name == "not_null"
        assert id_result.passed is True


# ──────────────────────────────────────────────────────────────────────────────
# Longitudinal end-to-end (with time_field)
# ──────────────────────────────────────────────────────────────────────────────


class TestEndToEndLongitudinal:
    """build_default_resolver(time_field="ts") through a full run_validation."""

    @pytest.fixture()
    def dataset(self) -> VariablesDataset:
        universe_df = pd.DataFrame({"entity_id": range(1, 61)})
        vars_df = pd.DataFrame(
            {"entity_id": range(1, 61), "ts": ["placeholder"] * 60, "score": [10.0] * 60}
        )
        # Both TrendCheck and StructuralBreakCheck share the same aggregation SQL
        trend_check = TrendCheck(time_field="ts", period="month")
        agg_sql = _agg_sql(trend_check, "score", "ts", _LONGITUDINAL_VARS_SQL)
        # Constant series: CUSUM stat = 0.0 (std=0 branch) → both checks pass
        agg_df = pd.DataFrame(
            {
                "period": [f"2024-0{i}-01" for i in range(1, 7)],
                "metric": [10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
                "n": [10] * 6,
            }
        )
        return _make_dataset(
            _UNIVERSE_SQL,
            _LONGITUDINAL_VARS_SQL,
            universe_df,
            vars_df,
            [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE)],
            extra_results={agg_sql: agg_df},
        )

    def test_numeric_continuous_with_time_runs_three_checks(
        self, dataset: VariablesDataset
    ) -> None:
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        names = [r.check_name for r in report.variable_results["score"]]
        assert names == ["null_rate", "trend", "structural_break"]

    def test_longitudinal_checks_have_metadata(self, dataset: VariablesDataset) -> None:
        """Verify that longitudinal checks execute and populate metadata."""
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        trend_result = next(r for r in report.variable_results["score"] if r.check_name == "trend")
        sb_result = next(
            r for r in report.variable_results["score"] if r.check_name == "structural_break"
        )
        assert "n_periods" in trend_result.metadata
        assert "n_periods" in sb_result.metadata
        assert trend_result.metadata["n_periods"] == 6
        assert sb_result.metadata["n_periods"] == 6

    def test_numeric_discrete_with_time_gets_chisquared_drift(self) -> None:
        vars_sql = "SELECT entity_id, ts, count FROM variables_discrete"
        chi_check = ChiSquaredDriftCheck(time_field="ts", period="month")
        agg_sql = _agg_sql(chi_check, "count", "ts", vars_sql)
        agg_df = pd.DataFrame(
            [(f"2024-0{i}-01", "1", 10) for i in range(1, 7)]
            + [(f"2024-0{i}-01", "2", 5) for i in range(1, 7)],
            columns=["period", "category", "count"],
        )
        universe_df = pd.DataFrame({"entity_id": range(1, 91)})
        vars_df = pd.DataFrame(
            {"entity_id": range(1, 91), "ts": ["placeholder"] * 90, "count": [1] * 90}
        )
        dataset = _make_dataset(
            _UNIVERSE_SQL,
            vars_sql,
            universe_df,
            vars_df,
            [Variable(name="count", dtype=DataType.NUMERIC_DISCRETE, role=VariableRole.FEATURE)],
            extra_results={agg_sql: agg_df},
        )
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        names = [r.check_name for r in report.variable_results["count"]]
        assert "chisquared_drift" in names


# ──────────────────────────────────────────────────────────────────────────────
# Target drift end-to-end (with time_field)
# ──────────────────────────────────────────────────────────────────────────────


class TestEndToEndTargetDrift:
    """TARGET drift checks route correctly through run_validation."""

    def test_binary_target_with_time_runs_proportion_drift(self) -> None:
        prop_check = ProportionDriftCheck(time_field="ts", period="month")
        agg_sql = _agg_sql(prop_check, "target", "ts", _TARGET_BINARY_SQL)
        agg_df = pd.DataFrame(
            {
                "period": [f"2024-0{i}-01" for i in range(1, 7)],
                "positive": [5] * 6,
                "n": [10] * 6,
            }
        )
        universe_df = pd.DataFrame({"entity_id": range(1, 61)})
        vars_df = pd.DataFrame(
            {"entity_id": range(1, 61), "ts": ["placeholder"] * 60, "target": [1] * 60}
        )
        dataset = _make_dataset(
            _UNIVERSE_SQL,
            _TARGET_BINARY_SQL,
            universe_df,
            vars_df,
            [Variable(name="target", dtype=DataType.BOOLEAN, role=VariableRole.TARGET)],
            extra_results={agg_sql: agg_df},
        )
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        names = [r.check_name for r in report.variable_results["target"]]
        assert names == ["not_null", "cardinality", "proportion_drift"]

    def test_continuous_target_with_time_runs_ks_drift(self) -> None:
        ks_check = KSDriftCheck(time_field="ts", period="month")
        agg_sql = _agg_sql(ks_check, "score", "ts", _TARGET_CONTINUOUS_SQL)
        # KSDriftCheck returns one row per entity value
        rows = [(f"2024-0{(i // 10) + 1}-01", float(i % 10)) for i in range(60)]
        agg_df = pd.DataFrame(rows, columns=["period", "value"])
        universe_df = pd.DataFrame({"entity_id": range(1, 61)})
        vars_df = pd.DataFrame(
            {
                "entity_id": range(1, 61),
                "ts": ["placeholder"] * 60,
                "score": [float(i % 10) for i in range(60)],
            }
        )
        dataset = _make_dataset(
            _UNIVERSE_SQL,
            _TARGET_CONTINUOUS_SQL,
            universe_df,
            vars_df,
            [Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.TARGET)],
            extra_results={agg_sql: agg_df},
        )
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        names = [r.check_name for r in report.variable_results["score"]]
        assert names == ["not_null", "cardinality", "ks_drift"]

    def test_categorical_target_with_time_runs_chisquared_drift(self) -> None:
        chi_check = ChiSquaredDriftCheck(time_field="ts", period="month")
        agg_sql = _agg_sql(chi_check, "label", "ts", _TARGET_CATEGORICAL_SQL)
        agg_df = pd.DataFrame(
            [(f"2024-0{i}-01", cat, 10) for i in range(1, 7) for cat in ["A", "B"]],
            columns=["period", "category", "count"],
        )
        universe_df = pd.DataFrame({"entity_id": range(1, 121)})
        vars_df = pd.DataFrame(
            {
                "entity_id": range(1, 121),
                "ts": ["placeholder"] * 120,
                "label": ["A"] * 120,
            }
        )
        dataset = _make_dataset(
            _UNIVERSE_SQL,
            _TARGET_CATEGORICAL_SQL,
            universe_df,
            vars_df,
            [Variable(name="label", dtype=DataType.CATEGORICAL, role=VariableRole.TARGET)],
            extra_results={agg_sql: agg_df},
        )
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        names = [r.check_name for r in report.variable_results["label"]]
        assert names == ["not_null", "cardinality", "chisquared_drift"]

    def test_stable_binary_proportion_passes(self) -> None:
        prop_check = ProportionDriftCheck(time_field="ts", period="month")
        agg_sql = _agg_sql(prop_check, "target", "ts", _TARGET_BINARY_SQL)
        # Perfectly stable 50% positive rate
        agg_df = pd.DataFrame(
            {
                "period": [f"2024-0{i}-01" for i in range(1, 7)],
                "positive": [50] * 6,
                "n": [100] * 6,
            }
        )
        universe_df = pd.DataFrame({"entity_id": range(1, 601)})
        vars_df = pd.DataFrame(
            {"entity_id": range(1, 601), "ts": ["placeholder"] * 600, "target": [1] * 600}
        )
        dataset = _make_dataset(
            _UNIVERSE_SQL,
            _TARGET_BINARY_SQL,
            universe_df,
            vars_df,
            [Variable(name="target", dtype=DataType.BOOLEAN, role=VariableRole.TARGET)],
            extra_results={agg_sql: agg_df},
        )
        report = dataset.run_validation(build_default_resolver(time_field="ts"))
        drift_result = next(
            r for r in report.variable_results["target"] if r.check_name == "proportion_drift"
        )
        assert drift_result.passed is True


# ──────────────────────────────────────────────────────────────────────────────
# ValidationReport outputs
# ──────────────────────────────────────────────────────────────────────────────


class TestValidationReportOutputs:
    """to_dataframe(), failed_variables(), render() in a real E2E scenario."""

    @pytest.fixture()
    def report_with_failure(self) -> ValidationReport:
        universe_df = pd.DataFrame({"entity_id": [1, 2, 3, 4, 5]})
        # 60% nulls in score — will fail null_rate with threshold=0.10
        vars_df = pd.DataFrame(
            {
                "entity_id": [1, 2, 3, 4, 5],
                "score": [None, None, None, 0.3, 0.7],
                "category": ["A", "B", "A", "C", "B"],
            }
        )
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE),
            Variable(name="category", dtype=DataType.CATEGORICAL, role=VariableRole.FEATURE),
        ]
        dataset = _make_dataset(_UNIVERSE_SQL, _FEATURE_VARS_SQL, universe_df, vars_df, variables)
        return dataset.run_validation(build_default_resolver(null_threshold=0.10))

    def test_to_dataframe_has_expected_columns(self, report_with_failure: ValidationReport) -> None:
        df = report_with_failure.to_dataframe()
        assert set(df.columns) == {
            "variable",
            "check_name",
            "passed",
            "severity",
            "rate",
            "threshold",
            "observed_value",
        }

    def test_to_dataframe_row_count_matches_total_checks(
        self, report_with_failure: ValidationReport
    ) -> None:
        df = report_with_failure.to_dataframe()
        total_checks = sum(len(v) for v in report_with_failure.variable_results.values())
        assert len(df) == total_checks

    def test_failed_variables_contains_score(self, report_with_failure: ValidationReport) -> None:
        assert "score" in report_with_failure.failed_variables()

    def test_failed_variables_excludes_passing_category(
        self, report_with_failure: ValidationReport
    ) -> None:
        assert "category" not in report_with_failure.failed_variables()

    def test_overall_status_failed_due_to_score(
        self, report_with_failure: ValidationReport
    ) -> None:
        assert report_with_failure.overall_status == ValidationStatus.FAILED

    def test_render_returns_html_string(self, report_with_failure: ValidationReport) -> None:
        html = report_with_failure.render()
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html
        assert "score" in html

    def test_universe_size_in_report(self) -> None:
        universe_df = pd.DataFrame({"entity_id": range(1, 11)})
        vars_df = pd.DataFrame({"entity_id": range(1, 11), "score": [1.0] * 10})
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE)
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, score FROM v", universe_df, vars_df, variables
        )
        report = dataset.run_validation(build_default_resolver(), dataset_name="test_ds")
        assert report.universe_size == 10
        assert report.dataset_name == "test_ds"


# ──────────────────────────────────────────────────────────────────────────────
# Custom resolver rules on top of the default
# ──────────────────────────────────────────────────────────────────────────────


class TestCustomResolverOnTopOfDefault:
    """Custom high-priority rules override default pipeline dispatch."""

    def test_high_priority_custom_rule_overrides_default(self) -> None:
        from dqf.checks.cross_sectional.range_check import RangeCheck

        universe_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3], "score": [0.1, 0.5, 0.9]})
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE)
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, score FROM v", universe_df, vars_df, variables
        )

        resolver = build_default_resolver()
        # Priority 100 beats the default NUMERIC_CONTINUOUS rule at priority 15
        resolver.register(
            predicate=lambda v: (
                v.dtype == DataType.NUMERIC_CONTINUOUS and v.role == VariableRole.FEATURE
            ),
            pipeline_factory=lambda: CheckPipeline(
                [("range", RangeCheck(min_value=0.0, max_value=1.0))]
            ),
            priority=100,
        )

        report = dataset.run_validation(resolver)
        names = [r.check_name for r in report.variable_results["score"]]
        assert names == ["range"]

    def test_low_priority_custom_rule_does_not_override_default(self) -> None:
        from dqf.checks.cross_sectional.allowed_values import AllowedValuesCheck

        universe_df = pd.DataFrame({"entity_id": [1, 2, 3]})
        vars_df = pd.DataFrame({"entity_id": [1, 2, 3], "score": [0.1, 0.5, 0.9]})
        variables = [
            Variable(name="score", dtype=DataType.NUMERIC_CONTINUOUS, role=VariableRole.FEATURE)
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, score FROM v", universe_df, vars_df, variables
        )

        resolver = build_default_resolver()
        # Priority 1 loses to the default NUMERIC_CONTINUOUS rule at priority 15
        resolver.register(
            predicate=lambda v: v.dtype == DataType.NUMERIC_CONTINUOUS,
            pipeline_factory=lambda: CheckPipeline(
                [("allowed", AllowedValuesCheck(allowed_values=[0.1, 0.5, 0.9]))]
            ),
            priority=1,
        )

        report = dataset.run_validation(resolver)
        names = [r.check_name for r in report.variable_results["score"]]
        # Default resolver wins — gets null_rate (no time_field)
        assert "null_rate" in names
        assert "allowed" not in names

    def test_max_cardinality_threshold_propagates_end_to_end(self) -> None:
        universe_df = pd.DataFrame({"entity_id": range(1, 11)})
        vars_df = pd.DataFrame(
            {
                "entity_id": range(1, 11),
                "category": [str(i) for i in range(10)],  # 10 distinct values
            }
        )
        variables = [
            Variable(name="category", dtype=DataType.CATEGORICAL, role=VariableRole.FEATURE)
        ]
        dataset = _make_dataset(
            _UNIVERSE_SQL, "SELECT entity_id, category FROM v", universe_df, vars_df, variables
        )

        # max_categorical_cardinality=5 → 10 distinct values → WARNING
        report = dataset.run_validation(build_default_resolver(max_categorical_cardinality=5))
        card_result = next(
            r for r in report.variable_results["category"] if r.check_name == "cardinality"
        )
        assert card_result.passed is False
        assert card_result.severity == Severity.WARNING
