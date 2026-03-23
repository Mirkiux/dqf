"""Tests for ValidationReport — structure, to_dataframe, and render."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from dqf import ValidationReport
from dqf.enums import Severity, ValidationStatus
from dqf.results import CheckResult, ValidationResult


def make_cr(
    check_name="null_rate",
    passed=True,
    severity=Severity.FAILURE,
    observed_value=0,
    population_size=100,
    threshold=0.05,
    rate=0.0,
    figure_factory=None,
):
    return CheckResult(
        check_name=check_name,
        passed=passed,
        severity=severity,
        observed_value=observed_value,
        population_size=population_size,
        threshold=threshold,
        rate=rate,
        figure_factory=figure_factory,
    )


def make_vr(check_name="pk_uniqueness", passed=True):
    return ValidationResult(check_name=check_name, passed=passed)


def make_report(**kwargs):
    defaults = dict(
        dataset_name="test_ds",
        run_timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        universe_size=100,
        dataset_level_checks=[make_vr("pk_uniqueness", passed=True)],
        variable_results={
            "age": [make_cr("null_rate", passed=True)],
        },
    )
    defaults.update(kwargs)
    return ValidationReport(**defaults)


class TestFields:
    def test_dataset_name(self):
        r = make_report(dataset_name="my_ds")
        assert r.dataset_name == "my_ds"

    def test_universe_size(self):
        r = make_report(universe_size=500)
        assert r.universe_size == 500

    def test_variable_results_keys(self):
        r = make_report()
        assert "age" in r.variable_results


class TestOverallStatus:
    def test_all_pass(self):
        r = make_report()
        assert r.overall_status == ValidationStatus.PASSED

    def test_dataset_level_fail(self):
        r = make_report(dataset_level_checks=[make_vr("pk_uniqueness", passed=False)])
        assert r.overall_status == ValidationStatus.FAILED

    def test_failure_severity_fails_overall(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.FAILURE)]})
        assert r.overall_status == ValidationStatus.FAILED

    def test_warning_only_passes_overall(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.WARNING)]})
        assert r.overall_status == ValidationStatus.PASSED


class TestVariableStatuses:
    def test_all_pass(self):
        r = make_report(variable_results={"x": [make_cr(passed=True)]})
        assert r.variable_statuses["x"] == ValidationStatus.PASSED

    def test_failure_fails_variable(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.FAILURE)]})
        assert r.variable_statuses["x"] == ValidationStatus.FAILED

    def test_warning_fails_variable(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.WARNING)]})
        assert r.variable_statuses["x"] == ValidationStatus.FAILED

    def test_empty_results(self):
        r = make_report(variable_results={})
        assert r.variable_statuses == {}


class TestFailedVariables:
    def test_no_failures(self):
        r = make_report()
        assert r.failed_variables() == []

    def test_failure_variable(self):
        r = make_report(
            variable_results={
                "x": [make_cr(passed=False, severity=Severity.FAILURE)],
                "y": [make_cr(passed=True)],
            }
        )
        assert r.failed_variables() == ["x"]

    def test_warning_not_in_failed(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.WARNING)]})
        assert r.failed_variables() == []


class TestWarnedVariables:
    def test_no_warnings(self):
        r = make_report()
        assert r.warned_variables() == []

    def test_warning_variable(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.WARNING)]})
        assert r.warned_variables() == ["x"]

    def test_failure_variable_not_in_warned(self):
        r = make_report(variable_results={"x": [make_cr(passed=False, severity=Severity.FAILURE)]})
        assert r.warned_variables() == []

    def test_mixed_not_in_warned(self):
        r = make_report(
            variable_results={
                "x": [
                    make_cr("c1", passed=False, severity=Severity.FAILURE),
                    make_cr("c2", passed=False, severity=Severity.WARNING),
                ]
            }
        )
        assert r.warned_variables() == []
        assert "x" in r.failed_variables()


class TestToDataframe:
    def test_returns_dataframe(self):
        r = make_report()
        df = r.to_dataframe()
        assert isinstance(df, pd.DataFrame)

    def test_columns(self):
        r = make_report()
        df = r.to_dataframe()
        expected = {
            "variable",
            "check_name",
            "passed",
            "severity",
            "rate",
            "threshold",
            "observed_value",
        }
        assert set(df.columns) == expected

    def test_one_row_per_check(self):
        r = make_report(
            variable_results={
                "x": [make_cr("c1"), make_cr("c2")],
                "y": [make_cr("c3")],
            }
        )
        assert len(r.to_dataframe()) == 3

    def test_values(self):
        r = make_report(variable_results={"age": [make_cr("null_rate", passed=False, rate=0.1)]})
        df = r.to_dataframe()
        assert df.iloc[0]["variable"] == "age"
        assert df.iloc[0]["check_name"] == "null_rate"
        assert df.iloc[0]["passed"] == False  # noqa: E712
        assert df.iloc[0]["rate"] == pytest.approx(0.1)

    def test_empty_results(self):
        r = make_report(variable_results={})
        assert len(r.to_dataframe()) == 0


class TestRender:
    def test_returns_string(self):
        r = make_report()
        assert isinstance(r.render(), str)

    def test_html_structure(self):
        html = make_report().render()
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "</html>" in html

    def test_dataset_name_in_html(self):
        html = make_report(dataset_name="my_dataset").render()
        assert "my_dataset" in html

    def test_status_in_html(self):
        html = make_report().render()
        assert "passed" in html  # ValidationStatus.PASSED.value is lowercase

    def test_variable_name_in_html(self):
        html = make_report(variable_results={"income": [make_cr()]}).render()
        assert "income" in html

    def test_figure_factory_none_no_error(self):
        html = make_report(variable_results={"x": [make_cr(figure_factory=None)]}).render()
        assert "x" in html

    def test_figure_factory_embeds_base64(self):
        import matplotlib.pyplot as plt

        def make_fig():
            fig, ax = plt.subplots()
            ax.plot([1, 2, 3])
            return fig

        html = make_report(variable_results={"x": [make_cr(figure_factory=make_fig)]}).render()
        assert "data:image/png;base64," in html

    def test_render_writes_file(self, tmp_path):
        out = str(tmp_path / "report.html")
        html = make_report().render(output_path=out)
        with open(out, encoding="utf-8") as f:
            assert f.read() == html

    def test_dataset_level_check_in_html(self):
        html = make_report(dataset_level_checks=[make_vr("pk_uniqueness", passed=False)]).render()
        assert "pk_uniqueness" in html

    def test_failed_overall_status_in_html(self):
        html = make_report(
            variable_results={"x": [make_cr(passed=False, severity=Severity.FAILURE)]}
        ).render()
        assert "failed" in html  # ValidationStatus.FAILED.value is lowercase
