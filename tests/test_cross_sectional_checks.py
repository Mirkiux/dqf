"""Unit tests for concrete cross-sectional checks (plan 10)."""

from __future__ import annotations

import pandas as pd
import pytest

from dqf.adapters.mock_adapter import MockAdapter
from dqf.checks.cross_sectional import (
    AllowedValuesCheck,
    CardinalityCheck,
    NullRateCheck,
    RangeCheck,
    ReferentialIntegrityCheck,
    RegexPatternCheck,
    UniquenessCheck,
)
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.enums import DataType, Severity
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UNIVERSE_SQL = "SELECT * FROM universe"
_VARIABLES_SQL = "SELECT * FROM variables"


def make_dataset(col_name: str, series: pd.Series) -> VariablesDataset:
    """Build a VariablesDataset with a single data column and fully-matched rows."""
    n = len(series)
    universe_df = pd.DataFrame({"_uid": range(n)})
    variables_df = pd.DataFrame({"_uid": range(n), col_name: series})
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


def make_variable(name: str = "x", dtype: DataType = DataType.NUMERIC_CONTINUOUS) -> Variable:
    return Variable(name=name, dtype=dtype)


# ---------------------------------------------------------------------------
# TestNullRateCheck
# ---------------------------------------------------------------------------


class TestNullRateCheck:
    def test_name(self) -> None:
        assert NullRateCheck(threshold=0.1).name == "null_rate"

    def test_default_severity_is_failure(self) -> None:
        assert NullRateCheck(threshold=0.1).severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        assert NullRateCheck(threshold=0.1, severity=Severity.WARNING).severity == Severity.WARNING

    def test_params(self) -> None:
        assert NullRateCheck(threshold=0.2).params == {"threshold": 0.2}

    def test_passes_when_no_nulls(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, 2.0, 3.0]))
        result = NullRateCheck(threshold=0.0).check(ds, make_variable())
        assert result.passed is True

    def test_passes_when_null_rate_at_threshold(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, None, None]))  # rate = 2/3
        result = NullRateCheck(threshold=2 / 3).check(ds, make_variable())
        assert result.passed is True

    def test_fails_when_null_rate_exceeds_threshold(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, None, None]))  # rate = 2/3
        result = NullRateCheck(threshold=0.5).check(ds, make_variable())
        assert result.passed is False

    def test_observed_value_is_null_count(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, None, None]))
        result = NullRateCheck(threshold=1.0).check(ds, make_variable())
        assert result.observed_value == 2

    def test_population_size_is_universe_size(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, 2.0, 3.0]))
        result = NullRateCheck(threshold=0.0).check(ds, make_variable())
        assert result.population_size == 3

    def test_rate_matches_null_fraction(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, None]))
        result = NullRateCheck(threshold=1.0).check(ds, make_variable())
        assert result.rate == pytest.approx(0.5)

    def test_threshold_stored_in_result(self) -> None:
        ds = make_dataset("x", pd.Series([1.0]))
        result = NullRateCheck(threshold=0.1).check(ds, make_variable())
        assert result.threshold == 0.1

    def test_figure_factory_is_callable(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, None]))
        result = NullRateCheck(threshold=1.0).check(ds, make_variable())
        assert callable(result.figure_factory)

    def test_render_figure_returns_figure(self) -> None:
        import matplotlib.figure

        ds = make_dataset("x", pd.Series([1.0, None]))
        result = NullRateCheck(threshold=1.0).check(ds, make_variable())
        fig = result.render_figure()
        assert isinstance(fig, matplotlib.figure.Figure)
        import matplotlib.pyplot as plt

        plt.close("all")


# ---------------------------------------------------------------------------
# TestRangeCheck
# ---------------------------------------------------------------------------


class TestRangeCheck:
    def test_name(self) -> None:
        assert RangeCheck(max_value=10).name == "range"

    def test_default_severity_is_failure(self) -> None:
        assert RangeCheck(max_value=10).severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        assert RangeCheck(max_value=10, severity=Severity.WARNING).severity == Severity.WARNING

    def test_params(self) -> None:
        assert RangeCheck(min_value=0, max_value=10).params == {
            "min_value": 0,
            "max_value": 10,
        }

    def test_raises_when_no_bounds(self) -> None:
        with pytest.raises(ValueError):
            RangeCheck()

    def test_passes_all_in_range(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, 5.0, 10.0]))
        result = RangeCheck(min_value=0, max_value=10).check(ds, make_variable())
        assert result.passed is True

    def test_fails_value_above_max(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, 5.0, 11.0]))
        result = RangeCheck(max_value=10).check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 1

    def test_fails_value_below_min(self) -> None:
        ds = make_dataset("x", pd.Series([-1.0, 5.0, 10.0]))
        result = RangeCheck(min_value=0).check(ds, make_variable())
        assert result.passed is False

    def test_boundary_values_pass(self) -> None:
        ds = make_dataset("x", pd.Series([0.0, 10.0]))
        result = RangeCheck(min_value=0, max_value=10).check(ds, make_variable())
        assert result.passed is True

    def test_nulls_excluded_from_check(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, None, 5.0]))
        result = RangeCheck(min_value=0, max_value=10).check(ds, make_variable())
        assert result.passed is True

    def test_rate_is_violation_fraction(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, 20.0, 30.0, 40.0]))  # 3 violations / 4
        result = RangeCheck(max_value=10).check(ds, make_variable())
        assert result.rate == pytest.approx(3 / 4)

    def test_figure_factory_is_callable(self) -> None:
        ds = make_dataset("x", pd.Series([1.0, 2.0]))
        result = RangeCheck(max_value=10).check(ds, make_variable())
        assert callable(result.figure_factory)

    def test_render_figure_returns_figure(self) -> None:
        import matplotlib.figure
        import matplotlib.pyplot as plt

        ds = make_dataset("x", pd.Series([1.0, 2.0]))
        result = RangeCheck(max_value=10).check(ds, make_variable())
        fig = result.render_figure()
        assert isinstance(fig, matplotlib.figure.Figure)
        plt.close("all")


# ---------------------------------------------------------------------------
# TestAllowedValuesCheck
# ---------------------------------------------------------------------------


class TestAllowedValuesCheck:
    def test_name(self) -> None:
        assert AllowedValuesCheck({"A", "B"}).name == "allowed_values"

    def test_default_severity_is_failure(self) -> None:
        assert AllowedValuesCheck({"A"}).severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        assert AllowedValuesCheck({"A"}, severity=Severity.WARNING).severity == Severity.WARNING

    def test_passes_with_valid_values(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", "A"]))
        result = AllowedValuesCheck({"A", "B"}).check(ds, make_variable())
        assert result.passed is True

    def test_fails_with_invalid_value(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", "C"]))
        result = AllowedValuesCheck({"A", "B"}).check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 1

    def test_accepts_list_as_allowed_values(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B"]))
        result = AllowedValuesCheck(["A", "B"]).check(ds, make_variable())
        assert result.passed is True

    def test_nulls_excluded(self) -> None:
        ds = make_dataset("x", pd.Series(["A", None, "B"]))
        result = AllowedValuesCheck({"A", "B"}).check(ds, make_variable())
        assert result.passed is True

    def test_rate_is_violation_fraction(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "X", "Y", "Z"]))  # 3 violations / 4
        result = AllowedValuesCheck({"A"}).check(ds, make_variable())
        assert result.rate == pytest.approx(3 / 4)

    def test_figure_factory_is_none(self) -> None:
        ds = make_dataset("x", pd.Series(["A"]))
        result = AllowedValuesCheck({"A"}).check(ds, make_variable())
        assert result.figure_factory is None


# ---------------------------------------------------------------------------
# TestCardinalityCheck
# ---------------------------------------------------------------------------


class TestCardinalityCheck:
    def test_name(self) -> None:
        assert CardinalityCheck(max_cardinality=5).name == "cardinality"

    def test_default_severity_is_failure(self) -> None:
        assert CardinalityCheck(max_cardinality=5).severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        check = CardinalityCheck(max_cardinality=5, severity=Severity.WARNING)
        assert check.severity == Severity.WARNING

    def test_raises_when_no_bounds(self) -> None:
        with pytest.raises(ValueError):
            CardinalityCheck()

    def test_passes_within_max(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", "C"]))
        result = CardinalityCheck(max_cardinality=5).check(ds, make_variable())
        assert result.passed is True

    def test_fails_exceeds_max(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", "C", "D", "E", "F"]))
        result = CardinalityCheck(max_cardinality=3).check(ds, make_variable())
        assert result.passed is False

    def test_passes_at_exact_max(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", "C"]))
        result = CardinalityCheck(max_cardinality=3).check(ds, make_variable())
        assert result.passed is True

    def test_fails_below_min(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "A", "A"]))  # cardinality = 1
        result = CardinalityCheck(min_cardinality=2).check(ds, make_variable())
        assert result.passed is False

    def test_passes_above_min(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B"]))
        result = CardinalityCheck(min_cardinality=2).check(ds, make_variable())
        assert result.passed is True

    def test_nulls_excluded(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", None]))
        result = CardinalityCheck(max_cardinality=2).check(ds, make_variable())
        assert result.passed is True

    def test_observed_value_is_cardinality(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B", "C"]))
        result = CardinalityCheck(max_cardinality=10).check(ds, make_variable())
        assert result.observed_value == 3

    def test_no_rate_on_cardinality_result(self) -> None:
        ds = make_dataset("x", pd.Series(["A", "B"]))
        result = CardinalityCheck(max_cardinality=5).check(ds, make_variable())
        assert result.rate is None


# ---------------------------------------------------------------------------
# TestUniquenessCheck
# ---------------------------------------------------------------------------


class TestUniquenessCheck:
    def test_name(self) -> None:
        assert UniquenessCheck().name == "uniqueness"

    def test_default_severity_is_failure(self) -> None:
        assert UniquenessCheck().severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        assert UniquenessCheck(severity=Severity.WARNING).severity == Severity.WARNING

    def test_passes_with_unique_values(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2, 3]))
        result = UniquenessCheck().check(ds, make_variable())
        assert result.passed is True

    def test_fails_with_duplicate(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2, 2]))
        result = UniquenessCheck().check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 1

    def test_nulls_allowed_to_repeat(self) -> None:
        ds = make_dataset("x", pd.Series([1, None, None]))
        result = UniquenessCheck().check(ds, make_variable())
        assert result.passed is True

    def test_rate_is_duplicate_fraction(self) -> None:
        ds = make_dataset("x", pd.Series([1, 1, 1, 4]))  # 2 duplicates / 4
        result = UniquenessCheck().check(ds, make_variable())
        assert result.rate == pytest.approx(2 / 4)

    def test_threshold_is_zero(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2]))
        result = UniquenessCheck().check(ds, make_variable())
        assert result.threshold == 0

    def test_figure_factory_is_none(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2]))
        result = UniquenessCheck().check(ds, make_variable())
        assert result.figure_factory is None


# ---------------------------------------------------------------------------
# TestRegexPatternCheck
# ---------------------------------------------------------------------------


class TestRegexPatternCheck:
    def test_name(self) -> None:
        assert RegexPatternCheck(r"\d+").name == "regex_pattern"

    def test_default_severity_is_failure(self) -> None:
        assert RegexPatternCheck(r"\d+").severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        check = RegexPatternCheck(r"\d+", severity=Severity.WARNING)
        assert check.severity == Severity.WARNING

    def test_params(self) -> None:
        assert RegexPatternCheck(r"\d{4}").params == {"pattern": r"\d{4}"}

    def test_passes_all_matching(self) -> None:
        ds = make_dataset("x", pd.Series(["123", "456", "789"]))
        result = RegexPatternCheck(r"\d+").check(ds, make_variable())
        assert result.passed is True

    def test_fails_non_matching(self) -> None:
        ds = make_dataset("x", pd.Series(["123", "abc", "456"]))
        result = RegexPatternCheck(r"\d+").check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 1

    def test_uses_fullmatch_semantics(self) -> None:
        # "12abc" does not fully match \d+ even though it starts with digits
        ds = make_dataset("x", pd.Series(["12abc"]))
        result = RegexPatternCheck(r"\d+").check(ds, make_variable())
        assert result.passed is False

    def test_nulls_excluded(self) -> None:
        ds = make_dataset("x", pd.Series(["123", None, "456"]))
        result = RegexPatternCheck(r"\d+").check(ds, make_variable())
        assert result.passed is True

    def test_rate_is_violation_fraction(self) -> None:
        ds = make_dataset("x", pd.Series(["123", "abc", "def", "ghi"]))  # 3 violations / 4
        result = RegexPatternCheck(r"\d+").check(ds, make_variable())
        assert result.rate == pytest.approx(3 / 4)

    def test_figure_factory_is_none(self) -> None:
        ds = make_dataset("x", pd.Series(["123"]))
        result = RegexPatternCheck(r"\d+").check(ds, make_variable())
        assert result.figure_factory is None


# ---------------------------------------------------------------------------
# TestReferentialIntegrityCheck
# ---------------------------------------------------------------------------


class TestReferentialIntegrityCheck:
    def test_name(self) -> None:
        assert ReferentialIntegrityCheck({1, 2, 3}).name == "referential_integrity"

    def test_default_severity_is_failure(self) -> None:
        assert ReferentialIntegrityCheck({1}).severity == Severity.FAILURE

    def test_configurable_severity(self) -> None:
        check = ReferentialIntegrityCheck({1}, severity=Severity.WARNING)
        assert check.severity == Severity.WARNING

    def test_params_exposes_reference_count(self) -> None:
        assert ReferentialIntegrityCheck({1, 2, 3}).params == {"reference_count": 3}

    def test_passes_all_values_in_reference(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2, 3]))
        result = ReferentialIntegrityCheck({1, 2, 3, 4}).check(ds, make_variable())
        assert result.passed is True

    def test_fails_orphan_value(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2, 99]))
        result = ReferentialIntegrityCheck({1, 2, 3}).check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 1

    def test_accepts_list_as_reference(self) -> None:
        ds = make_dataset("x", pd.Series([1, 2]))
        result = ReferentialIntegrityCheck([1, 2, 3]).check(ds, make_variable())
        assert result.passed is True

    def test_nulls_excluded(self) -> None:
        ds = make_dataset("x", pd.Series([1, None, 2]))
        result = ReferentialIntegrityCheck({1, 2}).check(ds, make_variable())
        assert result.passed is True

    def test_rate_is_orphan_fraction(self) -> None:
        ds = make_dataset("x", pd.Series([1, 99, 98, 97]))  # 3 orphans / 4
        result = ReferentialIntegrityCheck({1, 2, 3}).check(ds, make_variable())
        assert result.rate == pytest.approx(3 / 4)

    def test_threshold_is_reference_count(self) -> None:
        ds = make_dataset("x", pd.Series([1]))
        result = ReferentialIntegrityCheck({1, 2, 3}).check(ds, make_variable())
        assert result.threshold == 3

    def test_figure_factory_is_none(self) -> None:
        ds = make_dataset("x", pd.Series([1]))
        result = ReferentialIntegrityCheck({1}).check(ds, make_variable())
        assert result.figure_factory is None
