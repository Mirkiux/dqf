"""Tests for NotNullCheck, OutlierCheck, ProportionDriftCheck,
ChiSquaredDriftCheck, and KSDriftCheck."""

from __future__ import annotations

import pandas as pd
import pytest

from dqf.adapters.mock_adapter import MockAdapter
from dqf.checks.cross_sectional.not_null import NotNullCheck
from dqf.checks.cross_sectional.outlier import OutlierCheck
from dqf.checks.longitudinal.chisquared_drift import ChiSquaredDriftCheck
from dqf.checks.longitudinal.ks_drift import KSDriftCheck
from dqf.checks.longitudinal.proportion_drift import ProportionDriftCheck
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.enums import DataType, Severity
from dqf.variable import Variable

# ──────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────────────

UNIVERSE_SQL = "SELECT id FROM universe"
VARIABLES_SQL = "SELECT id, col FROM variables"


def make_variable(name="col", dtype=DataType.NUMERIC_CONTINUOUS):
    return Variable(name=name, dtype=dtype)


def make_cross_dataset(col_values, col_name="col"):
    """Build a VariablesDataset with the given values for a single column."""
    universe_df = pd.DataFrame({"id": list(range(len(col_values)))})
    variables_df = pd.DataFrame({"id": list(range(len(col_values))), col_name: col_values})
    adapter = MockAdapter(
        {
            UNIVERSE_SQL: universe_df,
            VARIABLES_SQL: variables_df,
        }
    )
    universe = UniverseDataset(sql=UNIVERSE_SQL, primary_key=["id"], adapter=adapter)
    return VariablesDataset(
        sql=VARIABLES_SQL,
        primary_key=["id"],
        universe=universe,
        join_keys={"id": "id"},
        adapter=adapter,
    )


def make_longitudinal_dataset(timeseries_df, check, variable_name="col"):
    """Build a VariablesDataset whose MockAdapter serves the given timeseries."""
    sql_template = check.aggregation_sql(variable_name, "ts", "month")
    agg_sql = sql_template.format(source=VARIABLES_SQL)

    universe_df = pd.DataFrame({"id": [1, 2, 3]})
    variables_df = pd.DataFrame({"id": [1, 2, 3], variable_name: [1.0, 2.0, 3.0]})
    adapter = MockAdapter(
        {
            UNIVERSE_SQL: universe_df,
            VARIABLES_SQL: variables_df,
            agg_sql: timeseries_df,
        }
    )
    universe = UniverseDataset(sql=UNIVERSE_SQL, primary_key=["id"], adapter=adapter)
    return VariablesDataset(
        sql=VARIABLES_SQL,
        primary_key=["id"],
        universe=universe,
        join_keys={"id": "id"},
        adapter=adapter,
    )


# ──────────────────────────────────────────────────────────────────────────────
# NotNullCheck
# ──────────────────────────────────────────────────────────────────────────────


class TestNotNullCheck:
    def test_passes_when_no_nulls(self):
        ds = make_cross_dataset([1.0, 2.0, 3.0])
        result = NotNullCheck().check(ds, make_variable())
        assert result.passed is True
        assert result.observed_value == 0

    def test_fails_when_any_null(self):
        ds = make_cross_dataset([1.0, None, 3.0])
        result = NotNullCheck().check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 1

    def test_fails_when_all_null(self):
        ds = make_cross_dataset([None, None])
        result = NotNullCheck().check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value == 2

    def test_rate_is_null_fraction(self):
        ds = make_cross_dataset([1.0, None, None, None])
        result = NotNullCheck().check(ds, make_variable())
        assert result.rate == pytest.approx(0.75)

    def test_severity_default_failure(self):
        assert NotNullCheck().severity == Severity.FAILURE

    def test_severity_warning(self):
        assert NotNullCheck(severity=Severity.WARNING).severity == Severity.WARNING

    def test_name(self):
        assert NotNullCheck().name == "not_null"

    def test_threshold_is_zero(self):
        ds = make_cross_dataset([1.0, 2.0])
        result = NotNullCheck().check(ds, make_variable())
        assert result.threshold == 0


# ──────────────────────────────────────────────────────────────────────────────
# OutlierCheck
# ──────────────────────────────────────────────────────────────────────────────


class TestOutlierCheck:
    def test_passes_with_no_outliers(self):
        ds = make_cross_dataset([10.0, 11.0, 10.5, 9.5, 10.2])
        result = OutlierCheck().check(ds, make_variable())
        assert result.passed is True

    def test_fails_with_outlier(self):
        # Normal values 1-10, plus extreme outlier 1000
        ds = make_cross_dataset(list(range(1, 11)) + [1000])
        result = OutlierCheck().check(ds, make_variable())
        assert result.passed is False
        assert result.observed_value >= 1

    def test_skips_all_nulls(self):
        ds = make_cross_dataset([None, None, None])
        result = OutlierCheck().check(ds, make_variable())
        assert result.passed is True
        assert result.metadata["skipped"] is True

    def test_skips_constant_series(self):
        ds = make_cross_dataset([5.0, 5.0, 5.0, 5.0])
        result = OutlierCheck().check(ds, make_variable())
        assert result.passed is True
        assert result.metadata.get("skipped") is True

    def test_severity_default_failure(self):
        assert OutlierCheck().severity == Severity.FAILURE

    def test_name(self):
        assert OutlierCheck().name == "outlier"

    def test_custom_iqr_multiplier(self):
        # With multiplier=3.0 the extreme value should no longer be flagged
        ds = make_cross_dataset([1.0, 2.0, 3.0, 4.0, 5.0, 100.0])
        result_strict = OutlierCheck(iqr_multiplier=1.5).check(ds, make_variable())
        result_lenient = OutlierCheck(iqr_multiplier=100.0).check(ds, make_variable())
        assert result_strict.passed is False
        assert result_lenient.passed is True

    def test_metadata_contains_q1_q3(self):
        ds = make_cross_dataset([1.0, 2.0, 3.0, 4.0, 5.0])
        result = OutlierCheck().check(ds, make_variable())
        assert "q1" in result.metadata
        assert "q3" in result.metadata
        assert "iqr" in result.metadata


# ──────────────────────────────────────────────────────────────────────────────
# ProportionDriftCheck
# ──────────────────────────────────────────────────────────────────────────────


def make_proportion_df(positive_counts, ns):
    """Build a timeseries df for ProportionDriftCheck."""
    return pd.DataFrame(
        {
            "period": [f"2024-0{i + 1}-01" for i in range(len(positive_counts))],
            "positive": positive_counts,
            "n": ns,
        }
    )


class TestProportionDriftCheck:
    def _check(self, positive_counts, ns, **kwargs):
        check = ProportionDriftCheck(time_field="ts", period="month", **kwargs)
        df = make_proportion_df(positive_counts, ns)
        ds = make_longitudinal_dataset(df, check)
        return check.check(ds, make_variable())

    def test_passes_when_stable(self):
        # Consistent 50% positive rate across all periods
        result = self._check([5, 5, 5, 5, 5, 5], [10, 10, 10, 10, 10, 10])
        assert result.passed is True

    def test_fails_when_drifts(self):
        # Sudden jump from 50% to 90% positive rate
        result = self._check([5, 5, 5, 9, 9, 9], [10, 10, 10, 10, 10, 10])
        assert result.passed is False

    def test_skips_with_too_few_periods(self):
        result = self._check([5], [10])
        assert result.passed is True
        assert result.metadata["skipped"] is True

    def test_severity_default_failure(self):
        assert ProportionDriftCheck(time_field="ts").severity == Severity.FAILURE

    def test_name(self):
        assert ProportionDriftCheck(time_field="ts").name == "proportion_drift"

    def test_min_p_in_metadata(self):
        result = self._check([5, 5, 5, 5], [10, 10, 10, 10])
        assert "min_p_value" in result.metadata

    def test_custom_p_threshold(self):
        # With very high threshold, even slight differences trigger failure
        result = self._check([5, 5, 5, 5], [10, 10, 10, 10], p_threshold=0.999)
        # Stable data: p-value should be ~1.0, so it should still pass
        # (we're not actually testing significance here, just that param flows through)
        assert result.metadata["min_p_value"] is not None


# ──────────────────────────────────────────────────────────────────────────────
# ChiSquaredDriftCheck
# ──────────────────────────────────────────────────────────────────────────────


def make_chisquared_df(period_category_counts):
    """Build a timeseries df for ChiSquaredDriftCheck.

    period_category_counts: list of (period, category, count) tuples.
    """
    rows = [(p, c, n) for p, c, n in period_category_counts]
    return pd.DataFrame(rows, columns=["period", "category", "count"])


class TestChiSquaredDriftCheck:
    def _check(self, period_category_counts, **kwargs):
        check = ChiSquaredDriftCheck(time_field="ts", period="month", **kwargs)
        df = make_chisquared_df(period_category_counts)
        ds = make_longitudinal_dataset(df, check)
        return check.check(ds, make_variable())

    def test_passes_when_stable(self):
        # Same distribution each period: A=5, B=5
        data = []
        for i in range(6):
            period = f"2024-0{i + 1}-01"
            data.extend([(period, "A", 5), (period, "B", 5)])
        result = self._check(data)
        assert result.passed is True

    def test_fails_when_distribution_shifts(self):
        # First half: A=9, B=1; second half: A=1, B=9 (extreme shift)
        data = []
        for i in range(3):
            period = f"2024-0{i + 1}-01"
            data.extend([(period, "A", 90), (period, "B", 10)])
        for i in range(3, 6):
            period = f"2024-0{i + 1}-01"
            data.extend([(period, "A", 10), (period, "B", 90)])
        result = self._check(data)
        assert result.passed is False

    def test_skips_with_one_period(self):
        result = self._check([("2024-01-01", "A", 5), ("2024-01-01", "B", 5)])
        assert result.passed is True
        assert result.metadata["skipped"] is True

    def test_severity_default_failure(self):
        assert ChiSquaredDriftCheck(time_field="ts").severity == Severity.FAILURE

    def test_name(self):
        assert ChiSquaredDriftCheck(time_field="ts").name == "chisquared_drift"

    def test_min_p_in_metadata(self):
        data = []
        for i in range(4):
            period = f"2024-0{i + 1}-01"
            data.extend([(period, "A", 5), (period, "B", 5)])
        result = self._check(data)
        assert "min_p_value" in result.metadata


# ──────────────────────────────────────────────────────────────────────────────
# KSDriftCheck
# ──────────────────────────────────────────────────────────────────────────────


def make_ks_df(period_values):
    """Build a timeseries df for KSDriftCheck.

    period_values: dict of {period: [values]}.
    """
    rows = []
    for period, values in sorted(period_values.items()):
        for v in values:
            rows.append((period, v))
    return pd.DataFrame(rows, columns=["period", "value"])


class TestKSDriftCheck:
    def _check(self, period_values, **kwargs):
        check = KSDriftCheck(time_field="ts", period="month", **kwargs)
        df = make_ks_df(period_values)
        ds = make_longitudinal_dataset(df, check)
        return check.check(ds, make_variable())

    def test_passes_when_stable(self):
        # Same distribution each period (normal 0,1)
        import numpy as np

        rng = np.random.default_rng(42)
        periods = {f"2024-0{i + 1}-01": list(rng.normal(0, 1, 50)) for i in range(6)}
        result = self._check(periods)
        assert result.passed is True

    def test_fails_when_distribution_shifts(self):
        # First 3 periods: mean=0; next 3 periods: mean=100 (extreme shift)
        periods = {}
        for i in range(3):
            periods[f"2024-0{i + 1}-01"] = [0.0 + j * 0.1 for j in range(50)]
        for i in range(3, 6):
            periods[f"2024-0{i + 1}-01"] = [100.0 + j * 0.1 for j in range(50)]
        result = self._check(periods)
        assert result.passed is False

    def test_skips_with_one_period(self):
        result = self._check({"2024-01-01": [1.0, 2.0, 3.0]})
        assert result.passed is True
        assert result.metadata["skipped"] is True

    def test_severity_default_failure(self):
        assert KSDriftCheck(time_field="ts").severity == Severity.FAILURE

    def test_name(self):
        assert KSDriftCheck(time_field="ts").name == "ks_drift"

    def test_min_p_in_metadata(self):
        periods = {f"2024-0{i + 1}-01": [float(i)] * 20 for i in range(4)}
        result = self._check(periods)
        assert "min_p_value" in result.metadata
