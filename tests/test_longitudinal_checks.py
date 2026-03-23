"""Tests for concrete longitudinal checks."""

from __future__ import annotations

import pandas as pd

from dqf import (
    ConceptDriftCheck,
    DistributionDriftCheck,
    MockAdapter,
    SeasonalityCheck,
    StructuralBreakCheck,
    TrendCheck,
    UniverseDataset,
    VariablesDataset,
)
from dqf.enums import DataType, Severity
from dqf.variable import Variable

UNIVERSE_SQL = "SELECT * FROM universe"
SOURCE_SQL = "SELECT * FROM variables"


def make_universe() -> UniverseDataset:
    universe_df = pd.DataFrame({"_uid": range(100)})
    return UniverseDataset(
        sql=UNIVERSE_SQL,
        primary_key=["_uid"],
        adapter=MockAdapter({UNIVERSE_SQL: universe_df}),
    )


def make_dataset(
    timeseries_df: pd.DataFrame, variable_name: str, aggregation_sql: str
) -> VariablesDataset:
    """Build a VariablesDataset where MockAdapter returns timeseries_df for aggregation_sql."""
    variables_df = pd.DataFrame({"_uid": range(100), variable_name: [1.0] * 100})
    adapter = MockAdapter({SOURCE_SQL: variables_df, aggregation_sql: timeseries_df})
    return VariablesDataset(
        sql=SOURCE_SQL,
        primary_key=["_uid"],
        universe=make_universe(),
        join_keys={"_uid": "_uid"},
        adapter=adapter,
    )


def make_variable(name: str) -> Variable:
    return Variable(name=name, dtype=DataType.NUMERIC_CONTINUOUS)


def flat_ts(values: list[float]) -> pd.DataFrame:
    """Build a simple timeseries DataFrame with the given metric values."""
    n = len(values)
    return pd.DataFrame(
        {
            "period": [f"2024-{i + 1:02d}" for i in range(n)],
            "metric": values,
            "n": [100] * n,
        }
    )


# ---------------------------------------------------------------------------
# TrendCheck
# ---------------------------------------------------------------------------


class TestTrendCheck:
    def test_name(self):
        assert TrendCheck(time_field="ts").name == "trend"

    def test_severity_default(self):
        assert TrendCheck(time_field="ts").severity == Severity.FAILURE

    def test_params(self):
        c = TrendCheck(time_field="ts", p_threshold=0.01)
        assert c.params["p_threshold"] == 0.01

    def _check(self, values, p_threshold=0.05, var_name="score"):
        check = TrendCheck(time_field="event_date", period="month", p_threshold=p_threshold)
        ts = flat_ts(values)
        agg_sql = check.aggregation_sql(var_name, "event_date", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, var_name, agg_sql)
        v = make_variable(var_name)
        return check.check(ds, v)

    def test_no_trend_passes(self):
        # Flat series → no trend
        result = self._check([10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 10.0, 10.1])
        assert result.passed is True

    def test_strong_upward_trend_fails(self):
        result = self._check([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        assert result.passed is False

    def test_strong_downward_trend_fails(self):
        result = self._check([8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0])
        assert result.passed is False

    def test_insufficient_periods_skipped(self):
        check = TrendCheck(time_field="event_date", period="month")
        ts = flat_ts([1.0, 2.0, 3.0])
        agg_sql = check.aggregation_sql("score", "event_date", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, "score", agg_sql)
        result = check.check(ds, make_variable("score"))
        assert result.passed is True
        assert result.metadata.get("skipped") is True

    def test_population_size_is_universe_size(self):
        result = self._check([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        assert result.population_size == 100

    def test_metadata_has_p_value(self):
        result = self._check([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        assert "p_value" in result.metadata
        assert "tau" in result.metadata


# ---------------------------------------------------------------------------
# StructuralBreakCheck
# ---------------------------------------------------------------------------


class TestStructuralBreakCheck:
    def test_name(self):
        assert StructuralBreakCheck(time_field="ts").name == "structural_break"

    def test_severity_default(self):
        assert StructuralBreakCheck(time_field="ts").severity == Severity.FAILURE

    def test_params(self):
        c = StructuralBreakCheck(time_field="ts", cusum_threshold=2.0)
        assert c.params["cusum_threshold"] == 2.0

    def _check(self, values, cusum_threshold=1.0, var_name="score"):
        check = StructuralBreakCheck(
            time_field="event_date", period="month", cusum_threshold=cusum_threshold
        )
        ts = flat_ts(values)
        agg_sql = check.aggregation_sql(var_name, "event_date", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, var_name, agg_sql)
        return check.check(ds, make_variable(var_name))

    def test_stable_series_passes(self):
        result = self._check([10.0] * 8, cusum_threshold=1.0)
        assert result.passed is True

    def test_abrupt_shift_fails(self):
        # First 4 periods at 10, last 4 at 100 → structural break
        result = self._check(
            [10.0, 10.0, 10.0, 10.0, 100.0, 100.0, 100.0, 100.0], cusum_threshold=1.0
        )
        assert result.passed is False

    def test_insufficient_periods_skipped(self):
        check = StructuralBreakCheck(time_field="ts", period="month")
        ts = flat_ts([1.0, 2.0])
        agg_sql = check.aggregation_sql("score", "ts", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, "score", agg_sql)
        result = check.check(ds, make_variable("score"))
        assert result.passed is True
        assert result.metadata.get("skipped") is True

    def test_zero_std_passes(self):
        # Identical values → std=0 → cusum_stat=0 → passes
        result = self._check([5.0] * 8)
        assert result.passed is True
        assert result.observed_value == 0.0


# ---------------------------------------------------------------------------
# SeasonalityCheck
# ---------------------------------------------------------------------------


class TestSeasonalityCheck:
    def test_name(self):
        assert SeasonalityCheck(time_field="ts").name == "seasonality"

    def test_params(self):
        c = SeasonalityCheck(time_field="ts", season_length=4)
        assert c.params["season_length"] == 4

    def _check(self, values, season_length=4, p_threshold=0.05):
        check = SeasonalityCheck(
            time_field="ts",
            period="month",
            season_length=season_length,
            p_threshold=p_threshold,
        )
        ts = flat_ts(values)
        agg_sql = check.aggregation_sql("score", "ts", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, "score", agg_sql)
        return check.check(ds, make_variable("score"))

    def test_insufficient_periods_skipped(self):
        # Need season_length*2 = 8 periods, only give 4
        result = self._check([1.0, 2.0, 3.0, 4.0], season_length=4)
        assert result.passed is True
        assert result.metadata.get("skipped") is True

    def test_flat_series_passes(self):
        # Flat values → no seasonal variation → passes (p > threshold)
        values = [10.0] * 12  # 3 full cycles of season_length=4 (12 periods)
        result = self._check(values, season_length=4)
        assert result.passed is True

    def test_population_size(self):
        values = [float(i % 4) for i in range(12)]
        result = self._check(values, season_length=4)
        assert result.population_size == 100


# ---------------------------------------------------------------------------
# DistributionDriftCheck
# ---------------------------------------------------------------------------


class TestDistributionDriftCheck:
    def test_name(self):
        assert DistributionDriftCheck(time_field="ts").name == "distribution_drift"

    def test_severity_default(self):
        assert DistributionDriftCheck(time_field="ts").severity == Severity.FAILURE

    def test_params(self):
        c = DistributionDriftCheck(time_field="ts", psi_threshold=0.1)
        assert c.params["psi_threshold"] == 0.1

    def _check(self, values, psi_threshold=0.2, reference=None):
        check = DistributionDriftCheck(time_field="ts", period="month", psi_threshold=psi_threshold)
        if reference is not None:
            check.set_reference(reference)
        ts = flat_ts(values)
        agg_sql = check.aggregation_sql("score", "ts", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, "score", agg_sql)
        return check.check(ds, make_variable("score"))

    def test_stable_series_passes(self):
        # Both halves are identical → PSI = 0
        values = [10.0, 11.0, 9.5, 10.5, 10.0, 11.0, 9.5, 10.5]
        result = self._check(values)
        assert result.passed is True

    def test_large_drift_fails(self):
        # First half ~10, second half ~100 → large PSI
        values = [10.0, 10.0, 10.0, 10.0, 100.0, 100.0, 100.0, 100.0]
        result = self._check(values, psi_threshold=0.1)
        assert result.passed is False

    def test_with_reference_stable_passes(self):
        # Identical reference and current → PSI = 0
        reference = [10.0, 11.0, 9.5, 10.5]
        current = [10.0, 11.0, 9.5, 10.5]
        result = self._check(current, reference=reference)
        assert result.passed is True

    def test_with_reference_drift_fails(self):
        reference = [10.0, 10.0, 10.0, 10.0]
        current = [100.0, 100.0, 100.0, 100.0]
        result = self._check(current, psi_threshold=0.1, reference=reference)
        assert result.passed is False

    def test_insufficient_periods_skipped(self):
        result = self._check([1.0, 2.0, 3.0])
        assert result.passed is True
        assert result.metadata.get("skipped") is True

    def test_metadata_has_window_info(self):
        result = self._check([10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0])
        assert "n_reference_periods" in result.metadata
        assert "n_current_periods" in result.metadata


# ---------------------------------------------------------------------------
# ConceptDriftCheck
# ---------------------------------------------------------------------------


class TestConceptDriftCheck:
    def test_name(self):
        assert ConceptDriftCheck(time_field="ts").name == "concept_drift"

    def test_inherits_distribution_drift(self):
        assert isinstance(ConceptDriftCheck(time_field="ts"), DistributionDriftCheck)

    def test_stable_passes(self):
        check = ConceptDriftCheck(time_field="ts", period="month")
        ts = flat_ts([10.0, 11.0, 9.5, 10.5, 10.0, 11.0, 9.5, 10.5])
        agg_sql = check.aggregation_sql("score", "ts", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, "score", agg_sql)
        result = check.check(ds, make_variable("score"))
        assert result.passed is True
        assert result.check_name == "concept_drift"

    def test_drift_fails(self):
        check = ConceptDriftCheck(time_field="ts", period="month", psi_threshold=0.1)
        ts = flat_ts([10.0, 10.0, 10.0, 10.0, 100.0, 100.0, 100.0, 100.0])
        agg_sql = check.aggregation_sql("score", "ts", "month").format(source=SOURCE_SQL)
        ds = make_dataset(ts, "score", agg_sql)
        result = check.check(ds, make_variable("score"))
        assert result.passed is False
