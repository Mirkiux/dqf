"""Tests for figure_factory on longitudinal CheckResult instances.

Each longitudinal check should attach a non-None figure_factory when it
produces a real result (sufficient data) and None when it skips (insufficient
periods).  render_figure() must return a matplotlib Figure without errors.
"""

from __future__ import annotations

import pandas as pd
from matplotlib.figure import Figure

from dqf.checks.longitudinal.chisquared_drift import ChiSquaredDriftCheck
from dqf.checks.longitudinal.distribution_drift import DistributionDriftCheck
from dqf.checks.longitudinal.ks_drift import KSDriftCheck
from dqf.checks.longitudinal.proportion_drift import ProportionDriftCheck
from dqf.checks.longitudinal.seasonality import SeasonalityCheck
from dqf.checks.longitudinal.structural_break import StructuralBreakCheck
from dqf.checks.longitudinal.trend import TrendCheck
from dqf.enums import DataType
from dqf.variable import Variable

_VAR = Variable(name="x", dtype=DataType.NUMERIC_CONTINUOUS)
_POP = 100


# ── Helpers ────────────────────────────────────────────────────────────────────


def _ts_df(n: int) -> pd.DataFrame:
    """Simple (period, metric, n) DataFrame with *n* rows."""
    return pd.DataFrame(
        {
            "period": [f"2024-{i:02d}" for i in range(1, n + 1)],
            "metric": [float(i) for i in range(1, n + 1)],
            "n": [10] * n,
        }
    )


def _cat_df(n_periods: int) -> pd.DataFrame:
    """(period, category, count) DataFrame with *n_periods* periods, 2 categories each."""
    rows = []
    for i in range(1, n_periods + 1):
        rows.append({"period": f"2024-{i:02d}", "category": "A", "count": 10})
        rows.append({"period": f"2024-{i:02d}", "category": "B", "count": 5})
    return pd.DataFrame(rows)


def _ks_df(n_periods: int, vals_per_period: int = 5) -> pd.DataFrame:
    """(period, value) DataFrame with *n_periods* periods."""
    rows = []
    for i in range(1, n_periods + 1):
        for j in range(vals_per_period):
            rows.append({"period": f"2024-{i:02d}", "value": float(i * vals_per_period + j)})
    return pd.DataFrame(rows)


def _prop_df(n: int) -> pd.DataFrame:
    """(period, positive, n) DataFrame with *n* rows."""
    return pd.DataFrame(
        {
            "period": [f"2024-{i:02d}" for i in range(1, n + 1)],
            "positive": [5] * n,
            "n": [10] * n,
        }
    )


# ── TrendCheck ─────────────────────────────────────────────────────────────────


class TestTrendFigure:
    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = TrendCheck("ts")._compute(_ts_df(5), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = TrendCheck("ts")._compute(_ts_df(5), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = TrendCheck("ts")._compute(_ts_df(3), _VAR, _POP)  # < 4 periods
        assert result.figure_factory is None


# ── StructuralBreakCheck ───────────────────────────────────────────────────────


class TestStructuralBreakFigure:
    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = StructuralBreakCheck("ts")._compute(_ts_df(5), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = StructuralBreakCheck("ts")._compute(_ts_df(5), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = StructuralBreakCheck("ts")._compute(_ts_df(3), _VAR, _POP)  # < 4 periods
        assert result.figure_factory is None


# ── SeasonalityCheck ───────────────────────────────────────────────────────────


class TestSeasonalityFigure:
    # season_length=2 → min_periods=4; use 6 rows for two full cycles
    _check = SeasonalityCheck("ts", season_length=2)

    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = self._check._compute(_ts_df(6), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = self._check._compute(_ts_df(6), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = self._check._compute(_ts_df(3), _VAR, _POP)  # < 4 periods
        assert result.figure_factory is None


# ── ChiSquaredDriftCheck ───────────────────────────────────────────────────────


class TestChiSquaredDriftFigure:
    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = ChiSquaredDriftCheck("ts")._compute(_cat_df(4), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = ChiSquaredDriftCheck("ts")._compute(_cat_df(4), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = ChiSquaredDriftCheck("ts")._compute(_cat_df(1), _VAR, _POP)  # 1 period
        assert result.figure_factory is None


# ── KSDriftCheck ───────────────────────────────────────────────────────────────


class TestKSDriftFigure:
    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = KSDriftCheck("ts")._compute(_ks_df(4), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = KSDriftCheck("ts")._compute(_ks_df(4), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = KSDriftCheck("ts")._compute(_ks_df(1), _VAR, _POP)  # 1 period
        assert result.figure_factory is None


# ── ProportionDriftCheck ───────────────────────────────────────────────────────


class TestProportionDriftFigure:
    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = ProportionDriftCheck("ts")._compute(_prop_df(4), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = ProportionDriftCheck("ts")._compute(_prop_df(4), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = ProportionDriftCheck("ts")._compute(_prop_df(1), _VAR, _POP)  # 1 period
        assert result.figure_factory is None


# ── DistributionDriftCheck ─────────────────────────────────────────────────────


class TestDistributionDriftFigure:
    def test_figure_factory_set_when_sufficient_data(self) -> None:
        result = DistributionDriftCheck("ts")._compute(_ts_df(5), _VAR, _POP)
        assert result.figure_factory is not None

    def test_render_returns_figure(self) -> None:
        result = DistributionDriftCheck("ts")._compute(_ts_df(5), _VAR, _POP)
        assert isinstance(result.render_figure(), Figure)

    def test_figure_factory_none_when_skipped(self) -> None:
        result = DistributionDriftCheck("ts")._compute(_ts_df(3), _VAR, _POP)  # < 4 periods
        assert result.figure_factory is None
