"""SeasonalityCheck — detects unexpected changes in seasonal patterns (Kruskal-Wallis)."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from scipy import stats

from dqf.checks.base import BaseLongitudinalCheck
from dqf.checks.longitudinal import figures
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    import pandas as pd

    from dqf.datasets.variables import VariablesDataset


class SeasonalityCheck(BaseLongitudinalCheck):
    """Fails when period-level means differ significantly across seasonal positions.

    Groups each period into its position within the season cycle (e.g. month 1-12
    for yearly seasonality) and applies the Kruskal-Wallis H-test across groups.
    A significant result (p <= p_threshold) indicates the metric varies
    systematically by season, which may signal unexpected seasonal drift.

    Requires at least ``2 * season_length`` periods to run.

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.
    period:
        Truncation period (e.g. ``"month"``).
    season_length:
        Number of periods in one seasonal cycle (e.g. 12 for monthly + yearly).
    p_threshold:
        Kruskal-Wallis p-value threshold; significant when p <= threshold.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        time_field: str,
        period: str = "month",
        season_length: int = 12,
        p_threshold: float = 0.05,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._time_field = time_field
        self._period = period
        self._season_length = season_length
        self._p_threshold = p_threshold
        self._severity = severity

    @property
    def name(self) -> str:
        return "seasonality"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {
            "season_length": self._season_length,
            "p_threshold": self._p_threshold,
            "time_field": self._time_field,
            "period": self._period,
        }

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        population_size = len(dataset.universe.materialise())
        sql_template = self.aggregation_sql(variable.name, self._time_field, self._period)
        sql = sql_template.format(source=self._strip_source(dataset.sql))
        timeseries_df = dataset.adapter.execute(sql)
        return self._compute(timeseries_df, variable, population_size)

    def _compute(
        self, timeseries_df: pd.DataFrame, variable: Variable, population_size: int
    ) -> CheckResult:
        min_periods = self._season_length * 2
        n = len(timeseries_df)
        if n < min_periods:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=None,
                population_size=population_size,
                threshold=self._p_threshold,
                metadata={
                    "skipped": True,
                    "reason": f"Need >= {min_periods} periods, got {n}",
                },
            )
        values = timeseries_df["metric"].tolist()
        groups: dict[int, list[float]] = {}
        for i, val in enumerate(values):
            label = i % self._season_length
            groups.setdefault(label, []).append(val)
        valid_groups = [g for g in groups.values() if len(g) >= 2]
        if len(valid_groups) < 2:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=None,
                population_size=population_size,
                threshold=self._p_threshold,
                metadata={"skipped": True, "reason": "Insufficient groups for Kruskal-Wallis"},
            )
        _constant_result = CheckResult(
            check_name=self.name,
            passed=True,
            severity=self.severity,
            observed_value=0.0,
            population_size=population_size,
            threshold=self._p_threshold,
            metadata={
                "n_periods": n,
                "n_seasons": self._season_length,
                "note": "constant series",
            },
        )
        try:
            stat, p_value = stats.kruskal(*valid_groups)
        except ValueError:
            # scipy <1.11 raises ValueError when all values are identical
            return _constant_result
        stat_f = float(stat)
        p_f = float(p_value)
        # scipy >=1.11 returns NaN when all values are identical — no seasonal signal
        if math.isnan(p_f) or math.isnan(stat_f):
            return _constant_result
        passed = p_f > self._p_threshold
        return CheckResult(
            check_name=self.name,
            passed=passed,
            severity=self.severity,
            observed_value=round(stat_f, 4),
            population_size=population_size,
            threshold=self._p_threshold,
            metadata={
                "p_value": round(p_f, 6),
                "n_periods": n,
                "n_seasons": self._season_length,
            },
            figure_factory=figures.seasonality_figure(
                groups, stat_f, p_f, self._p_threshold,
                self._season_length, passed, variable.name,
            ),
        )
