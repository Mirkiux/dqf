"""TrendCheck — detects statistically significant monotonic trends (Mann-Kendall)."""

from __future__ import annotations

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

_MIN_PERIODS = 4


class TrendCheck(BaseLongitudinalCheck):
    """Fails when a statistically significant monotonic trend is detected.

    Uses Kendall's tau (Mann-Kendall test) via :func:`scipy.stats.kendalltau`.
    A significant trend (p-value <= p_threshold) is a failure — trends indicate
    the metric is drifting over time rather than remaining stable.

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.
    period:
        Truncation period passed to DATE_TRUNC (e.g. ``"month"``, ``"week"``).
    p_threshold:
        p-value threshold; trend is significant when p <= threshold.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        time_field: str,
        period: str = "month",
        p_threshold: float = 0.05,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._time_field = time_field
        self._period = period
        self._p_threshold = p_threshold
        self._severity = severity

    @property
    def name(self) -> str:
        return "trend"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {
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
        n = len(timeseries_df)
        if n < _MIN_PERIODS:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=None,
                population_size=population_size,
                threshold=self._p_threshold,
                metadata={"skipped": True, "reason": f"Need >= {_MIN_PERIODS} periods, got {n}"},
            )
        values = timeseries_df["metric"].tolist()
        tau, p_value = stats.kendalltau(range(n), values)
        tau_f = float(tau)
        p_f = float(p_value)
        passed = p_f > self._p_threshold
        return CheckResult(
            check_name=self.name,
            passed=passed,
            severity=self.severity,
            observed_value=round(tau_f, 4),
            population_size=population_size,
            threshold=self._p_threshold,
            metadata={"p_value": round(p_f, 6), "tau": round(tau_f, 4), "n_periods": n},
            figure_factory=figures.trend_figure(
                timeseries_df, tau_f, p_f, self._p_threshold, passed, variable.name
            ),
        )
