"""StructuralBreakCheck — detects level shifts in a metric time series via CUSUM."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np

from dqf.checks.base import BaseLongitudinalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    import pandas as pd

    from dqf.datasets.variables import VariablesDataset

_MIN_PERIODS = 4


class StructuralBreakCheck(BaseLongitudinalCheck):
    """Fails when the CUSUM statistic exceeds *cusum_threshold* standard deviations.

    The CUSUM (cumulative sum of mean-adjusted deviations) detects abrupt level
    shifts in a time series of period-aggregated metrics.  A large CUSUM peak
    relative to the series standard deviation signals a structural break.

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.
    period:
        Truncation period (e.g. ``"month"``).
    cusum_threshold:
        Maximum allowed CUSUM statistic (in standard deviations). Default 1.0.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        time_field: str,
        period: str = "month",
        cusum_threshold: float = 1.0,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._time_field = time_field
        self._period = period
        self._cusum_threshold = cusum_threshold
        self._severity = severity

    @property
    def name(self) -> str:
        return "structural_break"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {
            "cusum_threshold": self._cusum_threshold,
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
                threshold=self._cusum_threshold,
                metadata={"skipped": True, "reason": f"Need >= {_MIN_PERIODS} periods, got {n}"},
            )
        values: np.ndarray[Any, np.dtype[np.float64]] = timeseries_df["metric"].to_numpy(
            dtype=float
        )
        mean = float(values.mean())
        std = float(values.std(ddof=1))
        if std == 0.0:
            cusum_stat = 0.0
        else:
            cusum = np.cumsum(values - mean)
            cusum_stat = float(np.abs(cusum).max() / std)
        return CheckResult(
            check_name=self.name,
            passed=cusum_stat <= self._cusum_threshold,
            severity=self.severity,
            observed_value=round(cusum_stat, 4),
            population_size=population_size,
            threshold=self._cusum_threshold,
            metadata={
                "n_periods": n,
                "series_mean": round(mean, 4),
                "series_std": round(std, 4),
            },
        )
