"""KSDriftCheck — sequential Kolmogorov-Smirnov test for continuous target drift."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from scipy.stats import ks_2samp

from dqf.checks.base import BaseLongitudinalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    import pandas as pd

    from dqf.datasets.variables import VariablesDataset

_MIN_PERIODS = 2


class KSDriftCheck(BaseLongitudinalCheck):
    """Detects continuous distribution drift via a sequential KS test.

    Designed for continuous numeric target variables.  The check splits the
    time series at the midpoint: the first half forms the initial baseline,
    then each subsequent period is tested against the *cumulative* baseline
    using the two-sample Kolmogorov-Smirnov test.  Each tested period is
    appended to the baseline before the next test (expanding window).

    Fails if any period shows statistically significant drift
    (p-value ≤ *p_threshold*).  The reported ``observed_value`` is the minimum
    p-value observed across all sequential tests (worst case).

    The aggregation SQL returns one row per non-null entity so that per-period
    value distributions are available for the KS test.  For large datasets
    consider using a sampled source query.

    Requires at least 2 periods.

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.
    period:
        DATE_TRUNC period (e.g. ``"month"``).
    p_threshold:
        Significance level; drift is flagged when p ≤ threshold.  Default ``0.05``.
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
        return "ks_drift"

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

    def aggregation_sql(self, variable_name: str, time_field: str, period: str) -> str:
        return (
            f"SELECT DATE_TRUNC('{period}', {time_field}) AS period,"
            + f" CAST({variable_name} AS DOUBLE) AS value"
            + " FROM ({source}) _vd"
            + f" WHERE {variable_name} IS NOT NULL"
            + " ORDER BY 1"
        )

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        population_size = len(dataset.universe.materialise())
        sql_template = self.aggregation_sql(variable.name, self._time_field, self._period)
        sql = sql_template.format(source=dataset.sql)
        df = dataset.adapter.execute(sql)
        return self._compute(df, variable, population_size)

    def _compute(self, df: pd.DataFrame, variable: Variable, population_size: int) -> CheckResult:
        # Group raw values by period
        period_values: dict[str, list[float]] = {}
        for _, row in df.iterrows():
            p = str(row["period"])
            period_values.setdefault(p, []).append(float(row["value"]))

        periods_sorted = sorted(period_values.keys())
        n = len(periods_sorted)

        if n < _MIN_PERIODS:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=None,
                population_size=population_size,
                threshold=self._p_threshold,
                metadata={
                    "skipped": True,
                    "reason": f"Need >= {_MIN_PERIODS} periods, got {n}",
                },
            )

        half = max(1, n // 2)
        baseline_values: list[float] = []
        for p in periods_sorted[:half]:
            baseline_values.extend(period_values[p])

        min_p = 1.0
        for p in periods_sorted[half:]:
            test_vals = period_values[p]

            if test_vals and baseline_values:
                stat, p_value = ks_2samp(baseline_values, test_vals)
                p_f = float(p_value)
                if not math.isnan(p_f):
                    min_p = min(min_p, p_f)

            baseline_values.extend(test_vals)

        return CheckResult(
            check_name=self.name,
            passed=min_p > self._p_threshold,
            severity=self.severity,
            observed_value=round(min_p, 6),
            population_size=population_size,
            threshold=self._p_threshold,
            metadata={
                "min_p_value": round(min_p, 6),
                "n_periods": n,
                "baseline_periods": half,
            },
        )
