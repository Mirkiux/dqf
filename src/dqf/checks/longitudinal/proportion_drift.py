"""ProportionDriftCheck — sequential Z-test for binary target drift."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from scipy.stats import norm

from dqf.checks.base import BaseLongitudinalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    import pandas as pd

    from dqf.datasets.variables import VariablesDataset

_MIN_PERIODS = 2


def _two_proportion_z_test(n1: int, N1: int, n2: int, N2: int) -> float:
    """Two-sample proportion Z-test (two-tailed).  Returns the p-value.

    Parameters
    ----------
    n1, N1:
        Number of positives and total observations in the baseline.
    n2, N2:
        Number of positives and total observations in the test period.
    """
    if N1 == 0 or N2 == 0:
        return 1.0
    p_pool = (n1 + n2) / (N1 + N2)
    if p_pool <= 0.0 or p_pool >= 1.0:
        # Degenerate: all values identical across both samples
        return 1.0
    se = math.sqrt(p_pool * (1.0 - p_pool) * (1.0 / N1 + 1.0 / N2))
    if se == 0.0:
        return 1.0
    z = ((n1 / N1) - (n2 / N2)) / se
    return float(2.0 * (1.0 - norm.cdf(abs(z))))


class ProportionDriftCheck(BaseLongitudinalCheck):
    """Detects binary proportion drift via a sequential two-proportion Z-test.

    Designed for binary (0/1 or True/False) target variables.  The check splits
    the time series at the midpoint: the first half forms the initial baseline,
    then each subsequent period is tested against the *cumulative* baseline —
    each tested period is appended to the baseline before the next test.

    Fails if any period shows statistically significant drift
    (p-value ≤ *p_threshold*).  The reported ``observed_value`` is the minimum
    p-value observed across all sequential tests (worst case).

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
        return "proportion_drift"

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

    @staticmethod
    def aggregation_sql(variable_name: str, time_field: str, period: str) -> str:
        return (
            f"SELECT DATE_TRUNC('{period}', {time_field}) AS period,"
            + f" SUM(CASE WHEN CAST({variable_name} AS DOUBLE) = 1.0"
            + " THEN 1 ELSE 0 END) AS positive,"
            + f" COUNT({variable_name}) AS n"
            + " FROM ({source}) _vd"
            + " GROUP BY 1 ORDER BY 1"
        )

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        population_size = len(dataset.universe.materialise())
        sql_template = self.aggregation_sql(variable.name, self._time_field, self._period)
        sql = sql_template.format(source=self._strip_source(dataset.sql))
        df = dataset.adapter.execute(sql)
        return self._compute(df, variable, population_size)

    def _compute(self, df: pd.DataFrame, variable: Variable, population_size: int) -> CheckResult:
        n = len(df)
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
        baseline_positive = int(df["positive"].iloc[:half].sum())
        baseline_n = int(df["n"].iloc[:half].sum())

        min_p = 1.0
        for idx in range(half, n):
            row = df.iloc[idx]
            test_positive = int(row["positive"])
            test_n = int(row["n"])

            if test_n > 0 and baseline_n > 0:
                p_value = _two_proportion_z_test(
                    baseline_positive, baseline_n, test_positive, test_n
                )
                if not math.isnan(p_value):
                    min_p = min(min_p, p_value)

            baseline_positive += test_positive
            baseline_n += test_n

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
