"""ChiSquaredDriftCheck — sequential Pearson chi-squared test for categorical drift."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from scipy.stats import chi2_contingency

from dqf.checks.base import BaseLongitudinalCheck
from dqf.checks.longitudinal import figures
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    import pandas as pd

    from dqf.datasets.variables import VariablesDataset

_MIN_PERIODS = 2


class ChiSquaredDriftCheck(BaseLongitudinalCheck):
    """Detects categorical distribution drift via a sequential chi-squared test.

    Designed for categorical and numeric-discrete target variables.  The check
    splits the time series at the midpoint: the first half forms the initial
    baseline category distribution, then each subsequent period is tested
    against the *cumulative* baseline using Pearson's chi-squared test.
    Each tested period is appended to the baseline before the next test
    (expanding window).

    Fails if any period shows statistically significant drift
    (p-value ≤ *p_threshold*).  The reported ``observed_value`` is the minimum
    p-value observed across all sequential tests (worst case).

    The aggregation SQL returns one row per (period, category) so that
    category-level counts are available for the contingency table construction.
    Null values are excluded at the SQL level.

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
        return "chisquared_drift"

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
            + f" CAST({variable_name} AS VARCHAR(1000)) AS category,"
            + " COUNT(*) AS count"
            + " FROM ({source}) _vd"
            + f" WHERE {variable_name} IS NOT NULL"
            + " GROUP BY 1, 2 ORDER BY 1, 2"
        )

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        population_size = len(dataset.universe.materialise())
        sql_template = self.aggregation_sql(variable.name, self._time_field, self._period)
        sql = sql_template.format(source=self._strip_source(dataset.sql))
        df = dataset.adapter.execute(sql)
        return self._compute(df, variable, population_size)

    def _chi2_p_value(
        self,
        baseline_counts: dict[str, int],
        test_counts: dict[str, int],
    ) -> float | None:
        """Return the chi-squared p-value comparing two category distributions.

        Builds a 2-row contingency table from *baseline_counts* and *test_counts*,
        runs :func:`scipy.stats.chi2_contingency`, and returns the p-value.
        Returns ``None`` when the test cannot be performed (empty samples,
        identical distributions, or numerical error).
        """
        all_cats = sorted(set(baseline_counts) | set(test_counts))
        b_arr = [baseline_counts.get(c, 0) for c in all_cats]
        t_arr = [test_counts.get(c, 0) for c in all_cats]

        if sum(b_arr) == 0 or sum(t_arr) == 0:
            return None
        try:
            _, p_value, _, _ = chi2_contingency([b_arr, t_arr])
            p_f = float(p_value)
            return None if math.isnan(p_f) else p_f
        except ValueError:
            return None

    def _compute(self, df: pd.DataFrame, variable: Variable, population_size: int) -> CheckResult:
        periods = sorted(df["period"].unique())
        n = len(periods)

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

        # Build baseline category counts from the first half of periods
        baseline_counts: dict[str, int] = {}
        for p in periods[:half]:
            sub = df[df["period"] == p]
            for _, row in sub.iterrows():
                cat = str(row["category"])
                baseline_counts[cat] = baseline_counts.get(cat, 0) + int(row["count"])

        min_p = 1.0
        for p in periods[half:]:
            sub = df[df["period"] == p]
            test_counts: dict[str, int] = {}
            for _, row in sub.iterrows():
                cat = str(row["category"])
                test_counts[cat] = test_counts.get(cat, 0) + int(row["count"])

            p_val = self._chi2_p_value(baseline_counts, test_counts)
            if p_val is not None:
                min_p = min(min_p, p_val)

            # Expand baseline with this period's counts
            for cat, cnt in test_counts.items():
                baseline_counts[cat] = baseline_counts.get(cat, 0) + cnt

        passed = min_p > self._p_threshold
        return CheckResult(
            check_name=self.name,
            passed=passed,
            severity=self.severity,
            observed_value=round(min_p, 6),
            population_size=population_size,
            threshold=self._p_threshold,
            metadata={
                "min_p_value": round(min_p, 6),
                "n_periods": n,
                "baseline_periods": half,
            },
            figure_factory=figures.chisquared_drift_figure(
                df, half, min_p, self._p_threshold, passed, variable.name
            ),
        )
