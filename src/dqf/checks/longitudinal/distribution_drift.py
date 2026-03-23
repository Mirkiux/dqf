"""DistributionDriftCheck — detects shifts in metric distribution across time (PSI)."""

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
_EPSILON = 1e-6


class DistributionDriftCheck(BaseLongitudinalCheck):
    """Fails when PSI between reference and current period metrics exceeds *psi_threshold*.

    The Population Stability Index (PSI) measures how much the distribution of
    period-level metric values has shifted.  If no reference has been set via
    :meth:`set_reference`, the first half of the time series is used as the
    reference and the second half as the current window.

    PSI < 0.1  — no significant shift
    PSI 0.1–0.25 — moderate shift
    PSI > 0.25 — significant shift (default threshold)

    Parameters
    ----------
    time_field:
        Name of the datetime column in the variables table.
    period:
        Truncation period (e.g. ``"month"``).
    psi_threshold:
        Maximum allowed PSI.  Default 0.2.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        time_field: str,
        period: str = "month",
        psi_threshold: float = 0.2,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._time_field = time_field
        self._period = period
        self._psi_threshold = psi_threshold
        self._severity = severity
        self._reference_metrics: list[float] | None = None

    @property
    def name(self) -> str:
        return "distribution_drift"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {
            "psi_threshold": self._psi_threshold,
            "time_field": self._time_field,
            "period": self._period,
        }

    def set_reference(self, reference_metrics: list[float]) -> None:
        """Set a reference distribution from historical period-level metrics."""
        self._reference_metrics = reference_metrics

    def aggregation_sql(self, variable_name: str, time_field: str, period: str) -> str:
        return (
            f"SELECT DATE_TRUNC('{period}', {time_field}) AS period,"
            f" AVG(CAST({variable_name} AS DOUBLE)) AS metric,"
            f" COUNT({variable_name}) AS n"
            f" FROM ({{source}}) _vd"
            f" GROUP BY 1 ORDER BY 1"
        )

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        population_size = len(dataset.universe.materialise())
        sql_template = self.aggregation_sql(variable.name, self._time_field, self._period)
        sql = sql_template.format(source=dataset.sql)
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
                threshold=self._psi_threshold,
                metadata={"skipped": True, "reason": f"Need >= {_MIN_PERIODS} periods, got {n}"},
            )
        all_values: np.ndarray[Any, np.dtype[np.float64]] = timeseries_df["metric"].to_numpy(
            dtype=float
        )
        if self._reference_metrics is not None:
            reference: np.ndarray[Any, np.dtype[np.float64]] = np.array(
                self._reference_metrics, dtype=float
            )
            current = all_values
        else:
            split = max(1, n // 2)
            reference = all_values[:split]
            current = all_values[split:]

        if len(reference) == 0 or len(current) == 0:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=None,
                population_size=population_size,
                threshold=self._psi_threshold,
                metadata={"skipped": True, "reason": "Empty reference or current window"},
            )

        combined = np.concatenate([reference, current])
        percentiles = np.linspace(0, 100, 11)
        bin_edges = np.unique(np.percentile(combined, percentiles))
        if len(bin_edges) < 2:
            psi = 0.0
        else:
            ref_counts, _ = np.histogram(reference, bins=bin_edges)
            cur_counts, _ = np.histogram(current, bins=bin_edges)
            n_bins = len(ref_counts)
            ref_pct = (ref_counts + _EPSILON) / (ref_counts.sum() + _EPSILON * n_bins)
            cur_pct = (cur_counts + _EPSILON) / (cur_counts.sum() + _EPSILON * n_bins)
            psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))

        return CheckResult(
            check_name=self.name,
            passed=psi <= self._psi_threshold,
            severity=self.severity,
            observed_value=round(psi, 4),
            population_size=population_size,
            threshold=self._psi_threshold,
            metadata={
                "n_reference_periods": len(reference),
                "n_current_periods": len(current),
            },
        )
