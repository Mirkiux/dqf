"""OutlierCheck — detects univariate outliers using Tukey's IQR method."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class OutlierCheck(BaseCrossSectionalCheck):
    """Fails when univariate outliers are detected using Tukey's IQR fences.

    Values outside ``[Q1 - iqr_multiplier * IQR, Q3 + iqr_multiplier * IQR]``
    are counted as outliers.  Null values are excluded from the computation.

    When the IQR is zero (constant or near-constant distribution), the check
    is skipped and returns ``passed=True`` — outlier detection is not meaningful
    for constant series.

    Parameters
    ----------
    iqr_multiplier:
        Multiplier applied to the IQR to set the fence bounds.
        Default ``1.5`` (standard Tukey's fences).  Use ``3.0`` for a more
        lenient "far outlier" threshold.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        iqr_multiplier: float = 1.5,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._iqr_multiplier = iqr_multiplier
        self._severity = severity

    @property
    def name(self) -> str:
        return "outlier"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"iqr_multiplier": self._iqr_multiplier}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        series = data[variable.name].dropna().astype(float)

        if len(series) == 0:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=0,
                population_size=population_size,
                threshold=None,
                metadata={"skipped": True, "reason": "No non-null values"},
            )

        q1 = float(series.quantile(0.25))
        q3 = float(series.quantile(0.75))
        iqr = q3 - q1

        if iqr == 0.0:
            return CheckResult(
                check_name=self.name,
                passed=True,
                severity=self.severity,
                observed_value=0,
                population_size=population_size,
                threshold={"lower_fence": q1, "upper_fence": q3},
                metadata={"skipped": True, "reason": "IQR is zero — constant series"},
            )

        lower_fence = q1 - self._iqr_multiplier * iqr
        upper_fence = q3 + self._iqr_multiplier * iqr
        outlier_count = int(((series < lower_fence) | (series > upper_fence)).sum())
        rate = outlier_count / population_size if population_size > 0 else 0.0

        return CheckResult(
            check_name=self.name,
            passed=outlier_count == 0,
            severity=self.severity,
            observed_value=outlier_count,
            population_size=population_size,
            threshold={
                "lower_fence": round(lower_fence, 4),
                "upper_fence": round(upper_fence, 4),
            },
            rate=rate,
            metadata={
                "q1": round(q1, 4),
                "q3": round(q3, 4),
                "iqr": round(iqr, 4),
                "iqr_multiplier": self._iqr_multiplier,
            },
        )
