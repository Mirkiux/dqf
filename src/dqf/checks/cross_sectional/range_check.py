"""RangeCheck — validates that all non-null values fall within [min_value, max_value]."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
import pandas as pd

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from matplotlib.figure import Figure

    from dqf.datasets.variables import VariablesDataset


def _range_figure(
    series: pd.Series,
    min_value: float | None,
    max_value: float | None,
    variable_name: str,
) -> Figure:
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(series.dropna(), bins=20, color="#2196F3", alpha=0.7)
    if min_value is not None:
        ax.axvline(min_value, color="red", linestyle="--", label=f"Min: {min_value}")
    if max_value is not None:
        ax.axvline(max_value, color="orange", linestyle="--", label=f"Max: {max_value}")
    ax.set_title(f"Range — {variable_name}")
    ax.set_xlabel("Value")
    ax.set_ylabel("Count")
    ax.legend()
    plt.tight_layout()
    return fig


class RangeCheck(BaseCrossSectionalCheck):
    """Fails if any non-null value falls outside the specified range.

    At least one bound must be provided.  Null values are excluded from the
    check — use :class:`~dqf.checks.cross_sectional.NullRateCheck` to validate
    null presence separately.

    Parameters
    ----------
    min_value:
        Lower bound (inclusive).  ``None`` means no lower bound.
    max_value:
        Upper bound (inclusive).  ``None`` means no upper bound.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        min_value: float | None = None,
        max_value: float | None = None,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        if min_value is None and max_value is None:
            raise ValueError("At least one of min_value or max_value must be provided.")
        self._min = min_value
        self._max = max_value
        self._severity = severity

    @property
    def name(self) -> str:
        return "range"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"min_value": self._min, "max_value": self._max}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        series = data[variable.name].dropna()

        in_range = pd.Series([True] * len(series), index=series.index)
        if self._min is not None:
            in_range &= series >= self._min
        if self._max is not None:
            in_range &= series <= self._max

        violation_count = int((~in_range).sum())
        rate = violation_count / population_size if population_size > 0 else 0.0

        _series = series.copy()
        _min = self._min
        _max = self._max
        _name = variable.name

        return CheckResult(
            check_name=self.name,
            passed=violation_count == 0,
            severity=self.severity,
            observed_value=violation_count,
            population_size=population_size,
            threshold={"min_value": self._min, "max_value": self._max},
            rate=rate,
            figure_factory=lambda: _range_figure(_series, _min, _max, _name),
        )
