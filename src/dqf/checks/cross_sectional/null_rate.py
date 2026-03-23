"""NullRateCheck — validates the null rate of a column against a threshold."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from matplotlib.figure import Figure

    from dqf.datasets.variables import VariablesDataset


def _null_rate_figure(
    null_count: int,
    non_null_count: int,
    variable_name: str,
    threshold: float,
) -> Figure:
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(["Non-null", "Null"], [non_null_count, null_count], color=["#2196F3", "#F44336"])
    total = null_count + non_null_count
    if total > 0:
        ax.axhline(
            threshold * total,
            color="orange",
            linestyle="--",
            label=f"Threshold ({threshold:.1%})",
        )
        ax.legend()
    ax.set_title(f"Null rate — {variable_name}")
    ax.set_ylabel("Count")
    plt.tight_layout()
    return fig


class NullRateCheck(BaseCrossSectionalCheck):
    """Fails when the fraction of null values in a column exceeds *threshold*.

    Both value-nulls (entity present, column null) and structural nulls
    (entity absent from the variables dataset) count toward the rate.
    The denominator is always the universe size.

    Parameters
    ----------
    threshold:
        Maximum allowed null rate in [0.0, 1.0].
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(self, threshold: float, severity: Severity = Severity.FAILURE) -> None:
        self._threshold = threshold
        self._severity = severity

    @property
    def name(self) -> str:
        return "null_rate"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"threshold": self._threshold}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        null_count = int(data[variable.name].isna().sum())
        non_null_count = population_size - null_count
        rate = null_count / population_size if population_size > 0 else 0.0

        _null = null_count
        _non_null = non_null_count
        _name = variable.name
        _thr = self._threshold

        return CheckResult(
            check_name=self.name,
            passed=rate <= self._threshold,
            severity=self.severity,
            observed_value=null_count,
            population_size=population_size,
            threshold=self._threshold,
            rate=rate,
            figure_factory=lambda: _null_rate_figure(_null, _non_null, _name, _thr),
        )
