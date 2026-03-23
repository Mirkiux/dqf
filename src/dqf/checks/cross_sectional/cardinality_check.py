"""CardinalityCheck — validates the number of distinct non-null values in a column."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class CardinalityCheck(BaseCrossSectionalCheck):
    """Fails if the number of distinct non-null values is outside the expected range.

    At least one bound must be provided.

    Parameters
    ----------
    min_cardinality:
        Minimum acceptable number of distinct values.  ``None`` means no lower bound.
    max_cardinality:
        Maximum acceptable number of distinct values.  ``None`` means no upper bound.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        min_cardinality: int | None = None,
        max_cardinality: int | None = None,
        severity: Severity = Severity.FAILURE,
    ) -> None:
        if min_cardinality is None and max_cardinality is None:
            raise ValueError("At least one of min_cardinality or max_cardinality must be provided.")
        self._min = min_cardinality
        self._max = max_cardinality
        self._severity = severity

    @property
    def name(self) -> str:
        return "cardinality"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"min_cardinality": self._min, "max_cardinality": self._max}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        cardinality = int(data[variable.name].dropna().nunique())

        passed = True
        if self._min is not None and cardinality < self._min:
            passed = False
        if self._max is not None and cardinality > self._max:
            passed = False

        return CheckResult(
            check_name=self.name,
            passed=passed,
            severity=self.severity,
            observed_value=cardinality,
            population_size=population_size,
            threshold={"min_cardinality": self._min, "max_cardinality": self._max},
        )
