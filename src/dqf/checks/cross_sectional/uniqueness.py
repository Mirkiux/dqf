"""UniquenessCheck — validates that all non-null values in a column are unique."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class UniquenessCheck(BaseCrossSectionalCheck):
    """Fails if any non-null value appears more than once.

    Null values are excluded — multiple nulls are permitted.

    Parameters
    ----------
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(self, severity: Severity = Severity.FAILURE) -> None:
        self._severity = severity

    @property
    def name(self) -> str:
        return "uniqueness"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        non_null = data[variable.name].dropna()
        duplicate_count = int(non_null.duplicated().sum())
        rate = duplicate_count / population_size if population_size > 0 else 0.0

        return CheckResult(
            check_name=self.name,
            passed=duplicate_count == 0,
            severity=self.severity,
            observed_value=duplicate_count,
            population_size=population_size,
            threshold=0,
            rate=rate,
        )
