"""AllowedValuesCheck — validates that all non-null values belong to an allowed set."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class AllowedValuesCheck(BaseCrossSectionalCheck):
    """Fails if any non-null value is not in *allowed_values*.

    Null values are excluded — use :class:`~dqf.checks.cross_sectional.NullRateCheck`
    to validate null presence separately.

    Parameters
    ----------
    allowed_values:
        The complete set of permitted values.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        allowed_values: set[Any] | list[Any],
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._allowed: frozenset[Any] = frozenset(allowed_values)
        self._severity = severity

    @property
    def name(self) -> str:
        return "allowed_values"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"allowed_values": self._allowed}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        non_null = data[variable.name].dropna()
        violation_count = int((~non_null.isin(self._allowed)).sum())
        rate = violation_count / population_size if population_size > 0 else 0.0

        return CheckResult(
            check_name=self.name,
            passed=violation_count == 0,
            severity=self.severity,
            observed_value=violation_count,
            population_size=population_size,
            threshold=sorted(str(v) for v in self._allowed),
            rate=rate,
        )
