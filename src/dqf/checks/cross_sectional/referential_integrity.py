"""ReferentialIntegrityCheck — validates FK-style column references."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class ReferentialIntegrityCheck(BaseCrossSectionalCheck):
    """Fails if any non-null value is absent from *reference_values*.

    Designed for foreign-key style validation: every non-null value in the
    column must appear in the provided reference collection.  Null values are
    excluded — use :class:`~dqf.checks.cross_sectional.NullRateCheck` to
    validate null presence separately.

    Parameters
    ----------
    reference_values:
        The complete set of valid reference values (e.g. primary keys of a
        dimension table).
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(
        self,
        reference_values: set[Any] | list[Any],
        severity: Severity = Severity.FAILURE,
    ) -> None:
        self._reference: frozenset[Any] = frozenset(reference_values)
        self._severity = severity

    @property
    def name(self) -> str:
        return "referential_integrity"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"reference_count": len(self._reference)}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        non_null = data[variable.name].dropna()
        violation_count = int((~non_null.isin(self._reference)).sum())
        rate = violation_count / population_size if population_size > 0 else 0.0

        return CheckResult(
            check_name=self.name,
            passed=violation_count == 0,
            severity=self.severity,
            observed_value=violation_count,
            population_size=population_size,
            threshold=len(self._reference),
            rate=rate,
        )
