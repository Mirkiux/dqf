"""NotNullCheck — validates that a column contains no null values."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class NotNullCheck(BaseCrossSectionalCheck):
    """Fails if any null value exists in the column.

    Stricter than :class:`~dqf.checks.cross_sectional.NullRateCheck` with
    ``threshold=0.0`` — the name makes the intent explicit: the column must be
    completely non-null.  Use this for identifier columns, required target
    variables, and other fields that must never be missing.

    Both value-nulls (entity present, column null) and structural nulls
    (entity absent from the variables dataset) count toward the failure.
    The denominator used for the rate is always the universe size.

    Parameters
    ----------
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(self, severity: Severity = Severity.FAILURE) -> None:
        self._severity = severity

    @property
    def name(self) -> str:
        return "not_null"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        null_count = int(data[variable.name].isna().sum())
        rate = null_count / population_size if population_size > 0 else 0.0

        return CheckResult(
            check_name=self.name,
            passed=null_count == 0,
            severity=self.severity,
            observed_value=null_count,
            population_size=population_size,
            threshold=0,
            rate=rate,
        )
