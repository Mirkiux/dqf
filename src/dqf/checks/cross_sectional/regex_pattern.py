"""RegexPatternCheck — validates that all non-null string values fully match a regex."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dqf.checks.base import BaseCrossSectionalCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class RegexPatternCheck(BaseCrossSectionalCheck):
    """Fails if any non-null value does not fully match *pattern*.

    Uses ``pd.Series.str.fullmatch`` (anchored at both ends).  Values are
    cast to ``str`` before matching, so numeric columns can be validated too.
    Null values are excluded.

    Parameters
    ----------
    pattern:
        Regular expression that the entire value must satisfy.
    severity:
        ``FAILURE`` (default) or ``WARNING``.
    """

    def __init__(self, pattern: str, severity: Severity = Severity.FAILURE) -> None:
        self._pattern = pattern
        self._severity = severity

    @property
    def name(self) -> str:
        return "regex_pattern"

    @property
    def severity(self) -> Severity:
        return self._severity

    @property
    def params(self) -> dict[str, Any]:
        return {"pattern": self._pattern}

    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        data = dataset.materialise()
        population_size = len(dataset.universe.materialise())
        non_null = data[variable.name].dropna().astype(str)
        violation_count = int((~non_null.str.fullmatch(self._pattern)).sum())
        rate = violation_count / population_size if population_size > 0 else 0.0

        return CheckResult(
            check_name=self.name,
            passed=violation_count == 0,
            severity=self.severity,
            observed_value=violation_count,
            population_size=population_size,
            threshold=self._pattern,
            rate=rate,
        )
