"""ValidationReport — top-level output of a validation run."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from dqf.enums import Severity, ValidationStatus
from dqf.results import CheckResult, ValidationResult


@dataclass
class ValidationReport:
    """Top-level output of :meth:`~dqf.datasets.variables.VariablesDataset.run_validation`.

    Captures both dataset-level invariant results (PK uniqueness, join integrity)
    and per-variable check results assembled during a single validation run.

    Parameters
    ----------
    dataset_name:
        Human-readable identifier for the dataset being validated.
    run_timestamp:
        UTC timestamp when the validation run started.
    dataset_level_checks:
        Results of dataset-level invariant checks (PK uniqueness, join integrity).
    variable_reports:
        Mapping of variable name → list of :class:`~dqf.results.CheckResult` instances
        collected during the run.
    """

    dataset_name: str
    run_timestamp: datetime
    dataset_level_checks: list[ValidationResult] = field(default_factory=list)
    variable_reports: dict[str, list[CheckResult]] = field(default_factory=dict)

    @property
    def overall_status(self) -> ValidationStatus:
        """Aggregate status derived from all checks in this report.

        Returns ``FAILED`` if any dataset-level invariant check failed or any
        variable has at least one ``FAILURE``-severity failed check.
        Returns ``PASSED`` otherwise (``WARNING``-severity failures do not
        cause an overall ``FAILED``).
        """
        if any(not r.passed for r in self.dataset_level_checks):
            return ValidationStatus.FAILED
        for results in self.variable_reports.values():
            if any(r.severity == Severity.FAILURE and not r.passed for r in results):
                return ValidationStatus.FAILED
        return ValidationStatus.PASSED

    def failed_variables(self) -> list[str]:
        """Return names of variables with at least one ``FAILURE``-severity failed check."""
        return [
            name
            for name, results in self.variable_reports.items()
            if any(r.severity == Severity.FAILURE and not r.passed for r in results)
        ]

    def warnings(self) -> list[CheckResult]:
        """Return all ``WARNING``-severity failed results across all variables."""
        return [
            result
            for results in self.variable_reports.values()
            for result in results
            if result.severity == Severity.WARNING and not result.passed
        ]
