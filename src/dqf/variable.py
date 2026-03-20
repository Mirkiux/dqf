from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dqf.enums import DataType, Severity, ValidationStatus, VariableRole
from dqf.results import CheckResult


@dataclass
class Variable:
    """Descriptor for a single dataset column.

    Accumulates metadata (populated by MetadataBuilderPipeline) and
    CheckResult instances (attached by the validation orchestrator) over
    its lifetime. Unlike CheckResult, Variable is mutable by design.

    Status transitions are driven by attach_result(). ERROR status is set
    externally by the orchestrator when an exception prevents evaluation.
    """

    name: str
    dtype: DataType
    nullable: bool = True
    role: VariableRole = VariableRole.FEATURE
    metadata: dict[str, Any] = field(default_factory=dict)
    status: ValidationStatus = field(default=ValidationStatus.PENDING)
    check_results: list[CheckResult] = field(default_factory=list)

    def attach_result(self, result: CheckResult) -> None:
        """Append a CheckResult and recompute status.

        Status rule:
          - Any FAILURE-severity result with passed=False → FAILED
          - No results → PENDING
          - Otherwise → PASSED
        WARNING-severity failures do not set status to FAILED.
        """
        self.check_results.append(result)
        if any(r.severity == Severity.FAILURE and not r.passed for r in self.check_results):
            self.status = ValidationStatus.FAILED
        else:
            self.status = ValidationStatus.PASSED

    def summary(self) -> dict[str, Any]:
        """Return a flat summary dict of this variable's validation state."""
        failed_checks = sum(
            1 for r in self.check_results if r.severity == Severity.FAILURE and not r.passed
        )
        warned_checks = sum(
            1 for r in self.check_results if r.severity == Severity.WARNING and not r.passed
        )
        return {
            "name": self.name,
            "dtype": self.dtype,
            "role": self.role,
            "status": self.status,
            "total_checks": len(self.check_results),
            "failed_checks": failed_checks,
            "warned_checks": warned_checks,
        }

    def reset(self) -> None:
        """Clear all check results and reset status to PENDING.

        Metadata is preserved — it describes the column structure and
        does not belong to a single validation run.
        """
        self.check_results = []
        self.status = ValidationStatus.PENDING
