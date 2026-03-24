from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from dqf.enums import DataType, Severity, ValidationStatus, VariableRole
from dqf.results import CheckResult

_COERCE_THRESHOLD = 0.95


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

    def infer_dtype(
        self,
        series: pd.Series,
        low_cardinality_threshold: int = 20,
    ) -> None:
        """Infer and set the semantic :class:`~dqf.enums.DataType` from *series*.

        Only acts when ``self.dtype`` is ``DataType.PENDING`` — a pre-declared
        dtype is never overwritten.

        Inference priority
        ------------------
        1. Boolean storage dtype → ``BOOLEAN``
        2. Numeric storage dtype → ``NUMERIC_CONTINUOUS``
        3. Datetime storage dtype → ``DATETIME``
        4. Object/string: ≥95 % of non-null values coerce to numeric
           → ``NUMERIC_CONTINUOUS``
        5. Object/string: ≥95 % of non-null values coerce to datetime
           → ``DATETIME``
        6. Distinct non-null values ≤ *low_cardinality_threshold*
           → ``CATEGORICAL``
        7. Default → ``TEXT``

        Parameters
        ----------
        series:
            The pandas Series for this column in the materialised dataset.
        low_cardinality_threshold:
            Maximum number of distinct non-null values for a string column to
            be classified as ``CATEGORICAL``.  Default ``20``.
        """
        if self.dtype != DataType.PENDING:
            return

        if pd.api.types.is_bool_dtype(series):
            self.dtype = DataType.BOOLEAN
            return

        if pd.api.types.is_numeric_dtype(series):
            self.dtype = DataType.NUMERIC_CONTINUOUS
            return

        if pd.api.types.is_datetime64_any_dtype(series):
            self.dtype = DataType.DATETIME
            return

        non_null = series.dropna()
        if len(non_null) > 0:
            if pd.to_numeric(non_null, errors="coerce").notna().mean() >= _COERCE_THRESHOLD:
                self.dtype = DataType.NUMERIC_CONTINUOUS
                return

            if pd.to_datetime(non_null, errors="coerce").notna().mean() >= _COERCE_THRESHOLD:
                self.dtype = DataType.DATETIME
                return

            if non_null.nunique() <= low_cardinality_threshold:
                self.dtype = DataType.CATEGORICAL
                return

        self.dtype = DataType.TEXT

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
