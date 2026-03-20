"""
dqf — Data Quality Framework

A composable, pipeline-based library for data quality validation of analytical datasets.
"""

__version__ = "0.1.0"

from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole
from dqf.results import CheckResult, ValidationResult
from dqf.variable import Variable

__all__ = [
    "DataType",
    "EngineType",
    "Severity",
    "ValidationStatus",
    "VariableRole",
    "CheckResult",
    "ValidationResult",
    "Variable",
]
