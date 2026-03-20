"""
dqf — Data Quality Framework

A composable, pipeline-based library for data quality validation of analytical datasets.
"""

__version__ = "0.1.0"

from dqf.adapters import (
    DatabricksAdapter,
    DataSourceAdapter,
    MockAdapter,
    SparkAdapter,
    SQLAlchemyAdapter,
)
from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole
from dqf.results import CheckResult, ValidationResult
from dqf.variable import Variable

__all__ = [
    "DataSourceAdapter",
    "DatabricksAdapter",
    "MockAdapter",
    "SparkAdapter",
    "SQLAlchemyAdapter",
    "DataType",
    "EngineType",
    "Severity",
    "ValidationStatus",
    "VariableRole",
    "CheckResult",
    "ValidationResult",
    "Variable",
]
