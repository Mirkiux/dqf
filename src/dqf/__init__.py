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
from dqf.checks import BaseCheck, BaseCrossSectionalCheck, BaseLongitudinalCheck, CheckPipeline
from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole
from dqf.resolver import CheckSuiteResolver
from dqf.results import CheckResult, ValidationResult
from dqf.variable import Variable

__all__ = [
    "BaseCheck",
    "BaseCrossSectionalCheck",
    "BaseLongitudinalCheck",
    "CheckPipeline",
    "CheckSuiteResolver",
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
