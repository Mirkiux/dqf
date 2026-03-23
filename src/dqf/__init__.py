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
from dqf.checks import (
    AllowedValuesCheck,
    BaseCheck,
    BaseCrossSectionalCheck,
    BaseLongitudinalCheck,
    CardinalityCheck,
    CheckPipeline,
    ConceptDriftCheck,
    DistributionDriftCheck,
    NullRateCheck,
    RangeCheck,
    ReferentialIntegrityCheck,
    RegexPatternCheck,
    SeasonalityCheck,
    StructuralBreakCheck,
    TrendCheck,
    UniquenessCheck,
)
from dqf.datasets import UniverseDataset, VariablesDataset
from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole
from dqf.report import ValidationReport
from dqf.resolver import CheckSuiteResolver
from dqf.results import CheckResult, ValidationResult
from dqf.variable import Variable

__all__ = [
    "BaseCheck",
    "BaseCrossSectionalCheck",
    "BaseLongitudinalCheck",
    "CheckPipeline",
    "CheckSuiteResolver",
    "UniverseDataset",
    "VariablesDataset",
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
    "AllowedValuesCheck",
    "CardinalityCheck",
    "ConceptDriftCheck",
    "DistributionDriftCheck",
    "NullRateCheck",
    "RangeCheck",
    "ReferentialIntegrityCheck",
    "RegexPatternCheck",
    "SeasonalityCheck",
    "StructuralBreakCheck",
    "TrendCheck",
    "UniquenessCheck",
    "CheckResult",
    "ValidationReport",
    "ValidationResult",
    "Variable",
]
