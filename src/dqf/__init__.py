"""
dqf — Data Quality Framework

A composable, pipeline-based library for data quality validation of analytical datasets.
"""

__version__ = "0.1.1"

from dqf.adapters import (
    DatabricksAdapter,
    DatabricksNotebookAdapter,
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
    ChiSquaredDriftCheck,
    ConceptDriftCheck,
    DistributionDriftCheck,
    KSDriftCheck,
    NotNullCheck,
    NullRateCheck,
    OutlierCheck,
    ProportionDriftCheck,
    RangeCheck,
    ReferentialIntegrityCheck,
    RegexPatternCheck,
    SeasonalityCheck,
    StructuralBreakCheck,
    TrendCheck,
    UniquenessCheck,
)
from dqf.datasets import UniverseDataset, VariablesDataset
from dqf.defaults import build_default_resolver
from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole
from dqf.report import ValidationReport
from dqf.resolver import CheckSuiteResolver
from dqf.results import CheckResult, ValidationResult
from dqf.variable import Variable

__all__ = [
    "build_default_resolver",
    "BaseCheck",
    "BaseCrossSectionalCheck",
    "BaseLongitudinalCheck",
    "CheckPipeline",
    "CheckSuiteResolver",
    "UniverseDataset",
    "VariablesDataset",
    "DataSourceAdapter",
    "DatabricksAdapter",
    "DatabricksNotebookAdapter",
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
    "ChiSquaredDriftCheck",
    "ConceptDriftCheck",
    "DistributionDriftCheck",
    "KSDriftCheck",
    "NotNullCheck",
    "NullRateCheck",
    "OutlierCheck",
    "ProportionDriftCheck",
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
