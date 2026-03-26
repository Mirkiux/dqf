"""
dqf — Data Quality Framework

A composable, pipeline-based library for data quality validation of analytical datasets.
"""

__version__ = "0.1.6"

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
from dqf.config import CardinalityThresholds
from dqf.datasets import UniverseDataset, VariablesDataset
from dqf.defaults import (
    build_default_metadata_pipeline,
    build_default_metadata_resolver,
    build_default_resolver,
)
from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole
from dqf.metadata.resolver import MetadataResolver
from dqf.report import ValidationReport
from dqf.resolver import CheckSuiteResolver
from dqf.results import CheckResult, ValidationResult
from dqf.variable import Variable

__all__ = [
    "CardinalityThresholds",
    "build_default_resolver",
    "build_default_metadata_pipeline",
    "build_default_metadata_resolver",
    "MetadataResolver",
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
