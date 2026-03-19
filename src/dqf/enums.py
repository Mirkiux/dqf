from enum import Enum


class DataType(Enum):
    """Semantic type of a variable. Richer than storage dtype — a varchar column
    storing numeric values should be classified as NUMERIC_CONTINUOUS or
    NUMERIC_DISCRETE after semantic inference, not as TEXT."""

    NUMERIC_CONTINUOUS = "numeric_continuous"
    NUMERIC_DISCRETE = "numeric_discrete"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    TEXT = "text"
    IDENTIFIER = "identifier"


class ValidationStatus(Enum):
    """Lifecycle status of a Variable or dataset-level check."""

    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


class Severity(Enum):
    """Impact level of a test failure.

    WARNING  — informational; does not set variable status to FAILED.
    FAILURE  — sets variable status to FAILED.
    """

    WARNING = "warning"
    FAILURE = "failure"


class EngineType(Enum):
    """Identifies the execution engine behind a DataSourceAdapter."""

    SQLALCHEMY = "sqlalchemy"
    DATABRICKS = "databricks"
    SPARK = "spark"
    MOCK = "mock"


class VariableRole(Enum):
    """Domain role of a variable in the analytical context.

    TARGET variables receive drift-aware longitudinal tests in addition to
    standard cross-sectional tests.
    """

    FEATURE = "feature"
    TARGET = "target"
    IDENTIFIER = "identifier"
    AUXILIARY = "auxiliary"
