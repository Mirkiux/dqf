
from dqf.enums import DataType, EngineType, Severity, ValidationStatus, VariableRole


class TestDataType:
    def test_all_members_exist(self):
        members = {e.value for e in DataType}
        assert members == {
            "numeric_continuous",
            "numeric_discrete",
            "categorical",
            "boolean",
            "datetime",
            "text",
            "identifier",
        }

    def test_lookup_by_value(self):
        assert DataType("categorical") is DataType.CATEGORICAL


class TestValidationStatus:
    def test_all_members_exist(self):
        members = {e.value for e in ValidationStatus}
        assert members == {"pending", "passed", "failed", "skipped", "error"}

    def test_lookup_by_value(self):
        assert ValidationStatus("failed") is ValidationStatus.FAILED


class TestSeverity:
    def test_all_members_exist(self):
        assert {e.value for e in Severity} == {"warning", "failure"}

    def test_lookup_by_value(self):
        assert Severity("warning") is Severity.WARNING


class TestEngineType:
    def test_all_members_exist(self):
        assert {e.value for e in EngineType} == {
            "sqlalchemy",
            "databricks",
            "spark",
            "mock",
        }


class TestVariableRole:
    def test_all_members_exist(self):
        assert {e.value for e in VariableRole} == {
            "feature",
            "target",
            "identifier",
            "auxiliary",
        }

    def test_target_member(self):
        assert VariableRole("target") is VariableRole.TARGET
