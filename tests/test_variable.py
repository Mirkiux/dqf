from dqf.enums import DataType, Severity, ValidationStatus, VariableRole
from dqf.results import CheckResult, ValidationResult  # noqa: F401
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_variable(**overrides: object) -> Variable:
    defaults: dict[str, object] = {"name": "age", "dtype": DataType.NUMERIC_CONTINUOUS}
    defaults.update(overrides)
    return Variable(**defaults)  # type: ignore[arg-type]


def make_result(
    passed: bool = True,
    severity: Severity = Severity.FAILURE,
) -> CheckResult:
    return CheckResult(
        check_name="test_check",
        passed=passed,
        severity=severity,
        observed_value=0,
        population_size=1000,
        threshold=0.1,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestVariableConstruction:
    def test_defaults(self) -> None:
        v = Variable(name="revenue", dtype=DataType.NUMERIC_CONTINUOUS)
        assert v.name == "revenue"
        assert v.dtype == DataType.NUMERIC_CONTINUOUS
        assert v.nullable is True
        assert v.role == VariableRole.FEATURE
        assert v.metadata == {}
        assert v.check_results == []
        assert v.status == ValidationStatus.PENDING

    def test_explicit_construction(self) -> None:
        v = Variable(
            name="label",
            dtype=DataType.CATEGORICAL,
            nullable=False,
            role=VariableRole.TARGET,
            metadata={"cardinality": 2},
        )
        assert v.nullable is False
        assert v.role == VariableRole.TARGET
        assert v.metadata["cardinality"] == 2


# ---------------------------------------------------------------------------
# attach_result — status transitions
# ---------------------------------------------------------------------------


class TestAttachResult:
    def test_attach_passing_failure_severity_sets_passed(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=True, severity=Severity.FAILURE))
        assert v.status == ValidationStatus.PASSED

    def test_attach_failing_failure_severity_sets_failed(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=False, severity=Severity.FAILURE))
        assert v.status == ValidationStatus.FAILED

    def test_attach_failing_warning_severity_sets_passed(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=False, severity=Severity.WARNING))
        assert v.status == ValidationStatus.PASSED

    def test_attach_multiple_results_any_failure_fails(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=True, severity=Severity.FAILURE))
        v.attach_result(make_result(passed=False, severity=Severity.FAILURE))
        assert v.status == ValidationStatus.FAILED

    def test_attach_multiple_results_all_pass(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=True, severity=Severity.FAILURE))
        v.attach_result(make_result(passed=True, severity=Severity.FAILURE))
        assert v.status == ValidationStatus.PASSED

    def test_attach_appends_to_list(self) -> None:
        v = make_variable()
        r1 = make_result(passed=True)
        r2 = make_result(passed=False, severity=Severity.WARNING)
        v.attach_result(r1)
        v.attach_result(r2)
        assert len(v.check_results) == 2
        assert v.check_results[0] is r1
        assert v.check_results[1] is r2

    def test_status_stays_failed_after_passing_result(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=False, severity=Severity.FAILURE))
        v.attach_result(make_result(passed=True, severity=Severity.FAILURE))
        assert v.status == ValidationStatus.FAILED


# ---------------------------------------------------------------------------
# summary()
# ---------------------------------------------------------------------------


class TestSummary:
    def test_summary_shape(self) -> None:
        v = make_variable()
        s = v.summary()
        assert set(s.keys()) == {
            "name",
            "dtype",
            "role",
            "status",
            "total_checks",
            "failed_checks",
            "warned_checks",
        }

    def test_summary_counts_with_no_results(self) -> None:
        v = make_variable()
        s = v.summary()
        assert s["total_checks"] == 0
        assert s["failed_checks"] == 0
        assert s["warned_checks"] == 0

    def test_summary_counts_with_mixed_results(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=True, severity=Severity.FAILURE))
        v.attach_result(make_result(passed=False, severity=Severity.FAILURE))
        v.attach_result(make_result(passed=False, severity=Severity.WARNING))
        s = v.summary()
        assert s["total_checks"] == 3
        assert s["failed_checks"] == 1
        assert s["warned_checks"] == 1


# ---------------------------------------------------------------------------
# reset()
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_clears_results(self) -> None:
        v = make_variable()
        v.attach_result(make_result())
        v.reset()
        assert v.check_results == []

    def test_reset_sets_status_pending(self) -> None:
        v = make_variable()
        v.attach_result(make_result(passed=False))
        v.reset()
        assert v.status == ValidationStatus.PENDING

    def test_reset_preserves_metadata(self) -> None:
        v = make_variable()
        v.metadata["key"] = "value"
        v.attach_result(make_result())
        v.reset()
        assert v.metadata == {"key": "value"}


# ---------------------------------------------------------------------------
# Mutability
# ---------------------------------------------------------------------------


class TestMutability:
    def test_metadata_is_mutable(self) -> None:
        v = make_variable()
        v.metadata["cardinality"] = 5
        assert v.metadata["cardinality"] == 5

    def test_status_can_be_set_externally(self) -> None:
        v = make_variable()
        v.status = ValidationStatus.ERROR
        assert v.status == ValidationStatus.ERROR
