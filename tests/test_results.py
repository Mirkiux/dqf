import pytest

from dqf.enums import Severity
from dqf.results import TestResult, ValidationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_test_result(**overrides) -> TestResult:
    defaults = dict(
        test_name="null_rate_check",
        passed=True,
        severity=Severity.FAILURE,
        observed_value=100,
        population_size=1000,
        threshold=0.10,
        rate=0.10,
    )
    defaults.update(overrides)
    return TestResult(**defaults)


# ---------------------------------------------------------------------------
# TestResult
# ---------------------------------------------------------------------------

class TestTestResultConstruction:
    def test_valid_construction(self):
        result = make_test_result()
        assert result.test_name == "null_rate_check"
        assert result.passed is True
        assert result.rate == 0.10

    def test_rate_none_is_allowed(self):
        result = make_test_result(rate=None)
        assert result.rate is None

    def test_rate_zero_is_allowed(self):
        result = make_test_result(rate=0.0, observed_value=0)
        assert result.rate == 0.0

    def test_rate_one_is_allowed(self):
        result = make_test_result(rate=1.0, observed_value=1000)
        assert result.rate == 1.0

    def test_empty_test_name_raises(self):
        with pytest.raises(ValueError, match="test_name"):
            make_test_result(test_name="")

    def test_zero_population_size_raises(self):
        with pytest.raises(ValueError, match="population_size"):
            make_test_result(population_size=0)

    def test_negative_population_size_raises(self):
        with pytest.raises(ValueError, match="population_size"):
            make_test_result(population_size=-1)

    def test_rate_above_one_raises(self):
        with pytest.raises(ValueError, match="rate"):
            make_test_result(rate=1.01)

    def test_rate_below_zero_raises(self):
        with pytest.raises(ValueError, match="rate"):
            make_test_result(rate=-0.01)


class TestTestResultImmutability:
    def test_cannot_set_attribute(self):
        result = make_test_result()
        with pytest.raises((AttributeError, TypeError)):
            result.passed = False  # type: ignore[misc]


class TestTestResultEquality:
    def test_equal_results(self):
        r1 = make_test_result()
        r2 = make_test_result()
        assert r1 == r2

    def test_different_figure_factory_still_equal(self):
        r1 = make_test_result(figure_factory=lambda: "plot_a")
        r2 = make_test_result(figure_factory=lambda: "plot_b")
        assert r1 == r2

    def test_different_metadata_still_equal(self):
        r1 = make_test_result(metadata={"col": "age"})
        r2 = make_test_result(metadata={"col": "income"})
        assert r1 == r2

    def test_different_passed_not_equal(self):
        r1 = make_test_result(passed=True)
        r2 = make_test_result(passed=False)
        assert r1 != r2


class TestTestResultFigureFactory:
    def test_render_figure_returns_none_when_no_factory(self):
        result = make_test_result(figure_factory=None)
        assert result.render_figure() is None

    def test_render_figure_invokes_factory(self):
        sentinel = object()
        result = make_test_result(figure_factory=lambda: sentinel)
        assert result.render_figure() is sentinel

    def test_factory_called_each_time(self):
        call_count = {"n": 0}

        def factory():
            call_count["n"] += 1
            return call_count["n"]

        result = make_test_result(figure_factory=factory)
        assert result.render_figure() == 1
        assert result.render_figure() == 2


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------

class TestValidationResultConstruction:
    def test_valid_construction(self):
        vr = ValidationResult(check_name="pk_uniqueness", passed=True)
        assert vr.passed is True
        assert vr.details == {}

    def test_with_details(self):
        vr = ValidationResult(
            check_name="join_integrity",
            passed=False,
            details={"fan_out_count": 3},
        )
        assert vr.details["fan_out_count"] == 3

    def test_empty_check_name_raises(self):
        with pytest.raises(ValueError, match="check_name"):
            ValidationResult(check_name="", passed=True)


class TestValidationResultImmutability:
    def test_cannot_set_attribute(self):
        vr = ValidationResult(check_name="pk_uniqueness", passed=True)
        with pytest.raises((AttributeError, TypeError)):
            vr.passed = False  # type: ignore[misc]
