from collections.abc import Callable
from typing import Any

import pytest

from dqf.enums import Severity
from dqf.results import CheckResult, ValidationResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_check_result(
    check_name: str = "null_rate_check",
    passed: bool = True,
    severity: Severity = Severity.FAILURE,
    observed_value: int = 100,
    population_size: int = 1000,
    threshold: float = 0.10,
    rate: float | None = 0.10,
    metadata: dict[str, Any] | None = None,
    figure_factory: Callable[[], Any] | None = None,
) -> CheckResult:
    return CheckResult(
        check_name=check_name,
        passed=passed,
        severity=severity,
        observed_value=observed_value,
        population_size=population_size,
        threshold=threshold,
        rate=rate,
        metadata=metadata if metadata is not None else {},
        figure_factory=figure_factory,
    )


# ---------------------------------------------------------------------------
# CheckResult
# ---------------------------------------------------------------------------

class TestCheckResultConstruction:
    def test_valid_construction(self) -> None:
        result = make_check_result()
        assert result.check_name == "null_rate_check"
        assert result.passed is True
        assert result.rate == 0.10

    def test_rate_none_is_allowed(self) -> None:
        result = make_check_result(rate=None)
        assert result.rate is None

    def test_rate_zero_is_allowed(self) -> None:
        result = make_check_result(rate=0.0, observed_value=0)
        assert result.rate == 0.0

    def test_rate_one_is_allowed(self) -> None:
        result = make_check_result(rate=1.0, observed_value=1000)
        assert result.rate == 1.0

    def test_empty_check_name_raises(self) -> None:
        with pytest.raises(ValueError, match="check_name"):
            make_check_result(check_name="")

    def test_zero_population_size_raises(self) -> None:
        with pytest.raises(ValueError, match="population_size"):
            make_check_result(population_size=0)

    def test_negative_population_size_raises(self) -> None:
        with pytest.raises(ValueError, match="population_size"):
            make_check_result(population_size=-1)

    def test_rate_above_one_raises(self) -> None:
        with pytest.raises(ValueError, match="rate"):
            make_check_result(rate=1.01)

    def test_rate_below_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="rate"):
            make_check_result(rate=-0.01)


class TestCheckResultImmutability:
    def test_cannot_set_attribute(self) -> None:
        result = make_check_result()
        with pytest.raises((AttributeError, TypeError)):
            result.passed = False  # type: ignore[misc]


class TestCheckResultEquality:
    def test_equal_results(self) -> None:
        r1 = make_check_result()
        r2 = make_check_result()
        assert r1 == r2

    def test_different_figure_factory_still_equal(self) -> None:
        r1 = make_check_result(figure_factory=lambda: "plot_a")
        r2 = make_check_result(figure_factory=lambda: "plot_b")
        assert r1 == r2

    def test_different_metadata_still_equal(self) -> None:
        r1 = make_check_result(metadata={"col": "age"})
        r2 = make_check_result(metadata={"col": "income"})
        assert r1 == r2

    def test_different_passed_not_equal(self) -> None:
        r1 = make_check_result(passed=True)
        r2 = make_check_result(passed=False)
        assert r1 != r2


class TestCheckResultFigureFactory:
    def test_render_figure_returns_none_when_no_factory(self) -> None:
        result = make_check_result(figure_factory=None)
        assert result.render_figure() is None

    def test_render_figure_invokes_factory(self) -> None:
        sentinel = object()
        result = make_check_result(figure_factory=lambda: sentinel)
        assert result.render_figure() is sentinel

    def test_factory_called_each_time(self) -> None:
        call_count = {"n": 0}

        def factory() -> int:
            call_count["n"] += 1
            return call_count["n"]

        result = make_check_result(figure_factory=factory)
        assert result.render_figure() == 1
        assert result.render_figure() == 2


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------

class TestValidationResultConstruction:
    def test_valid_construction(self) -> None:
        vr = ValidationResult(check_name="pk_uniqueness", passed=True)
        assert vr.passed is True
        assert vr.details == {}

    def test_with_details(self) -> None:
        vr = ValidationResult(
            check_name="join_integrity",
            passed=False,
            details={"fan_out_count": 3},
        )
        assert vr.details["fan_out_count"] == 3

    def test_empty_check_name_raises(self) -> None:
        with pytest.raises(ValueError, match="check_name"):
            ValidationResult(check_name="", passed=True)


class TestValidationResultImmutability:
    def test_cannot_set_attribute(self) -> None:
        vr = ValidationResult(check_name="pk_uniqueness", passed=True)
        with pytest.raises((AttributeError, TypeError)):
            vr.passed = False  # type: ignore[misc]
