from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from dqf.enums import Severity


@dataclass(frozen=True)
class CheckResult:
    """Immutable output of a single check applied to a single variable.

    population_size is always the universe size — never the raw variable
    dataset size. This ensures percentage-based metrics are computed against
    the correct denominator.

    figure_factory, if provided, is a zero-argument callable that returns a
    matplotlib Figure. It is intentionally excluded from equality checks and
    hashing so that two CheckResult instances with identical metrics but
    different plot implementations compare as equal.
    """

    check_name: str
    passed: bool
    severity: Severity
    observed_value: Any
    population_size: int
    threshold: Any
    rate: float | None = None
    metadata: dict = field(default_factory=dict, compare=False, hash=False)
    figure_factory: Callable[[], Any] | None = field(
        default=None, compare=False, hash=False
    )

    def __post_init__(self) -> None:
        if not self.check_name:
            raise ValueError("check_name must be a non-empty string")
        if self.population_size <= 0:
            raise ValueError(
                f"population_size must be a positive integer, got {self.population_size}"
            )
        if self.rate is not None and not (0.0 <= self.rate <= 1.0):
            raise ValueError(
                f"rate must be between 0.0 and 1.0, got {self.rate}"
            )

    def render_figure(self) -> Any:
        """Invoke the figure_factory and return the resulting Figure.
        Returns None if no figure_factory was provided."""
        if self.figure_factory is None:
            return None
        return self.figure_factory()


@dataclass(frozen=True)
class ValidationResult:
    """Immutable output of a dataset-level invariant check such as
    primary key uniqueness or join integrity."""

    check_name: str
    passed: bool
    details: dict = field(default_factory=dict, compare=False, hash=False)

    def __post_init__(self) -> None:
        if not self.check_name:
            raise ValueError("check_name must be a non-empty string")
