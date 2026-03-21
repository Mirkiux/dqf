from __future__ import annotations

from typing import Any

import pandas as pd

from dqf.checks.base import BaseCheck
from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable


class CheckPipeline(BaseCheck):
    """Runs a sequence of checks and collects results.

    Implements the Composite pattern: a ``CheckPipeline`` is itself a
    :class:`~dqf.checks.base.BaseCheck` and can be nested as a step inside
    another pipeline.

    Parameters
    ----------
    steps:
        Ordered list of ``(name, check)`` pairs.
    stop_on_failure:
        When ``True``, execution halts after the first check whose severity
        is ``FAILURE`` **and** whose result is not passed.  ``WARNING``-
        severity failures and passing ``FAILURE``-severity checks do not
        trigger the stop.
    """

    def __init__(
        self,
        steps: list[tuple[str, BaseCheck]],
        stop_on_failure: bool = False,
    ) -> None:
        self._steps = steps
        self._stop_on_failure = stop_on_failure

    # ------------------------------------------------------------------
    # BaseCheck interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "pipeline"

    @property
    def severity(self) -> Severity:
        return Severity.FAILURE

    @property
    def params(self) -> dict[str, Any]:
        return {}

    def calibrate(self, reference_data: pd.DataFrame) -> None:
        """Delegate calibration to every step."""
        for _, check in self._steps:
            check.calibrate(reference_data)

    def check(self, data: pd.DataFrame, variable: Variable) -> CheckResult:
        """Run all steps and return a single aggregated result.

        Passes if every step passes; fails otherwise.  Used when the pipeline
        itself is a step inside another pipeline.
        """
        results = self.run(data, variable)
        all_passed = all(r.passed for r in results)
        population_size = results[0].population_size if results else 1
        return CheckResult(
            check_name=self.name,
            passed=all_passed,
            severity=self.severity,
            observed_value=len(results),
            population_size=population_size,
            threshold=None,
        )

    # ------------------------------------------------------------------
    # Pipeline-specific interface
    # ------------------------------------------------------------------

    def run(self, data: pd.DataFrame, variable: Variable) -> list[CheckResult]:
        """Execute all steps in order and return the collected results.

        Short-circuits when ``stop_on_failure=True`` and a ``FAILURE``-
        severity check does not pass.
        """
        results: list[CheckResult] = []
        for _, check in self._steps:
            result = check.check(data, variable)
            results.append(result)
            if self._stop_on_failure and not result.passed and result.severity == Severity.FAILURE:
                break
        return results
