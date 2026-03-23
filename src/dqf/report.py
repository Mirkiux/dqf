"""ValidationReport — top-level output of a validation run."""

from __future__ import annotations

import base64
import io
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

from dqf.enums import Severity, ValidationStatus
from dqf.results import CheckResult, ValidationResult


@dataclass
class ValidationReport:
    """Top-level output of :meth:`~dqf.datasets.variables.VariablesDataset.run_validation`.

    Captures both dataset-level invariant results (PK uniqueness, join integrity)
    and per-variable check results assembled during a single validation run.

    Parameters
    ----------
    dataset_name:
        Human-readable identifier for the dataset being validated.
    run_timestamp:
        UTC timestamp when the validation run started.
    universe_size:
        Number of entities in the universe dataset.
    dataset_level_checks:
        Results of dataset-level invariant checks (PK uniqueness, join integrity).
    variable_results:
        Mapping of variable name → list of :class:`~dqf.results.CheckResult` instances
        collected during the run.
    """

    dataset_name: str
    run_timestamp: datetime
    universe_size: int = 0
    dataset_level_checks: list[ValidationResult] = field(default_factory=list)
    variable_results: dict[str, list[CheckResult]] = field(default_factory=dict)

    @property
    def overall_status(self) -> ValidationStatus:
        """Aggregate status derived from all checks in this report.

        Returns ``FAILED`` if any dataset-level invariant check failed or any
        variable has at least one ``FAILURE``-severity failed check.
        Returns ``PASSED`` otherwise (``WARNING``-severity failures do not
        cause an overall ``FAILED``).
        """
        if any(not r.passed for r in self.dataset_level_checks):
            return ValidationStatus.FAILED
        for results in self.variable_results.values():
            if any(r.severity == Severity.FAILURE and not r.passed for r in results):
                return ValidationStatus.FAILED
        return ValidationStatus.PASSED

    @property
    def variable_statuses(self) -> dict[str, ValidationStatus]:
        """Per-variable validation status derived from check results.

        A variable is ``FAILED`` if any of its check results did not pass
        (regardless of severity).  It is ``PASSED`` only when all checks pass.
        """
        statuses: dict[str, ValidationStatus] = {}
        for name, results in self.variable_results.items():
            if any(not r.passed for r in results):
                statuses[name] = ValidationStatus.FAILED
            else:
                statuses[name] = ValidationStatus.PASSED
        return statuses

    def failed_variables(self) -> list[str]:
        """Return names of variables with at least one ``FAILURE``-severity failed check."""
        return [
            name
            for name, results in self.variable_results.items()
            if any(r.severity == Severity.FAILURE and not r.passed for r in results)
        ]

    def warned_variables(self) -> list[str]:
        """Return variable names with ``WARNING``-severity failures but no ``FAILURE`` failures."""
        return [
            name
            for name, results in self.variable_results.items()
            if (
                any(r.severity == Severity.WARNING and not r.passed for r in results)
                and not any(r.severity == Severity.FAILURE and not r.passed for r in results)
            )
        ]

    def to_dataframe(self) -> pd.DataFrame:
        """Return a flat tabular summary with one row per check result.

        Columns: ``variable``, ``check_name``, ``passed``, ``severity``,
        ``rate``, ``threshold``, ``observed_value``.
        """
        rows: list[dict[str, Any]] = []
        for var_name, results in self.variable_results.items():
            for r in results:
                rows.append(
                    {
                        "variable": var_name,
                        "check_name": r.check_name,
                        "passed": r.passed,
                        "severity": r.severity.value,
                        "rate": r.rate,
                        "threshold": r.threshold,
                        "observed_value": r.observed_value,
                    }
                )
        return pd.DataFrame(rows)

    def render(self, output_path: str | None = None) -> str:
        """Materialise all figure factories and assemble an HTML report.

        Each :class:`~dqf.results.CheckResult` with a non-``None``
        ``figure_factory`` is rendered into a PNG and embedded as a base64
        ``<img>`` tag.  The figure is closed after rendering to avoid memory
        leaks.

        Parameters
        ----------
        output_path:
            If provided, the HTML string is also written to this file path.

        Returns
        -------
        str
            The complete HTML report as a string.
        """
        status_class = "passed" if self.overall_status == ValidationStatus.PASSED else "failed"
        parts: list[str] = [
            "<!DOCTYPE html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="UTF-8">',
            f"<title>Validation Report — {self.dataset_name}</title>",
            "<style>",
            "  body { font-family: sans-serif; margin: 2em; }",
            "  h1 { color: #333; }",
            "  .passed { color: green; } .failed { color: red; }",
            "  table { border-collapse: collapse; width: 100%; margin-bottom: 1em; }",
            "  th, td { border: 1px solid #ccc; padding: 0.4em 0.8em; text-align: left; }",
            "  th { background: #f5f5f5; }",
            "  img { max-width: 600px; display: block; margin: 0.5em 0; }",
            "</style>",
            "</head>",
            "<body>",
            f"<h1>Validation Report: {self.dataset_name}</h1>",
            (
                f"<p>Run: {self.run_timestamp.isoformat()} | "
                f"Universe size: {self.universe_size} | "
                f'Status: <span class="{status_class}">{self.overall_status.value}</span></p>'
            ),
        ]

        # Dataset-level checks
        if self.dataset_level_checks:
            parts += [
                "<h2>Dataset-Level Checks</h2>",
                "<table>",
                "<tr><th>Check</th><th>Passed</th><th>Details</th></tr>",
            ]
            for r in self.dataset_level_checks:
                icon = "&#10003;" if r.passed else "&#10007;"
                parts.append(f"<tr><td>{r.check_name}</td><td>{icon}</td><td>{r.details}</td></tr>")
            parts.append("</table>")

        # Per-variable results
        if self.variable_results:
            parts.append("<h2>Variable Results</h2>")
            for var_name, results in self.variable_results.items():
                var_status = self.variable_statuses.get(var_name, ValidationStatus.PASSED)
                status_cls = "passed" if var_status == ValidationStatus.PASSED else "failed"
                parts += [
                    f'<h3><span class="{status_cls}">{var_name}</span></h3>',
                    "<table>",
                    (
                        "<tr><th>Check</th><th>Passed</th><th>Severity</th>"
                        "<th>Observed</th><th>Threshold</th><th>Rate</th></tr>"
                    ),
                ]
                for cr in results:
                    icon = "&#10003;" if cr.passed else "&#10007;"
                    rate_str = f"{cr.rate:.2%}" if cr.rate is not None else "&#8212;"
                    parts.append(
                        f"<tr><td>{cr.check_name}</td><td>{icon}</td>"
                        f"<td>{cr.severity.value}</td><td>{cr.observed_value}</td>"
                        f"<td>{cr.threshold}</td><td>{rate_str}</td></tr>"
                    )
                    if cr.figure_factory is not None:
                        fig = cr.figure_factory()
                        buf = io.BytesIO()
                        fig.savefig(buf, format="png", bbox_inches="tight")
                        buf.seek(0)
                        img_b64 = base64.b64encode(buf.read()).decode()
                        plt.close(fig)
                        parts.append(
                            f'<tr><td colspan="6">'
                            f'<img src="data:image/png;base64,{img_b64}" '
                            f'alt="{var_name} — {r.check_name}"/>'
                            f"</td></tr>"
                        )
                parts.append("</table>")

        parts += ["</body>", "</html>"]
        html = "\n".join(parts)

        if output_path is not None:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html)

        return html
