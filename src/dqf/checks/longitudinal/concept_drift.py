"""ConceptDriftCheck — distribution drift for target variables (model monitoring)."""

from __future__ import annotations

from dqf.checks.longitudinal.distribution_drift import DistributionDriftCheck


class ConceptDriftCheck(DistributionDriftCheck):
    """Distribution drift check for VariableRole.TARGET columns.

    Semantically identical to :class:`DistributionDriftCheck` but reports
    ``name="concept_drift"`` and is intended to be automatically routed to
    ``VariableRole.TARGET`` variables by the default resolver (Plan 13).
    A failing concept drift check signals potential model retraining need.
    """

    @property
    def name(self) -> str:
        return "concept_drift"
