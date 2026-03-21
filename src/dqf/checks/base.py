from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from dqf.enums import Severity
from dqf.results import CheckResult
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class BaseCheck(ABC):
    """Abstract base for all data quality checks.

    Subclasses must implement :meth:`check`. Optionally override
    :meth:`calibrate` for checks that derive statistical baselines from
    reference data (deliberately named to differ from sklearn's ``fit``).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this check."""

    @property
    @abstractmethod
    def severity(self) -> Severity:
        """Whether a failure is a hard ``FAILURE`` or a ``WARNING``."""

    @property
    def params(self) -> dict[str, Any]:
        """Business thresholds and configuration for this check."""
        return {}

    def calibrate(self, dataset: VariablesDataset) -> None:  # noqa: B027
        """Establish a statistical baseline from *dataset*.

        The default implementation is a no-op. Override in checks that need
        to derive thresholds from historical data (e.g. drift detection).
        """

    @abstractmethod
    def check(self, dataset: VariablesDataset, variable: Variable) -> CheckResult:
        """Run the check against *dataset* for *variable* and return a result."""


class BaseCrossSectionalCheck(BaseCheck, ABC):
    """Base for checks that operate on entity-level (point-in-time) data.

    ``check()`` receives a VariablesDataset where each row is one
    entity. ``__vd_matched__`` marks whether the entity appeared in the
    variables dataset.
    """


class BaseLongitudinalCheck(BaseCheck, ABC):
    """Base for checks that operate on time-aggregated summary data.

    Implementors must also provide :meth:`aggregation_sql` so the framework
    can materialise the time-indexed summary DataFrame engine-side before
    calling ``check()``.
    """

    @abstractmethod
    def aggregation_sql(self, variable_name: str, time_field: str, period: str) -> str:
        """Return SQL that produces a time-indexed summary for *variable_name*.

        Parameters
        ----------
        variable_name:
            The column name in the variables table to aggregate.
        time_field:
            The datetime column to group by.
        period:
            Truncation period (e.g. ``"month"``, ``"week"``).
        """
