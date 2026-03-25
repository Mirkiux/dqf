from __future__ import annotations

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.results import ValidationResult

_PK_CHECK = "pk_uniqueness"


class UniverseDataset:
    """Represents the universe (population) of entities for a validation run.

    The universe is the authoritative source of entity identity.  All
    percentage-based metrics are measured against ``len(universe)``.

    SQL is executed in the native engine via *adapter* — Oracle runs on Oracle,
    Databricks on Databricks.  Only the result set crosses into pandas, keeping
    all check logic engine-agnostic.  The materialised DataFrame is cached after
    the first call to :meth:`materialise` so the dataset can be passed around as
    a rich stateful object without re-querying the engine.

    Parameters
    ----------
    sql:
        Query that returns the universe.
    primary_key:
        Column(s) that uniquely identify each entity.
    adapter:
        Adapter used to execute *sql*.
    time_field:
        Optional datetime column; required for longitudinal checks.
    target:
        Optional name of the target column for supervised models (e.g.
        ``"churn"``, ``"price"``).  Cannot be inferred from the data, so it
        must be declared explicitly.  When set, downstream components can
        automatically assign :attr:`~dqf.enums.VariableRole.TARGET` to the
        corresponding :class:`~dqf.variable.Variable`.
    """

    def __init__(
        self,
        sql: str,
        primary_key: list[str],
        adapter: DataSourceAdapter,
        time_field: str | None = None,
        target: str | None = None,
    ) -> None:
        self.sql = sql
        self.primary_key = primary_key
        self.adapter = adapter
        self.time_field = time_field
        self.target = target
        self._data: pd.DataFrame | None = None
        self.pk_validation: ValidationResult | None = None

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def materialise(self, force: bool = False) -> pd.DataFrame:
        """Execute the universe query and return the cached DataFrame.

        Parameters
        ----------
        force:
            When ``True`` the query is re-executed and the cache is refreshed
            even if data was previously materialised.
        """
        if force or self._data is None:
            self._data = self.adapter.execute(self.sql)
        return self._data

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def validate_pk_uniqueness(self) -> ValidationResult:
        """Check that *primary_key* columns form a unique key in the materialised data."""
        data = self.materialise()
        has_duplicates = data.duplicated(subset=self.primary_key).any()
        self.pk_validation = ValidationResult(
            check_name=_PK_CHECK,
            passed=not bool(has_duplicates),
            details={"primary_key": self.primary_key},
        )
        return self.pk_validation
