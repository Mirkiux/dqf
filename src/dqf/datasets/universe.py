from __future__ import annotations

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.results import ValidationResult

_PK_CHECK = "pk_uniqueness"


class UniverseDataset:
    """Represents the universe (population) of entities for a validation run.

    The universe is the authoritative source of entity identity.  All
    percentage-based metrics are measured against ``len(universe)``.

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
    """

    def __init__(
        self,
        sql: str,
        primary_key: list[str],
        adapter: DataSourceAdapter,
        time_field: str | None = None,
    ) -> None:
        self.sql = sql
        self.primary_key = primary_key
        self.adapter = adapter
        self.time_field = time_field

    def to_pandas(self) -> pd.DataFrame:
        """Execute the universe query and return the result as a DataFrame."""
        return self.adapter.execute(self.sql)

    def validate_pk_uniqueness(self, data: pd.DataFrame) -> ValidationResult:
        """Check that *primary_key* columns form a unique key in *data*."""
        has_duplicates = data.duplicated(subset=self.primary_key).any()
        return ValidationResult(
            check_name=_PK_CHECK,
            passed=not bool(has_duplicates),
            details={"primary_key": self.primary_key},
        )
