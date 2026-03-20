from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from dqf.enums import EngineType


class DataSourceAdapter(ABC):
    """Abstract base for all data source adapters.

    Every adapter must be able to execute a SQL string and return
    the result as a pandas DataFrame. The engine_type() method
    identifies which backend this adapter wraps.
    """

    @abstractmethod
    def execute(self, sql: str) -> pd.DataFrame:
        """Execute *sql* against the underlying engine and return a DataFrame."""

    @abstractmethod
    def engine_type(self) -> EngineType:
        """Return the :class:`~dqf.enums.EngineType` for this adapter."""
