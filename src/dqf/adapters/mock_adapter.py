from __future__ import annotations

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.enums import EngineType


class MockAdapter(DataSourceAdapter):
    """In-memory adapter for tests and examples.

    Accepts a dict mapping SQL strings to pre-built DataFrames.
    Raises ``KeyError`` when an unregistered SQL string is executed.
    Has no external dependencies.
    """

    def __init__(self, results: dict[str, pd.DataFrame]) -> None:
        self._results = results

    def execute(self, sql: str) -> pd.DataFrame:
        return self._results[sql]

    def engine_type(self) -> EngineType:
        return EngineType.MOCK
