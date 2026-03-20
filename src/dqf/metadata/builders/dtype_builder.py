from __future__ import annotations

from typing import Any

import pandas as pd

from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable


class StorageDtypeBuilder(BaseMetadataBuilder):
    """Records the pandas storage dtype of the series."""

    @property
    def name(self) -> str:
        return "storage_dtype"

    def profile(self, series: pd.Series, variable: Variable) -> dict[str, Any]:
        result: dict[str, Any] = {"storage_dtype": str(series.dtype)}
        variable.metadata.update(result)
        return result
