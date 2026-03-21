from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class StorageDtypeBuilder(BaseMetadataBuilder):
    """Records the pandas storage dtype of the series."""

    @property
    def name(self) -> str:
        return "storage_dtype"

    def profile(self, dataset: VariablesDataset, variable: Variable) -> dict[str, Any]:
        series: pd.Series = dataset.materialise()[variable.name]
        result: dict[str, Any] = {"storage_dtype": str(series.dtype)}
        variable.metadata.update(result)
        return result
