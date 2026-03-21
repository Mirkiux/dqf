from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset


class CardinalityBuilder(BaseMetadataBuilder):
    """Counts unique non-null values and flags high-cardinality series."""

    def __init__(self, high_cardinality_threshold: int = 50) -> None:
        self._threshold = high_cardinality_threshold

    @property
    def name(self) -> str:
        return "cardinality"

    def profile(self, dataset: VariablesDataset, variable: Variable) -> dict[str, Any]:
        series: pd.Series = dataset.materialise()[variable.name]
        cardinality = int(series.dropna().nunique())
        result: dict[str, Any] = {
            "cardinality": cardinality,
            "is_high_cardinality": cardinality > self._threshold,
        }
        variable.metadata.update(result)
        return result
