from __future__ import annotations

from typing import Any

import pandas as pd

from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable


class CardinalityBuilder(BaseMetadataBuilder):
    """Counts unique non-null values and flags high-cardinality series."""

    def __init__(self, high_cardinality_threshold: int = 50) -> None:
        self._threshold = high_cardinality_threshold

    @property
    def name(self) -> str:
        return "cardinality"

    def profile(self, series: pd.Series, variable: Variable) -> dict[str, Any]:
        cardinality = int(series.dropna().nunique())
        result: dict[str, Any] = {
            "cardinality": cardinality,
            "is_high_cardinality": cardinality > self._threshold,
        }
        variable.metadata.update(result)
        return result
