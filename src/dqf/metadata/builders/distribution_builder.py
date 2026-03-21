from __future__ import annotations

from typing import Any

import pandas as pd

from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable


class DistributionShapeBuilder(BaseMetadataBuilder):
    """Computes descriptive statistics for numeric series.

    Returns an empty dict (and leaves ``variable.metadata`` unchanged) for
    non-numeric series so that downstream consumers can safely skip it.
    """

    @property
    def name(self) -> str:
        return "distribution"

    def profile(self, series: pd.Series, variable: Variable) -> dict[str, Any]:
        if not pd.api.types.is_numeric_dtype(series):
            return {}

        numeric = series.dropna().astype(float)
        result: dict[str, Any] = {
            "mean": float(numeric.mean()),
            "std": float(numeric.std()),
            "min": float(numeric.min()),
            "max": float(numeric.max()),
            "skewness": float(numeric.skew()),
            "kurtosis": float(numeric.kurt()),
        }
        variable.metadata.update(result)
        return result
