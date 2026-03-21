from __future__ import annotations

from typing import Any

import pandas as pd

from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable


class NullabilityProfileBuilder(BaseMetadataBuilder):
    """Profiles null rates and counts for a series."""

    @property
    def name(self) -> str:
        return "nullability"

    def profile(self, series: pd.Series, variable: Variable) -> dict[str, Any]:
        null_count = int(series.isna().sum())
        total = len(series)
        empirical_null_rate = null_count / total if total > 0 else 0.0
        result: dict[str, Any] = {
            "null_count": null_count,
            "empirical_null_rate": empirical_null_rate,
            "is_nullable": null_count > 0,
        }
        variable.metadata.update(result)
        return result
