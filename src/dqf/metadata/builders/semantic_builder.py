from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from dqf.enums import DataType
from dqf.metadata.base import BaseMetadataBuilder
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset

_COERCE_THRESHOLD = 0.95


class SemanticTypeInferenceBuilder(BaseMetadataBuilder):
    """Infers a :class:`~dqf.enums.DataType` from the series content.

    Mirrors the priority of :meth:`~dqf.variable.Variable.infer_dtype` so that
    metadata and variable dtype always agree when built from the same data.

    Inference priority:
    1. Boolean dtype → ``BOOLEAN``
    2. Numeric dtype:

       - Distinct non-null values ≤ ``low_cardinality_threshold`` → ``NUMERIC_DISCRETE``
       - Otherwise → ``NUMERIC_CONTINUOUS``

    3. Datetime dtype → ``DATETIME``
    4. Object/string: attempt numeric coercion (≥95% success) → ``NUMERIC_CONTINUOUS``
    5. Object/string: attempt datetime coercion (≥95% success) → ``DATETIME``
    6. Distinct non-null values ≤ ``low_cardinality_threshold`` → ``CATEGORICAL``
    7. Default → ``TEXT``
    """

    def __init__(self, low_cardinality_threshold: int = 20) -> None:
        self._low_cardinality_threshold = low_cardinality_threshold

    @property
    def name(self) -> str:
        return "semantic_type"

    def profile(self, dataset: VariablesDataset, variable: Variable) -> dict[str, Any]:
        series: pd.Series = dataset.materialise()[variable.name]
        dtype = _infer(series, self._low_cardinality_threshold)
        result: dict[str, Any] = {"semantic_dtype": dtype}
        variable.metadata.update(result)
        return result


def _infer(series: pd.Series, low_cardinality_threshold: int) -> DataType:
    if pd.api.types.is_bool_dtype(series):
        return DataType.BOOLEAN

    non_null = series.dropna()

    if pd.api.types.is_numeric_dtype(series):
        if non_null.nunique() <= low_cardinality_threshold:
            return DataType.NUMERIC_DISCRETE
        return DataType.NUMERIC_CONTINUOUS

    if pd.api.types.is_datetime64_any_dtype(series):
        return DataType.DATETIME

    # Object / string — try coercions before cardinality fallback
    if len(non_null) > 0:
        numeric_converted = pd.to_numeric(non_null, errors="coerce")
        if numeric_converted.notna().mean() >= _COERCE_THRESHOLD:
            return DataType.NUMERIC_CONTINUOUS

        datetime_converted = pd.to_datetime(non_null, errors="coerce")
        if datetime_converted.notna().mean() >= _COERCE_THRESHOLD:
            return DataType.DATETIME

    if non_null.nunique() <= low_cardinality_threshold:
        return DataType.CATEGORICAL

    return DataType.TEXT
