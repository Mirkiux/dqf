from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from dqf.variable import Variable


class BaseMetadataBuilder(ABC):
    """Abstract base for all metadata builders.

    Each builder inspects a ``pd.Series`` and returns a flat dict of
    key/value pairs to be merged into ``Variable.metadata``.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this builder."""

    @abstractmethod
    def profile(self, series: pd.Series, variable: Variable) -> dict[str, Any]:
        """Profile *series* and return a flat metadata dict.

        Implementations must also update ``variable.metadata`` in-place.
        """


class MetadataBuilderPipeline(BaseMetadataBuilder):
    """Chains multiple builders and merges their outputs.

    Implements the Composite pattern: a pipeline is itself a
    ``BaseMetadataBuilder`` and can be used wherever a single builder
    is expected.
    """

    def __init__(self, steps: list[tuple[str, BaseMetadataBuilder]]) -> None:
        self._steps = steps

    @property
    def name(self) -> str:
        return "pipeline"

    def profile(self, series: pd.Series, variable: Variable) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for _, builder in self._steps:
            result = builder.profile(series, variable)
            merged.update(result)
        variable.metadata.update(merged)
        return merged
