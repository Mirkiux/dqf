from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CardinalityThresholds:
    """Single source of truth for cardinality cutoff values.

    Pass the **same** instance to both :func:`~dqf.defaults.build_default_resolver`
    and :func:`~dqf.defaults.build_default_metadata_resolver` to guarantee that
    dtype inference, metadata profiling, and cardinality checks all use identical
    thresholds.

    Parameters
    ----------
    low:
        Maximum distinct non-null values for a column to be classified as
        ``NUMERIC_DISCRETE`` (numeric storage) or ``CATEGORICAL``
        (non-numeric storage) during dtype inference.  Default ``20``.
    high:
        Upper bound used as a cardinality warning ceiling in
        ``NUMERIC_DISCRETE`` and ``CATEGORICAL`` check pipelines, and as the
        ``is_high_cardinality`` flag threshold in the metadata
        ``CardinalityBuilder``.  Default ``50``.
    """

    low: int = 20
    high: int = 50
