from __future__ import annotations

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.datasets.universe import UniverseDataset
from dqf.enums import DataType
from dqf.metadata.base import MetadataBuilderPipeline
from dqf.results import ValidationResult
from dqf.variable import Variable

_VD_MATCHED = "__vd_matched__"
_PK_CHECK = "pk_uniqueness"
_JOIN_CHECK = "join_integrity"


class VariablesDataset:
    """The dataset that carries the variable values to be validated.

    ``to_pandas()`` always performs a LEFT JOIN from the universe to the
    variables data, preserving all universe entities.  The framework-managed
    column ``__vd_matched__`` distinguishes structural nulls (entity absent
    from the variables dataset) from value nulls (entity present but column
    is null).

    Parameters
    ----------
    sql:
        Query that returns the variables dataset.
    primary_key:
        Column(s) that form the unique key of the variables dataset.
    universe:
        The universe this dataset is joined against.
    join_keys:
        Mapping ``{"variables_col": "universe_col"}`` used for the join.
    adapter:
        Adapter used to execute *sql* (may differ from ``universe.adapter``).
    variables:
        Optional pre-populated list of :class:`~dqf.variable.Variable`.
        Populated automatically by :meth:`resolve_variables` if not provided.
    """

    def __init__(
        self,
        sql: str,
        primary_key: list[str],
        universe: UniverseDataset,
        join_keys: dict[str, str],
        adapter: DataSourceAdapter,
        variables: list[Variable] | None = None,
    ) -> None:
        self.sql = sql
        self.primary_key = primary_key
        self.universe = universe
        self.join_keys = join_keys
        self.adapter = adapter
        self.variables: list[Variable] = variables or []

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """Materialise both datasets and return a universe-anchored left join.

        The result always contains every universe row.  The boolean column
        ``__vd_matched__`` is ``True`` where the universe entity was found in
        the variables dataset and ``False`` for structural absences.
        """
        universe_df = self.universe.to_pandas()
        variables_df = self.adapter.execute(self.sql)

        # Build rename map: variables_col → universe_col for merge
        left_on = list(self.join_keys.values())  # universe columns
        right_on = list(self.join_keys.keys())  # variables columns

        # Add indicator so we can derive __vd_matched__
        merged = universe_df.merge(
            variables_df,
            how="left",
            left_on=left_on,
            right_on=right_on,
            indicator=True,
        )
        merged[_VD_MATCHED] = merged["_merge"] == "both"
        merged = merged.drop(columns=["_merge"])

        # Drop duplicate join key columns introduced by the merge when keys differ
        for var_col, uni_col in self.join_keys.items():
            if var_col != uni_col and var_col in merged.columns:
                merged = merged.drop(columns=[var_col])

        return merged

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def validate_pk_uniqueness(self, data: pd.DataFrame) -> ValidationResult:
        """Check that *primary_key* columns form a unique key in *data*."""
        has_duplicates = data.duplicated(subset=self.primary_key).any()
        return ValidationResult(
            check_name=_PK_CHECK,
            passed=not bool(has_duplicates),
            details={"primary_key": self.primary_key},
        )

    def validate_join_integrity(
        self,
        variables_data: pd.DataFrame,
        universe_data: pd.DataFrame,
    ) -> ValidationResult:
        """Check that join keys in *variables_data* are unique (no fan-out).

        Fan-out occurs when a single universe entity matches multiple rows in
        the variables dataset, inflating result counts.
        """
        join_cols = list(self.join_keys.keys())
        # Only check columns that exist in variables_data
        existing = [c for c in join_cols if c in variables_data.columns]
        has_fanout = variables_data.duplicated(subset=existing).any() if existing else False
        return ValidationResult(
            check_name=_JOIN_CHECK,
            passed=not bool(has_fanout),
            details={"join_keys": self.join_keys},
        )

    # ------------------------------------------------------------------
    # Variable resolution
    # ------------------------------------------------------------------

    def resolve_variables(
        self,
        data: pd.DataFrame,
        builder_pipeline: MetadataBuilderPipeline,
    ) -> list[Variable]:
        """Create and profile one :class:`~dqf.variable.Variable` per data column.

        Framework columns (``__vd_matched__``) are excluded.  Each variable
        is profiled by running its series through *builder_pipeline*.

        The resolved list is also stored on ``self.variables``.
        """
        resolved: list[Variable] = []
        for col in data.columns:
            if col == _VD_MATCHED:
                continue
            v = Variable(name=col, dtype=DataType.TEXT)
            builder_pipeline.profile(data[col], v)
            resolved.append(v)
        self.variables = resolved
        return resolved
