from __future__ import annotations

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.datasets.universe import UniverseDataset
from dqf.enums import DataType
from dqf.metadata.base import MetadataBuilderPipeline
from dqf.results import ValidationResult
from dqf.variable import Variable

_VD_MATCHED = "__vd_matched__"
_MERGE_INDICATOR = "__vd_merge__"
_PK_CHECK = "pk_uniqueness"
_JOIN_CHECK = "join_integrity"


class VariablesDataset:
    """The dataset that carries the variable values to be validated.

    :meth:`to_pandas` performs a LEFT JOIN from the universe to the variables
    data, preserving all universe entities, and appends the framework-managed
    column ``__vd_matched__``.  This distinguishes structural nulls (entity
    absent from the variables dataset) from value nulls (entity present but
    column is null).

    The materialized DataFrame is cached on first call so the dataset is a
    rich stateful object that can be passed to metadata builders, check
    pipelines, and validation helpers without re-executing the queries.

    SQL is executed in the native engine via each dataset's own adapter —
    the universe may live on Databricks while the variables live on Oracle.
    Only the result sets cross into pandas, keeping all downstream logic
    engine-agnostic.

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
        self.variables: list[Variable] = variables if variables is not None else []
        self._data: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """Materialise both datasets and return a cached universe-anchored left join.

        The result always contains every universe row.  The boolean column
        ``__vd_matched__`` is ``True`` where the universe entity was found in
        the variables dataset and ``False`` for structural absences.

        Raises
        ------
        ValueError
            If the input datasets already contain a column named
            ``__vd_matched__``, which would be silently overwritten.
        """
        if self._data is None:
            self._data = self._materialise()
        return self._data

    def _materialise(self) -> pd.DataFrame:
        universe_df = self.universe.to_pandas()
        variables_df = self.adapter.execute(self.sql)

        # Guard: framework column must not already exist in either source
        for col, source in ((_VD_MATCHED, "universe"), (_VD_MATCHED, "variables")):
            df = universe_df if source == "universe" else variables_df
            if col in df.columns:
                raise ValueError(
                    f"Input {source} dataset already contains the framework-reserved "
                    f"column '{col}'. Rename it before passing to VariablesDataset."
                )

        # Build merge key lists: universe columns (left_on) and variables columns (right_on)
        left_on = list(self.join_keys.values())  # universe columns
        right_on = list(self.join_keys.keys())  # variables columns

        # Use a dedicated indicator column name to avoid collisions with user data
        merged = universe_df.merge(
            variables_df,
            how="left",
            left_on=left_on,
            right_on=right_on,
            indicator=_MERGE_INDICATOR,
        )
        merged[_VD_MATCHED] = merged[_MERGE_INDICATOR] == "both"
        merged = merged.drop(columns=[_MERGE_INDICATOR])

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

        Fails immediately with a descriptive message if any required join key
        column is missing from *variables_data*.

        Null values in join key columns are excluded before the uniqueness
        check — this matches SQL semantics where ``NULL != NULL`` and multiple
        null-key rows do not create a join fan-out.
        """
        join_cols = list(self.join_keys.keys())

        # Fail fast if any required join keys are missing
        missing = [c for c in join_cols if c not in variables_data.columns]
        if missing:
            return ValidationResult(
                check_name=_JOIN_CHECK,
                passed=False,
                details={"join_keys": self.join_keys, "missing_join_keys": missing},
            )

        # Exclude rows with nulls in any join key (SQL NULL semantics)
        non_null_mask = variables_data[join_cols].notna().all(axis=1)
        non_null_rows = variables_data[non_null_mask]
        has_fanout = bool(non_null_rows.duplicated(subset=join_cols).any())

        return ValidationResult(
            check_name=_JOIN_CHECK,
            passed=not has_fanout,
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
