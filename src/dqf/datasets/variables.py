from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.config import CardinalityThresholds
from dqf.datasets.universe import UniverseDataset
from dqf.enums import DataType, ValidationStatus, VariableRole
from dqf.metadata.resolver import MetadataResolver
from dqf.report import ValidationReport
from dqf.resolver import CheckSuiteResolver
from dqf.results import ValidationResult
from dqf.variable import Variable

_VD_MATCHED = "__vd_matched__"
_MERGE_INDICATOR = "__vd_merge__"
_PK_CHECK = "pk_uniqueness"
_JOIN_CHECK = "join_integrity"


class VariablesDataset:
    """The dataset that carries the variable values to be validated.

    :meth:`materialise` performs a LEFT JOIN from the universe to the variables
    data, preserving all universe entities, and appends the framework-managed
    column ``__vd_matched__``.  This distinguishes structural nulls (entity
    absent from the variables dataset) from value nulls (entity present but
    column is null).

    The materialised DataFrame is cached after the first call so the dataset is
    a rich stateful object that can be passed to metadata builders, check
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
        self._raw_variables_data: pd.DataFrame | None = None
        self.pk_validation: ValidationResult | None = None
        self.join_validation: ValidationResult | None = None
        self.validation_report: ValidationReport | None = None
        self.validation_state: ValidationStatus = ValidationStatus.PENDING

    # ------------------------------------------------------------------
    # Materialisation
    # ------------------------------------------------------------------

    def _fetch_raw_variables(self, force: bool = False) -> pd.DataFrame:
        """Execute the variables query and return the cached raw DataFrame.

        Called by :meth:`materialise` and :meth:`validate_join_integrity` so
        that raw variables data is available independently of the full join.
        """
        if force or self._raw_variables_data is None:
            self._raw_variables_data = self.adapter.execute(self.sql)
        return self._raw_variables_data

    def materialise(self, force: bool = False) -> pd.DataFrame:
        """Materialise both datasets and return a cached universe-anchored left join.

        The result always contains every universe row.  The boolean column
        ``__vd_matched__`` is ``True`` where the universe entity was found in
        the variables dataset and ``False`` for structural absences.

        Parameters
        ----------
        force:
            When ``True`` both queries are re-executed and the cache is refreshed
            even if data was previously materialised.

        Raises
        ------
        ValueError
            If the input datasets already contain a column named
            ``__vd_matched__``, which would be silently overwritten.
        """
        if force or self._data is None:
            universe_df = self.universe.materialise(force=force)
            variables_df = self._fetch_raw_variables(force=force)

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

            self._data = merged

        return self._data

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def validate_pk_uniqueness(self) -> ValidationResult:
        """Check that *primary_key* columns form a unique key in the variables dataset."""
        data = self._fetch_raw_variables()
        has_duplicates = data.duplicated(subset=self.primary_key).any()
        self.pk_validation = ValidationResult(
            check_name=_PK_CHECK,
            passed=not bool(has_duplicates),
            details={"primary_key": self.primary_key},
        )
        return self.pk_validation

    def validate_join_integrity(self) -> ValidationResult:
        """Check that join keys in the variables dataset are unique (no fan-out).

        Fan-out occurs when a single universe entity matches multiple rows in
        the variables dataset, inflating result counts.

        Fails immediately with a descriptive message if any required join key
        column is missing from the variables dataset.

        Null values in join key columns are excluded before the uniqueness
        check — this matches SQL semantics where ``NULL != NULL`` and multiple
        null-key rows do not create a join fan-out.
        """
        # Use the raw variables data without running the full join so that
        # missing-key errors are reported as validation failures, not exceptions.
        variables_data = self._fetch_raw_variables()
        join_cols = list(self.join_keys.keys())

        # Fail fast if any required join keys are missing
        missing = [c for c in join_cols if c not in variables_data.columns]
        if missing:
            self.join_validation = ValidationResult(
                check_name=_JOIN_CHECK,
                passed=False,
                details={"join_keys": self.join_keys, "missing_join_keys": missing},
            )
            return self.join_validation

        # Exclude rows with nulls in any join key (SQL NULL semantics)
        non_null_mask = variables_data[join_cols].notna().all(axis=1)
        non_null_rows = variables_data[non_null_mask]
        has_fanout = bool(non_null_rows.duplicated(subset=join_cols).any())

        self.join_validation = ValidationResult(
            check_name=_JOIN_CHECK,
            passed=not has_fanout,
            details={"join_keys": self.join_keys},
        )
        return self.join_validation

    # ------------------------------------------------------------------
    # Variable resolution
    # ------------------------------------------------------------------

    def resolve_variables(
        self,
        metadata_resolver: MetadataResolver,
        cardinality: CardinalityThresholds | None = None,
    ) -> list[Variable]:
        """Create and profile one :class:`~dqf.variable.Variable` per data column.

        Framework columns (``__vd_matched__``) are excluded.  For each column
        the resolver selects the appropriate
        :class:`~dqf.metadata.base.MetadataBuilderPipeline` based on the
        variable's role and dtype before calling ``profile()``.

        If :attr:`~dqf.datasets.universe.UniverseDataset.target` is set on the
        associated universe, the matching variable is automatically assigned
        :attr:`~dqf.enums.VariableRole.TARGET` before profiling.

        The resolved list is also stored on ``self.variables``.

        Parameters
        ----------
        metadata_resolver:
            Resolver that selects a
            :class:`~dqf.metadata.base.MetadataBuilderPipeline` for each
            variable.
        cardinality:
            :class:`~dqf.config.CardinalityThresholds` instance.  Pass the
            **same** instance used to build *metadata_resolver* so that dtype
            inference and metadata profiling use identical thresholds.  ``None``
            uses the library defaults (``low=20``, ``high=50``).
        """
        _card = cardinality if cardinality is not None else CardinalityThresholds()
        data = self.materialise()
        resolved: list[Variable] = []
        for col in data.columns:
            if col == _VD_MATCHED:
                continue
            v = Variable(name=col, dtype=DataType.PENDING)
            v.infer_dtype(data[col], _card.low)
            if col == self.universe.target:
                v.role = VariableRole.TARGET
            metadata_resolver.resolve(v).profile(self, v)
            resolved.append(v)
        self.variables = resolved
        return resolved

    # ------------------------------------------------------------------
    # Validation orchestration
    # ------------------------------------------------------------------

    def run_validation(
        self,
        check_suite_resolver: CheckSuiteResolver,
        metadata_resolver: MetadataResolver | None = None,
        dataset_name: str = "",
        force: bool = False,
        cardinality: CardinalityThresholds | None = None,
    ) -> ValidationReport:
        """Run the full validation pipeline and return a :class:`~dqf.report.ValidationReport`.

        If ``self.validation_state`` is already ``PASSED`` and *force* is
        ``False``, the cached :attr:`validation_report` is returned immediately
        without re-executing any queries or checks.

        Steps:

        1. Materialise both datasets (universe left join variables).
        2. Run dataset-level invariant checks (PK uniqueness, join integrity).
        3. Auto-resolve variables via *metadata_resolver* if
           ``self.variables`` is empty and a resolver is provided.
        4. Dispatch each variable to its check pipeline via *check_suite_resolver*.
        5. Run each pipeline; attach results to the corresponding
           :class:`~dqf.variable.Variable`.
        6. Assemble, store, and return the :class:`~dqf.report.ValidationReport`.

        Parameters
        ----------
        check_suite_resolver:
            Registry that maps each :class:`~dqf.variable.Variable` to a
            :class:`~dqf.checks.pipeline.CheckPipeline`.
        metadata_resolver:
            Optional :class:`~dqf.metadata.resolver.MetadataResolver`.  If
            ``self.variables`` is empty and *metadata_resolver* is provided,
            :meth:`resolve_variables` is called automatically before
            dispatching checks.
        dataset_name:
            Human-readable identifier stored in the report.
        force:
            When ``True``, re-runs validation even if ``self.validation_state``
            is already ``PASSED``.  Also forces re-materialisation of both
            datasets.
        cardinality:
            :class:`~dqf.config.CardinalityThresholds` instance forwarded to
            :meth:`resolve_variables` when auto-resolving.  Pass the **same**
            instance used to build *metadata_resolver* to keep dtype inference
            and metadata profiling in sync.  ``None`` uses the library defaults
            (``low=20``, ``high=50``).
        """
        if self.validation_state == ValidationStatus.PASSED and not force:
            assert self.validation_report is not None
            return self.validation_report

        # 1. Materialise
        self.materialise(force=force)

        # 2. Dataset-level invariant checks
        pk_result = self.validate_pk_uniqueness()
        join_result = self.validate_join_integrity()

        # 3. Auto-resolve variables when needed
        if not self.variables and metadata_resolver is not None:
            self.resolve_variables(metadata_resolver, cardinality)

        # 4. Get per-variable pipelines
        pipelines = check_suite_resolver.resolve_all(self)

        # 5. Run checks and attach results
        for variable in self.variables:
            pipeline = pipelines[variable.name]
            results = pipeline.run(self, variable)
            for result in results:
                variable.attach_result(result)

        # 6. Assemble, store, and return
        report = ValidationReport(
            dataset_name=dataset_name,
            run_timestamp=datetime.now(timezone.utc),
            universe_size=len(self.universe.materialise()),
            dataset_level_checks=[pk_result, join_result],
            variable_results={v.name: list(v.check_results) for v in self.variables},
        )
        self.validation_report = report
        self.validation_state = report.overall_status
        return report
