"""Tests for UniverseDataset and VariablesDataset (plan 8)."""

from __future__ import annotations

import pandas as pd

from dqf.adapters.mock_adapter import MockAdapter
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.metadata.base import MetadataBuilderPipeline
from dqf.metadata.builders.dtype_builder import StorageDtypeBuilder
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_UNIVERSE_SQL = "SELECT * FROM universe"
_VARIABLES_SQL = "SELECT * FROM variables"

_UNIVERSE_DF = pd.DataFrame({"entity_id": [1, 2, 3]})
_VARIABLES_DF = pd.DataFrame({"entity_id": [1, 2], "score": [0.9, 0.4]})


def make_universe(extra_rows: pd.DataFrame | None = None) -> UniverseDataset:
    df = extra_rows if extra_rows is not None else _UNIVERSE_DF
    adapter = MockAdapter({_UNIVERSE_SQL: df})
    return UniverseDataset(
        sql=_UNIVERSE_SQL,
        primary_key=["entity_id"],
        adapter=adapter,
    )


def make_variables(
    universe: UniverseDataset | None = None,
    variables_df: pd.DataFrame | None = None,
) -> VariablesDataset:
    u = universe or make_universe()
    vdf = variables_df if variables_df is not None else _VARIABLES_DF
    adapter = MockAdapter({_VARIABLES_SQL: vdf})
    return VariablesDataset(
        sql=_VARIABLES_SQL,
        primary_key=["entity_id"],
        universe=u,
        join_keys={"entity_id": "entity_id"},
        adapter=adapter,
    )


def make_pipeline() -> MetadataBuilderPipeline:
    return MetadataBuilderPipeline([("dtype", StorageDtypeBuilder())])


# ---------------------------------------------------------------------------
# TestUniverseDataset
# ---------------------------------------------------------------------------


class TestUniverseDataset:
    def test_to_pandas_delegates_to_adapter(self) -> None:
        u = make_universe()
        result = u.to_pandas()
        assert result.equals(_UNIVERSE_DF)

    def test_to_pandas_returns_correct_shape(self) -> None:
        u = make_universe()
        result = u.to_pandas()
        assert result.shape == (3, 1)

    def test_validate_pk_uniqueness_passes_for_unique_keys(self) -> None:
        u = make_universe()
        result = u.validate_pk_uniqueness(_UNIVERSE_DF)
        assert result.passed is True

    def test_validate_pk_uniqueness_fails_for_duplicate_keys(self) -> None:
        u = make_universe()
        dup_df = pd.DataFrame({"entity_id": [1, 1, 2]})
        result = u.validate_pk_uniqueness(dup_df)
        assert result.passed is False

    def test_validate_pk_uniqueness_check_name(self) -> None:
        u = make_universe()
        result = u.validate_pk_uniqueness(_UNIVERSE_DF)
        assert result.check_name == "pk_uniqueness"

    def test_time_field_default_is_none(self) -> None:
        u = make_universe()
        assert u.time_field is None

    def test_time_field_stored(self) -> None:
        adapter = MockAdapter({_UNIVERSE_SQL: _UNIVERSE_DF})
        u = UniverseDataset(
            sql=_UNIVERSE_SQL,
            primary_key=["entity_id"],
            adapter=adapter,
            time_field="date",
        )
        assert u.time_field == "date"


# ---------------------------------------------------------------------------
# TestVariablesDatasetToPandas
# ---------------------------------------------------------------------------


class TestVariablesDatasetToPandas:
    def setup_method(self) -> None:
        self.vd = make_variables()
        self.result = self.vd.to_pandas()

    def test_to_pandas_contains_vd_matched_column(self) -> None:
        assert "__vd_matched__" in self.result.columns

    def test_to_pandas_universe_rows_all_present(self) -> None:
        assert len(self.result) == 3

    def test_to_pandas_matched_flag_true_for_joined_rows(self) -> None:
        matched = self.result[self.result["entity_id"].isin([1, 2])]
        assert matched["__vd_matched__"].all()

    def test_to_pandas_matched_flag_false_for_unmatched_rows(self) -> None:
        unmatched = self.result[self.result["entity_id"] == 3]
        assert (~unmatched["__vd_matched__"]).all()

    def test_to_pandas_structural_nulls_in_unmatched_rows(self) -> None:
        unmatched = self.result[self.result["entity_id"] == 3]
        assert unmatched["score"].isna().all()

    def test_to_pandas_value_columns_correct_for_matched_rows(self) -> None:
        row1 = self.result[self.result["entity_id"] == 1].iloc[0]
        assert row1["score"] == 0.9
        row2 = self.result[self.result["entity_id"] == 2].iloc[0]
        assert row2["score"] == 0.4


# ---------------------------------------------------------------------------
# TestVariablesDatasetValidation
# ---------------------------------------------------------------------------


class TestVariablesDatasetValidation:
    def test_validate_pk_uniqueness_passes(self) -> None:
        vd = make_variables()
        result = vd.validate_pk_uniqueness(_VARIABLES_DF)
        assert result.passed is True

    def test_validate_pk_uniqueness_fails(self) -> None:
        vd = make_variables()
        dup = pd.DataFrame({"entity_id": [1, 1], "score": [0.9, 0.8]})
        result = vd.validate_pk_uniqueness(dup)
        assert result.passed is False

    def test_validate_join_integrity_passes_when_no_fanout(self) -> None:
        vd = make_variables()
        result = vd.validate_join_integrity(_VARIABLES_DF, _UNIVERSE_DF)
        assert result.passed is True

    def test_validate_join_integrity_fails_when_fanout(self) -> None:
        vd = make_variables()
        fanout = pd.DataFrame({"entity_id": [1, 1, 2], "score": [0.9, 0.7, 0.4]})
        result = vd.validate_join_integrity(fanout, _UNIVERSE_DF)
        assert result.passed is False

    def test_validate_join_integrity_check_name(self) -> None:
        vd = make_variables()
        result = vd.validate_join_integrity(_VARIABLES_DF, _UNIVERSE_DF)
        assert result.check_name == "join_integrity"


# ---------------------------------------------------------------------------
# TestVariablesDatasetResolveVariables
# ---------------------------------------------------------------------------


class TestVariablesDatasetResolveVariables:
    def setup_method(self) -> None:
        self.vd = make_variables()
        self.joined = self.vd.to_pandas()
        self.pipeline = make_pipeline()

    def test_resolve_variables_returns_list_of_variables(self) -> None:
        result = self.vd.resolve_variables(self.joined, self.pipeline)
        assert isinstance(result, list)
        assert all(isinstance(v, Variable) for v in result)

    def test_resolve_variables_excludes_vd_matched_column(self) -> None:
        result = self.vd.resolve_variables(self.joined, self.pipeline)
        names = [v.name for v in result]
        assert "__vd_matched__" not in names

    def test_resolve_variables_one_variable_per_column(self) -> None:
        df = pd.DataFrame({"entity_id": [1], "a": [1], "b": [2], "__vd_matched__": [True]})
        result = self.vd.resolve_variables(df, self.pipeline)
        assert len(result) == 3  # entity_id, a, b

    def test_resolve_variables_metadata_populated_by_pipeline(self) -> None:
        result = self.vd.resolve_variables(self.joined, self.pipeline)
        for v in result:
            assert "storage_dtype" in v.metadata

    def test_resolve_variables_stores_result_on_self(self) -> None:
        result = self.vd.resolve_variables(self.joined, self.pipeline)
        assert self.vd.variables is result
