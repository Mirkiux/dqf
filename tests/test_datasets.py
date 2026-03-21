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
    def test_materialise_delegates_to_adapter(self) -> None:
        u = make_universe()
        result = u.materialise()
        assert result.equals(_UNIVERSE_DF)

    def test_materialise_returns_correct_shape(self) -> None:
        u = make_universe()
        result = u.materialise()
        assert result.shape == (3, 1)

    def test_validate_pk_uniqueness_passes_for_unique_keys(self) -> None:
        u = make_universe()
        result = u.validate_pk_uniqueness()
        assert result.passed is True

    def test_validate_pk_uniqueness_fails_for_duplicate_keys(self) -> None:
        dup_df = pd.DataFrame({"entity_id": [1, 1, 2]})
        u = make_universe(extra_rows=dup_df)
        result = u.validate_pk_uniqueness()
        assert result.passed is False

    def test_validate_pk_uniqueness_check_name(self) -> None:
        u = make_universe()
        result = u.validate_pk_uniqueness()
        assert result.check_name == "pk_uniqueness"

    def test_validate_pk_uniqueness_stores_result_on_self(self) -> None:
        u = make_universe()
        assert u.pk_validation is None
        result = u.validate_pk_uniqueness()
        assert u.pk_validation is result

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
# TestVariablesDatasetMaterialise
# ---------------------------------------------------------------------------


class TestVariablesDatasetMaterialise:
    def setup_method(self) -> None:
        self.vd = make_variables()
        self.result = self.vd.materialise()

    def test_materialise_contains_vd_matched_column(self) -> None:
        assert "__vd_matched__" in self.result.columns

    def test_materialise_universe_rows_all_present(self) -> None:
        assert len(self.result) == 3

    def test_materialise_matched_flag_true_for_joined_rows(self) -> None:
        matched = self.result[self.result["entity_id"].isin([1, 2])]
        assert matched["__vd_matched__"].all()

    def test_materialise_matched_flag_false_for_unmatched_rows(self) -> None:
        unmatched = self.result[self.result["entity_id"] == 3]
        assert (~unmatched["__vd_matched__"]).all()

    def test_materialise_structural_nulls_in_unmatched_rows(self) -> None:
        unmatched = self.result[self.result["entity_id"] == 3]
        assert unmatched["score"].isna().all()

    def test_materialise_value_columns_correct_for_matched_rows(self) -> None:
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
        result = vd.validate_pk_uniqueness()
        assert result.passed is True

    def test_validate_pk_uniqueness_fails_when_fanout_in_materialised_data(self) -> None:
        fanout = pd.DataFrame({"entity_id": [1, 1, 2], "score": [0.9, 0.7, 0.4]})
        vd = make_variables(variables_df=fanout)
        result = vd.validate_pk_uniqueness()
        assert result.passed is False

    def test_validate_pk_uniqueness_stores_result_on_self(self) -> None:
        vd = make_variables()
        assert vd.pk_validation is None
        result = vd.validate_pk_uniqueness()
        assert vd.pk_validation is result

    def test_validate_join_integrity_passes_when_no_fanout(self) -> None:
        vd = make_variables()
        result = vd.validate_join_integrity()
        assert result.passed is True

    def test_validate_join_integrity_fails_when_fanout(self) -> None:
        fanout = pd.DataFrame({"entity_id": [1, 1, 2], "score": [0.9, 0.7, 0.4]})
        vd = make_variables(variables_df=fanout)
        result = vd.validate_join_integrity()
        assert result.passed is False

    def test_validate_join_integrity_check_name(self) -> None:
        vd = make_variables()
        result = vd.validate_join_integrity()
        assert result.check_name == "join_integrity"

    def test_validate_join_integrity_stores_result_on_self(self) -> None:
        vd = make_variables()
        assert vd.join_validation is None
        result = vd.validate_join_integrity()
        assert vd.join_validation is result


# ---------------------------------------------------------------------------
# TestVariablesDatasetResolveVariables
# ---------------------------------------------------------------------------


class TestVariablesDatasetResolveVariables:
    def setup_method(self) -> None:
        self.vd = make_variables()
        self.pipeline = make_pipeline()

    def test_resolve_variables_returns_list_of_variables(self) -> None:
        result = self.vd.resolve_variables(self.pipeline)
        assert isinstance(result, list)
        assert all(isinstance(v, Variable) for v in result)

    def test_resolve_variables_excludes_vd_matched_column(self) -> None:
        result = self.vd.resolve_variables(self.pipeline)
        names = [v.name for v in result]
        assert "__vd_matched__" not in names

    def test_resolve_variables_one_variable_per_column(self) -> None:
        three_col_df = pd.DataFrame({"entity_id": [1], "a": [1], "b": [2]})
        vd = make_variables(variables_df=three_col_df)
        result = vd.resolve_variables(self.pipeline)
        assert len(result) == 3  # entity_id, a, b (plus __vd_matched__ excluded)

    def test_resolve_variables_metadata_populated_by_pipeline(self) -> None:
        result = self.vd.resolve_variables(self.pipeline)
        for v in result:
            assert "storage_dtype" in v.metadata

    def test_resolve_variables_stores_result_on_self(self) -> None:
        result = self.vd.resolve_variables(self.pipeline)
        assert self.vd.variables is result


# ---------------------------------------------------------------------------
# TestCaching
# ---------------------------------------------------------------------------


class TestCaching:
    def test_universe_materialise_cached_on_second_call(self) -> None:
        u = make_universe()
        first = u.materialise()
        second = u.materialise()
        assert first is second

    def test_universe_materialise_force_calls_adapter_again(self) -> None:
        adapter = MockAdapter({_UNIVERSE_SQL: _UNIVERSE_DF})
        u = UniverseDataset(sql=_UNIVERSE_SQL, primary_key=["entity_id"], adapter=adapter)
        u.materialise()
        call_count_before = adapter.call_count(_UNIVERSE_SQL)
        u.materialise(force=True)
        assert adapter.call_count(_UNIVERSE_SQL) == call_count_before + 1

    def test_variables_materialise_cached_on_second_call(self) -> None:
        vd = make_variables()
        first = vd.materialise()
        second = vd.materialise()
        assert first is second

    def test_variables_materialise_force_calls_adapter_again(self) -> None:
        u = make_universe()
        adapter = MockAdapter({_VARIABLES_SQL: _VARIABLES_DF})
        vd = VariablesDataset(
            sql=_VARIABLES_SQL,
            primary_key=["entity_id"],
            universe=u,
            join_keys={"entity_id": "entity_id"},
            adapter=adapter,
        )
        vd.materialise()
        call_count_before = adapter.call_count(_VARIABLES_SQL)
        vd.materialise(force=True)
        assert adapter.call_count(_VARIABLES_SQL) == call_count_before + 1

    def test_variables_data_attribute_populated_after_materialise(self) -> None:
        vd = make_variables()
        assert vd._data is None
        vd.materialise()
        assert vd._data is not None


# ---------------------------------------------------------------------------
# TestVariablesDatasetEdgeCases
# ---------------------------------------------------------------------------


class TestVariablesDatasetEdgeCases:
    def test_validate_join_integrity_fails_for_missing_join_keys(self) -> None:
        no_key_df = pd.DataFrame({"score": [0.9, 0.4]})
        vd = make_variables(variables_df=no_key_df)
        result = vd.validate_join_integrity()
        assert result.passed is False
        assert "missing_join_keys" in result.details

    def test_validate_join_integrity_null_join_keys_not_flagged_as_fanout(self) -> None:
        # Multiple null join keys should NOT count as fan-out (SQL NULL semantics)
        null_keys = pd.DataFrame({"entity_id": [None, None, 1], "score": [0.1, 0.2, 0.9]})
        vd = make_variables(variables_df=null_keys)
        result = vd.validate_join_integrity()
        assert result.passed is True

    def test_materialise_raises_if_vd_matched_in_universe(self) -> None:
        import pytest

        clash_df = pd.DataFrame({"entity_id": [1, 2, 3], "__vd_matched__": [True, True, False]})
        adapter = MockAdapter({_UNIVERSE_SQL: clash_df})
        u = UniverseDataset(sql=_UNIVERSE_SQL, primary_key=["entity_id"], adapter=adapter)
        vd = make_variables(universe=u)
        with pytest.raises(ValueError, match="__vd_matched__"):
            vd.materialise()

    def test_raw_variables_data_populated_after_materialise(self) -> None:
        vd = make_variables()
        assert vd._raw_variables_data is None
        vd.materialise()
        assert vd._raw_variables_data is not None
