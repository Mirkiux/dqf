"""Tests for the metadata builder subsystem (plan 5)."""

from __future__ import annotations

import pandas as pd
import pytest

from dqf.adapters.mock_adapter import MockAdapter
from dqf.datasets.universe import UniverseDataset
from dqf.datasets.variables import VariablesDataset
from dqf.enums import DataType
from dqf.enums import DataType as DT
from dqf.metadata.base import BaseMetadataBuilder, MetadataBuilderPipeline
from dqf.metadata.builders import (
    CardinalityBuilder,
    DistributionShapeBuilder,
    NullabilityProfileBuilder,
    SemanticTypeInferenceBuilder,
    StorageDtypeBuilder,
)
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UNIVERSE_SQL = "SELECT * FROM universe"
_VARIABLES_SQL = "SELECT * FROM variables"


def make_variable(**kwargs) -> Variable:  # type: ignore[no-untyped-def]
    defaults = {"name": "x", "dtype": DataType.NUMERIC_CONTINUOUS}
    defaults.update(kwargs)
    return Variable(**defaults)  # type: ignore[arg-type]


def make_dataset_for_column(col_name: str, series: pd.Series) -> VariablesDataset:
    n = len(series)
    universe_df = pd.DataFrame({"_uid": range(n)})
    variables_df = pd.DataFrame({"_uid": range(n), col_name: series})
    universe = UniverseDataset(
        sql=_UNIVERSE_SQL,
        primary_key=["_uid"],
        adapter=MockAdapter({_UNIVERSE_SQL: universe_df}),
    )
    return VariablesDataset(
        sql=_VARIABLES_SQL,
        primary_key=["_uid"],
        universe=universe,
        join_keys={"_uid": "_uid"},
        adapter=MockAdapter({_VARIABLES_SQL: variables_df}),
    )


# ---------------------------------------------------------------------------
# TestBaseMetadataBuilderAbstract
# ---------------------------------------------------------------------------


class TestBaseMetadataBuilderAbstract:
    def test_cannot_instantiate_abstract_class(self) -> None:
        with pytest.raises(TypeError):
            BaseMetadataBuilder()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# TestStorageDtypeBuilder
# ---------------------------------------------------------------------------


class TestStorageDtypeBuilder:
    def setup_method(self) -> None:
        self.builder = StorageDtypeBuilder()

    def test_name_is_storage_dtype(self) -> None:
        assert self.builder.name == "storage_dtype"

    def test_int64_series_returns_int64(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3], dtype="int64"))
        result = self.builder.profile(dataset, v)
        assert result == {"storage_dtype": "int64"}

    def test_float_series_returns_float64(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, 2.0]))
        result = self.builder.profile(dataset, v)
        assert result == {"storage_dtype": "float64"}

    def test_object_series_returns_object(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(["a", "b"], dtype=object))
        result = self.builder.profile(dataset, v)
        assert result == {"storage_dtype": "object"}

    def test_updates_variable_metadata(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3], dtype="int64"))
        self.builder.profile(dataset, v)
        assert "storage_dtype" in v.metadata


# ---------------------------------------------------------------------------
# TestNullabilityProfileBuilder
# ---------------------------------------------------------------------------


class TestNullabilityProfileBuilder:
    def setup_method(self) -> None:
        self.builder = NullabilityProfileBuilder()

    def test_name_is_nullability(self) -> None:
        assert self.builder.name == "nullability"

    def test_no_nulls(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        result = self.builder.profile(dataset, v)
        assert result["null_count"] == 0
        assert result["empirical_null_rate"] == 0.0
        assert result["is_nullable"] is False

    def test_with_nulls(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, None, 3.0]))
        result = self.builder.profile(dataset, v)
        assert result["null_count"] == 1
        assert result["empirical_null_rate"] == pytest.approx(1 / 3)
        assert result["is_nullable"] is True

    def test_all_nulls(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([None, None], dtype=object))
        result = self.builder.profile(dataset, v)
        assert result["null_count"] == 2
        assert result["empirical_null_rate"] == 1.0
        assert result["is_nullable"] is True

    def test_updates_variable_metadata(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        self.builder.profile(dataset, v)
        assert "null_count" in v.metadata
        assert "empirical_null_rate" in v.metadata
        assert "is_nullable" in v.metadata


# ---------------------------------------------------------------------------
# TestCardinalityBuilder
# ---------------------------------------------------------------------------


class TestCardinalityBuilder:
    def setup_method(self) -> None:
        self.builder = CardinalityBuilder()

    def test_name_is_cardinality(self) -> None:
        assert self.builder.name == "cardinality"

    def test_cardinality_counts_unique_nonnull(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 2, None]))
        result = self.builder.profile(dataset, v)
        assert result["cardinality"] == 2

    def test_is_high_cardinality_false(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        result = self.builder.profile(dataset, v)
        assert result["is_high_cardinality"] is False

    def test_is_high_cardinality_true(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(range(100)))
        result = self.builder.profile(dataset, v)
        assert result["is_high_cardinality"] is True

    def test_custom_threshold(self) -> None:
        builder = CardinalityBuilder(high_cardinality_threshold=5)
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(range(6)))
        result = builder.profile(dataset, v)
        assert result["is_high_cardinality"] is True

    def test_updates_variable_metadata(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        self.builder.profile(dataset, v)
        assert "cardinality" in v.metadata
        assert "is_high_cardinality" in v.metadata


# ---------------------------------------------------------------------------
# TestDistributionShapeBuilder
# ---------------------------------------------------------------------------


class TestDistributionShapeBuilder:
    def setup_method(self) -> None:
        self.builder = DistributionShapeBuilder()

    def test_name_is_distribution(self) -> None:
        assert self.builder.name == "distribution"

    def test_numeric_series_returns_all_keys(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, 2.0, 3.0, 4.0, 5.0]))
        result = self.builder.profile(dataset, v)
        for key in ("mean", "std", "min", "max", "skewness", "kurtosis"):
            assert key in result

    def test_mean_value_correct(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, 2.0, 3.0]))
        result = self.builder.profile(dataset, v)
        assert result["mean"] == pytest.approx(2.0)

    def test_non_numeric_returns_empty_dict(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(["a", "b", "c"]))
        result = self.builder.profile(dataset, v)
        assert result == {}

    def test_non_numeric_does_not_update_variable_metadata(self) -> None:
        v = make_variable()
        before = dict(v.metadata)
        dataset = make_dataset_for_column("x", pd.Series(["a", "b", "c"]))
        self.builder.profile(dataset, v)
        assert v.metadata == before

    def test_updates_variable_metadata_for_numeric(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, 2.0, 3.0]))
        self.builder.profile(dataset, v)
        assert "mean" in v.metadata
        assert "std" in v.metadata


# ---------------------------------------------------------------------------
# TestSemanticTypeInferenceBuilder
# ---------------------------------------------------------------------------


class TestSemanticTypeInferenceBuilder:
    def setup_method(self) -> None:
        self.builder = SemanticTypeInferenceBuilder()

    def test_name_is_semantic_type(self) -> None:
        assert self.builder.name == "semantic_type"

    def test_bool_series_infers_boolean(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([True, False, True]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.BOOLEAN

    def test_low_cardinality_int_series_infers_numeric_discrete(self) -> None:
        # Few distinct integers → NUMERIC_DISCRETE (low cardinality + numeric storage)
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.NUMERIC_DISCRETE

    def test_high_cardinality_int_series_infers_numeric_continuous(self) -> None:
        # Many distinct integers → NUMERIC_CONTINUOUS (passes low-cardinality threshold)
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(range(30)))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.NUMERIC_CONTINUOUS

    def test_low_cardinality_float_series_infers_numeric_discrete(self) -> None:
        # Few distinct floats → NUMERIC_DISCRETE
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, 2.5, 3.7]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.NUMERIC_DISCRETE

    def test_high_cardinality_float_series_infers_numeric_continuous(self) -> None:
        # Many distinct floats → NUMERIC_CONTINUOUS
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([i * 0.1 for i in range(30)]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.NUMERIC_CONTINUOUS

    def test_datetime_series_infers_datetime(self) -> None:
        # Many distinct datetimes → DATETIME
        v = make_variable()
        series = pd.Series(pd.date_range("2024-01-01", periods=30, freq="D"))
        dataset = make_dataset_for_column("x", series)
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.DATETIME

    def test_low_cardinality_datetime_series_infers_datetime(self) -> None:
        # Low-cardinality datetime64 storage → DATETIME (not CATEGORICAL)
        v = make_variable()
        series = pd.Series(pd.date_range("2024-01-01", periods=2, freq="D"))
        dataset = make_dataset_for_column("x", series)
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.DATETIME

    def test_string_numeric_infers_numeric(self) -> None:
        # Many distinct string-encoded numbers → NUMERIC_CONTINUOUS via coercion
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([str(i) for i in range(30)]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.NUMERIC_CONTINUOUS

    def test_low_cardinality_string_numeric_infers_numeric(self) -> None:
        # Low-cardinality string-encoded numbers → NUMERIC_CONTINUOUS via coercion
        # (coerce check runs before cardinality fallback for object/string)
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(["1", "2", "3"]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.NUMERIC_CONTINUOUS

    def test_string_datetime_infers_datetime(self) -> None:
        # Many distinct string-encoded dates → DATETIME via coercion
        v = make_variable()
        dates = pd.date_range("2024-01-01", periods=30, freq="D").strftime("%Y-%m-%d").tolist()
        dataset = make_dataset_for_column("x", pd.Series(dates))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.DATETIME

    def test_low_cardinality_string_datetime_infers_datetime(self) -> None:
        # Low-cardinality string-encoded dates → DATETIME via coercion
        # (coerce check runs before cardinality fallback for object/string)
        v = make_variable()
        dates = pd.date_range("2024-01-01", periods=2, freq="D").strftime("%Y-%m-%d").tolist()
        dataset = make_dataset_for_column("x", pd.Series(dates))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.DATETIME

    def test_low_cardinality_string_infers_categorical(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series(["cat", "dog", "cat", "bird"]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.CATEGORICAL

    def test_high_cardinality_string_infers_text(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([f"token_{i}" for i in range(30)]))
        result = self.builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.TEXT

    def test_custom_low_cardinality_threshold(self) -> None:
        builder = SemanticTypeInferenceBuilder(low_cardinality_threshold=2)
        v = make_variable()
        # 3 unique values exceeds threshold of 2 → TEXT
        dataset = make_dataset_for_column("x", pd.Series(["cat", "dog", "bird"]))
        result = builder.profile(dataset, v)
        assert result["semantic_dtype"] == DT.TEXT

    def test_updates_variable_metadata(self) -> None:
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        self.builder.profile(dataset, v)
        assert "semantic_dtype" in v.metadata


# ---------------------------------------------------------------------------
# TestMetadataBuilderPipeline
# ---------------------------------------------------------------------------


class TestMetadataBuilderPipeline:
    def test_name_is_pipeline(self) -> None:
        pipeline = MetadataBuilderPipeline([])
        assert pipeline.name == "pipeline"

    def test_empty_pipeline_returns_empty_dict(self) -> None:
        pipeline = MetadataBuilderPipeline([])
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3]))
        result = pipeline.profile(dataset, v)
        assert result == {}

    def test_single_builder_result_merged(self) -> None:
        pipeline = MetadataBuilderPipeline([("dtype", StorageDtypeBuilder())])
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1, 2, 3], dtype="int64"))
        result = pipeline.profile(dataset, v)
        assert "storage_dtype" in result

    def test_two_builders_merged(self) -> None:
        pipeline = MetadataBuilderPipeline(
            [
                ("dtype", StorageDtypeBuilder()),
                ("nullability", NullabilityProfileBuilder()),
            ]
        )
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, None, 3.0]))
        result = pipeline.profile(dataset, v)
        assert "storage_dtype" in result
        assert "null_count" in result

    def test_variable_metadata_updated_in_place(self) -> None:
        pipeline = MetadataBuilderPipeline(
            [
                ("dtype", StorageDtypeBuilder()),
                ("nullability", NullabilityProfileBuilder()),
            ]
        )
        v = make_variable()
        dataset = make_dataset_for_column("x", pd.Series([1.0, 2.0, 3.0]))
        pipeline.profile(dataset, v)
        assert "storage_dtype" in v.metadata
        assert "null_count" in v.metadata

    def test_pipeline_is_base_metadata_builder(self) -> None:
        pipeline = MetadataBuilderPipeline([])
        assert isinstance(pipeline, BaseMetadataBuilder)
