# Plan 5 — Metadata Subsystem

## Goal

Implement the metadata builder abstraction and all concrete builders. Builders profile a `pd.Series` and return a flat dict to be merged into `Variable.metadata`. Rich metadata is what enables the `CheckSuiteResolver` to make good dispatch decisions.

## Scope

| Item | File |
|---|---|
| `BaseMetadataBuilder`, `MetadataBuilderPipeline` | `src/dqf/metadata/base.py` |
| `StorageDtypeBuilder` | `src/dqf/metadata/builders/dtype_builder.py` |
| `NullabilityProfileBuilder` | `src/dqf/metadata/builders/nullability_builder.py` |
| `CardinalityBuilder` | `src/dqf/metadata/builders/cardinality_builder.py` |
| `DistributionShapeBuilder` | `src/dqf/metadata/builders/distribution_builder.py` |
| `SemanticTypeInferenceBuilder` | `src/dqf/metadata/builders/semantic_builder.py` |
| Public exports | `src/dqf/metadata/__init__.py`, `src/dqf/metadata/builders/__init__.py` |
| Unit tests | `tests/test_metadata.py` |

## Detailed Specification

### `BaseMetadataBuilder` (Abstract)
- `name: str` — abstract property
- `profile(series: pd.Series, variable: Variable) -> dict[str, Any]` — abstract method; inspects the series, returns a flat dict of keys to add to `variable.metadata`

### `MetadataBuilderPipeline`
- `__init__(steps: list[tuple[str, BaseMetadataBuilder]])` — list of (name, builder) pairs
- `name` property returns `"pipeline"`
- `profile(series, variable)` — calls each builder in order, merges all returned dicts together, updates `variable.metadata` in-place, and returns the merged dict
- Composite pattern: `MetadataBuilderPipeline` is itself a `BaseMetadataBuilder`

### Concrete Builders

| Builder | `name` | Keys added to `variable.metadata` |
|---|---|---|
| `StorageDtypeBuilder` | `"storage_dtype"` | `storage_dtype` (pandas dtype as string, e.g. `"int64"`, `"object"`) |
| `NullabilityProfileBuilder` | `"nullability"` | `empirical_null_rate` (float 0–1), `null_count` (int), `is_nullable` (bool: True if null_count > 0) |
| `CardinalityBuilder` | `"cardinality"` | `cardinality` (int: number of unique non-null values), `is_high_cardinality` (bool: cardinality > threshold) |
| `DistributionShapeBuilder` | `"distribution"` | `mean`, `std`, `min`, `max`, `skewness`, `kurtosis` (all floats); returns `{}` if series is not numeric |
| `SemanticTypeInferenceBuilder` | `"semantic_type"` | `semantic_dtype` (a `DataType` enum member) |

### `CardinalityBuilder` configuration
- `__init__(high_cardinality_threshold: int = 50)` — threshold is constructor-injected, not hardcoded

### `SemanticTypeInferenceBuilder` inference rules (in priority order)
1. If pandas dtype is bool → `DataType.BOOLEAN`
2. If pandas dtype is numeric (int or float) → `DataType.NUMERIC_CONTINUOUS`
3. If pandas dtype is datetime → `DataType.DATETIME`
4. If object/string dtype: attempt `pd.to_numeric` on non-null values; if conversion succeeds for ≥ 95% → `DataType.NUMERIC_CONTINUOUS`
5. If object/string dtype: attempt `pd.to_datetime` on non-null values; if conversion succeeds for ≥ 95% → `DataType.DATETIME`
6. If unique non-null count ≤ 20 → `DataType.CATEGORICAL`
7. Default → `DataType.TEXT`

### Engine Agnosticism
All builders receive a `pd.Series`. The caller is responsible for materializing data via the adapter before passing columns to builders. Builders never import any database or Spark library.

## Explicit Test Case Specifications

### Class `TestBaseMetadataBuilderAbstract`
1. `test_cannot_instantiate_abstract_class` — `BaseMetadataBuilder()` raises `TypeError`

### Class `TestStorageDtypeBuilder`
2. `test_name_is_storage_dtype` — `builder.name == "storage_dtype"`
3. `test_int64_series_returns_int64` — `pd.Series([1, 2, 3], dtype="int64")` → `{"storage_dtype": "int64"}`
4. `test_float_series_returns_float64` — `pd.Series([1.0, 2.0])` → `{"storage_dtype": "float64"}`
5. `test_object_series_returns_object` — `pd.Series(["a", "b"])` → `{"storage_dtype": "object"}`
6. `test_updates_variable_metadata` — after `profile()`, `variable.metadata["storage_dtype"]` is set

### Class `TestNullabilityProfileBuilder`
7. `test_name_is_nullability` — `builder.name == "nullability"`
8. `test_no_nulls` — all-non-null series → `null_count=0`, `empirical_null_rate=0.0`, `is_nullable=False`
9. `test_with_nulls` — `pd.Series([1.0, None, 3.0])` → `null_count=1`, `empirical_null_rate=approx(1/3)`, `is_nullable=True`
10. `test_all_nulls` — `pd.Series([None, None])` → `null_count=2`, `empirical_null_rate=1.0`, `is_nullable=True`
11. `test_updates_variable_metadata` — after `profile()`, keys present in `variable.metadata`

### Class `TestCardinalityBuilder`
12. `test_name_is_cardinality` — `builder.name == "cardinality"`
13. `test_cardinality_counts_unique_nonnull` — `pd.Series([1, 2, 2, None])` → `cardinality=2`
14. `test_is_high_cardinality_false` — 3 unique values, threshold=50 → `is_high_cardinality=False`
15. `test_is_high_cardinality_true` — 100 unique values, threshold=50 → `is_high_cardinality=True`
16. `test_custom_threshold` — `CardinalityBuilder(high_cardinality_threshold=5)`, 6 unique values → `is_high_cardinality=True`
17. `test_updates_variable_metadata` — after `profile()`, keys present in `variable.metadata`

### Class `TestDistributionShapeBuilder`
18. `test_name_is_distribution` — `builder.name == "distribution"`
19. `test_numeric_series_returns_all_keys` — `pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])` → result has keys `mean`, `std`, `min`, `max`, `skewness`, `kurtosis`
20. `test_mean_value_correct` — series `[1, 2, 3]` → `mean == pytest.approx(2.0)`
21. `test_non_numeric_returns_empty_dict` — `pd.Series(["a", "b", "c"])` → `{}`
22. `test_non_numeric_does_not_update_variable_metadata` — string series → `variable.metadata` unchanged after `profile()`
23. `test_updates_variable_metadata_for_numeric` — numeric series → keys present in `variable.metadata`

### Class `TestSemanticTypeInferenceBuilder`
24. `test_name_is_semantic_type` — `builder.name == "semantic_type"`
25. `test_bool_series_infers_boolean` — `pd.Series([True, False])` → `semantic_dtype == DataType.BOOLEAN`
26. `test_int_series_infers_numeric_continuous` — `pd.Series([1, 2, 3])` → `DataType.NUMERIC_CONTINUOUS`
27. `test_float_series_infers_numeric_continuous` — `pd.Series([1.0, 2.5])` → `DataType.NUMERIC_CONTINUOUS`
28. `test_datetime_series_infers_datetime` — `pd.Series(pd.to_datetime(["2024-01-01"]))` → `DataType.DATETIME`
29. `test_string_numeric_infers_numeric` — `pd.Series(["1", "2", "3"])` → `DataType.NUMERIC_CONTINUOUS`
30. `test_string_datetime_infers_datetime` — `pd.Series(["2024-01-01", "2024-01-02"])` → `DataType.DATETIME`
31. `test_low_cardinality_string_infers_categorical` — `pd.Series(["cat", "dog", "cat"])` → `DataType.CATEGORICAL`
32. `test_high_cardinality_string_infers_text` — series of 30 distinct strings → `DataType.TEXT`
33. `test_updates_variable_metadata` — after `profile()`, `variable.metadata["semantic_dtype"]` is set

### Class `TestMetadataBuilderPipeline`
34. `test_name_is_pipeline` — `pipeline.name == "pipeline"`
35. `test_empty_pipeline_returns_empty_dict` — zero steps → `profile()` returns `{}`
36. `test_single_builder_result_merged` — pipeline with one `StorageDtypeBuilder` → result contains `storage_dtype` key
37. `test_two_builders_merged` — pipeline with `StorageDtypeBuilder` + `NullabilityProfileBuilder` → result contains keys from both
38. `test_variable_metadata_updated_in_place` — after pipeline `profile()`, `variable.metadata` contains all merged keys
39. `test_pipeline_is_base_metadata_builder` — `isinstance(pipeline, BaseMetadataBuilder)` is `True`

## Definition of Done

- [ ] All five concrete builders implemented
- [ ] `MetadataBuilderPipeline` chains and merges correctly
- [ ] `SemanticTypeInferenceBuilder` correctly infers numeric/datetime from varchar series
- [ ] All builders gracefully skip inapplicable columns (e.g. `DistributionShapeBuilder` on a string column returns `{}`)
- [ ] `tests/test_metadata.py` has all 39 test cases above
- [ ] All CI jobs pass (lint, typecheck, test 3.10–3.14)
