# Plan 8 — Dataset Classes and Materialization

## Goal

Implement `UniverseDataset` and `VariablesDataset` with full materialization logic (universe-anchored left join), dataset-level invariant checks, and `resolve_variables()`. The `run_validation()` orchestration method is deferred to Plan 9.

## Scope

| Item | File |
|---|---|
| `UniverseDataset` | `src/dqf/datasets/universe.py` |
| `VariablesDataset` (without `run_validation`) | `src/dqf/datasets/variables.py` |
| Public exports | `src/dqf/datasets/__init__.py` |
| Update top-level exports | `src/dqf/__init__.py` |
| Unit tests | `tests/test_datasets.py` |

## Detailed Specification

### `UniverseDataset`

**Constructor**: `sql: str`, `primary_key: list[str]`, `adapter: DataSourceAdapter`, `time_field: str | None = None`

**Methods**:
- `to_pandas() -> pd.DataFrame` — executes `self.sql` via adapter
- `validate_pk_uniqueness(data: pd.DataFrame) -> ValidationResult` — checks for duplicate rows on `primary_key` columns; passed if no duplicates, failed otherwise

### `VariablesDataset`

**Constructor**: `sql: str`, `primary_key: list[str]`, `universe: UniverseDataset`, `join_keys: dict[str, str]`, `adapter: DataSourceAdapter`, `variables: list[Variable] | None = None`

**Methods**:
- `to_pandas() -> pd.DataFrame` — materializes both datasets via their respective adapters, LEFT JOINs from universe to variables on `join_keys`, appends `__vd_matched__` boolean column
- `validate_pk_uniqueness(data: pd.DataFrame) -> ValidationResult`
- `validate_join_integrity(variables_data: pd.DataFrame, universe_data: pd.DataFrame) -> ValidationResult` — checks that join keys in `variables_data` are unique (no fan-out of universe rows); passed if unique, failed otherwise
- `resolve_variables(data: pd.DataFrame, builder_pipeline: MetadataBuilderPipeline) -> list[Variable]` — iterates non-framework columns (excludes `__vd_matched__`), creates a `Variable` per column using `DataType.TEXT` as default dtype, runs each column's series through `builder_pipeline.profile()`, returns the populated list

### The `__vd_matched__` Contract
Always present in `to_pandas()` output. `True` = entity found in variables dataset; `False` = structural absence.

## Explicit Test Case Specifications

All tests use `MockAdapter`. Universe SQL maps to a 3-row DataFrame; variables SQL maps to a 2-row DataFrame (one universe entity unmatched).

### Class `TestUniverseDataset`
1. `test_to_pandas_delegates_to_adapter` — `to_pandas()` returns the DataFrame registered in MockAdapter
2. `test_to_pandas_returns_correct_shape` — returned DataFrame has expected rows/columns
3. `test_validate_pk_uniqueness_passes_for_unique_keys` — DataFrame with unique PK → `ValidationResult.passed == True`
4. `test_validate_pk_uniqueness_fails_for_duplicate_keys` — DataFrame with duplicate PK row → `ValidationResult.passed == False`
5. `test_validate_pk_uniqueness_check_name` — result `check_name == "pk_uniqueness"`
6. `test_time_field_default_is_none` — `UniverseDataset(...).time_field is None`
7. `test_time_field_stored` — `UniverseDataset(..., time_field="date").time_field == "date"`

### Class `TestVariablesDatasetToPandas`
8. `test_to_pandas_contains_vd_matched_column` — result always has `__vd_matched__` column
9. `test_to_pandas_universe_rows_all_present` — left join preserves all universe rows
10. `test_to_pandas_matched_flag_true_for_joined_rows` — rows that matched variables have `__vd_matched__ == True`
11. `test_to_pandas_matched_flag_false_for_unmatched_rows` — universe rows with no variable match have `__vd_matched__ == False`
12. `test_to_pandas_structural_nulls_in_unmatched_rows` — variable columns are NaN for unmatched rows
13. `test_to_pandas_value_columns_correct_for_matched_rows` — values from variables dataset appear correctly in matched rows

### Class `TestVariablesDatasetValidation`
14. `test_validate_pk_uniqueness_passes` — unique keys in variables data → passed
15. `test_validate_pk_uniqueness_fails` — duplicate keys in variables data → failed
16. `test_validate_join_integrity_passes_when_no_fanout` — join keys unique in variables → passed
17. `test_validate_join_integrity_fails_when_fanout` — duplicate join keys in variables → failed
18. `test_validate_join_integrity_check_name` — result `check_name == "join_integrity"`

### Class `TestVariablesDatasetResolveVariables`
19. `test_resolve_variables_returns_list_of_variables` — result is `list[Variable]`
20. `test_resolve_variables_excludes_vd_matched_column` — `__vd_matched__` not in returned variable names
21. `test_resolve_variables_one_variable_per_column` — DataFrame with 3 data columns → 3 variables
22. `test_resolve_variables_metadata_populated_by_pipeline` — after resolve, each variable's metadata has keys from the builder pipeline
23. `test_resolve_variables_stores_result_on_self` — after call, `dataset.variables` equals the returned list

## Definition of Done

- [ ] `UniverseDataset` and `VariablesDataset` implemented per spec
- [ ] `to_pandas()` produces correct left join with `__vd_matched__` column
- [ ] `validate_pk_uniqueness()` and `validate_join_integrity()` return correct `ValidationResult`
- [ ] `resolve_variables()` correctly creates and profiles `Variable` instances
- [ ] `src/dqf/__init__.py` exports `UniverseDataset`, `VariablesDataset`
- [ ] `tests/test_datasets.py` contains all 23 test cases above
- [ ] All CI jobs pass (lint, typecheck, test 3.10–3.14)
