# Plan 8 — Dataset Classes and Materialization

## Goal

Implement `UniverseDataset` and `VariablesDataset` with full materialization logic (universe-anchored left join), dataset-level invariant checks, and `resolve_variables()`. The `run_validation()` orchestration method is deferred to Plan 9.

## Scope

| Item | File |
|---|---|
| `UniverseDataset` | `src/dqf/datasets/universe.py` |
| `VariablesDataset` (without `run_validation`) | `src/dqf/datasets/variables.py` |
| Public exports | `src/dqf/datasets/__init__.py` |
| Unit and integration tests | `tests/test_datasets.py` |

## Detailed Specification

### `UniverseDataset`

**Attributes**:
- `sql: str`
- `primary_key: List[str]`
- `time_field: Optional[str]` — required for longitudinal checks
- `adapter: DataSourceAdapter`

**Methods**:
- `to_pandas() -> pd.DataFrame` — executes `self.sql` via adapter
- `validate_pk_uniqueness(data: pd.DataFrame) -> ValidationResult` — checks for duplicate rows on `primary_key` columns

### `VariablesDataset`

**Attributes**:
- `sql: str`
- `primary_key: List[str]`
- `universe: UniverseDataset`
- `join_keys: Dict[str, str]` — `{"variables_col": "universe_col"}`
- `variables: List[Variable]` — populated by `resolve_variables()` or set manually
- `adapter: DataSourceAdapter` — may differ from `universe.adapter`

**Methods**:
- `to_pandas() -> pd.DataFrame` — materializes both datasets via their respective adapters, then performs a LEFT JOIN from universe to variables in pandas. Appends a boolean column `__vd_matched__` (True where the universe row had a match in the variables dataset).
- `validate_pk_uniqueness(data: pd.DataFrame) -> ValidationResult`
- `validate_join_integrity(variables_data: pd.DataFrame, universe_data: pd.DataFrame) -> ValidationResult` — verifies the join does not fan out universe rows (i.e. the join keys form at least a unique key in the variables dataset)
- `resolve_variables(data: pd.DataFrame, builder_pipeline: MetadataBuilderPipeline) -> List[Variable]` — iterates over non-framework columns (excludes `__vd_matched__`), creates a `Variable` per column, runs each series through `builder_pipeline`, returns the list

### The `__vd_matched__` Contract
This framework-managed column is always present in the result of `to_pandas()`. It allows downstream checks to distinguish:
- Structural nulls: `__vd_matched__ == False` (entity absent from variables dataset)
- Value nulls: `__vd_matched__ == True` and variable column is null (entity present but value missing)

## Definition of Done

- [ ] `UniverseDataset` and `VariablesDataset` implemented per spec
- [ ] `to_pandas()` produces correct left join with `__vd_matched__` column
- [ ] `validate_pk_uniqueness()` and `validate_join_integrity()` return correct `ValidationResult`
- [ ] `resolve_variables()` correctly creates and profiles `Variable` instances
- [ ] Tests use `MockAdapter` — no external DB required
- [ ] Tests cover: left join correctness, `__vd_matched__` flag, PK duplicate detection, join fan-out detection
- [ ] All tests pass
- [ ] Committed and pushed to `main`
