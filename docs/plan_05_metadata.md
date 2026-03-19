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
- `name: str`
- `profile(series: pd.Series, variable: Variable) -> dict` — inspects series, returns flat dict

### `MetadataBuilderPipeline`
- Chains builders sequentially; merges each output dict into `variable.metadata`
- Same interface as `BaseMetadataBuilder` (Composite pattern)
- `steps: List[Tuple[str, BaseMetadataBuilder]]`

### Concrete Builders

| Builder | Keys added to `variable.metadata` |
|---|---|
| `StorageDtypeBuilder` | `storage_dtype` (pandas dtype string) |
| `NullabilityProfileBuilder` | `empirical_null_rate`, `null_count`, `is_nullable` |
| `CardinalityBuilder` | `cardinality`, `is_high_cardinality` (bool, threshold configurable) |
| `DistributionShapeBuilder` | `mean`, `std`, `min`, `max`, `skewness`, `kurtosis` (numeric only; skipped otherwise) |
| `SemanticTypeInferenceBuilder` | `semantic_dtype` (DataType enum value) — attempts coercion to numeric/datetime for varchar columns |

### Engine Agnosticism
All builders receive a `pd.Series`. The caller (i.e. `resolve_variables()` in Plan 8) is responsible for materializing data via the adapter before passing columns to builders. Builders never import any database or Spark library.

## Definition of Done

- [ ] All five concrete builders implemented
- [ ] `MetadataBuilderPipeline` chains and merges correctly
- [ ] `SemanticTypeInferenceBuilder` correctly infers numeric/datetime from varchar series
- [ ] All builders gracefully skip inapplicable columns (e.g. `DistributionShapeBuilder` on a string column returns `{}`)
- [ ] Unit tests cover each builder individually and the pipeline
- [ ] All tests pass
- [ ] Committed and pushed to `main`
