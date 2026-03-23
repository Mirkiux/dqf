# API Reference

Complete reference for all public classes and functions in `dqf`.

All symbols listed below are importable directly from the top-level `dqf` package:

```python
import dqf

dqf.Variable(...)
dqf.build_default_resolver(...)
dqf.MockAdapter(...)
# etc.
```

## Sections

| Section | Contents |
|---|---|
| [Enumerations](enums.md) | `DataType`, `ValidationStatus`, `Severity`, `VariableRole`, `EngineType` |
| [Variable](variable.md) | `Variable` — column descriptor |
| [Results](results.md) | `CheckResult`, `ValidationResult` — immutable value objects |
| [Adapters](adapters.md) | `DataSourceAdapter`, `MockAdapter`, `SQLAlchemyAdapter`, `DatabricksAdapter`, `SparkAdapter` |
| [Checks — Pipeline](checks/pipeline.md) | `CheckPipeline` |
| [Checks — Cross-sectional](checks/cross_sectional.md) | `NullRateCheck`, `NotNullCheck`, `CardinalityCheck`, … |
| [Checks — Longitudinal](checks/longitudinal.md) | `TrendCheck`, `StructuralBreakCheck`, `ProportionDriftCheck`, … |
| [Resolver](resolver.md) | `CheckSuiteResolver` |
| [Datasets](datasets.md) | `UniverseDataset`, `VariablesDataset` |
| [Default Resolver](defaults.md) | `build_default_resolver` and pipeline factories |
| [Report](report.md) | `ValidationReport` |
