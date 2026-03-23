# Contributing to dqf

Thank you for your interest in contributing.

---

## Development setup

```bash
git clone https://github.com/Mirkiux/dqf
cd dqf
pip install -e ".[dev]"
```

This installs the package in editable mode with all development dependencies (`pytest`, `ruff`, `mypy`, `hatch`).

---

## Running the test suite

```bash
pytest
```

Coverage is reported automatically. The target is ≥90% for all modules.

Run a specific test file:

```bash
pytest tests/test_integration.py -v
```

---

## Code style

All code is formatted and linted with [ruff](https://docs.astral.sh/ruff/):

```bash
ruff check src tests      # lint
ruff format src tests     # format
```

Type annotations are checked with mypy:

```bash
mypy src
```

Both must pass cleanly before submitting a pull request.

---

## Project structure

```
src/dqf/
├── __init__.py           # Public API — all exports here
├── enums.py              # DataType, ValidationStatus, Severity, VariableRole, EngineType
├── results.py            # CheckResult, ValidationResult (immutable value objects)
├── variable.py           # Variable descriptor
├── resolver.py           # CheckSuiteResolver
├── report.py             # ValidationReport
├── adapters/             # DataSourceAdapter + concrete adapters
├── checks/
│   ├── base.py           # BaseCheck, BaseCrossSectionalCheck, BaseLongitudinalCheck
│   ├── pipeline.py       # CheckPipeline
│   ├── cross_sectional/  # NullRateCheck, NotNullCheck, …
│   └── longitudinal/     # TrendCheck, StructuralBreakCheck, ProportionDriftCheck, …
├── datasets/             # UniverseDataset, VariablesDataset
├── defaults/             # build_default_resolver + pipeline factories
└── metadata/             # MetadataBuilder implementations
```

---

## Adding a new check

1. Choose the base class:
   - `BaseCrossSectionalCheck` — operates on the materialised dataset snapshot
   - `BaseLongitudinalCheck` — issues an aggregation SQL query and analyses the time series

2. Create a file in `src/dqf/checks/cross_sectional/` or `src/dqf/checks/longitudinal/`.

3. Implement `run(data, variable, adapter) -> CheckResult`.
   - For longitudinal checks, also implement `aggregation_sql(variable_name, time_field, period) -> str`.

4. Export from `src/dqf/checks/__init__.py` and `src/dqf/__init__.py`.

5. Add unit tests in `tests/` — one test file per check class, following the existing pattern.

---

## Adding a new adapter

1. Subclass `DataSourceAdapter` in `src/dqf/adapters/`.
2. Implement `execute(sql: str) -> pd.DataFrame`.
3. Export from `src/dqf/adapters/__init__.py` and `src/dqf/__init__.py`.

---

## Pull requests

- Branch from `main`, named `plan-NN-short-description` for planned work or `fix/short-description` for bug fixes.
- Keep PRs focused — one logical change per PR.
- All CI checks (tests, ruff, mypy) must pass.
- Include a clear description of what changed and why.

---

## License

By contributing you agree that your contributions will be licensed under the Apache License 2.0.
