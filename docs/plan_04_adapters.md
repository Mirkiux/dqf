# Plan 4 — DataSourceAdapter Abstraction and Implementations

## Goal

Implement the `DataSourceAdapter` abstraction and all concrete adapter implementations. Each dataset instance carries its own adapter, enabling multi-engine setups (e.g. UniverseDataset on Databricks, VariablesDataset on Oracle).

## Scope

| Item | File |
|---|---|
| `DataSourceAdapter` abstract base | `src/dqf/adapters/base.py` |
| `SQLAlchemyAdapter` | `src/dqf/adapters/sqlalchemy_adapter.py` |
| `DatabricksAdapter` | `src/dqf/adapters/databricks_adapter.py` |
| `SparkAdapter` | `src/dqf/adapters/spark_adapter.py` |
| `MockAdapter` | `src/dqf/adapters/mock_adapter.py` |
| Public exports | `src/dqf/adapters/__init__.py` |
| Unit tests | `tests/test_adapters.py` |

## Detailed Specification

### `DataSourceAdapter` (Abstract)

- `execute(sql: str) -> pd.DataFrame` — executes SQL, always returns a pandas DataFrame
- `engine_type() -> EngineType` — identifies the engine

### `SQLAlchemyAdapter`
- Constructor: `connection_string: str` (standard SQLAlchemy DSN)
- Covers Oracle, PostgreSQL, Snowflake, MySQL, SQLite, and any JDBC-compatible engine
- Creates engine lazily on first `execute()` call

### `DatabricksAdapter`
- Constructor: `host: str`, `token: str`, `http_path: str`, `catalog: str`, `schema: str`
- Uses `databricks-sql-connector` (optional dependency)

### `SparkAdapter`
- Constructor: `spark_session` (accepts a live `SparkSession`)
- `execute()` runs `spark.sql(sql).toPandas()`

### `MockAdapter`
- Constructor: `results: Dict[str, pd.DataFrame]` — maps SQL strings to DataFrames
- `execute(sql)` looks up the SQL string in the dict; raises `KeyError` if not found
- Used exclusively in tests and examples — zero external dependencies

## Key Design Contract

Adapters **always return pandas DataFrames**. Engine-native execution is pushed via SQL strings composed by the framework; only the result set crosses the boundary into pandas. This keeps all downstream framework code engine-agnostic.

## Definition of Done

- [ ] All four concrete adapters implemented
- [ ] `MockAdapter` fully functional with no optional dependencies
- [ ] `dqf/adapters/__init__.py` exports all adapters
- [ ] `dqf/__init__.py` updated to export `DataSourceAdapter` and concrete adapters
- [ ] `tests/test_adapters.py` covers `MockAdapter` fully; `SQLAlchemyAdapter` tested with SQLite (no external DB required)
- [ ] `DatabricksAdapter` and `SparkAdapter` guarded with `pytest.importorskip` in tests
- [ ] All tests pass
- [ ] Committed and pushed to `main`
