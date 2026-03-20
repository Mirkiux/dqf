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
| Update top-level exports | `src/dqf/__init__.py` |
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
- Import happens inside `execute()` — raises `ImportError` with install hint if package absent

### `SparkAdapter`
- Constructor: `spark_session: Any` (accepts a live `SparkSession`)
- `execute()` runs `spark_session.sql(sql).toPandas()`
- Import of pyspark is NOT required at class definition time — accepts `Any`

### `MockAdapter`
- Constructor: `results: dict[str, pd.DataFrame]` — maps SQL strings to DataFrames
- `execute(sql)` looks up the SQL string in the dict; raises `KeyError` if not found
- Used exclusively in tests and examples — zero external dependencies

## Key Design Contract

Adapters **always return pandas DataFrames**. Engine-native execution is pushed via SQL strings composed by the framework; only the result set crosses the boundary into pandas. This keeps all downstream framework code engine-agnostic.

## Explicit Test Case Specifications

### Class `TestDataSourceAdapterAbstract`
1. `test_cannot_instantiate_abstract_class` — `DataSourceAdapter()` raises `TypeError`

### Class `TestMockAdapter`
2. `test_engine_type_is_mock` — `MockAdapter({}).engine_type() == EngineType.MOCK`
3. `test_execute_returns_registered_dataframe` — execute known key returns the exact registered DataFrame object
4. `test_execute_raises_key_error_for_unknown_sql` — execute unregistered SQL string raises `KeyError`
5. `test_multiple_queries_routed_correctly` — adapter with two registered queries returns correct df per key
6. `test_execute_returns_same_object_not_copy` — `result is df` (identity check, not just equality)

### Class `TestSQLAlchemyAdapter`
7. `test_engine_type_is_sqlalchemy` — `adapter.engine_type() == EngineType.SQLALCHEMY`
8. `test_engine_is_none_before_execute` — `adapter._engine is None` before any call
9. `test_execute_with_sqlite_returns_dataframe` — create in-memory SQLite with a table, execute SELECT, result is a non-empty `pd.DataFrame`
10. `test_engine_created_after_first_execute` — `adapter._engine is not None` after `execute()`
11. `test_execute_returns_correct_values` — values in returned DataFrame match inserted SQLite data

### Class `TestDatabricksAdapter`
12. `test_engine_type_is_databricks` — `adapter.engine_type() == EngineType.DATABRICKS`
13. `test_execute_raises_import_error_when_not_installed` — patch `sys.modules["databricks.sql"] = None`; `execute()` raises `ImportError`

### Class `TestSparkAdapter`
14. `test_engine_type_is_spark` — `adapter.engine_type() == EngineType.SPARK`
15. `test_execute_calls_sql_and_to_pandas` — pass a fake spark session object with a `sql()` method returning a fake result with `toPandas()`; verify the returned DataFrame matches

## Definition of Done

- [ ] All four concrete adapters implemented
- [ ] `MockAdapter` fully functional with no optional dependencies
- [ ] `dqf/adapters/__init__.py` exports all adapters
- [ ] `dqf/__init__.py` updated to export `DataSourceAdapter` and concrete adapters
- [ ] `tests/test_adapters.py` has all 15 test cases above
- [ ] `DatabricksAdapter` and `SparkAdapter` tested without real external dependencies (patching/fakes)
- [ ] All CI jobs pass (lint, typecheck, test 3.10–3.14)
