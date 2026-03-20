"""Tests for DataSourceAdapter and all concrete adapter implementations."""

from __future__ import annotations

import sys
from typing import Any
from unittest import mock

import pandas as pd
import pytest
import sqlalchemy

from dqf.adapters import (
    DatabricksAdapter,
    DataSourceAdapter,
    MockAdapter,
    SparkAdapter,
    SQLAlchemyAdapter,
)
from dqf.enums import EngineType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sqlite_adapter() -> SQLAlchemyAdapter:
    return SQLAlchemyAdapter("sqlite:///:memory:")


def _make_sqlite_db_with_table() -> SQLAlchemyAdapter:
    """Return an adapter whose underlying SQLite db has a populated table."""
    adapter = _sqlite_adapter()
    engine = adapter._get_engine()
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE TABLE IF NOT EXISTS sample (id INTEGER, value TEXT)"))
        conn.execute(sqlalchemy.text("INSERT INTO sample VALUES (1, 'alpha'), (2, 'beta')"))
    return adapter


# ---------------------------------------------------------------------------
# TestDataSourceAdapterAbstract
# ---------------------------------------------------------------------------


class TestDataSourceAdapterAbstract:
    def test_cannot_instantiate_abstract_class(self) -> None:
        with pytest.raises(TypeError):
            DataSourceAdapter()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# TestMockAdapter
# ---------------------------------------------------------------------------


class TestMockAdapter:
    def test_engine_type_is_mock(self) -> None:
        adapter = MockAdapter({})
        assert adapter.engine_type() == EngineType.MOCK

    def test_execute_returns_registered_dataframe(self) -> None:
        df = pd.DataFrame({"x": [1, 2, 3]})
        sql = "SELECT x FROM t"
        adapter = MockAdapter({sql: df})
        result = adapter.execute(sql)
        assert result.equals(df)

    def test_execute_raises_key_error_for_unknown_sql(self) -> None:
        adapter = MockAdapter({})
        with pytest.raises(KeyError):
            adapter.execute("SELECT 1")

    def test_multiple_queries_routed_correctly(self) -> None:
        df_a = pd.DataFrame({"a": [1]})
        df_b = pd.DataFrame({"b": [2]})
        sql_a = "SELECT a FROM a_table"
        sql_b = "SELECT b FROM b_table"
        adapter = MockAdapter({sql_a: df_a, sql_b: df_b})
        assert adapter.execute(sql_a).equals(df_a)
        assert adapter.execute(sql_b).equals(df_b)

    def test_execute_returns_same_object_not_copy(self) -> None:
        df = pd.DataFrame({"x": [42]})
        sql = "SELECT x FROM t"
        adapter = MockAdapter({sql: df})
        assert adapter.execute(sql) is df


# ---------------------------------------------------------------------------
# TestSQLAlchemyAdapter
# ---------------------------------------------------------------------------


class TestSQLAlchemyAdapter:
    def test_engine_type_is_sqlalchemy(self) -> None:
        adapter = _sqlite_adapter()
        assert adapter.engine_type() == EngineType.SQLALCHEMY

    def test_engine_is_none_before_execute(self) -> None:
        adapter = _sqlite_adapter()
        assert adapter._engine is None

    def test_execute_with_sqlite_returns_dataframe(self) -> None:
        adapter = _make_sqlite_db_with_table()
        result = adapter.execute("SELECT * FROM sample")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_engine_created_after_first_execute(self) -> None:
        adapter = _make_sqlite_db_with_table()
        adapter.execute("SELECT * FROM sample")
        assert adapter._engine is not None

    def test_execute_returns_correct_values(self) -> None:
        adapter = _make_sqlite_db_with_table()
        result = adapter.execute("SELECT * FROM sample ORDER BY id")
        assert list(result["id"]) == [1, 2]
        assert list(result["value"]) == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# TestDatabricksAdapter
# ---------------------------------------------------------------------------


class TestDatabricksAdapter:
    def _make_adapter(self) -> DatabricksAdapter:
        return DatabricksAdapter(
            host="host.databricks.com",
            token="dapi_token",
            http_path="/sql/1.0/warehouses/abc",
            catalog="main",
            schema="default",
        )

    def test_engine_type_is_databricks(self) -> None:
        adapter = self._make_adapter()
        assert adapter.engine_type() == EngineType.DATABRICKS

    def test_execute_raises_import_error_when_not_installed(self) -> None:
        adapter = self._make_adapter()
        patched: dict[str, Any] = {"databricks": None, "databricks.sql": None}
        with (
            mock.patch.dict(sys.modules, patched),
            pytest.raises(ImportError, match="databricks-sql-connector"),
        ):
            adapter.execute("SELECT 1")


# ---------------------------------------------------------------------------
# TestSparkAdapter
# ---------------------------------------------------------------------------


class TestSparkAdapter:
    def test_engine_type_is_spark(self) -> None:
        adapter = SparkAdapter(object())
        assert adapter.engine_type() == EngineType.SPARK

    def test_execute_calls_sql_and_to_pandas(self) -> None:
        expected = pd.DataFrame({"col": [10, 20]})

        class _FakeResult:
            def toPandas(self) -> pd.DataFrame:
                return expected

        class _FakeSpark:
            def sql(self, query: str) -> _FakeResult:
                return _FakeResult()

        adapter = SparkAdapter(_FakeSpark())
        result = adapter.execute("SELECT col FROM t")
        assert result.equals(expected)
