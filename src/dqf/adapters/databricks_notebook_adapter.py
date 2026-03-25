"""Adapter for Databricks notebook environments.

In Databricks notebooks the Spark session is pre-instantiated and injected
into the notebook's global namespace as ``spark``.  This adapter discovers
that session automatically — no constructor arguments required.

Usage inside a Databricks notebook::

    import dqf

    adapter = dqf.DatabricksNotebookAdapter()

    universe = dqf.UniverseDataset(
        sql="SELECT customer_id FROM customers",
        primary_key=["customer_id"],
        adapter=adapter,
    )
"""

from __future__ import annotations

import sys
from typing import Any

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.enums import EngineType


def _resolve_spark() -> Any:
    """Locate the ``spark`` session injected by the Databricks notebook runtime.

    Databricks injects ``spark`` into the ``__main__`` module namespace before
    the first notebook cell executes.  This function retrieves it without
    requiring the caller to hold a reference or import PySpark explicitly.

    Raises
    ------
    RuntimeError
        When no ``spark`` object can be found.  This most commonly means the
        adapter is being used outside a Databricks notebook environment.
    """
    spark = sys.modules.get("__main__", None)
    if spark is not None:
        spark = getattr(spark, "spark", None)

    if spark is None:
        raise RuntimeError(
            "Could not locate 'spark' in the notebook runtime namespace. "
            "DatabricksNotebookAdapter is intended for use inside Databricks "
            "notebooks where 'spark' is pre-injected by the runtime. "
            "Outside that environment, use SparkAdapter(spark_session) instead."
        )
    return spark


class DatabricksNotebookAdapter(DataSourceAdapter):
    """Adapter for Databricks notebook environments.

    Wraps the ``spark`` session that the Databricks runtime pre-injects into
    every notebook's global namespace.  No constructor arguments are needed —
    the session is resolved lazily on the first :meth:`execute` call.

    This adapter is a thin convenience wrapper around :class:`SparkAdapter`.
    The only difference is that the caller does not need to hold or pass a
    ``SparkSession`` reference — useful when the notebook is the entry point
    and ``spark`` is simply *there*.

    Examples
    --------
    Inside a Databricks notebook::

        adapter = dqf.DatabricksNotebookAdapter()

        universe = dqf.UniverseDataset(
            sql="SELECT id FROM entities",
            primary_key=["id"],
            adapter=adapter,
        )

    Outside a notebook (e.g. in a standalone script or test), use
    :class:`SparkAdapter` instead and pass the session explicitly::

        adapter = dqf.SparkAdapter(spark_session)
    """

    def __init__(self) -> None:
        self._spark: Any = None

    def engine_execute(self, sql: str) -> pd.DataFrame:
        """Execute *sql* via the notebook's pre-injected ``spark`` session.

        The session is resolved once and cached for the lifetime of this
        adapter instance.
        """
        if self._spark is None:
            self._spark = _resolve_spark()
        result: pd.DataFrame = self._spark.sql(sql).toPandas()
        return result

    def engine_type(self) -> EngineType:
        return EngineType.SPARK
