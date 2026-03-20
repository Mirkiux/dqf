from __future__ import annotations

import pandas as pd

from dqf.adapters.base import DataSourceAdapter
from dqf.enums import EngineType


class DatabricksAdapter(DataSourceAdapter):
    """Adapter for Databricks SQL warehouses.

    Requires the optional ``databricks-sql-connector`` package::

        pip install dqf[databricks]

    The connector is imported lazily inside ``execute()`` so that the class
    can be referenced in code that runs on environments without the package
    installed — the ``ImportError`` is only raised when a query is actually
    attempted.
    """

    def __init__(
        self,
        host: str,
        token: str,
        http_path: str,
        catalog: str,
        schema: str,
    ) -> None:
        self._host = host
        self._token = token
        self._http_path = http_path
        self._catalog = catalog
        self._schema = schema

    def execute(self, sql: str) -> pd.DataFrame:
        try:
            from databricks import sql as dbsql
        except ImportError as exc:
            raise ImportError(
                "databricks-sql-connector is required for DatabricksAdapter. "
                "Install with: pip install dqf[databricks]"
            ) from exc

        with (
            dbsql.connect(
                server_hostname=self._host,
                http_path=self._http_path,
                access_token=self._token,
                catalog=self._catalog,
                schema=self._schema,
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute(sql)
            return cursor.fetchall_arrow().to_pandas()

    def engine_type(self) -> EngineType:
        return EngineType.DATABRICKS
