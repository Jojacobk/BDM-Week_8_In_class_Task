"""Trino + Iceberg adapter.

Speaks to the iceberg_datalake catalog registered in trino/iceberg_datalake.properties.
Open/Closed exhibit: filling this file is the only change needed to run all DAGs
on Trino — merger.py, quality.py, ledger.py, and dag_factory.py are untouched.

The trino Python DBAPI client is already installed in the airflow image.
Connection parameters are read from environment variables (same pattern as spark_engine.py).
"""
from __future__ import annotations

import os

import trino

from internal_etl_package.engines.base import TableEngine


_TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_TRINO_USER = os.environ.get("TRINO_USER", "airflow")
_CATALOG = "iceberg_datalake"


class TrinoIcebergEngine(TableEngine):
    """Production TableEngine backed by Trino + Iceberg + HMS.

    Demonstrates Open/Closed: arrives as a new file with zero edits to any
    reliability logic outside engines/.
    """

    def __init__(self, trino_conn=None):
        self._trino = trino_conn or trino.dbapi.connect(
            host=_TRINO_HOST,
            port=_TRINO_PORT,
            user=_TRINO_USER,
            catalog=_CATALOG,
        )

    def _cursor(self):
        return self._trino.cursor()

    def current_snapshot_id(self, table: str) -> int:
        """Return the snapshot id of the latest committed version of `table`."""
        schema, tbl = table.split(".", 1)
        cur = self._cursor()
        cur.execute(
            f'SELECT snapshot_id FROM {_CATALOG}.{schema}."{tbl}$snapshots" '
            "ORDER BY committed_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        return int(row[0])

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        """Restore `table` to the given historical snapshot."""
        schema, tbl = table.split(".", 1)
        cur = self._cursor()
        cur.execute(
            f"CALL {_CATALOG}.system.rollback_to_snapshot("
            f"'{schema}', '{tbl}', {int(snapshot_id)})"
        )

    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        """Apply pending source data to `table`. Return the new snapshot id.

        Trino cannot read raw CSVs directly — it expects a pre-populated Iceberg
        staging table at <schema>.<table>_staging loaded by an upstream task.
        The MERGE logic below is otherwise identical to the Spark engine.
        """
        schema, tbl = table.split(".", 1)
        cur = self._cursor()
        cur.execute(
            f"""
            MERGE INTO {_CATALOG}.{schema}.{tbl} AS target
            USING {_CATALOG}.{schema}.{tbl}_staging AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        return self.current_snapshot_id(table)

    def count_duplicates(self, table: str, primary_key: str) -> int:
        """Return how many `primary_key` values appear more than once."""
        schema, tbl = table.split(".", 1)
        cur = self._cursor()
        cur.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {primary_key}
                FROM {_CATALOG}.{schema}.{tbl}
                GROUP BY {primary_key}
                HAVING COUNT(*) > 1
            )
            """
        )
        row = cur.fetchone()
        return int(row[0])

    def row_count(self, table: str) -> int:
        """Return the total number of rows in `table`."""
        schema, tbl = table.split(".", 1)
        cur = self._cursor()
        cur.execute(f"SELECT COUNT(*) FROM {_CATALOG}.{schema}.{tbl}")
        row = cur.fetchone()
        return int(row[0])

    def null_rate(self, table: str, column: str) -> float:
        """Return the fraction of rows where `column` is NULL (0.0 to 1.0)."""
        schema, tbl = table.split(".", 1)
        cur = self._cursor()
        cur.execute(
            f"""
            SELECT
              CAST(SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS DOUBLE)
              / NULLIF(CAST(COUNT(*) AS DOUBLE), 0)
            FROM {_CATALOG}.{schema}.{tbl}
            """
        )
        row = cur.fetchone()
        rate = row[0]
        return float(rate) if rate is not None else 0.0
