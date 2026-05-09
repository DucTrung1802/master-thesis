import psycopg2
from psycopg2.extras import execute_values
from datetime import date, datetime, timezone
from typing import Any, List
import pandas as pd

from logger.logger import Logger
from tabular_database_driver.tabular_database_driver_interface import (
    TabularDatabaseDriverInterface,
)
from dtos.tabular_database_driver_dtos.postgre_sql_connection_dto import (
    PostgreSQLConnectionDto,
)
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import *
from utils.enums import DatabaseExecutionStatus
from utils.constants import *
from utils.utils import *


class PostgreSQLDriver(TabularDatabaseDriverInterface):
    def __init__(self, logger: Logger):
        self._logger = logger
        self._connections: dict[str, psycopg2.extensions.connection] = {}
        self._cursors: dict[str, psycopg2.extensions.cursor] = {}
        self._cursor = None
        self._current_db: str = None
        self._connection_models: dict[str, PostgreSQLConnectionDto] = {}
        # ── Cache: avoids re-querying information_schema on every upsert ──
        self._column_cache: dict[str, set[str]] = {}

    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────

    def _build_join_clause(self, join_model_list: List[JoinModel]) -> str:
        if not join_model_list:
            return ""
        return " ".join(
            f"{jm.join_type.value} {jm.schema_right}.{jm.table_right} ON {jm.build_on_clause()}"
            for jm in join_model_list
        )

    def _cache_key(self, schema_name: str, table_name: str) -> str:
        return f"{schema_name}.{table_name}"

    def _get_table_columns(self, schema_name: str, table_name: str) -> set[str]:
        """
        Return the set of column names for a table.
        Result is cached in-memory; invalidated on create_table / drop_table.
        Uses a parameterized query to avoid SQL injection.
        """
        key = self._cache_key(schema_name, table_name)
        if key not in self._column_cache:
            self._cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                """,
                (schema_name, table_name),
            )
            self._column_cache[key] = {row[0] for row in self._cursor.fetchall()}
        return self._column_cache[key]

    def _invalidate_column_cache(self, schema_name: str, table_name: str) -> None:
        self._column_cache.pop(self._cache_key(schema_name, table_name), None)

    # ──────────────────────────────────────────────────────────────────────
    # Connection management  (unchanged)
    # ──────────────────────────────────────────────────────────────────────

    def connect(
        self, connection_model: PostgreSQLConnectionDto
    ) -> DatabaseExecutionStatus:
        db_name = connection_model.database
        if db_name in self._connections:
            self._logger.log_info(f"Already connected to database: '{db_name}'")
            self._current_db = db_name
            return DatabaseExecutionStatus.SUCCESS

        try:
            try:
                conn = psycopg2.connect(
                    host=connection_model.host,
                    user=connection_model.user,
                    password=connection_model.password,
                    port=connection_model.port,
                    database=connection_model.database,
                )
            except psycopg2.OperationalError as e:
                if "does not exist" in str(e):
                    self._logger.log_warning(
                        f'Database "{db_name}" does not exist. Creating it...'
                    )
                    temp_conn = psycopg2.connect(
                        host=connection_model.host,
                        user=connection_model.user,
                        password=connection_model.password,
                        port=connection_model.port,
                        database="postgres",
                    )
                    temp_conn.autocommit = True
                    temp_cursor = temp_conn.cursor()
                    temp_cursor.execute(f'CREATE DATABASE "{db_name}"')
                    temp_cursor.close()
                    temp_conn.close()
                    conn = psycopg2.connect(
                        host=connection_model.host,
                        user=connection_model.user,
                        password=connection_model.password,
                        port=connection_model.port,
                        database=db_name,
                    )
                else:
                    raise ConnectionError(e)

            conn.autocommit = True
            cursor = conn.cursor()

            self._connections[db_name] = conn
            self._cursors[db_name] = cursor
            self._cursor = cursor
            self._connection_models[db_name] = connection_model
            self._current_db = db_name

            self._logger.log_info(f'Connected to database: "{db_name}"')
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._logger.log_error(f"Error connecting to PostgreSQL: {e}")
            return DatabaseExecutionStatus.ERROR

    def disconnect(self, database_name: str = None) -> DatabaseExecutionStatus:
        try:
            if database_name:
                if database_name in self._cursors:
                    self._cursors[database_name].close()
                    del self._cursors[database_name]
                if database_name in self._connections:
                    self._connections[database_name].close()
                    del self._connections[database_name]
                self._logger.log_info(f'Disconnected from database "{database_name}"')
            else:
                for cursor in self._cursors.values():
                    cursor.close()
                for conn in self._connections.values():
                    conn.close()
                self._cursors.clear()
                self._connections.clear()
                self._logger.log_info("Disconnected from all databases")
            self._current_db = None
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            self._logger.log_error(f"Error disconnecting: {e}")
            return DatabaseExecutionStatus.ERROR

    def change_database(self, database_name_to_select: str) -> DatabaseExecutionStatus:
        if database_name_to_select not in self._connections:
            first_key = next(iter(self._connection_models))
            connection_model = self._connection_models[first_key]
            connection_model.database = database_name_to_select
            return self.connect(connection_model)
        else:
            self._cursor = self._cursors[database_name_to_select]
            self._current_db = database_name_to_select
            return DatabaseExecutionStatus.SUCCESS

    # ──────────────────────────────────────────────────────────────────────
    # Query execution
    # CHANGED: accepts optional params tuple for parameterized queries,
    # which are faster (server-side plan caching) and safe from SQL injection.
    # ──────────────────────────────────────────────────────────────────────

    def execute_query(self, query: str, params: tuple = None) -> None:
        self._logger.log_debug(f"Executing query:\n{query.strip()}")
        self._cursor.execute(query, params)

    def fetch_result(self) -> list:
        try:
            return self._cursor.fetchall()
        except Exception as e:
            self._logger.log_error(f"Error fetching results: {e}")
            return []

    # ──────────────────────────────────────────────────────────────────────
    # DDL  (unchanged logic, cache invalidation added)
    # ──────────────────────────────────────────────────────────────────────

    def create_database(self, database_name: str) -> DatabaseExecutionStatus:
        try:
            self.execute_query(f"CREATE DATABASE {database_name}")
            self._logger.log_info(f'Database "{database_name}" created successfully.')
            self.change_database(database_name)
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self._logger.log_warning(f'Database "{database_name}" already exists.')
                self.change_database(database_name)
                return DatabaseExecutionStatus.ALREADY_EXISTS
            self._logger.log_error(f"Error creating database: {e}")
            return DatabaseExecutionStatus.ERROR

    def drop_database(self, database_name: str):
        try:
            self.execute_query(f"DROP DATABASE {database_name}")
            self._logger.log_info(f'Database "{database_name}" dropped successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self._logger.log_warning(f'Database "{database_name}" does not exist.')
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            self._logger.log_error(f"Error dropping database: {e}")
            return DatabaseExecutionStatus.ERROR

    def create_schema(self, schema_name: str):
        try:
            self.execute_query(f"CREATE SCHEMA {schema_name}")
            self._logger.log_info(f'Schema "{schema_name}" created successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self._logger.log_warning(f'Schema "{schema_name}" already exists.')
                return DatabaseExecutionStatus.ALREADY_EXISTS
            self._logger.log_error(f"Error creating schema: {e}")
            return DatabaseExecutionStatus.ERROR

    def drop_schema(self, schema_name: str):
        try:
            self.execute_query(f"DROP SCHEMA {schema_name}")
            self._logger.log_info(f'Schema "{schema_name}" dropped successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self._logger.log_warning(f'Schema "{schema_name}" does not exist.')
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            elif "other objects" in message:
                self._logger.log_warning(
                    f'Schema "{schema_name}" contains other objects. Cannot be dropped.'
                )
                return DatabaseExecutionStatus.OTHER_OBJECT_DEPEND
            self._logger.log_error(f"Error dropping schema: {e}")
            return DatabaseExecutionStatus.ERROR

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: List[Column],
        primary_keys: List[str],
        foreign_keys: List[ForeignKey] = None,
    ):
        try:
            column_definitions = ",\n    ".join(
                f"{col.name} {col.data_type}{' NOT NULL' if not col.nullable else ''}"
                for col in columns
            )
            primary_key_definition = (
                f"PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else None
            )
            foreign_key_definitions = [
                f"FOREIGN KEY ({fk.column_name}) REFERENCES {fk.ref_table}({fk.ref_column})"
                for fk in (foreign_keys or [])
            ]
            constraints = ", ".join(
                filter(None, [primary_key_definition] + foreign_key_definitions)
            )
            query = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
    {column_definitions},
    {constraints}
)
"""
            self.execute_query(query)
            # Invalidate cache so the next upsert re-reads the fresh schema
            self._invalidate_column_cache(schema_name, table_name)
            self._logger.log_info(
                f'Table "{schema_name}.{table_name}" created successfully.'
            )
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self._logger.log_warning(
                    f'Table "{schema_name}.{table_name}" already exists.'
                )
                return DatabaseExecutionStatus.ALREADY_EXISTS
            self._logger.log_error(f"Error creating table: {e}")
            return DatabaseExecutionStatus.ERROR

    def drop_table(self, schema_name: str, table_name: str):
        try:
            self.execute_query(f"DROP TABLE {schema_name}.{table_name}")
            # Remove stale cache entry
            self._invalidate_column_cache(schema_name, table_name)
            self._logger.log_info(
                f'Table "{schema_name}.{table_name}" dropped successfully.'
            )
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self._logger.log_warning(
                    f'Table "{schema_name}.{table_name}" does not exist.'
                )
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            self._logger.log_error(f"Error dropping table: {e}")
            return DatabaseExecutionStatus.ERROR

    # ──────────────────────────────────────────────────────────────────────
    # DML
    # ──────────────────────────────────────────────────────────────────────

    def insert(self, schema_name: str, table_name: str, records: List[Record]):
        """
        CHANGED: single execute_values call instead of one query per record.
        Before: N round-trips for N records.
        After:  1 round-trip for all records.
        """
        if not records:
            return DatabaseExecutionStatus.SUCCESS
        try:
            col_names = [col.column_name for col in records[0].data_dto_list]
            col_str = ", ".join(col_names)

            data = [
                tuple(col.value for col in record.data_dto_list) for record in records
            ]

            query = f"INSERT INTO {schema_name}.{table_name} ({col_str}) VALUES %s"
            execute_values(self._cursor, query, data)

            self._logger.log_info(
                f'Inserted {len(records)} record(s) into "{schema_name}.{table_name}".'
            )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(f"Error inserting records: {e}. Rolled back.")
            return DatabaseExecutionStatus.ERROR

    def update(
        self,
        schema_name: str,
        table_name: str,
        update_record: Record,
        join_model_list: List[JoinModel] = None,
        conditions: List[Condition] = None,
    ):
        try:
            set_clause = ",\n    ".join(
                f"{col.column_name} = {format_value(col.value, col.data_type)}"
                for col in update_record.data_dto_list
            )
            where_clause = (
                "WHERE\n    "
                + "\n    AND ".join(
                    f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"
                    for cond in conditions
                )
                if conditions
                else ""
            )
            join_clause = self._build_join_clause(join_model_list)
            query = f"""
UPDATE {schema_name}.{table_name}
SET
    {set_clause}{f' {join_clause}' if join_clause else ''}
{where_clause}
"""
            self.execute_query(query)
            count = (
                int(self._cursor.statusmessage.split()[-1])
                if self._cursor.statusmessage.startswith("UPDATE")
                else 0
            )
            self._logger.log_info(
                f'Updated {count} records in "{schema_name}.{table_name}".'
            )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(f"Error updating records: {e}. Rolled back.")
            return DatabaseExecutionStatus.ERROR

    def upsert(
        self,
        schema_name: str,
        table_name: str,
        records: List[Record],
        primary_keys: List[str],
    ):
        """
        CHANGED: all records upserted in a single execute_values call.

        Before: 1 introspection query + 1 INSERT + 1 fetchone  ×  N records
                = 3N round-trips

        After:  1 introspection query (cached after first call)
              + 1 INSERT for all records
              + 1 fetchone
                = 3 round-trips total, regardless of N
        """
        if not records:
            return DatabaseExecutionStatus.SUCCESS, 0, 0

        try:
            # ── introspection: served from cache on every call after the first ──
            available_columns = self._get_table_columns(schema_name, table_name)
            has_create_date = "create_date" in available_columns
            has_update_date = "update_date" in available_columns

            col_names = [col.column_name for col in records[0].data_dto_list]

            # Columns to INSERT (add create_date if the table has it)
            insert_cols = list(col_names)
            if has_create_date and "create_date" not in insert_cols:
                insert_cols.append("create_date")

            # SET clause for the DO UPDATE branch
            update_parts = [
                f"{c} = EXCLUDED.{c}" for c in col_names if c not in primary_keys
            ]
            if has_update_date:
                update_parts.append("update_date = now()")

            col_str = ", ".join(insert_cols)
            pk_str = ", ".join(primary_keys)
            update_str = ", ".join(update_parts)

            # ── build data tuples (no DataModel string-formatting overhead) ──
            now = datetime.now(timezone.utc) if has_create_date else None

            data = [
                tuple(col.value for col in record.data_dto_list)
                + ((now,) if has_create_date and "create_date" not in col_names else ())
                for record in records
            ]

            # ── single round-trip for ALL records ────────────────────────────
            query = f"""
WITH upserted AS (
    INSERT INTO {schema_name}.{table_name} ({col_str})
    VALUES %s
    ON CONFLICT ({pk_str})
    DO UPDATE SET {update_str}
    RETURNING xmax
)
SELECT
    COUNT(*) FILTER (WHERE xmax = 0)  AS inserted,
    COUNT(*) FILTER (WHERE xmax <> 0) AS updated
FROM upserted
"""
            execute_values(self._cursor, query, data)
            # ─────────────────────────────────────────────────────────────────

            row = self._cursor.fetchone()
            inserted_count = row[0] or 0 if row else 0
            updated_count = row[1] or 0 if row else 0

            self._logger.log_debug(
                f'upsert() "{schema_name}.{table_name}" — '
                f"inserted: {inserted_count}, updated: {updated_count}"
            )
            return DatabaseExecutionStatus.SUCCESS, inserted_count, updated_count

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(f"Error upserting records: {e}. Rolled back.")
            return DatabaseExecutionStatus.ERROR

    def delete(
        self,
        schema_name: str,
        table_name: str,
        join_model_list: List[JoinModel] = None,
        conditions: List[Condition] = None,
    ):
        try:
            where_clause = (
                "WHERE\n    "
                + "\n    AND ".join(
                    f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"
                    for cond in conditions
                )
                if conditions
                else ""
            )
            join_clause = self._build_join_clause(join_model_list)
            query = f"""
DELETE FROM {schema_name}.{table_name}
{join_clause}
{where_clause}
"""
            self.execute_query(query)
            count = (
                int(self._cursor.statusmessage.split()[-1])
                if self._cursor.statusmessage.startswith("DELETE")
                else 0
            )
            self._logger.log_info(
                f'Deleted {count} records from "{schema_name}.{table_name}".'
            )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(f"Error deleting records: {e}. Rolled back.")
            return DatabaseExecutionStatus.ERROR

    def soft_delete(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: Dict[str, Any],
    ):
        try:
            columns = self._get_table_columns(schema_name, table_name)
            if "delete_date" not in columns:
                self._logger.log_info(
                    f'"{schema_name}.{table_name}" has no delete_date. Skipping.'
                )
                return DatabaseExecutionStatus.SUCCESS

            conditions = [
                f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
                for k, v in primary_keys.items()
            ]
            query = f"""
UPDATE {schema_name}.{table_name}
SET delete_date = now()
WHERE {" AND ".join(conditions)}
"""
            self.execute_query(query)
            self._logger.log_info(
                f'Soft-deleted in "{schema_name}.{table_name}" where {primary_keys}.'
            )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(f"Error soft-deleting: {e}. Rolled back.")
            return DatabaseExecutionStatus.ERROR

    def select(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str] = None,
        join_model_list: List[JoinModel] = None,
        conditions: List[Condition] = None,
        order_by: List[str] = None,
        limit: int = None,
    ) -> pd.DataFrame:
        try:
            if columns and not isinstance(columns, list):
                columns = [columns]

            columns_clause = ",\n    ".join(columns) if columns else "*"
            where_clause = (
                "WHERE\n    "
                + "\n    AND ".join(
                    f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"
                    for cond in conditions
                )
                if conditions
                else ""
            )
            join_clause = self._build_join_clause(join_model_list)
            order_by_clause = (
                "ORDER BY\n    " + ",\n    ".join(order_by) if order_by else ""
            )

            query = f"""
SELECT
    {columns_clause}
FROM
    {schema_name}.{table_name}
{join_clause}
{where_clause}
{order_by_clause}
{f'LIMIT {limit}' if limit else ''}
"""
            self.execute_query(query)
            results = self.fetch_result()
            column_names = [desc[0] for desc in self._cursor.description]

            self._logger.log_info(
                f'Selected {len(results)} records from "{schema_name}.{table_name}".'
            )
            return pd.DataFrame(results, columns=column_names)

        except Exception as e:
            self._logger.log_error(f"Error selecting records: {e}")
            return pd.DataFrame()
