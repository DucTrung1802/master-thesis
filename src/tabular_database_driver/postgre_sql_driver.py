import threading
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

        # ── Main-thread connection bookkeeping (unchanged) ────────────────────
        self._connections: dict[str, psycopg2.extensions.connection] = {}
        self._cursors: dict[str, psycopg2.extensions.cursor] = {}
        self._cursor = None
        self._current_db: str = None
        self._connection_models: dict[str, PostgreSQLConnectionDto] = {}

        # ── Thread-safety additions ───────────────────────────────────────────
        # Each worker thread gets its own connection + cursor so they never
        # share a socket or cursor state.
        self._local = threading.local()

        # The column cache is read/written from many threads; a reentrant lock
        # lets the same thread re-enter (e.g. _ensure_table_exists → _get_table_columns).
        self._column_cache: dict[str, set[str]] = {}
        self._cache_lock = threading.RLock()

    # ──────────────────────────────────────────────────────────────────────────
    # NEW: thread-local cursor
    # ──────────────────────────────────────────────────────────────────────────

    def _get_cursor(self) -> psycopg2.extensions.cursor:
        """
        Return a cursor that is private to the calling thread.

        - Main thread  → reuses the shared cursor created by connect().
        - Worker thread → lazily opens its own connection + cursor the first
                          time it calls this method, then reuses them.

        Why not a connection pool (ThreadedConnectionPool)?
        Because the existing API separates execute_query() from fetch_result(),
        meaning a single logical operation spans two method calls.  A pool
        would require the caller to bracket every pair with getconn/putconn,
        which would break the public API.  Thread-local connections give each
        thread a stable cursor for the lifetime of the thread instead.
        """
        if threading.current_thread() is threading.main_thread():
            return self._cursor  # unchanged path for single-threaded usage

        db = self._current_db
        if not hasattr(self._local, "connections"):
            self._local.connections: dict[str, psycopg2.extensions.connection] = {}
            self._local.cursors: dict[str, psycopg2.extensions.cursor] = {}

        if db not in self._local.cursors:
            model = self._connection_models[db]
            conn = psycopg2.connect(
                host=model.host,
                user=model.user,
                password=model.password,
                port=model.port,
                database=model.database,
            )
            conn.autocommit = True
            self._local.connections[db] = conn
            self._local.cursors[db] = conn.cursor()
            self._logger.log_info(
                f"Worker thread {threading.current_thread().name} opened "
                f'its own connection to "{db}".'
            )

        return self._local.cursors[db]

    def _close_thread_local_connections(self) -> None:
        """Close any thread-local connections held by the calling thread."""
        if not hasattr(self._local, "cursors"):
            return
        for db, cursor in self._local.cursors.items():
            try:
                cursor.close()
            except Exception:
                pass
        for db, conn in self._local.connections.items():
            try:
                conn.close()
            except Exception:
                pass
        self._local.cursors.clear()
        self._local.connections.clear()

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers  (cache now protected by _cache_lock)
    # ──────────────────────────────────────────────────────────────────────────

    def _build_join_clause(self, join_model_list: List[JoinModel]) -> str:
        if not join_model_list:
            return ""
        return " ".join(
            f"{jm.join_type.value} {jm.schema_right}.{jm.table_right} ON {jm.build_on_clause()}"
            for jm in join_model_list
        )

    def _cache_key(self, schema_name: str, table_name: str) -> str:
        return f"{schema_name}.{table_name}"

    def _format_condition(self, cond: Condition) -> str:
        if cond.value is None:
            if cond.operator not in (SqlOperator.IS, SqlOperator.IS_NOT):
                raise ValueError(
                    f"Operator '{cond.operator.value}' is not valid for NULL values. "
                    f"Use SqlOperator.IS or SqlOperator.IS_NOT."
                )
            return f"{cond.column} {cond.operator.value} NULL"
        return f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"

    def _get_table_columns(self, schema_name: str, table_name: str) -> set[str]:
        """
        Return the column-name set for a table; result is cached.

        CHANGED: the cache dict is now guarded by _cache_lock so concurrent
        threads don't race on a cache miss (double-query / partial write).
        """
        key = self._cache_key(schema_name, table_name)

        # Fast path — read under lock to avoid a torn read of the dict
        with self._cache_lock:
            if key in self._column_cache:
                return self._column_cache[key]

        # Slow path — query outside the lock so other threads aren't blocked
        # while we wait for the DB, then re-enter to store the result.
        cursor = self._get_cursor()
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name   = %s
            """,
            (schema_name, table_name),
        )
        columns = {row[0] for row in cursor.fetchall()}

        with self._cache_lock:
            # Another thread may have populated the key while we queried; that's
            # fine — both results are identical, last-writer wins harmlessly.
            self._column_cache[key] = columns

        return columns

    def _invalidate_column_cache(self, schema_name: str, table_name: str) -> None:
        with self._cache_lock:
            self._column_cache.pop(self._cache_key(schema_name, table_name), None)

    # ──────────────────────────────────────────────────────────────────────────
    # Connection management
    # ──────────────────────────────────────────────────────────────────────────

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
            self._cursor = cursor  # main-thread cursor
            self._connection_models[db_name] = connection_model
            self._current_db = db_name

            self._logger.log_info(f'Connected to database: "{db_name}"')
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._logger.log_error(f"Error connecting to PostgreSQL: {e}")
            return DatabaseExecutionStatus.ERROR

    def disconnect(self, database_name: str = None) -> DatabaseExecutionStatus:
        """
        CHANGED: also closes any thread-local connections held by the calling
        thread so worker threads clean up after themselves.
        """
        try:
            # Close thread-local connections for this thread first
            self._close_thread_local_connections()

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
            self._cursor = self._cursors[database_name_to_select]  # main thread
            self._current_db = database_name_to_select
            return DatabaseExecutionStatus.SUCCESS

    # ──────────────────────────────────────────────────────────────────────────
    # Query execution  — all self._cursor refs → self._get_cursor()
    # ──────────────────────────────────────────────────────────────────────────

    def execute_query(self, query: str, params: tuple = None) -> None:
        self._logger.log_debug(f"Executing query:\n{query.strip()}")
        self._get_cursor().execute(query, params)

    def fetch_result(self) -> list:
        try:
            return self._get_cursor().fetchall()
        except Exception as e:
            self._logger.log_error(f"Error fetching results: {e}")
            return []

    # ──────────────────────────────────────────────────────────────────────────
    # DDL
    # ──────────────────────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────────────────────
    # DML  — self._cursor → self._get_cursor()  in every method
    # ──────────────────────────────────────────────────────────────────────────

    def insert(self, schema_name: str, table_name: str, records: List[Record]):
        if not records:
            return DatabaseExecutionStatus.SUCCESS
        try:
            col_names = [col.column_name for col in records[0].data_dto_list]
            col_str = ", ".join(col_names)
            data = [
                tuple(col.value for col in record.data_dto_list) for record in records
            ]
            query = f"INSERT INTO {schema_name}.{table_name} ({col_str}) VALUES %s"
            execute_values(self._get_cursor(), query, data)
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
                + "\n    AND ".join(self._format_condition(c) for c in conditions)
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
            cursor = self._get_cursor()
            cursor.execute(query)
            count = (
                int(cursor.statusmessage.split()[-1])
                if cursor.statusmessage.startswith("UPDATE")
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
        if not records:
            return DatabaseExecutionStatus.SUCCESS, 0, 0
        try:
            available_columns = self._get_table_columns(schema_name, table_name)
            has_create_date = "create_date" in available_columns
            has_update_date = "update_date" in available_columns

            col_names = [col.column_name for col in records[0].data_dto_list]
            insert_cols = list(col_names)
            if has_create_date and "create_date" not in insert_cols:
                insert_cols.append("create_date")

            update_parts = [
                f"{c} = EXCLUDED.{c}" for c in col_names if c not in primary_keys
            ]
            if has_update_date:
                update_parts.append("update_date = now()")

            col_str = ", ".join(insert_cols)
            pk_str = ", ".join(primary_keys)
            update_str = ", ".join(update_parts)
            now = datetime.now(timezone.utc) if has_create_date else None

            data = [
                tuple(col.value for col in record.data_dto_list)
                + ((now,) if has_create_date and "create_date" not in col_names else ())
                for record in records
            ]

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
            cursor = self._get_cursor()
            execute_values(cursor, query, data)
            row = cursor.fetchone()
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
                + "\n    AND ".join(self._format_condition(c) for c in conditions)
                if conditions
                else ""
            )
            join_clause = self._build_join_clause(join_model_list)
            query = f"""
DELETE FROM {schema_name}.{table_name}
{join_clause}
{where_clause}
"""
            cursor = self._get_cursor()
            cursor.execute(query)
            count = (
                int(cursor.statusmessage.split()[-1])
                if cursor.statusmessage.startswith("DELETE")
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
        self, schema_name: str, table_name: str, primary_keys: Dict[str, Any]
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
                + "\n    AND ".join(self._format_condition(c) for c in conditions)
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
            cursor = self._get_cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            self._logger.log_info(
                f'Selected {len(results)} records from "{schema_name}.{table_name}".'
            )
            return pd.DataFrame(results, columns=column_names)
        except Exception as e:
            self._logger.log_error(f"Error selecting records: {e}")
            return pd.DataFrame()
