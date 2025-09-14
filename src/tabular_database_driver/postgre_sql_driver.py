import psycopg2
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
from datetime import date


class PostgreSQLDriver(TabularDatabaseDriverInterface):
    def __init__(self, logger: Logger):
        self._logger = logger
        self._connections: dict[str, psycopg2.extensions.connection] = {}
        self._cursors: dict[str, psycopg2.extensions.cursor] = {}
        self._cursor = None
        self._current_db: str = None
        self._connection_models: dict[str, PostgreSQLConnectionDto] = {}

    def connect(
        self, connection_model: PostgreSQLConnectionDto
    ) -> DatabaseExecutionStatus:
        """Establish a connection to a PostgreSQL database."""
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
        """Close connection to a specific database or all if None."""
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
                for db, cursor in self._cursors.items():
                    cursor.close()
                for db, conn in self._connections.items():
                    conn.close()
                self._cursors.clear()
                self._connections.clear()
                self._logger.log_info("Disconnected from all databases")
            self._current_db = None
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            self._logger.log_error(f"Error disconnecting: {e}")
            return DatabaseExecutionStatus.ERROR

    def execute_query(self, query: str) -> None:
        self._logger.log_debug(f"Executing query:\n{query.strip()}")
        self._cursor.execute(query)

    def fetch_result(self) -> list:
        """Fetch results from the last executed query."""
        try:
            return self._cursor.fetchall()
        except Exception as e:
            self._logger.log_error(f"Error fetching results: {e}")
            return []

    def create_database(self, database_name: str) -> DatabaseExecutionStatus:
        """Create a new database in PostgreSQL and change to this new database."""
        try:
            query = f"CREATE DATABASE {database_name}"
            self.execute_query(query)
            self._logger.log_info(f'Database "{database_name}" created successfully.')
            self.change_database(database_name)
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self._logger.log_warning(f'Database "{database_name}" already exists.')
                self.change_database(database_name)
                return DatabaseExecutionStatus.ALREADY_EXISTS
            else:
                self._logger.log_error(f"Error creating database: {e}")
                return DatabaseExecutionStatus.ERROR

    def drop_database(self, database_name: str):
        """Drop an existing database in PostgreSQL."""
        try:
            query = f"DROP DATABASE {database_name}"
            self.execute_query(query)
            self._logger.log_info(f'Database "{database_name}" dropped successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self._logger.log_warning(f'Database "{database_name}" does not exist.')
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            else:
                self._logger.log_error(f"Error dropping database: {e}")
                return DatabaseExecutionStatus.ERROR

    def change_database(self, database_name_to_select: str) -> DatabaseExecutionStatus:
        """Switch to a different database connection."""
        if database_name_to_select not in self._connections.keys():
            first_key = next(iter(self._connection_models))
            connection_model = self._connection_models[first_key]
            connection_model.database = database_name_to_select
            return self.connect(connection_model)
        else:
            self._cursor = self._cursors[database_name_to_select]
            self._current_db = database_name_to_select
            return DatabaseExecutionStatus.SUCCESS

    def create_schema(self, schema_name: str):
        """Create a new schema in the current database."""
        try:
            query = f"CREATE SCHEMA {schema_name}"
            self.execute_query(query)
            self._logger.log_info(f'Schema "{schema_name}" created successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self._logger.log_warning(f'Schema "{schema_name}" already exists.')
                return DatabaseExecutionStatus.ALREADY_EXISTS
            else:
                self._logger.log_error(f"Error creating schema: {e}")
                return DatabaseExecutionStatus.ERROR

    def drop_schema(self, schema_name: str):
        """Drop an existing schema in the current database."""
        try:
            query = f"DROP SCHEMA {schema_name}"
            self.execute_query(query)
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
            else:
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
        """Create a new table in the current database."""
        try:
            column_definitions = ",\n    ".join(
                [
                    f"{col.name} {col.data_type}{" NOT NULL" if not col.nullable else ""}"
                    for col in columns
                ]
            )

            primary_key_definition = None
            if len(primary_keys) > 0:
                primary_key_definition = f"PRIMARY KEY ({', '.join(primary_keys)})"

            foreign_key_definitions = [
                f"FOREIGN KEY ({fk.column_name}) REFERENCES {fk.ref_table}({fk.ref_column})"
                for fk in foreign_keys or []
            ]

            # Build all constraints together
            table_constraints = [primary_key_definition] + foreign_key_definitions

            query = f"""
CREATE TABLE {schema_name}.{table_name} (
    {column_definitions},
    {', '.join(table_constraints)}
)
"""
            self.execute_query(query)
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
            else:
                self._logger.log_error(f"Error creating table: {e}")
                return DatabaseExecutionStatus.ERROR

    def drop_table(self, schema_name: str, table_name: str):
        """Drop an existing table in the current database."""
        try:
            query = f"DROP TABLE {schema_name}.{table_name}"
            self.execute_query(query)
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
            else:
                self._logger.log_error(f"Error dropping table: {e}")
                return DatabaseExecutionStatus.ERROR

    def insert(self, schema_name: str, table_name: str, records: List[Record]):
        """Insert records into a table."""
        try:
            for record in records:
                columns = ", ".join([col.column_name for col in record.data_dto_list])
                values = ", ".join(
                    [
                        (
                            "NULL"
                            if col.value is None
                            else (
                                f"'{col.value}'"
                                if isinstance(col.value, (str, date))
                                else str(col.value)
                            )
                        )
                        for col in record.data_dto_list
                    ]
                )
                query = f"""
INSERT INTO 
    {schema_name}.{table_name} ({columns})
VALUES
    ({values})
"""
                self.execute_query(query)
            # self._logger.log_info(
            #     f'Insert {len(records)} record(s) into table "{schema_name}.{table_name}" successfully.'
            # )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(
                f"Error inserting records: {e}. Rolled back transaction."
            )
            return DatabaseExecutionStatus.ERROR

    def update(
        self,
        schema_name: str,
        table_name: str,
        update_record: Record,
        join_model: JoinModel = None,
        conditions: List[Condition] = None,
    ):
        """Update records in a table."""
        try:
            set_clause = ",\n    ".join(
                [
                    f"{col.column_name} = {format_value(col.value, col.data_type)}"
                    for col in update_record.data_dto_list
                ]
            )
            where_clause = (
                "WHERE\n    "
                + "\n    AND ".join(
                    [
                        f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"
                        for cond in conditions or []
                    ]
                )
                if conditions
                else ""
            )
            join_clause = (
                f"JOIN {join_model.table_right} ON {join_model.table_left}.{join_model.column_left} = {join_model.table_right}.{join_model.column_right}"
                if join_model
                else ""
            )

            query = f"""
UPDATE {schema_name}.{table_name}
SET
    {set_clause} {f"\n    {join_clause}" if join_clause else ""}
{where_clause}
"""
            self.execute_query(query)
            number_of_records_updated = (
                int(self._cursor.statusmessage.split()[-1])
                if self._cursor.statusmessage.startswith("UPDATE")
                else 0
            )
            # self._logger.log_info(
            #     f'Updated {number_of_records_updated} records in table "{schema_name}.{table_name}" successfully.'
            # )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(
                f"Error inserting records: {e}. Rolled back transaction."
            )
            return DatabaseExecutionStatus.ERROR

    def upsert(
        self,
        schema_name: str,
        table_name: str,
        records: List[Record],
        primary_keys: List[str],
    ):
        """
        Upsert records into a table using INSERT ... ON CONFLICT(<primary_key>) DO UPDATE SET ...
        Tracks number of inserted and updated records.
        Automatically manages create_date/update_date if those columns exist in the table.
        """
        try:
            inserted_count = 0
            updated_count = 0

            # Fetch column names from database (schema introspection)
            self.execute_query(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
            """
            )
            available_columns = {row[0] for row in self._cursor.fetchall()}

            has_create_date = "create_date" in available_columns
            has_update_date = "update_date" in available_columns

            for record in records:
                # Collect user-provided columns/values
                column_names = [col.column_name for col in record.data_dto_list]
                values = [
                    (
                        "NULL"
                        if col.value is None
                        else (
                            f"'{col.value}'"
                            if isinstance(col.value, (str, date))
                            else str(col.value)
                        )
                    )
                    for col in record.data_dto_list
                ]

                # If create_date column exists and not provided → set it on INSERT
                if has_create_date and "create_date" not in column_names:
                    column_names.append("create_date")
                    values.append("now()")

                columns = ", ".join(column_names)
                values_str = ", ".join(values)

                # Build update clause: all non-PK cols
                update_set_parts = [
                    f"{col.column_name} = EXCLUDED.{col.column_name}"
                    for col in record.data_dto_list
                    if col.column_name not in primary_keys
                ]

                # If update_date exists → always bump it
                if has_update_date:
                    update_set_parts.append("update_date = now()")

                update_set_clause = ", ".join(update_set_parts)
                conflict_clause = ", ".join(primary_keys)

                # CTE to track inserted vs updated rows
                query = f"""
WITH upserted AS (
    INSERT INTO {schema_name}.{table_name} ({columns})
    VALUES ({values_str})
    ON CONFLICT ({conflict_clause})
    DO UPDATE SET {update_set_clause}
    RETURNING xmax
)
SELECT COUNT(*) FILTER (WHERE xmax = 0) AS inserted,
       COUNT(*) FILTER (WHERE xmax <> 0) AS updated
FROM upserted;
"""

                self.execute_query(query)

                if hasattr(self._cursor, "fetchone"):
                    row = self._cursor.fetchone()
                    if row:
                        inserted_count += row[0] or 0
                        updated_count += row[1] or 0

                self._logger.log_debug(
                    f'upsert() - Query status: "{self._cursor.statusmessage}"'
                )

            return DatabaseExecutionStatus.SUCCESS, inserted_count, updated_count

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(
                f"Error inserting records: {e}. Rolled back transaction."
            )
            return DatabaseExecutionStatus.ERROR

    def delete(
        self,
        schema_name: str,
        table_name: str,
        join_model: JoinModel = None,
        conditions: List[Condition] = None,
    ):
        """Delete records in a table."""
        try:
            where_clause = (
                "WHERE\n    "
                + "\n    AND ".join(
                    [
                        f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"
                        for cond in conditions or []
                    ]
                )
                if conditions
                else ""
            )
            join_clause = (
                f"JOIN {join_model.table_right} ON {join_model.table_left}.{join_model.column_left} = {join_model.table_right}.{join_model.column_right}"
                if join_model
                else ""
            )

            query = f"""
DELETE FROM {schema_name}.{table_name}
{f"\n    {join_clause}" if join_clause else ""}
{where_clause}
            """
            self.execute_query(query)
            number_of_records_updated = (
                int(self._cursor.statusmessage.split()[-1])
                if self._cursor.statusmessage.startswith("DELETE")
                else 0
            )
            # self._logger.log_info(
            #     f'Delete {number_of_records_updated} records in table "{schema_name}.{table_name}" successfully.'
            # )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(
                f"Error inserting records: {e}. Rolled back transaction."
            )
            return DatabaseExecutionStatus.ERROR

    def soft_delete(
        self,
        schema_name: str,
        table_name: str,
        primary_keys: Dict[str, Any],  # e.g. {"id": 1, "code": "HSX"}
    ):
        """
        Soft delete: sets delete_date = now() if the table has that column.
        Supports single or composite primary keys.

        Args:
            schema_name (str): Database schema name (e.g. "stock_market").
            table_name (str): Table name (e.g. "market").
            primary_keys (Dict[str, Any]): Dictionary of primary key column/value pairs.
                - For single PK: {"id": 1}
                - For composite PK: {"id": 1, "code": "HSX"}

        Returns:
            DatabaseExecutionStatus: SUCCESS if soft delete executed or skipped,
                                    ERROR if query failed.

        Example:
            # Single primary key
            driver.soft_delete("stock_market", "market", {"id": 1})

            # Composite primary key
            driver.soft_delete("stock_market", "market", {"id": 1, "code": "HSX"})
        """
        try:
            # Check if delete_date exists
            self.execute_query(
                f"""
SELECT 1
FROM information_schema.columns
WHERE table_schema = '{schema_name}'
AND table_name = '{table_name}'
AND column_name = 'delete_date'
"""
            )
            if not self._cursor.fetchone():
                self._logger.log_info(
                    f"Table {schema_name}.{table_name} has no delete_date column. Skipping soft delete."
                )
                return DatabaseExecutionStatus.SUCCESS

            # Build WHERE clause for multiple PKs
            conditions = []
            for pk_name, pk_value in primary_keys.items():
                if isinstance(pk_value, str):
                    pk_value_formatted = f"'{pk_value}'"
                else:
                    pk_value_formatted = str(pk_value)
                conditions.append(f"{pk_name} = {pk_value_formatted}")

            where_clause = " AND ".join(conditions)

            query = f"""
UPDATE {schema_name}.{table_name}
SET delete_date = now()
WHERE {where_clause}
"""
            self.execute_query(query)

            self._logger.log_info(
                f'Soft deleted record in "{schema_name}.{table_name}" where "{where_clause}"'
            )
            return DatabaseExecutionStatus.SUCCESS

        except Exception as e:
            self._connections[self._current_db].rollback()
            self._logger.log_error(
                f"Error inserting records: {e}. Rolled back transaction."
            )
            return DatabaseExecutionStatus.ERROR

    def select(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str] = None,
        join_model: JoinModel = None,
        conditions: List[Condition] = None,
        order_by: List[str] = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select records from a table."""
        try:
            if columns and not isinstance(columns, List):
                columns = [columns]

            columns_clause = ",\n    ".join(columns) if columns else "*"
            where_clause = (
                "WHERE\n    "
                + "\n    AND ".join(
                    [
                        f"{cond.column} {cond.operator.value} {format_value(cond.value, cond.data_type)}"
                        for cond in conditions or []
                    ]
                )
                if conditions
                else ""
            )
            join_clause = (
                f"JOIN {join_model.table_right} ON {join_model.table_left}.{join_model.column_left} = {join_model.table_right}.{join_model.column_right}"
                if join_model
                else ""
            )
            order_by_clause = (
                "ORDER BY\n    " + ",\n    ".join(order_by) if order_by else ""
            )

            query = f"""
SELECT
    {columns_clause}
FROM
    {schema_name}.{table_name} {f"\n    {join_clause}" if join_clause else ""}
{where_clause}
{order_by_clause}
{f"LIMIT {limit}" if limit else ""}
"""
            self.execute_query(query)
            results = self.fetch_result()
            column_names = [desc[0] for desc in self._cursor.description]
            df = pd.DataFrame(results, columns=column_names)
            # self._logger.log_info(
            #     f'Selected {len(results)} records from table "{schema_name}.{table_name}" successfully.'
            # )
            return df
        except Exception as e:
            self._logger.log_error(f"Error selecting records: {e}")
            return pd.DataFrame()
