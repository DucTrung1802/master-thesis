import psycopg2
from typing import List

from logger.logger import Logger
from tabular_database_driver.tabular_database_driver_interface import (
    TabularDatabaseDriverInterface,
)
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import *
from utils.enums import DatabaseExecutionStatus
from utils.constants import *
from utils.utils import *


class PostgreSQLDriver(TabularDatabaseDriverInterface):
    def __init__(self, logger: Logger):
        self._logger = logger
        self._connection = None
        self._cursor = None
        self._connection_model: PostgreSQLConnectionModel = None

    def connect(
        self, connection_model: PostgreSQLConnectionModel
    ) -> DatabaseExecutionStatus:
        """Establish a connection to the PostgreSQL database."""
        try:
            self._connection_model = connection_model

            self._connection = psycopg2.connect(
                host=self._connection_model.host,
                user=self._connection_model.user,
                password=self._connection_model.password,
                port=self._connection_model.port,
                database=self._connection_model.database,
            )

            # Set autocommit to True to allow database creation without a transaction block
            self._connection.autocommit = True

            self._cursor = self._connection.cursor()
            self._logger.log_info(
                f'Connection to PostgreSQL established. Database: "{self._connection_model.database}"'
            )
        except Exception as e:
            self._logger.log_error(f"Error connecting to PostgreSQL: {e}")
            raise ValueError(f"Error executing query: {e}")

    def disconnect(self) -> DatabaseExecutionStatus:
        """Close the connection to the PostgreSQL database."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        self._logger.log_info(
            f'PostgreSQL connection to database "{self._connection_model.database}" closed.'
        )

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

    def change_database(self, database_name_to_select: str):
        """Change the current database."""
        try:
            self.disconnect()
            self._connection_model.database = database_name_to_select
            self.connect(self._connection_model)
            self._logger.log_info(
                f'Changed current database to: "{self._connection_model.database}"'
            )
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            self._logger.log_error(f"Error changing database: {e}")
            return DatabaseExecutionStatus.ERROR

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
                columns = ", ".join([col.column_name for col in record.data_model_list])
                values = ", ".join(
                    [
                        (
                            f"'{col.value}'"
                            if isinstance(col.value, str)
                            else str(col.value)
                        )
                        for col in record.data_model_list
                    ]
                )
                query = f"""
INSERT INTO 
    {schema_name}.{table_name} ({columns})
VALUES
    ({values})
"""
                self.execute_query(query)
            self._logger.log_info(
                f'Insert {len(records)} record(s) into table "{schema_name}.{table_name}" successfully.'
            )
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            self._logger.log_error(f"Error inserting records: {e}")
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
                    for col in update_record.data_model_list
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
            self._logger.log_info(
                f'Updated {number_of_records_updated} records in table "{schema_name}.{table_name}" successfully.'
            )
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            self._logger.log_error(f"Error updating records: {e}")
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
            self._logger.log_info(
                f'Delete {number_of_records_updated} records in table "{schema_name}.{table_name}" successfully.'
            )
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            self._logger.log_error(f"Error deleting records: {e}")
            return DatabaseExecutionStatus.ERROR

    def select(
        self,
        schema_name: str,
        table_name: str,
        columns: List[str] = None,
        join_model: JoinModel = None,
        conditions: List[Condition] = None,
    ) -> List:
        """Select records from a table."""
        try:
            if not isinstance(columns, List):
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

            query = f"""
SELECT
    {columns_clause}
FROM
    {schema_name}.{table_name} {f"\n    {join_clause}" if join_clause else ""}
{where_clause}
"""
            self.execute_query(query)
            results = self.fetch_result()
            self._logger.log_info(
                f'Selected {len(results)} records from table "{schema_name}.{table_name}" successfully.'
            )
            return results
        except Exception as e:
            self._logger.log_error(f"Error selecting records: {e}")
            return []
