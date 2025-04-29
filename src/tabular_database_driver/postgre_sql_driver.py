import psycopg2
from typing import List

from logger.logger import Logger
from tabular_database_driver.tabular_database_driver_interface import (
    TabularDatabaseDriverInterface,
)
from models.tabular_database_driver_models.postgre_sql_connection_model import (
    PostgreSQLConnectionModel,
)
from models.tabular_database_driver_models.tabular_database_driver_models import (
    Column,
    ForeignKey,
)
from utils.enums import DatabaseExecutionStatus


class PostgreSQLDriver(TabularDatabaseDriverInterface):
    def __init__(self, logger: Logger):
        self.logger = logger
        self.connection = None
        self.cursor = None
        self._current_database = "postgres"

    def connect(
        self, connection_model: PostgreSQLConnectionModel
    ) -> DatabaseExecutionStatus:
        """Establish a connection to the PostgreSQL database."""
        try:
            if connection_model.database:
                self._current_database = connection_model.database

            self.connection = psycopg2.connect(
                host=connection_model.host,
                user=connection_model.user,
                password=connection_model.password,
                port=connection_model.port,
                database=self._current_database,
            )

            # Set autocommit to True to allow database creation without a transaction block
            self.connection.autocommit = True

            self.cursor = self.connection.cursor()
            self.logger.log_info(
                f'Connection to PostgreSQL established. Database: "{self._current_database}"'
            )
        except Exception as e:
            self.logger.log_error(f"Error connecting to PostgreSQL: {e}")
            raise ValueError(f"Error executing query: {e}")

    def disconnect(self) -> DatabaseExecutionStatus:
        """Close the connection to the PostgreSQL database."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.log_info("PostgreSQL connection closed.")

    def create_database(self, database_name: str) -> DatabaseExecutionStatus:
        """Create a new database in PostgreSQL."""
        try:
            query = f"CREATE DATABASE {database_name};"
            self.cursor.execute(query)
            self.logger.log_info(f'Database "{database_name}" created successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self.logger.log_warning(f'Database "{database_name}" already exists.')
                return DatabaseExecutionStatus.ALREADY_EXISTS
            else:
                self.logger.log_error(f"Error creating database: {e}")
                return DatabaseExecutionStatus.ERROR

    def drop_database(self, database_name: str):
        """Drop an existing database in PostgreSQL."""
        try:
            query = f"DROP DATABASE {database_name};"
            self.cursor.execute(query)
            self.logger.log_info(f'Database "{database_name}" dropped successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self.logger.log_warning(f'Database "{database_name}" does not exist.')
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            else:
                self.logger.log_error(f"Error dropping database: {e}")
                return DatabaseExecutionStatus.ERROR

    def create_schema(self, schema_name: str):
        """Create a new schema in the current database."""
        try:
            query = f"CREATE SCHEMA {schema_name};"
            self.cursor.execute(query)
            self.logger.log_info(f'Schema "{schema_name}" created successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self.logger.log_warning(f'Schema "{schema_name}" already exists.')
                return DatabaseExecutionStatus.ALREADY_EXISTS
            else:
                self.logger.log_error(f"Error creating schema: {e}")
                return DatabaseExecutionStatus.ERROR

    def drop_schema(self, schema_name: str):
        """Drop an existing schema in the current database."""
        try:
            query = f"DROP SCHEMA {schema_name};"
            self.cursor.execute(query)
            self.logger.log_info(f'Schema "{schema_name}" dropped successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self.logger.log_warning(f'Schema "{schema_name}" does not exist.')
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            elif "other objects" in message:
                self.logger.log_warning(
                    f'Schema "{schema_name}" contains other objects. Cannot be dropped.'
                )
                return DatabaseExecutionStatus.OTHER_OBJECT_DEPEND
            else:
                self.logger.log_error(f"Error dropping schema: {e}")
                return DatabaseExecutionStatus.ERROR

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        columns: List[Column],
        key_column_name: str,
        foreign_keys: List[ForeignKey] = None,
    ):
        """Create a new table in the current database."""
        try:
            column_definitions = ", ".join(
                [
                    f"{col.name} {col.data_type} {"NOT NULL" if not col.nullable else ""}"
                    for col in columns
                ]
            )
            primary_key_definition = f"PRIMARY KEY ({key_column_name})"
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
                );
            """
            self.cursor.execute(query)
            self.logger.log_info(f'Table "{table_name}" created successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "already exists" in message:
                self.logger.log_warning(f'Table "{table_name}" already exists.')
                return DatabaseExecutionStatus.ALREADY_EXISTS
            else:
                self.logger.log_error(f"Error creating table: {e}")
                return DatabaseExecutionStatus.ERROR

    def drop_table(self, schema_name: str, table_name: str):
        """Drop an existing table in the current database."""
        try:
            query = f"DROP TABLE {schema_name}.{table_name};"
            self.cursor.execute(query)
            self.logger.log_info(f'Table "{table_name}" dropped successfully.')
            return DatabaseExecutionStatus.SUCCESS
        except Exception as e:
            message = str(e)
            if "does not exist" in message:
                self.logger.log_warning(f'Table "{table_name}" does not exist.')
                return DatabaseExecutionStatus.DOES_NOT_EXIST
            else:
                self.logger.log_error(f"Error dropping table: {e}")
                return DatabaseExecutionStatus.ERROR

    # def fetch_results(self) -> list:
    #     """Fetch results from the last executed query."""
    #     try:
    #         return self.cursor.fetchall()
    #     except Exception as e:
    #         self.logger.log_error(f"Error fetching results: {e}")
    #         return []
