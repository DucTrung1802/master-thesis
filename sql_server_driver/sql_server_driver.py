import pyodbc
from .models import *
from logger.logger import Logger
from helper.helper import Helper


class SqlServerDriver(Helper):

    def __init__(self, _logger: Logger, _authentication: SqlServerAuthentication):
        self._logger = _logger
        self._authentication = _authentication

        try:
            self._logger.log_info(
                context=self.get_context(),
                messsage=f"Start to connect to SQL Server.",
            )

            _connection_string = f"DRIVER={self._authentication.driver};SERVER={self._authentication.server};DATABASE={self._authentication.database};UID={self._authentication.login};PWD={self._authentication.password}"
            self._connection = pyodbc.connect(_connection_string, autocommit=True)
            self._cursor = self._connection.cursor()
            self._current_database = None

            self._logger.log_info(
                context=self.get_context(),
                messsage=f"Connected to SQL Server.",
            )

        except Exception as e:
            self._logger.log_error(
                context=self.get_context(),
                messsage=f"Error connecting to SQL Server: {e}.",
            )

    def close_connection(self):
        if "connection" in locals():
            self._connection.close()
            self._logger.log_info(
                context=self.get_context(),
                messsage=f"Closed connection to SQL Server.",
            )

    def set_current_database(self, current_database: str):
        self._current_database = current_database
        self._logger.log_info(
            context=self.get_context(),
            messsage=f"Set current database name as {current_database}",
        )

    def get_current_database(self):
        return self._current_database

    def check_database_exists(self, database_name: str):
        query = f"""
            SELECT 
            CASE 
                WHEN EXISTS (SELECT 1 FROM sys.databases WHERE name = '{database_name}') 
                THEN 'True' 
                ELSE 'False' 
            END AS Result;
        """
        self._cursor.execute(query)

        result = self._cursor.fetchall()

        if result[0][0] == "True":
            return True
        else:
            return False

    def create_new_database(self, new_database_name: str):
        if self.check_database_exists(new_database_name):
            self._logger.log_info(
                context=self.get_context(),
                messsage=f'Table "{new_database_name}" already exists.',
            )
            return False

        query = f"""
            CREATE DATABASE [{new_database_name}];
        """
        self._cursor.execute(query)

        query = f"""
            USE [{new_database_name}];
        """
        self._cursor.execute(query)

        self._current_database = new_database_name

        self._logger.log_info(
            context=self.get_context(),
            messsage=f'Created database with name "{new_database_name}".',
        )

        return True

    def check_table_exists(self, database_name: str, table_name: str):
        query = f"""
            SELECT CASE 
                WHEN OBJECT_ID('{database_name}.dbo.{table_name}', 'U') IS NOT NULL THEN 'True'
                ELSE 'False'
            END AS Result;
        """
        self._cursor.execute(query)

        result = self._cursor.fetchall()

        if result[0][0] == "True":
            return True
        else:
            return False

    def create_table(
        self,
        database_name: str,
        table_name: str,
        columns: List[Column],
        key_column_name: str,
        foreign_keys: List[ForeignKey] = None,
    ):
        # Check whether database already exists
        if not self.check_database_exists(database_name):
            self._logger.log_info(
                context=self.get_context(),
                messsage=f'Datbase "{database_name}" does not exist yet. Cannot create table.',
            )
            return False

        # Check whether table already exists
        if self.check_table_exists(database_name=database_name, table_name=table_name):
            self._logger.log_info(
                context=self.get_context(),
                messsage=f'Table "{database_name}.dbo.{table_name}" already exists.',
            )
            return False

        has_foreign_keys = False
        # Check whether foreign table already exists
        if foreign_keys and isinstance(foreign_keys, List) and len(foreign_keys) > 0:
            for foreign_key in foreign_keys:
                if not self.check_table_exists(
                    database_name=database_name, table_name=foreign_key.tableToRefer
                ):
                    self._logger.log_info(
                        context=self.get_context(),
                        messsage=f'Foreign table "{database_name}.dbo.{foreign_key.tableToRefer}" does not exists. Cannot refer to it.',
                    )
                    return False

            has_foreign_keys = True

        # Check if the key column exists in the provided columns
        key_column_exists = any(
            column.columnName == key_column_name for column in columns
        )
        if not key_column_exists:
            self._logger.log_info(
                context=self.get_context(),
                messsage=f"Key column '{key_column_name}' does not exist in the provided columns.",
            )
            return False

        # Separate the key column and other columns
        key_column, *other_columns = sorted(
            columns, key=lambda x: x.columnName != key_column_name
        )

        if not has_foreign_keys:

            query = f"""USE {database_name};
            
CREATE TABLE {table_name} (
    {f"{key_column.columnName} {key_column.dataType} PRIMARY KEY NOT NULL,"}
    {',\n    '.join([f"{column.columnName} {column.dataType} {'NULL' if column.nullable else 'NOT NULL'}" for column in other_columns])}
);"""

        else:
            query = f"""USE {database_name};
            
CREATE TABLE {table_name} (
    {f"{key_column.columnName} {key_column.dataType} PRIMARY KEY NOT NULL,"}
    {',\n    '.join([f"{column.columnName} {column.dataType} {'NULL' if column.nullable else 'NOT NULL'}" for column in other_columns])}
    {',\n    '.join([f"FOREIGN KEY ({foreign_key.name}) REFERENCES {foreign_key.tableToRefer}({foreign_key.columnToRefer})" for foreign_key in foreign_keys])}
);"""

        self._cursor.execute(query)

        self._logger.log_info(
            context=self.get_context(),
            messsage=f"Table '{database_name}.dbo.{table_name}' is created successfully.",
        )

        return True
