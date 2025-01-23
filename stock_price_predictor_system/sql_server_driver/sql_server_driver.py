import datetime
import pyodbc
from typing import List, Tuple
from .models import *
from ..logger.logger import Logger


class SqlServerDriver:

    def __init__(self, _logger: Logger):
        self._logger = _logger
        self._connected = False

    def get_connection_status(self):
        return self._connected

    def open_connection(self, _authentication: SqlServerAuthentication):
        self._authentication = _authentication

        try:
            print("\nStart to connect to SQL Server.")
            self._logger.log_info(f"Start to connect to SQL Server.")

            _connection_string = f"DRIVER={self._authentication.driver};SERVER={self._authentication.server};DATABASE={self._authentication.database};UID={self._authentication.login};PWD={self._authentication.password}"
            self._connection = pyodbc.connect(
                _connection_string, autocommit=True, timeout=10
            )
            self._cursor = self._connection.cursor()
            self._current_database = None

            print("Connected to SQL Server.")
            self._logger.log_info(
                f"Connected to SQL Server.",
            )

            self._connected = True
            return True

        except Exception as e:
            print(f"Error connecting to SQL Server: {e}.")
            self._logger.log_error(f"Error connecting to SQL Server: {e}.")

            self._connected = False
            return False

    def close_connection(self):
        if "connection" in locals():
            self._connection.close()
            self._logger.log_info(
                f"Closed connection to SQL Server.",
            )

    def _execute_query(self, query: str):
        try:
            self._cursor.execute(query)
            return True

        except Exception as e:
            print(f"Error executing query:\n{query}")
            print(f"Error detail: {e}")
            self._logger.log_error(f"Error executing query:\n{query}")
            self._logger.log_error(f"Error detail: {e}")
            return False

    def set_current_database(self, current_database: str):
        self._current_database = current_database
        self._logger.log_info(
            f"Set current database name as {current_database}",
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
        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        result = self._cursor.fetchall()

        if result[0][0] == "True":
            return True
        else:
            return False

    def create_new_database(self, new_database_name: str):
        if self.check_database_exists(new_database_name):
            print(
                f"Database [{new_database_name}] already exists.",
            )
            self._logger.log_info(
                f"Database [{new_database_name}] already exists.",
            )
            return False

        query = f"""
            CREATE DATABASE [{new_database_name}];
        """
        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        query = f"""
            USE [{new_database_name}];
        """
        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        self._current_database = new_database_name

        print(
            f"Created database with name [{new_database_name}].",
        )
        self._logger.log_info(
            f"Created database with name [{new_database_name}].",
        )

        return True

    def check_table_exists(self, database_name: str, table_name: str):
        query = f"""
            SELECT CASE 
                WHEN OBJECT_ID('{database_name}.dbo.{table_name}', 'U') IS NOT NULL THEN 'True'
                ELSE 'False'
            END AS Result;
        """
        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

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
            print(
                f"Datbase [{database_name}] does not exist yet. Cannot create table.",
            )
            self._logger.log_info(
                f"Datbase [{database_name}] does not exist yet. Cannot create table.",
            )
            return False

        # Check whether table already exists
        if self.check_table_exists(database_name=database_name, table_name=table_name):
            print(
                f"Table [{database_name}].[dbo].[{table_name}] already exists.",
            )
            self._logger.log_info(
                f"Table [{database_name}].[dbo].[{table_name}] already exists.",
            )
            return False

        has_foreign_keys = False
        # Check whether foreign table already exists
        if foreign_keys and isinstance(foreign_keys, List) and len(foreign_keys) > 0:
            for foreign_key in foreign_keys:
                if not self.check_table_exists(
                    database_name=database_name, table_name=foreign_key.tableToRefer
                ):
                    print(
                        f"Foreign table [{database_name}].[dbo].[{foreign_key.tableToRefer}] does not exists. Cannot refer to it.",
                    )
                    self._logger.log_info(
                        f"Foreign table [{database_name}].[dbo].[{foreign_key.tableToRefer}] does not exists. Cannot refer to it.",
                    )
                    return False

            has_foreign_keys = True

        # Check if the key column exists in the provided columns
        key_column_exists = any(
            column.columnName == key_column_name for column in columns
        )
        if not key_column_exists:
            print(
                f"Key column '{key_column_name}' does not exist in the provided columns.",
            )
            self._logger.log_info(
                f"Key column '{key_column_name}' does not exist in the provided columns.",
            )
            return False

        # Separate the key column and other columns
        key_column, *other_columns = sorted(
            columns, key=lambda x: x.columnName != key_column_name
        )
        key_column_in_query = f"{key_column.columnName} {key_column.dataType} IDENTITY(1,1) PRIMARY KEY NOT NULL,"
        other_columns_in_query = ",\n\t".join(
            [
                f"{column.columnName} {column.dataType} {'NULL' if column.nullable else 'NOT NULL'}"
                for column in other_columns
            ]
        )

        if not has_foreign_keys:

            query = f"""USE {database_name};
            
CREATE TABLE {table_name} (
    {key_column_in_query}
    {other_columns_in_query}
);"""

        else:

            foreign_keys_in_query = ",\n\t".join(
                [
                    f"FOREIGN KEY ({foreign_key.name}) REFERENCES {foreign_key.tableToRefer}({foreign_key.columnToRefer})"
                    for foreign_key in foreign_keys
                ]
            )

            query = f"""USE {database_name};
            
CREATE TABLE {table_name} (
    {key_column_in_query}
    {other_columns_in_query}
    {foreign_keys_in_query}
);"""

        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        print(
            f"Table [{database_name}].[dbo].[{table_name}] is created successfully.",
        )
        self._logger.log_info(
            f"Table [{database_name}].[dbo].[{table_name}] is created successfully.",
        )

        return True

    def truncate_table(self, database_name: str, table_name: str):
        if not self.check_database_exists(database_name):
            print(
                f"Cannot truncate table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            self._logger.log_error(
                f"Cannot truncate table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            return False

        if not self.check_table_exists(database_name, table_name):
            print(
                f"Cannot truncate table [{database_name}].[dbo].[{table_name}] since table does not exist."
            )
            self._logger.log_error(
                f"Cannot truncate table [{database_name}].[dbo].[{table_name}] since table does not exist."
            )
            return False

        query = f"""TRUNCATE TABLE [{database_name}].[dbo].[{table_name}]"""
        if not self._execute_query(query):
            return

    def format_value(self, value, data_type: DataType):
        """Format value based on its data type for SQL query."""
        if data_type in [DataType.NVARCHAR]:
            return f"N'{value.replace("'", "''") if value else value}'"
        elif data_type in [DataType.DATETIME]:
            return f"CAST ('{value.replace(microsecond=0)}' AS DATETIME)"
        return str(value)

    def _add_join(self, join_model: JoinModel) -> str:

        if not join_model:
            print(f"Invalid join model.")
            self._logger.log_error(f"Invalid join model.")
            return None

        # Check database existence
        if not self.check_database_exists(join_model.database):
            print(f"Add join - Database [{join_model.database}] does not exist.")
            self._logger.log_error(
                f"Add join - Database [{join_model.database}] does not exist."
            )
            return None

        # Validat join combination list
        if (
            not join_model.join_combination_list
            or not isinstance(join_model.join_combination_list, List)
            or len(join_model.join_combination_list) <= 0
        ):
            print(f"Invalid join combination list. Found no join combinations.")
            self._logger.log_error(
                f"Invalid join combination list. Found no join combinations."
            )
            return None

        # Check tables existence
        for join_combination in join_model.join_combination_list:

            left_table_exist = self.check_table_exists(
                database_name=join_model.database,
                table_name=join_combination.table_left,
            )
            if not left_table_exist:
                print(
                    f"Add join - Table [{join_model.database}].[dbo].[{join_combination.table_left}] does not exist."
                )
                self._logger.log_error(
                    f"Add join - Table [{join_model.database}].[dbo].[{join_combination.table_left}] does not exist."
                )
                return None

            right_table_exist = self.check_table_exists(
                database_name=join_model.database,
                table_name=join_combination.table_right,
            )
            if not right_table_exist:
                print(
                    f"Add join - Table [{join_model.database}].[dbo].[{join_combination.table_right}] does not exist."
                )
                self._logger.log_error(
                    f"Add join - Table [{join_model.database}].[dbo].[{join_combination.table_rightF}] does not exist."
                )
                return None

        # Compose query
        database = join_model.database
        table = join_model.table
        query = f"FROM {database}.dbo.{table}\n"
        query += "\n".join(
            f"{join_combination.join_type.value} JOIN {database}.dbo.{join_combination.table_right} "
            f"ON {join_combination.table_left}.{join_combination.column_left} = "
            f"{join_combination.table_right}.{join_combination.column_right}"
            for join_combination in join_model.join_combination_list
        )

        return query

    def _add_condition(self, condition_list: List[Condition]):
        if (
            condition_list
            and isinstance(condition_list, List)
            and len(condition_list) > 0
        ):
            query += f"""WHERE {" AND ".join(f"{condition.column} {condition.operator.value} {self.format_value(condition.value, condition.dataType)}" for condition in condition_list)}"""
            return query

        return None

    def insert_data(self, database_name: str, table_name: str, records: List[Record]):
        # Check whether records has at least one record
        if not records or not isinstance(records, List) or not len(records) > 0:
            print(
                f"'Invalid data for 'records'.",
            )
            self._logger.log_warning(
                f"'Invalid data for 'records'.",
            )
            return False

        # Check whether database already exists
        if not self.check_database_exists(database_name):
            print(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            self._logger.log_info(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            return False

        # Check whether table already exists
        if not self.check_table_exists(
            database_name=database_name, table_name=table_name
        ):
            print(
                f"Table [{database_name}].[dbo].[{table_name}] does not exist yet. Cannot insert data",
            )
            self._logger.log_info(
                f"Table [{database_name}].[dbo].[{table_name}] does not exist yet. Cannot insert data",
            )
            return False

        column_names_in_query = ",\n    ".join(
            [f"[{data_model.columnName}]" for data_model in records[0].dataModelList]
        )

        new_values = ",\n".join(
            f"""(
    {", ".join(self.format_value(data_model.value, data_model.dataType) for data_model in record.dataModelList)}
)"""
            for record in records
        )

        query = f"""INSERT INTO [{database_name}].[dbo].[{table_name}]
(
    {column_names_in_query}
)
VALUES
{new_values}
"""

        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        print(
            f"Inserted {len(records)} records into table [{database_name}].[dbo].[{table_name}]",
        )
        self._logger.log_info(
            f"Inserted {len(records)} records into table [{database_name}].[dbo].[{table_name}]",
        )

    def _internal_update_data(
        self,
        database_name: str,
        table_name: str,
        record: Record,
        join_model: JoinModel = None,
        condition_list: List[Condition] = None,
    ):
        # Check whether records has at least one record
        if not record or not isinstance(record, Record):
            print(
                f"'Invalid data for 'record'.",
            )
            self._logger.log_warning(
                f"'Invalid data for 'record'.",
            )
            return False

        # Check whether database already exists
        if not self.check_database_exists(database_name):
            print(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            self._logger.log_info(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            return False

        # Check whether table already exists
        if not self.check_table_exists(
            database_name=database_name, table_name=table_name
        ):
            print(
                f"Table [{database_name}].[dbo].[{table_name}] does not exist yet. Cannot insert data",
            )
            self._logger.log_info(
                f"Table [{database_name}].[dbo].[{table_name}] does not exist yet. Cannot insert data",
            )
            return False

        query = f"""UPDATE [{database_name}].[dbo].[{table_name}]
SET {",\n\t".join(f"{data_model.columnName} = {self.format_value(data_model.value, data_model.dataType)}" for data_model in record.dataModelList)}
"""

        join_query = self._add_join(current_query=query, join_model=join_model)
        if join_query:
            query += join_query

        condition_query = self._add_condition(condition_list=condition_list)
        if condition_query:
            query += condition_query

        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

    def update_data(
        self,
        database_name: str,
        table_name: str,
        record: Record,
        join_model: JoinModel = None,
        condition_list: List[Condition] = None,
    ):
        self._internal_update_data(
            database_name=database_name,
            table_name=table_name,
            record=record,
            join_model=join_model,
            condition_list=condition_list,
        )

        print(
            f"Updated [{database_name}].[dbo].[{table_name}].",
        )
        self._logger.log_info(
            f"Updated [{database_name}].[dbo].[{table_name}].",
        )

    def detele_data(
        self,
        database_name: str,
        table_name: str,
        condition_list: List[Condition],
    ):
        record: Record = Record(
            [
                DataModel(
                    columnName="DeleteDate",
                    value=datetime.datetime.now(),
                    dataType=DataType.DATETIME,
                )
            ]
        )

        self._internal_update_data(
            database_name=database_name,
            table_name=table_name,
            record=record,
            condition_list=condition_list,
        )

        print(
            f"Mark 'Deleted' for some records in [{database_name}].[dbo].[{table_name}].",
        )
        self._logger.log_info(
            f"Mark 'Deleted' for some records in [{database_name}].[dbo].[{table_name}].",
        )

    def purge_data(
        self,
        database_name: str,
        table_name: str,
    ):
        if not self.check_database_exists(database_name):
            print(
                f"Cannot purge table '{table_name}' since database {database_name} does not exist."
            )
            self._logger.log_warning(
                f"Cannot purge table '{table_name}' since database {database_name} does not exist."
            )
            return False

        if not self.check_table_exists(database_name, table_name):
            print(
                f"Cannot purge table '{table_name}' since table {table_name} does not exist."
            )
            self._logger.log_warning(
                f"Cannot purge table '{table_name}' since table {table_name} does not exist."
            )
            return False

        query = f"DELETE FROM [{database_name}].[dbo].[{table_name}]"

        if not self._execute_query(query):
            return

        print(f"Successfully purge data from table '{table_name}'.")
        self._logger.log_info(f"Successfully purge data from table '{table_name}'.")

        return True

    def retrieve_data(
        self,
        database_name: str,
        table_name: str,
        columns: List[str] = None,
        limit: int = None,
    ) -> List[Tuple]:
        # region Validate inputs
        # Validate the database
        if not self.check_database_exists(database_name):
            print(
                f"Cannot retrieve data from table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            self._logger.log_warning(
                f"Cannot retrieve data from table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            return None

        # Validate the table
        if not self.check_table_exists(database_name, table_name):
            print(
                f"Cannot retrieve data from table [{database_name}].[dbo].[{table_name}] since table [{database_name}].[dbo].[{table_name}] does not exist."
            )
            self._logger.log_warning(
                f"Cannot retrieve data from table [{database_name}].[dbo].[{table_name}] since table [{database_name}].[dbo].[{table_name}] does not exist."
            )
            return None

        # Validate columns to be retrieved
        if columns and (not isinstance(columns, List) or len(columns) < 1):
            print(
                f"Invalid columns to retrieve data from table [{database_name}].[dbo].[{table_name}]. 'columns' value will not be applied."
            )
            self._logger.log_warning(
                f"Invalid columns to retrieve data from table [{database_name}].[dbo].[{table_name}]. 'columns' value will not be applied."
            )
            columns = None

        # Validate limit value
        if limit and (not isinstance(limit, int) or limit < 0):
            print(
                f"Invalid 'limit' value in SELECT query from table [{database_name}].[dbo].[{table_name}]. 'limit' value will not be applied."
            )
            self._logger.log_warning(
                f"Invalid 'limit' value in SELECT query from table [{database_name}].[dbo].[{table_name}]. 'limit' value will not be applied."
            )
            limit = None
        # endregion

        columns_string = ""
        if not columns:
            columns_string = "*"
        else:
            columns_string = ",".join(columns)

        query = ""
        if limit:
            query = f"SELECT TOP ({limit}) {columns_string} FROM [{database_name}].[dbo].[{table_name}]"

        else:
            query = (
                f"SELECT {columns_string} FROM [{database_name}].[dbo].[{table_name}]"
            )

        self._cursor.fetchall()

        try:
            if not self._execute_query(query):
                return

            result = self._cursor.fetchall()

            print(
                f"Successfully retrieved data from table '[{database_name}].[dbo].[{table_name}]'. Retrieved {len(result)} records."
            )
            self._logger.log_info(
                f"Successfully retrieved data from table '[{database_name}].[dbo].[{table_name}]'. Retrieved {len(result)} records."
            )

            return result

        except Exception as e:
            print(
                f"Cannot retrieve data with columns: {columns} from table '[{database_name}].[dbo].[{table_name}]'\nError: {e}."
            )
            self._logger.log_error(
                f"Cannot retrieve data with columns: {columns} from table '[{database_name}].[dbo].[{table_name}]'\nError: {e}."
            )

            return None
