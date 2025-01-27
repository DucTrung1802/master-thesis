import pyodbc
from typing import List, Tuple

from .relational_database_driver import RelationalDatabaseDriver
from .model import *
from ..logger.logger import Logger


class SqlServerDriver(RelationalDatabaseDriver):

    def __init__(self, _logger: Logger):
        self._logger = _logger

    # region Public methods

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

            return True

        except Exception as e:
            print(f"Error connecting to SQL Server: {e}.")
            self._logger.log_error(f"Error connecting to SQL Server: {e}.")

            return False

    def close_connection(self):
        if "connection" in locals():
            self._connection.close()

            print("\nClosed connection to SQL Server.")
            self._logger.log_info(
                f"Closed connection to SQL Server.",
            )

    def set_current_database(self, current_database: str):
        self._current_database = current_database
        self._logger.log_info(
            f"Set current database name as {current_database}",
        )

    def get_current_database(self):
        return self._current_database

    def check_database_exist(self, database_name: str):
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

    def check_table_exist(self, database_name: str, table_name: str):
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

    def create_database(self, database_name: str):
        if self.check_database_exist(database_name):
            print(
                f"Database [{database_name}] already exists.",
            )
            self._logger.log_info(
                f"Database [{database_name}] already exists.",
            )
            return False

        query = f"""
            CREATE DATABASE [{database_name}];
        """
        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        query = f"""
            USE [{database_name}];
        """
        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return

        self._current_database = database_name

        print(
            f"Created database with name [{database_name}].",
        )
        self._logger.log_info(
            f"Created database with name [{database_name}].",
        )

        return True

    def create_table(
        self,
        database_name: str,
        table_name: str,
        columns: List[Column],
        key_column_name: str,
        foreign_keys: List[ForeignKey] = None,
    ):
        # Check whether database already exists
        if not self.check_database_exist(database_name):
            print(
                f"Datbase [{database_name}] does not exist yet. Cannot CREATE table.",
            )
            self._logger.log_info(
                f"Datbase [{database_name}] does not exist yet. Cannot CREATE table.",
            )
            return False

        # Check whether table already exists
        if self.check_table_exist(database_name=database_name, table_name=table_name):
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
                if not self.check_table_exist(
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
            return False

        print(
            f"Table [{database_name}].[dbo].[{table_name}] is created successfully.",
        )
        self._logger.log_info(
            f"Table [{database_name}].[dbo].[{table_name}] is created successfully.",
        )

        return True

    def truncate_table(self, database_name: str, table_name: str):
        if not self.check_database_exist(database_name):
            print(
                f"Cannot TRUNCATE table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            self._logger.log_error(
                f"Cannot TRUNCATE table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            return False

        if not self.check_table_exist(database_name, table_name):
            print(
                f"Cannot TRUNCATE table [{database_name}].[dbo].[{table_name}] since table does not exist."
            )
            self._logger.log_error(
                f"Cannot TRUNCATE table [{database_name}].[dbo].[{table_name}] since table does not exist."
            )
            return False

        query = f"""TRUNCATE TABLE [{database_name}].[dbo].[{table_name}]"""
        if not self._execute_query(query):
            return

    def drop_table(self, database_name: str, table_name: str):
        if not self.check_database_exist(database_name):
            print(
                f"Cannot DROP table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            self._logger.log_error(
                f"Cannot DROP table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            return False

        if not self.check_table_exist(database_name, table_name):
            print(
                f"Cannot DROP table [{database_name}].[dbo].[{table_name}] since table does not exist."
            )
            self._logger.log_error(
                f"Cannot DROP table [{database_name}].[dbo].[{table_name}] since table does not exist."
            )
            return False

        query = f"""DROP TABLE [{database_name}].[dbo].[{table_name}]"""
        if not self._execute_query(query):
            return

    def select(
        self,
        database_name: str,
        table_name: str,
        columns: List[str] = None,
        limit: int = None,
        condition_list: List[Condition] = None,
    ) -> List[Tuple]:
        # Validate the database
        if not self.check_database_exist(database_name):
            print(
                f"Cannot SELECT from table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            self._logger.log_warning(
                f"Cannot SELECT from table [{database_name}].[dbo].[{table_name}] since database [{database_name}] does not exist."
            )
            return None

        # Validate the table
        if not self.check_table_exist(database_name, table_name):
            print(
                f"Cannot SELECT from table [{database_name}].[dbo].[{table_name}] since table [{database_name}].[dbo].[{table_name}] does not exist."
            )
            self._logger.log_warning(
                f"Cannot SELECT from table [{database_name}].[dbo].[{table_name}] since table [{database_name}].[dbo].[{table_name}] does not exist."
            )
            return None

        # Validate columns to be retrieved
        if columns and (not isinstance(columns, List) or len(columns) < 1):
            print(
                f"Invalid columns to SELECT from table [{database_name}].[dbo].[{table_name}]. 'columns' value will not be applied."
            )
            self._logger.log_warning(
                f"Invalid columns to SELECT from table [{database_name}].[dbo].[{table_name}]. 'columns' value will not be applied."
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

        condition_query = self._add_condition(condition_list=condition_list)
        if condition_query:
            query += condition_query

        self._cursor.fetchall()

        try:
            if not self._execute_query(query):
                return

            result = self._cursor.fetchall()

            print(
                f"Successfully SELECT from table '[{database_name}].[dbo].[{table_name}]'. Retrieved {len(result)} records."
            )
            self._logger.log_info(
                f"Successfully SELECT from table '[{database_name}].[dbo].[{table_name}]'. Retrieved {len(result)} records."
            )

            return result

        except Exception as e:
            print(
                f"Cannot SELECT with columns: {columns} from table '[{database_name}].[dbo].[{table_name}]'\nError: {e}."
            )
            self._logger.log_error(
                f"Cannot SELECT with columns: {columns} from table '[{database_name}].[dbo].[{table_name}]'\nError: {e}."
            )

            return None

    def insert(self, database_name: str, table_name: str, records: List[Record]):
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
        if not self.check_database_exist(database_name):
            print(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            self._logger.log_info(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            return False

        # Check whether table already exists
        if not self.check_table_exist(
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
    {", ".join(self._format_value(data_model.value, data_model.dataType) for data_model in record.dataModelList)}
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
            return False

        print(
            f"Inserted {len(records)} records into table [{database_name}].[dbo].[{table_name}]",
        )
        self._logger.log_info(
            f"Inserted {len(records)} records into table [{database_name}].[dbo].[{table_name}]",
        )

        return True

    def update(
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
        if not self.check_database_exist(database_name):
            print(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            self._logger.log_info(
                f"Datbase [{database_name}] does not exist yet. Cannot insert data.",
            )
            return False

        # Check whether table already exists
        if not self.check_table_exist(
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
SET {",\n\t".join(f"{data_model.columnName} = {self._format_value(data_model.value, data_model.dataType)}" for data_model in record.dataModelList)}
"""

        join_query = self._add_join(current_query=query, join_model=join_model)
        if join_query:
            query += join_query

        condition_query = self._add_condition(condition_list=condition_list)
        if condition_query:
            query += condition_query

        self._logger.log_debug(f"\n{query}")

        return self._execute_query(query)

    def delete(
        self,
        database_name: str,
        table_name: str,
        condition_list: List[Condition],
    ):
        if not self.check_database_exist(database_name):
            print(
                f"Cannot purge table '{table_name}' since database {database_name} does not exist."
            )
            self._logger.log_warning(
                f"Cannot purge table '{table_name}' since database {database_name} does not exist."
            )
            return False

        if not self.check_table_exist(database_name, table_name):
            print(
                f"Cannot purge table '{table_name}' since table {table_name} does not exist."
            )
            self._logger.log_warning(
                f"Cannot purge table '{table_name}' since table {table_name} does not exist."
            )
            return False

        query = f"DELETE FROM [{database_name}].[dbo].[{table_name}]"

        conditions = self._add_condition(condition_list)

        if conditions:
            query += conditions

        if not self._execute_query(query):
            return False

        print(f"Successfully purge data from table '{table_name}'.")
        self._logger.log_info(f"Successfully purge data from table '{table_name}'.")

        return True

    def merge(
        self,
        database_name: str,
        source_table: str,
        target_table: str,
        matching_column: str,
        action_when_match: ActionInMerge,
        action_when_not_match_by_target: ActionInMerge,
        action_when_not_match_by_source: ActionInMerge,
    ):
        if not self.check_database_exist(database_name):
            print(
                f"Cannot merge table [{database_name}].[dbo].[{source_table}] to [{database_name}].[dbo].[{target_table}] since database [{database_name}] does not exist."
            )
            self._logger.log_error(
                f"Cannot merge table [{database_name}].[dbo].[{source_table}] to [{database_name}].[dbo].[{target_table}] since database [{database_name}] does not exist."
            )
            return False

        if not self.check_table_exist(database_name, source_table):
            print(
                f"Cannot merge table [{database_name}].[dbo].[{source_table}] to [{database_name}].[dbo].[{target_table}] since source table does not exist."
            )
            self._logger.log_error(
                f"Cannot merge table [{database_name}].[dbo].[{source_table}] to [{database_name}].[dbo].[{target_table}] since source table does not exist."
            )
            return False

        if not self.check_table_exist(database_name, target_table):
            print(
                f"Cannot merge table [{database_name}].[dbo].[{source_table}] to [{database_name}].[dbo].[{target_table}] since target table does not exist."
            )
            self._logger.log_error(
                f"Cannot merge table [{database_name}].[dbo].[{source_table}] to [{database_name}].[dbo].[{target_table}] since target table does not exist."
            )
            return False

        parsed_action_when_match = self._parse_action_when_merge(action_when_match)

        parsed_action_when_not_match_by_target = self._parse_action_when_merge(
            action_when_not_match_by_target
        )

        parsed_action_when_not_match_by_source = self._parse_action_when_merge(
            action_when_not_match_by_source
        )

        if not (
            parsed_action_when_match
            or parsed_action_when_not_match_by_target
            or parsed_action_when_not_match_by_source
        ):
            print(
                f"This merge query from table [{database_name}].[dbo].[{source_table}] to table [{database_name}].[dbo].[{target_table}] does nothing. Return immediately."
            )
            self._logger.log_warning(
                f"This merge query from table [{database_name}].[dbo].[{source_table}] to table [{database_name}].[dbo].[{target_table}] does nothing. Return immediately."
            )
            return False

        query = f"""MERGE INTO [{database_name}].[dbo].[{target_table}] AS T
USING [{database_name}].[dbo].[{source_table}] AS S
ON (T.{matching_column} = S.{matching_column})
WHEN MATCHED THEN
    {parsed_action_when_match}
WHEN NOT MATCHED BY TARGET THEN
    {parsed_action_when_not_match_by_target}
WHEN NOT MATCHED BY SOURCE THEN
    {parsed_action_when_not_match_by_source};
"""

        self._logger.log_debug(f"\n{query}")
        if not self._execute_query(query):
            return False

        print(
            f"\nMerged table [{database_name}].[dbo].[{source_table}] to table [{database_name}].[dbo].[{target_table}].",
        )
        self._logger.log_info(
            f"Merged table [{database_name}].[dbo].[{source_table}] to table [{database_name}].[dbo].[{target_table}].",
        )

        return True

    def begin_transaction(self) -> bool:
        pass

    def commit_transaction(self) -> bool:
        pass

    def rollback_transaction(self) -> bool:
        pass

    # endregion

    # region Private methods

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

    def _format_value(self, value, data_type: DataType):
        """Format value based on its data type for SQL query."""
        if data_type in [DataType.NVARCHAR]:
            return f"N'{value.replace("'", "''") if value else value}'"
        elif data_type in [DataType.DATETIME]:
            return f"CAST ('{value}' AS DATETIME)"
        return str(value)

    def _add_join(self, join_model: JoinModel) -> str:

        if not join_model:
            print(f"Invalid join model.")
            self._logger.log_error(f"Invalid join model.")
            return None

        # Check database existence
        if not self.check_database_exist(join_model.database):
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

            left_table_exist = self.check_table_exist(
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

            right_table_exist = self.check_table_exist(
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
        query = f"\nFROM {database}.dbo.{table}\n"
        query += "\n".join(
            f"{join_combination.join_type.value} JOIN {database}.dbo.{join_combination.table_right} "
            f"ON {join_combination.table_left}.{join_combination.column_left} = "
            f"{join_combination.table_right}.{join_combination.column_right}"
            for join_combination in join_model.join_combination_list
        )

        return query

    def _add_condition(self, condition_list: List[Condition]):
        if condition_list and isinstance(condition_list, Condition):
            condition_list = [condition_list]

        if (
            condition_list
            and isinstance(condition_list, List)
            and len(condition_list) > 0
        ):
            query = f"""\nWHERE {" AND ".join(f"{condition.column} {condition.operator.value} {self._format_value(condition.value, condition.dataType)}" for condition in condition_list)}"""
            return query

        return None

    def _parse_action_when_merge(self, action: ActionInMerge):
        if not action:
            print("Invalid action.")
            self._logger.log_error("Invalid action.")
            return None

        if isinstance(action, InsertInMerge):
            return f"""INSERT ({", ".join([column_pair.target_column for column_pair in action.column_to_update_list])})
    VALUES ({", ".join([f"S.{column_pair.source_column}" if column_pair.source_column else self._format_value(column_pair.value, column_pair.dataType) for column_pair in action.column_to_update_list])})"""

        elif isinstance(action, UpdateInMerge):
            return f"UPDATE SET {", ".join([f"T.{column_pair.target_column} = S.{column_pair.source_column}" if column_pair.source_column else f"T.{column_pair.target_column} = {self._format_value(column_pair.value, column_pair.dataType)}" for column_pair in action.column_to_update_list])}"

        elif isinstance(action, DeleteInMerge):
            return "DELETE"

        else:
            print(
                f"Invalid action type: {type(action)}. Must be InsertInMerge or UpdateInMerge or DeleteInMerge."
            )
            self._logger.log_error(
                f"Invalid action type: {type(action)}. Must be InsertInMerge or UpdateInMerge or DeleteInMerge."
            )
            return None

    # endregion
