import os
import pyodbc
from models import *
import inspect


class SqlServerDriver:

    def __init__(self, _authentication: SqlServerAuthentication):
        self._authentication = _authentication

        try:
            _connection_string = f"DRIVER={self._authentication.driver};SERVER={self._authentication.server};DATABASE={self._authentication.database};UID={self._authentication.login};PWD={self._authentication.password}"
            _connection = pyodbc.connect(_connection_string, autocommit=True)
            self._cursor = _connection.cursor()
            self._current_database = None
            print(f"Connected to SQL Server.")
        except Exception as e:
            print(f"Error connecting to SQL Server: {e}.")

    def set_current_database(self, current_database: str):
        self._current_database = current_database

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
            print(f'Database "{database_name}" already exists.')
            return True
        else:
            print(f'Database "{database_name}" does not exist yet.')
            return False

    def create_new_database(self, new_database_name: str):
        if self.check_database_exists(new_database_name):
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

        print(f'Created database with name "{new_database_name}".')

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
            print(f'Table "{database_name}.dbo.{table_name}" already exists.')
            return True
        else:
            print(f'Table "{database_name}.dbo.{table_name}" does not exist yet.')
            return False

    def create_table(
        self,
        database_name: str,
        table_name: str,
        columns: List[Column],
        key_column_name: str,
        foreignKeys: List[ForeignKey] = None,
    ):
        if not self.check_database_exists(database_name):
            print(f"Cannot create table.")
            return False

        if self.check_table_exists(database_name=database_name, table_name=table_name):
            return False

        # Check if the key column exists in the provided columns
        key_column_exists = any(
            column.columnName == key_column_name for column in columns
        )
        if not key_column_exists:
            print(
                f"Key column '{key_column_name}' does not exist in the provided columns."
            )
            return False

        # Separate the key column and other columns
        key_column, *other_columns = sorted(
            columns, key=lambda x: x.columnName != key_column_name
        )

        query = f"""USE {database_name};
        
CREATE TABLE {table_name} (
    {f"{key_column.columnName} {key_column.dataType} PRIMARY KEY NOT NULL,"}
    {',\n    '.join([f"{column.columnName} {column.dataType} {'NULL' if column.nullable else 'NOT NULL'}" for column in other_columns])}
);"""

        self._cursor.execute(query)
        print(f"Table '{database_name}.dbo.{table_name}' is created successfully.")

        return True


def main():

    sql_server_authentication = SqlServerAuthentication(
        server=os.getenv("sql_server_server"),
        login=os.getenv("sql_server_login"),
        password=os.getenv("sql_server_password"),
    )
    sql_server_driver = SqlServerDriver(sql_server_authentication)

    sql_server_driver.create_new_database("SSI_STOCKS")

    market_table_columns: List[Column] = [
        Column(columnName="ID", dataType=DataType.INT(), nullable=False),
        Column(columnName="Symbol", dataType=DataType.NVARCHAR(5), nullable=False),
        Column(columnName="Name", dataType=DataType.NVARCHAR(200), nullable=False),
        Column(columnName="EnName", dataType=DataType.NVARCHAR(200), nullable=False),
        Column(columnName="CreateDate", dataType=DataType.DATETIME(), nullable=False),
        Column(columnName="UpdateDate", dataType=DataType.DATETIME(), nullable=True),
        Column(columnName="DeleteDate", dataType=DataType.DATETIME(), nullable=True),
    ]

    sql_server_driver.create_table(
        database_name="SSI_STOCKS",
        table_name="Market",
        columns=market_table_columns,
        key_column_name="ID",
    )


if __name__ == "__main__":
    main()
