import pyodbc
from dataclasses import dataclass


@dataclass
class SqlServerAuthentication:
    server: str
    login: str
    password: str
    driver: str = "{ODBC Driver 17 for SQL Server}"
    database: str = ""


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

        return result[0][0] == "True"

    def create_new_database(self, new_database_name: str):
        if self.check_database_exists(new_database_name):
            print(f'Database "{new_database_name}" already exists.')
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


def main():
    sql_server_authentication = SqlServerAuthentication(
        
    )
    sql_server_driver = SqlServerDriver(sql_server_authentication)

    sql_server_driver.create_new_database("SSI_STOCKS")


if __name__ == "__main__":
    main()
