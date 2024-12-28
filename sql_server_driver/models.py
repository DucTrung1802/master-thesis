from dataclasses import dataclass
from enum import Enum
from typing import List


@dataclass
class SqlServerAuthentication:
    server: str
    login: str
    password: str
    driver: str = "{ODBC Driver 17 for SQL Server}"
    database: str = ""


@dataclass
class DataType:

    @classmethod
    def INT(cls):
        return "INT"

    @classmethod
    def NVARCHAR(cls, length):
        return f"NVARCHAR({length})"

    @classmethod
    def BIGINT(cls):
        return "BIGINT"

    @classmethod
    def DATETIME(cls):
        return "DATETIME"


@dataclass
class ForeignKey:
    name: str
    tableToRefer: str
    columnToRefer: str


@dataclass
class Column:
    columnName: str
    dataType: DataType
    nullable: bool
