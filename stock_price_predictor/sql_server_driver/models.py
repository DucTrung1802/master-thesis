from dataclasses import dataclass
from enum import Enum
from typing import List


from enum import Enum


class Operator(Enum):
    EQUAL_TO = "="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL_TO = ">="
    LESS_THAN_OR_EQUAL_TO = "<="
    NOT_EQUAL_TO = "<>"
    ALL = "ALL"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    EXISTS = "EXISTS"
    IN = "IN"
    LIKE = "LIKE"
    SOME = "SOME"
    ANY = "ANY"
    BETWEEN = "BETWEEN"


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


@dataclass
class DataModel:
    columnName: str
    value: str | int | float
    dataType: DataType


@dataclass
class Record:
    dataModelList: List[DataModel]


@dataclass
class Condition:
    column: str
    operator: Operator
    value: str | int | float  # Need to upgrade to avoid SQL injection
    dataType: DataType
