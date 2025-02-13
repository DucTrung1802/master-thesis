from dataclasses import dataclass
from typing import List
from .enums import *


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

    @classmethod
    def RAW(cls):
        return "RAW"


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


@dataclass
class JoinCombination:
    join_type: JoinType
    table_left: str
    table_right: str
    column_left: str
    column_right: str


@dataclass
class JoinModel:
    database: str
    table: str
    join_combination_list: List[JoinCombination]


@dataclass
class ColumnToUpdate:
    target_column: str
    source_column: str = None
    value: str | int | float = None  # Need to upgrade to avoid SQL injection
    dataType: DataType = None


@dataclass
class ActionInMerge:
    pass


@dataclass
class InsertInMerge(ActionInMerge):
    column_to_update_list: List[ColumnToUpdate]


@dataclass
class UpdateInMerge(ActionInMerge):
    column_to_update_list: List[ColumnToUpdate]


@dataclass
class DeleteInMerge(ActionInMerge):
    pass
