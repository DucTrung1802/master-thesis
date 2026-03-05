from dataclasses import dataclass
from typing import List

from utils.enums import SqlOperator, SqlJoinType


@dataclass
class DataType:

    @classmethod
    def INT(cls):
        return "INT"

    @classmethod
    def VARCHAR(cls, length: int = None):
        if length is None:
            return "VARCHAR(255)"
        else:
            return f"VARCHAR({length})"

    @classmethod
    def TEXT(cls):
        return "TEXT"

    @classmethod
    def TIMESTAMP(cls):
        return "TIMESTAMP"

    @classmethod
    def AUTO_TIMESTAMP(cls):
        return "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"

    @classmethod
    def DECIMAL(cls, precision: int = None, scale: int = None):
        if precision and scale:
            return f"DECIMAL({precision}, {scale})"
        elif precision:
            return f"DECIMAL({precision})"
        else:
            return f"DECIMAL"

    @classmethod
    def BOOLEAN(cls):
        return "BOOLEAN"

    @classmethod
    def BLOB(cls):
        return "BLOB"

    @classmethod
    def DATE(cls):
        return "DATE"

    @classmethod
    def TIME(cls):
        return "TIME"

    @classmethod
    def FLOAT(cls):
        return "FLOAT"

    @classmethod
    def SERIAL(cls):
        return "SERIAL"

    @classmethod
    def BIGINT(cls):
        return "BIGINT"


@dataclass
class ForeignKey:
    column_name: str
    ref_table: str
    ref_column: str


@dataclass
class Column:
    name: str
    data_type: DataType
    nullable: bool


@dataclass
class DataModel:
    column_name: str
    value: str | int | float
    data_type: DataType = None


@dataclass
class Record:
    data_dto_list: List[DataModel]


@dataclass
class Condition:
    column: str
    operator: SqlOperator
    value: str | int | float  # Need to upgrade to avoid SQL injection
    data_type: DataType


@dataclass
class JoinModel:
    join_type: SqlJoinType
    schema_left: str
    schema_right: str
    table_left: str
    table_right: str
    column_left: str
    column_right: str
