from dataclasses import dataclass
from typing import List


@dataclass
class DataType:

    @classmethod
    def INT(cls):
        return "INT"

    @classmethod
    def VARCHAR(cls, length: int):
        return f"VARCHAR({length})"

    @classmethod
    def TEXT(cls):
        return "TEXT"

    @classmethod
    def TIMESTAMP(cls):
        return "TIMESTAMP"

    @classmethod
    def DECIMAL(cls, precision: int, scale: int):
        return f"DECIMAL({precision}, {scale})"

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
