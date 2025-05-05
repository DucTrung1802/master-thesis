from enum import Enum
from dataclasses import dataclass
from typing import Dict, Type, Union, Tuple

from utils.constants import *


class DatabaseExecutionStatus(Enum):
    """
    Enum for representing the status of a database query.
    """

    SUCCESS = "success"
    ALREADY_EXISTS = "already_exists"
    DOES_NOT_EXIST = "does_not_exist"
    OTHER_OBJECT_DEPEND = "other_object_depend"

    ERROR = "error"


class SqlOperator(Enum):
    """
    SqlOperator is an enumeration that defines various SQL operators as constants.
    These operators can be used to construct SQL queries programmatically.
    """

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
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    SOME = "SOME"
    ANY = "ANY"
    BETWEEN = "BETWEEN"
    IS = "IS"
    IS_NOT = "IS NOT"


class SqlJoinType(Enum):
    """
    An enumeration representing different types of SQL join operations.

    Attributes:
        INNER_JOIN (str): Represents an inner join, which returns rows when there is a match in both tables.
        LEFT_OUTER_JOIN (str): Represents a left outer join, which returns all rows from the left table and the matched rows from the right table.
        RIGHT_OUTER_JOIN (str): Represents a right outer join, which returns all rows from the right table and the matched rows from the left table.
        FULL_OUTER_JOIN (str): Represents a full outer join, which returns all rows when there is a match in either table.
        CROSS_JOIN (str): Represents a cross join, which returns the Cartesian product of the two tables.
    """

    INNER_JOIN = "INNER JOIN"
    LEFT_OUTER_JOIN = "LEFT OUTER JOIN"
    RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN"
    FULL_OUTER_JOIN = "FULL OUTER JOIN"
    CROSS_JOIN = "CROSS JOIN"


class FileExtension(Enum):
    CSV = "csv"
    TXT = "txt"
    LOG = "log"
    JSON = "json"
    XML = "xml"
    ZIP = "zip"
    PDF = "pdf"
    XLSX = "xlsx"
    DOCX = "docx"
    PNG = "png"
    JPG = "jpg"
    MP4 = "mp4"


# Enum for Main Scraping Types
class ScrapeMainType(Enum):
    MACROECONOMICS = "macroeconomics"
    STOCK_MARKET = "stock_market"
    ENTERPRISE = "enterprise"


# ================================================


# Enums for Macroeconomics, Stock Market, and Enterprise Subtypes
class MacroeconomicsSubType(Enum):
    GDP = "gdp"
    CPI = "cpi"
    EXCHANGE_RATE = "exchange_rate"
    INTEREST_RATE = "interest_rate"
    EXPORT_IMPORT = "export_import"
    IPI = "ipi"
    FDI = "fdi"
    M2 = "m2"
    RETAIL = "retail"
    POPULATION_UNEMPLOYMENT = "population_unemployment"


class StockMarketSubType(Enum):
    VN_HNX_INDEX = "vn_hnx_index"
    VN30_INDEX = "vn30_index"
    VN100_INDEX = "vn100_index"
    HNX30_INDEX = "hnx30_index"
    UPCOM_INDEX = "upcom_index"


class EnterpriseSubType(Enum):
    FINANCE_INFO = "finance_info"
    DAILY_PRICE = "daily_price"


ScrapeSubType = Union[
    MacroeconomicsSubType,
    StockMarketSubType,
    EnterpriseSubType,
]

# ================================================


# Enum for sources
class GdpSource(Enum):
    VIETSTOCK = "vietstock"
    WORLDOMETER = "worldometer"


class CpiSource(Enum):
    VIETSTOCK = "vietstock"


Source = Union[GdpSource, CpiSource]

# ================================================


@dataclass(frozen=True)
class SourceInfo:
    url: str


SCRAPE_MAPPING: Dict[Tuple[ScrapeMainType, ScrapeSubType, Source], SourceInfo] = {
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.GDP,
        GdpSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/43/thu-nhap.htm",
    ),
}
