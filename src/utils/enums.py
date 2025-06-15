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
    EXPORT = "export"
    IMPORT = "import"
    IPI = "ipi"
    FDI = "fdi"
    M2 = "m2"
    RETAIL = "retail"
    POPULATION_UNEMPLOYMENT = "population_unemployment"
    GOLD_PRICE = "gold_price"
    OIL_PRICE = "oil_price"
    DOW_JONES = "dow_jones"


class StockMarketSubType(Enum):
    VN_HNX_INDEX = "vn_hnx_index"
    VN_30_INDEX = "vn30_index"
    VN_100_INDEX = "vn100_index"
    HNX_30_INDEX = "hnx30_index"
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


# MACROECONOMICS
class GdpSource(Enum):
    VIETSTOCK = "vietstock"
    WORLDOMETER = "worldometer"


class CpiSource(Enum):
    VIETSTOCK = "vietstock"


class ExchangeRateSource(Enum):
    VIETSTOCK = "vietstock"


class InterestRateSource(Enum):
    VIETSTOCK = "vietstock"


class ExportImportSource(Enum):
    VIETSTOCK = "vietstock"


class IpiSource(Enum):
    VIETSTOCK = "vietstock"


class FdiSource(Enum):
    VIETSTOCK = "vietstock"


class M2Source(Enum):
    VIETSTOCK = "vietstock"


class RetailSource(Enum):
    VIETSTOCK = "vietstock"


class PopulationUnemploymentSource(Enum):
    VIETSTOCK = "vietstock"


class GoldPriceSource(Enum):
    INVESTING = "investing"


class OilPriceSource(Enum):
    INVESTING = "investing"


class DowJonesSource(Enum):
    INVESTING = "investing"


# STOCK_MARKET
class VnHnxIndexSource(Enum):
    CAFEF = "cafef"


class Vn30IndexSource(Enum):
    CAFEF = "cafef"


class Vn100IndexSource(Enum):
    CAFEF = "cafef"


class Hnx30IndexSource(Enum):
    CAFEF = "cafef"


class UpcomIndexSource(Enum):
    CAFEF = "cafef"


# ENTERPRISE
class FinanceInfoSource(Enum):
    CAFEF = "cafef"


class DailyPriceSource(Enum):
    CAFEF = "cafef"


# Union of all sources
Source = Union[
    GdpSource,
    CpiSource,
    ExchangeRateSource,
    InterestRateSource,
    ExportImportSource,
    IpiSource,
    FdiSource,
    M2Source,
    RetailSource,
    PopulationUnemploymentSource,
    VnHnxIndexSource,
    Vn30IndexSource,
    Vn100IndexSource,
    Hnx30IndexSource,
    UpcomIndexSource,
    FinanceInfoSource,
    DailyPriceSource,
    GoldPriceSource,
    OilPriceSource,
    DowJonesSource
]

# ================================================


@dataclass(frozen=True)
class SourceInfo:
    url: str


SCRAPE_MAPPING: Dict[Tuple[ScrapeMainType, ScrapeSubType, Source], SourceInfo] = {
    # MACROECONOMICS
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.GDP,
        GdpSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/43/thu-nhap.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.GDP,
        GdpSource.WORLDOMETER,
    ): SourceInfo(
        url="https://www.worldometers.info/gdp/vietnam-gdp/",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.CPI,
        CpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/52/cpi.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.EXCHANGE_RATE,
        ExchangeRateSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/53-64/ty-gia-lai-suat.htm"
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.INTEREST_RATE,
        InterestRateSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/53-64/ty-gia-lai-suat.htm"
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.EXPORT,
        ExportImportSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/48-49/xuat-nhap-khau.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IMPORT,
        ExportImportSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/48-49/xuat-nhap-khau.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IPI,
        IpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/46/san-xuat-cong-nghiep.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.FDI,
        FdiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/50/fdi.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.M2,
        M2Source.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/51/tin-dung.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.RETAIL,
        RetailSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/47/ban-le.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.POPULATION_UNEMPLOYMENT,
        PopulationUnemploymentSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/55-56/dan-so-va-lao-dong.htm",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.GOLD_PRICE,
        GoldPriceSource.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/commodities/gold-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.OIL_PRICE,
        OilPriceSource.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/commodities/brent-oil-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.DOW_JONES,
        DowJonesSource.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/indices/us-30-historical-data",
    ),
    # STOCK_MARKET
    (
        ScrapeMainType.STOCK_MARKET,
        StockMarketSubType.VN_HNX_INDEX,
        VnHnxIndexSource.CAFEF,
    ): SourceInfo(url="https://cafef.vn/du-lieu/du-lieu-download.chn"),
    (
        ScrapeMainType.STOCK_MARKET,
        StockMarketSubType.VN_30_INDEX,
        Vn30IndexSource.CAFEF,
    ): SourceInfo(
        url="https://cafef.vn/du-lieu/lich-su-giao-dich-vn30index-1.chn#data",
    ),
    (
        ScrapeMainType.STOCK_MARKET,
        StockMarketSubType.VN_100_INDEX,
        Vn100IndexSource.CAFEF,
    ): SourceInfo(
        url="https://cafef.vn/du-lieu/lich-su-giao-dich-vn100-index-1.chn",
    ),
    (
        ScrapeMainType.STOCK_MARKET,
        StockMarketSubType.HNX_30_INDEX,
        Hnx30IndexSource.CAFEF,
    ): SourceInfo(
        url="https://cafef.vn/du-lieu/lich-su-giao-dich-hnx30-index-1.chn",
    ),
    (
        ScrapeMainType.STOCK_MARKET,
        StockMarketSubType.UPCOM_INDEX,
        UpcomIndexSource.CAFEF,
    ): SourceInfo(url="https://cafef.vn/du-lieu/lich-su-giao-dich-upcom-index-1.chn"),
    # ENTERPRISE
    (
        ScrapeMainType.ENTERPRISE,
        EnterpriseSubType.FINANCE_INFO,
        FinanceInfoSource.CAFEF,
    ): SourceInfo(url="https://cafef.vn/"),
    (
        ScrapeMainType.ENTERPRISE,
        EnterpriseSubType.DAILY_PRICE,
        DailyPriceSource.CAFEF,
    ): SourceInfo(url="https://cafef.vn/du-lieu/du-lieu-download.chn"),
}


class Schema(Enum):
    MACROECONOMICS = "macroeconomics"
    STOCK_MARKET = "stock_market"
    ENTERPRISE = "enterprise"


class Table:

    # MACROECONOMICS
    class GDP:
        class Column(Enum):
            YEAR = "year"
            QUARTER = "quarter"
            AGRICULTURE_SHARE = "agriculture_share"
            INDUSTRY_SHARE = "industry_share"
            SERVICE_SHARE = "service_share"
            GDP_TRUE_GROWTH_ACC = "gdp_true_growth_acc"
            AGRICULTURE_TRUE_GROWTH_ACC = "agriculture_true_growth_acc"
            INDUSTRY_TRUE_GROWTH_ACC = "industry_true_growth_acc"
            SERVICE_TRUE_GROWTH_ACC = "service_true_growth_acc"

        name = "gdp"
        primary_key = [Column.YEAR.value, Column.QUARTER.value]

    class CPI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            CPI = "cpi"
            FNB_SERVICES = "fnb_services"
            STAPLE_FOOD = "staple_food"
            FOOD = "food"
            FAFH = "fafh"
            DRINK_AND_TOBACO = "drink_and_tobaco"
            WEARING = "wearing"
            HOUSING_AND_BUILDING_MATERIALS = "housing_and_building_materials"
            HOUSEHOLD_APPLIANCES_AND_EQUIPMENT = "household_appliances_and_equipment"
            MEDICINES_AND_MEDICAL_SERVICES = "medicines_and_medical_services"
            TRAFFIC = "traffic"
            POST_AND_TELECOMMUNICATIONS = "post_and_telecommunications"
            EDUCATION = "education"
            CULTURE_ENTERTAINMENT_AND_TOURISM = "culture_entertainment_and_tourism"
            OTHER_SUPPLIES_AND_SERVICES = "other_supplies_and_services"

        name = "cpi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class EXCHANGE_RATE:
        class Column(Enum):
            DATE = "date"
            EXCHANGE_RATE = "exchange_rate"

        name = "exchange_rate"
        primary_key = [Column.DATE.value]

    class INTEREST_RATE:
        class Column(Enum):
            DATE = "date"
            ONE_WEEK = "one_week"
            TWO_WEEK = "two_week"
            ONE_MONTH = "one_month"
            THREE_MONTH = "three_month"
            SIX_MONTH = "six_month"
            NINE_MONTH = "nine_month"

        name = "interest_rate"
        primary_key = [Column.DATE.value]

    class EXPORT:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            TOTAL = "total"
            LEATHER_SHOES = "leather_shoes"
            TEXTILES = "textiles"
            WOOD_PRODUCTS = "wood_products"
            SEAFOOD = "seafood"
            CRUDE_OIL = "crude_oil"
            RICE = "rice"
            COFFEE = "coffee"
            COMPUTER_ELECTRONICS = "computer_electronics"
            MACHINERY_EQUIPMENT = "machinery_equipment"

        name = "export"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class IMPORT:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            TOTAL = "total"
            ELECTRONICS_COMPUTERS_COMPONENTS = "electronics_computers_components"
            MACHINERY_EQUIPMENT = "machinery_equipment"
            GASOLINE = "gasoline"
            CHEMICAL = "chemical"
            CHEMICAL_PRODUCTS = "chemical_products"
            IRON_STEEL = "iron_steel"
            FABRIC = "fabric"
            CAR = "car"
            ANIMAL_FEED = "animal_feed"

        name = "import"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class IPI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            TOTAL = "total"
            EXTRACTIVE = "extractive"
            PROCESSING_AND_MANUFACTURING_INDUSTRY = (
                "processing_and_manufacturing_industry"
            )
            ELECTRICITY_GENERATION_AND_DISTRIBUTION = (
                "electricity_generation_and_distribution"
            )
            WATER_SUPPLY_AND_WASTE_MANAGEMENT = "water_supply_and_waste_management"

        name = "ipi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class FDI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            REGISTERED = "registered"
            DISBURSEMENTED = "disbursemented"

        name = "fdi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class M2:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            CREDITS = "credits"
            M2_MONEY_SUPPLY = "m2_money_supply"
            CREDITS_GROWTH_YTD = "credits_growth_ytd"
            M2_MONEY_SUPPLY_GROWTH_YTD = "m2_money_supply_growth_ytd"

        name = "m2"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class RETAIL:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            TOTAL = "total"
            COMMERCIAL = "commercial"
            HOTEL_RESTAURANT = "hotel_restaurant"
            TOURISM = "tourism"
            SERVICE = "service"

        name = "retail"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class POPULATION_UNEMPLOYMENT:
        class Column(Enum):
            YEAR = "year"
            POPULATION = "population"
            POPULATION_DENSITY = "population_density"
            POPULATION_GROWTH_RATIO = "population_growth_ratio"
            URBAN_POPULATION_RATIO = "urban_population_ratio"
            LABOR_FORCE_COUNT = "labor_force_count"
            AGRICULTURE_FORESTRY_AND_FISHERIES = "agriculture_forestry_and_fisheries"
            INDUSTRY_AND_CONSTRUCTION = "industry_and_construction"
            SERVICE = "service"
            URBAN_UNEMPLOYED_COUNT = "urban_unemployed_count"
            LABOR_FORCE_GROWTH = "labor_force_growth"
            LABOR_FORCE_RATIO = "labor_force_ratio"
            MALE_RATIO = "male_ratio"
            FEMALE_RATIO = "female_ratio"
            URBAN_UNEMPLOYED_RATIO = "urban_unemployed_ratio"

        name = "population_unemployment"
        primary_key = [Column.YEAR.value]

    class GOLD_PRICE:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "gold_price"
        primary_key = [Column.DATE.value]

    class OIL_PRICE:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "oil_price"
        primary_key = [Column.DATE.value]

    class DOW_JONES:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "dow_jones"
        primary_key = [Column.DATE.value]

    # STOCK_MARKET
    class MARKET:
        class Column(Enum):
            ID = "id"
            CODE = "code"
            NAME = "name"
            CREATE_DATE = "create_date"
            UPDATE_DATE = "update_date"
            DELETE_DATE = "delete_date"

        name = "market"
        primary_key = [Column.ID.value]

    # ENTERPRISE
    class STOCK:
        class Column(Enum):
            ID = "id"
            CODE = "code"
            ISSUED_SHARES = "issued_shares"
            OUTSTANDING_SHARES = "outstanding_shares"
            OUTSTANDING_RATE = "outstanding_rate"
            MARKET_CAP = "market_cap"
            MARKET_ID = "market_id"
            STOCK_TYPE = "stock_type"
            CREATE_DATE = "create_date"
            UPDATE_DATE = "update_date"
            DELETE_DATE = "delete_date"

        name = "stock"
        primary_key = [Column.ID.value]
