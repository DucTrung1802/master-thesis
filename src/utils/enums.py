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
    GDP = "gdp"  # Gross Domestic Product
    CPI = "cpi"  # Consumer Price Index
    PPI = "ppi"  # Producer Price Index
    IPI = "ipi"  # Industrial Production Index
    XPI = "xpi"  # Export Price Index
    MPI = "mpi"  # Import Price Index
    POPULATION = "population"  # Population statistics
    LABOR = "labor"  # Labor statistics
    RETAIL = "retail"  # Retail sales statistics
    PMI = "pmi"  # Purchasing Managers' Index
    IIP = "iip"  # Index of Industrial Production
    IPV = "ipv"  # Industrial Production Volume
    IPV_BY_INDUSTRY = "ipv_by_industry"  # Industrial Production Volume by Industry
    MIP = "mip"  # Major industries production
    FA_BY_HOUSE_TYPES = "fa_by_house_types"  # Floor area of ​​completed housing construction in the year by type of house
    IT_BOP = "it_bop"  # International Trade and Balance of Payments
    TSBR = "tsbr"  # Total State Budget Revenue
    TSBE = "tsbe"  # Total State Budget Expenditure
    GD = "gd"  # Gorvernment Debt
    BRD = "brd"  # Business Registered Dissolved
    IISD = "iisd"  # Investing In Social Development
    TREG = "treg"  # Total Reserves excluding gold (USD)
    CREDIT = "credit"  # Credit statistics
    MOBILIZATION = "mobilization"  # Mobilization statistics
    EXCHANGE_RATE = "exchange_rate"  # Exchange rate statistics
    IIR = "iir"  # Interbank interest rates
    RRRR = "rrrr"  # Rediscount rate, Refinancing rate
    FDI_SECTOR = "fdi_sector"  # Foreign Direct Investment by Sector
    GOLD_PRICE = "gold_price"
    OIL_PRICE = "oil_price"
    DOW_JONES = "dow_jones"
    NYSE_COMPOSITE = "nyse_composite"
    SNP_500 = "snp_500"
    NASDAQ_COMPOSITE = "nasdaq_composite"
    NASDAQ_100 = "nasdaq_100"


class StockMarketSubType(Enum):
    VN_HNX_INDEX = "vn_hnx_index"
    VN_30_INDEX = "vn30_index"
    VN_100_INDEX = "vn100_index"
    HNX_30_INDEX = "hnx30_index"
    UPCOM_INDEX = "upcom_index"


class EnterpriseSubType(Enum):
    FINANCE_INFO = "finance_info"
    DAILY_PRICE = "daily_price"
    STOCK_INFORMATION = "stock_information"


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


class CpiSource(Enum):
    VIETSTOCK = "vietstock"


class PpiSource(Enum):
    VIETSTOCK = "vietstock"


class IpiSource(Enum):
    VIETSTOCK = "vietstock"


class XpiSource(Enum):
    VIETSTOCK = "vietstock"


class MpiSource(Enum):
    VIETSTOCK = "vietstock"


class PopulationSource(Enum):
    VIETSTOCK = "vietstock"


class LaborSource(Enum):
    VIETSTOCK = "vietstock"


class RetailSource(Enum):
    VIETSTOCK = "vietstock"


class PmiSource(Enum):
    VIETSTOCK = "vietstock"


class IipSource(Enum):
    VIETSTOCK = "vietstock"


class IpvSource(Enum):
    VIETSTOCK = "vietstock"


class IpvByIndustrySource(Enum):
    VIETSTOCK = "vietstock"


class MipSource(Enum):
    VIETSTOCK = "vietstock"


class FaByHouseTypeSource(Enum):
    VIETSTOCK = "vietstock"


class ItBopSource(Enum):
    VIETSTOCK = "vietstock"


class TsbrSource(Enum):
    VIETSTOCK = "vietstock"


class TsbeSource(Enum):
    VIETSTOCK = "vietstock"


class GdSource(Enum):
    VIETSTOCK = "vietstock"


class BrdSource(Enum):
    VIETSTOCK = "vietstock"


class IisdSource(Enum):
    VIETSTOCK = "vietstock"


class TregSource(Enum):
    VIETSTOCK = "vietstock"


class CreditSource(Enum):
    VIETSTOCK = "vietstock"


class MobilizationSource(Enum):
    VIETSTOCK = "vietstock"


class ExchangeRateSource(Enum):
    VIETSTOCK = "vietstock"


class IirSource(Enum):
    VIETSTOCK = "vietstock"


class RrrrSource(Enum):
    VIETSTOCK = "vietstock"


class FdiSectorSource(Enum):
    VIETSTOCK = "vietstock"


class GoldPriceSource(Enum):
    INVESTING = "investing"


class OilPriceSource(Enum):
    INVESTING = "investing"


class DowJonesSource(Enum):
    INVESTING = "investing"


class NYSECompositeSource(Enum):
    INVESTING = "investing"


class SNP500Source(Enum):
    INVESTING = "investing"


class NASDAQCompositeSource(Enum):
    INVESTING = "investing"


class NASDAQ100Source(Enum):
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


class StockInformationSource(Enum):
    CAFEF = "cafef"


# Union of all sources
Source = Union[
    # MACROECONOMICS
    GdpSource,
    CpiSource,
    PpiSource,
    IpiSource,
    XpiSource,
    MpiSource,
    PopulationSource,
    LaborSource,
    RetailSource,
    PmiSource,
    IipSource,
    IpvSource,
    IpvByIndustrySource,
    MipSource,
    FaByHouseTypeSource,
    ItBopSource,
    TsbrSource,
    TsbeSource,
    GdSource,
    BrdSource,
    IisdSource,
    TregSource,
    CreditSource,
    MobilizationSource,
    ExchangeRateSource,
    IirSource,
    RrrrSource,
    FdiSectorSource,
    GoldPriceSource,
    OilPriceSource,
    DowJonesSource,
    NYSECompositeSource,
    SNP500Source,
    NASDAQCompositeSource,
    NASDAQ100Source,
    # STOCK_MARKET
    VnHnxIndexSource,
    Vn30IndexSource,
    Vn100IndexSource,
    Hnx30IndexSource,
    UpcomIndexSource,
    # ENTERPRISE
    FinanceInfoSource,
    DailyPriceSource,
    StockInformationSource,
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
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.CPI,
        CpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.PPI,
        PpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IPI,
        IpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.XPI,
        XpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.MPI,
        MpiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.POPULATION,
        PopulationSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.LABOR,
        LaborSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.RETAIL,
        RetailSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.PMI,
        PmiSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IIP,
        IipSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IPV,
        IpvSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IPV_BY_INDUSTRY,
        IpvByIndustrySource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.MIP,
        MipSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.FA_BY_HOUSE_TYPES,
        FaByHouseTypeSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IT_BOP,
        ItBopSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.TSBR,
        TsbrSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.TSBE,
        TsbeSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.GD,
        GdSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.BRD,
        BrdSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IISD,
        IisdSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.TREG,
        TregSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.CREDIT,
        CreditSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.MOBILIZATION,
        MobilizationSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.EXCHANGE_RATE,
        ExchangeRateSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IIR,
        IirSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.RRRR,
        RrrrSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.FDI_SECTOR,
        FdiSectorSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
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
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.NYSE_COMPOSITE,
        NYSECompositeSource.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/indices/nyse-composite-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.SNP_500,
        SNP500Source.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/indices/us-spx-500",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.NASDAQ_COMPOSITE,
        NASDAQCompositeSource.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/indices/nasdaq-composite-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.NASDAQ_100,
        NASDAQ100Source.INVESTING,
    ): SourceInfo(
        url="https://www.investing.com/indices/nq-100",
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
    (
        ScrapeMainType.ENTERPRISE,
        EnterpriseSubType.STOCK_INFORMATION,
        StockInformationSource.CAFEF,
    ): SourceInfo(
        url="https://cafef.vn/du-lieu/hose/vic-tap-doan-vingroup-cong-ty-co-phan.chn"
    ),
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

    class NYSE_COMPOSITE:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "nyse_composite"
        primary_key = [Column.DATE.value]

    class SNP_500:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "snp_500"
        primary_key = [Column.DATE.value]

    class NASDAQ_COMPOSITE:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "nasdaq_composite"
        primary_key = [Column.DATE.value]

    class NASDAQ_100:
        class Column(Enum):
            DATE = "date"
            PRICE = "price"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            VOLUME = "volume"
            CHANGE = "change"

        name = "nasdaq_100"
        primary_key = [Column.DATE.value]

    # STOCK_MARKET
    class MARKET:
        class Column(Enum):
            ID = "id"
            CODE = "code"
            NAME = "name"
            SAVE_PROGRESS_YEAR = "save_progress_year"
            CREATE_DATE = "create_date"
            UPDATE_DATE = "update_date"
            DELETE_DATE = "delete_date"

        name = "market"
        primary_key = [Column.ID.value]

    class VN_INDEX:
        class Column(Enum):
            DATE = "date"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CLOSE = "close"
            VOLUME = "volume"

        name = "vn_index"
        primary_key = [Column.DATE.value]

    class HNX_INDEX:
        class Column(Enum):
            DATE = "date"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CLOSE = "close"
            VOLUME = "volume"

        name = "hnx_index"
        primary_key = [Column.DATE.value]

    class VN_30_INDEX:
        class Column(Enum):
            DATE = "date"
            CLOSE = "close"
            ADJUSTED_CLOSE = "adjusted_close"
            MATCHED_VOLUME = "matched_volume"
            MATCHED_VALUE = "matched_value"
            NEGOTIATED_VOLUME = "negotiated_volume"
            NEGOTIATED_VALUE = "negotiated_value"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CHANGE_VALUE = "change_value"
            CHANGE_PERCENTAGE = "change_percentage"

        name = "vn_30_index"
        primary_key = [Column.DATE.value]

    class VN_100_INDEX:
        class Column(Enum):
            DATE = "date"
            CLOSE = "close"
            ADJUSTED_CLOSE = "adjusted_close"
            MATCHED_VOLUME = "matched_volume"
            MATCHED_VALUE = "matched_value"
            NEGOTIATED_VOLUME = "negotiated_volume"
            NEGOTIATED_VALUE = "negotiated_value"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CHANGE_VALUE = "change_value"
            CHANGE_PERCENTAGE = "change_percentage"

        name = "vn_100_index"
        primary_key = [Column.DATE.value]

    class HNX_30_INDEX:
        class Column(Enum):
            DATE = "date"
            CLOSE = "close"
            ADJUSTED_CLOSE = "adjusted_close"
            MATCHED_VOLUME = "matched_volume"
            MATCHED_VALUE = "matched_value"
            NEGOTIATED_VOLUME = "negotiated_volume"
            NEGOTIATED_VALUE = "negotiated_value"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CHANGE_VALUE = "change_value"
            CHANGE_PERCENTAGE = "change_percentage"

        name = "hnx_30_index"
        primary_key = [Column.DATE.value]

    class UPCOM_INDEX:
        class Column(Enum):
            DATE = "date"
            CLOSE = "close"
            ADJUSTED_CLOSE = "adjusted_close"
            MATCHED_VOLUME = "matched_volume"
            MATCHED_VALUE = "matched_value"
            NEGOTIATED_VOLUME = "negotiated_volume"
            NEGOTIATED_VALUE = "negotiated_value"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CHANGE_VALUE = "change_value"
            CHANGE_PERCENTAGE = "change_percentage"

        name = "upcom_index"
        primary_key = [Column.DATE.value]

    # ENTERPRISE
    class STOCK:
        class Column(Enum):
            ID = "id"
            CODE = "code"
            LISTED_SHARES = "listed_shares"
            OUTSTANDING_SHARES = "outstanding_shares"
            OUTSTANDING_RATE = "outstanding_rate"
            MARKET_CAP = "market_cap"
            MARKET_ID = "market_id"
            CREATE_DATE = "create_date"
            UPDATE_DATE = "update_date"
            DELETE_DATE = "delete_date"

        name = "stock"
        primary_key = [Column.CODE.value]

    class DAILY_PRICE:
        class Column(Enum):
            DATE = "date"
            CODE = "code"
            MARKET_ID = "market_id"
            OPEN = "open"
            HIGH = "high"
            LOW = "low"
            CLOSE = "close"
            VOLUME = "volume"

        name = "daily_price"
        primary_key = [Column.DATE.value, Column.CODE.value]
