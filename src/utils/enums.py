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


class TimeFormat(Enum):
    YEAR = "2000"
    MONTH_NAME_YEAR = "Feb-2000"  # month name + year
    DAY_MONTH_YEAR = "18/02/2000"  # full date (day, month, year)
    QUARTER_YEAR = "Q1/2000"  # quarter + year
    MONTH_INDEX_YEAR = "M2/2000"  # month index + year
    THREE_MONTH_INDEX_YEAR = "3M/2000"  # 3 month index + year


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
    FDI_RD = "fdi_rd"  # Foreign Direct Investment Registration, Disbursement
    EXPORT = "export"  # Export statistics
    IMPORT = "import"  # Import statistics
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
# region Automatically scraped from Vietstock
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


class FdiRdSource(Enum):
    VIETSTOCK = "vietstock"


class ExportSource(Enum):
    VIETSTOCK = "vietstock"


class ImportSource(Enum):
    VIETSTOCK = "vietstock"


# endregion Automatically scraped from Vietstock


# region Manually scraped from Investing.com
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


# endregion Manually scraped from Investing.com


# Union of all sources
Source = Union[
    # MACROECONOMICS
    # Automatically scraped from Vietstock
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
    FdiRdSource,
    ExportSource,
    ImportSource,
    # Manually scraped from Investing.com
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
        MacroeconomicsSubType.FDI_RD,
        FdiRdSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.EXPORT,
        ExportSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.IMPORT,
        ImportSource.VIETSTOCK,
    ): SourceInfo(
        url="https://finance.vietstock.vn/du-lieu-vi-mo/macro-data?group=7&languageid=2",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.GOLD_PRICE,
        GoldPriceSource.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/commodities/gold-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.OIL_PRICE,
        OilPriceSource.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/commodities/brent-oil-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.DOW_JONES,
        DowJonesSource.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/indices/us-30-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.NYSE_COMPOSITE,
        NYSECompositeSource.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/indices/nyse-composite-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.SNP_500,
        SNP500Source.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/indices/us-spx-500-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.NASDAQ_COMPOSITE,
        NASDAQCompositeSource.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/indices/nasdaq-composite-historical-data",
    ),
    (
        ScrapeMainType.MACROECONOMICS,
        MacroeconomicsSubType.NASDAQ_100,
        NASDAQ100Source.INVESTING,
    ): SourceInfo(
        url="https://vn.investing.com/indices/nq-100-historical-data",
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
            AGRICULTURE = "agriculture"
            INDUSTRY = "industry"
            SERVICES = "services"
            GDP_GROWTH = "gdp_growth"
            GDP_REAL = "gdp_real"

        name = "gdp"
        primary_key = [Column.YEAR.value, Column.QUARTER.value]

    class CPI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            BEVERAGE_AND_CIGARETTE = "beverage_and_cigarette"
            CONSUMER_PRICE_INDEX = "consumer_price_index"
            CULTURE_ENTERTAINMENT_AND_TOURISM = "culture_entertainment_and_tourism"
            EATING_OUTSIDE = "eating_outside"
            EDUCATION = "education"
            FOOD = "food"
            FOOD_AND_FOODSTUFF = "food_and_foodstuff"
            FOODSTUFF = "foodstuff"
            GARMENT_FOOTWEAR_HAT = "garment_footwear_hat"
            HOUSEHOLD_APPLIANCES_AND_GOODS = "household_appliances_and_goods"
            HOUSING_AND_CONSTRUCTION_MATERIALS = "housing_and_construction_materials"
            MEDICINE_AND_HEALTH_CARE = "medicine_and_health_care"
            OTHER_GOODS_AND_SERVICES = "other_goods_and_services"
            POSTAL_SERVICES_AND_TELECOMMUNICATION = (
                "postal_services_and_telecommunication"
            )
            TRAFFIC = "traffic"

        name = "cpi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class PPI:
        class Column(Enum):
            YEAR = "year"
            GENERAL_INDEX = "general_index"
            FORESTRY_SERVICES = "forestry_services"
            AGRICULTURAL_SERVICES = "agricultural_services"
            FORESTRY_AND_RELATED_SERVICES = "forestry_and_related_services"
            EXPLOITED_FOREST_PRODUCTS = "exploited_forest_products"
            COLLECTED_FOREST_PRODUCTS = "collected_forest_products"
            AGRICULTURE_AND_RELATED_SERVICES = "agriculture_and_related_services"
            LIVESTOCK_PRODUCTS = "livestock_products"
            ANNUAL_CROP_PRODUCTS = "annual_crop_products"
            PERENNIAL_CROP_PRODUCTS = "perennial_crop_products"
            EXPLOITED_AQUATIC_PRODUCTS = "exploited_aquatic_products"
            AQUATIC_PRODUCTS_EXPLOITATION_AND_FARMING = (
                "aquatic_products_exploitation_and_farming"
            )
            AQUATIC_FARMING_PRODUCTS = "aquatic_farming_products"
            FOREST_PLANTING_AND_CARE = "forest_planting_and_care"

        name = "ppi"
        primary_key = [Column.YEAR.value]

    class IPI:
        class Column(Enum):
            YEAR = "year"
            GENERAL_INDEX = "general_index"
            PROFESSIONAL_SCIENTIFIC_AND_TECHNICAL_SERVICES = (
                "professional_scientific_and_technical_services"
            )
            CONSTRUCTION_SERVICES = "construction_services"
            PAPER_AND_PAPER_PRODUCTS = "paper_and_paper_products"
            CHEMICALS_AND_CHEMICAL_PRODUCTS = "chemicals_and_chemical_products"
            MACHINERY_AND_EQUIPMENT_NOT_ELSEWHERE_CLASSIFIED = (
                "machinery_and_equipment_not_elsewhere_classified"
            )
            NATURAL_WATER_EXTRACTION = "natural_water_extraction"
            NATURAL_WATER_EXTRACTION_AND_WASTE_MANAGEMENT_SERVICES = (
                "natural_water_extraction_and_waste_management_services"
            )
            OTHER_TRANSPORT_EQUIPMENT = "other_transport_equipment"
            METAL_ORES = "metal_ores"
            PROCESSED_FOOD_PRODUCTS = "processed_food_products"
            MANUFACTURING_PRODUCTS = "manufacturing_products"
            TEXTILES_AND_LEATHER_PRODUCTS = "textiles_and_leather_products"
            MINING_PRODUCTS = "mining_products"
            OTHER_MINING_PRODUCTS = "other_mining_products"
            METAL_PRODUCTS = "metal_products"
            FORESTRY_PRODUCTS_AND_RELATED_SERVICES = (
                "forestry_products_and_related_services"
            )
            AGRICULTURE_FORESTRY_AND_FISHERY_PRODUCTS = (
                "agriculture_forestry_and_fishery_products"
            )
            AGRICULTURE_PRODUCTS_AND_RELATED_SERVICES = (
                "agriculture_products_and_related_services"
            )
            FISHING_AND_AQUACULTURE_PRODUCTS = "fishing_and_aquaculture_products"
            RUBBER_AND_PLASTIC_PRODUCTS = "rubber_and_plastic_products"
            WOOD_PRODUCTS = "wood_products"
            OTHER_NON_METALLIC_MINERAL_PRODUCTS = "other_non_metallic_mineral_products"
            FABRICATED_METAL_PRODUCTS_EXCEPT_MACHINERY_AND_EQUIPMENT = (
                "fabricated_metal_products_except_machinery_and_equipment"
            )
            ELECTRONIC_COMPUTER_AND_OPTICAL_PRODUCTS = (
                "electronic_computer_and_optical_products"
            )
            USED_FOR_MANUFACTURING_INDUSTRY = "used_for_manufacturing_industry"
            USED_FOR_AGRICULTURE_FORESTRY_AND_FISHERY = (
                "used_for_agriculture_forestry_and_fishery"
            )
            USED_FOR_CONSTRUCTION = "used_for_construction"
            COKE_AND_REFINED_PETROLEUM_PRODUCTS = "coke_and_refined_petroleum_products"
            HARD_COAL_AND_LIGNITE = "hard_coal_and_lignite"
            ELECTRICAL_EQUIPMENT = "electrical_equipment"
            PHARMACEUTICALS_AND_MEDICINAL_CHEMICALS = (
                "pharmaceuticals_and_medicinal_chemicals"
            )
            MOTOR_VEHICLES_AND_TRAILERS = "motor_vehicles_and_trailers"
            ELECTRICITY_GAS_STEAM_AND_AIR_CONDITIONING = (
                "electricity_gas_steam_and_air_conditioning"
            )
            BEVERAGES_AND_TOBACCO = "beverages_and_tobacco"

        name = "ipi"
        primary_key = [Column.YEAR.value]

    class XPI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            ANIMAL_FEED_AND_RAW_MATERIALS = "animal_feed_and_raw_materials"
            AQUATIC_PRODUCTS = "aquatic_products"
            CAMERAS_CAMCORDERS_AND_COMPONENTS = "cameras_camcorders_and_components"
            CASHEW_NUTS = "cashew_nuts"
            CASSAVA_AND_CASSAVA_PRODUCTS = "cassava_and_cassava_products"
            CHEMICAL_PRODUCTS = "chemical_products"
            CHEMICALS = "chemicals"
            CLINKER_AND_CEMENT = "clinker_and_cement"
            COFFEE = "coffee"
            CONFECTIONERY_AND_CEREAL_PRODUCTS = "confectionery_and_cereal_products"
            CRUDE_OIL = "crude_oil"
            DOMESTIC_ECONOMIC_SECTOR = "domestic_economic_sector"
            ELECTRICAL_WIRES_AND_CABLES = "electrical_wires_and_cables"
            ELECTRONICS_COMPUTERS_AND_COMPONENTS = (
                "electronics_computers_and_components"
            )
            FOOTWEAR = "footwear"
            FOREIGN_INVESTED_SECTOR = "foreign_invested_sector"
            FOREIGN_CRUDE_OIL = "foreign_crude_oil"
            FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD = (
                "furniture_products_from_materials_other_than_wood"
            )
            GLASS_AND_GLASS_PRODUCTS = "glass_and_glass_products"
            HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS = (
                "handbags_wallets_suitcases_hats_umbrellas"
            )
            IRON_AND_STEEL = "iron_and_steel"
            IRON_AND_STEEL_PRODUCTS = "iron_and_steel_products"
            MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER = (
                "machinery_equipment_tools_spare_parts_other"
            )
            MAIN_PRODUCTS = "main_products"
            OTHER_BASE_METALS_AND_PRODUCTS = "other_base_metals_and_products"
            OTHER_GOODS = "other_goods"
            PAPER_AND_PAPER_PRODUCTS = "paper_and_paper_products"
            PEPPER = "pepper"
            PETROLEUM = "petroleum"
            PHONES_AND_COMPONENTS = "phones_and_components"
            PLASTIC_PRODUCTS = "plastic_products"
            RAW_PLASTICS = "raw_plastics"
            RICE = "rice"
            RUBBER = "rubber"
            RUBBER_PRODUCTS = "rubber_products"
            TEA = "tea"
            TEXTILE_FIBERS_YARNS_OF_ALL_KINDS = "textile_fibers_yarns_of_all_kinds"
            TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS = (
                "textile_garment_leather_footwear_raw_materials"
            )
            TEXTILES_GARMENTS = "textiles_garments"
            TOTAL_VALUE = "total_value"
            TOYS_SPORTS_EQUIPMENT_AND_PARTS = "toys_sports_equipment_and_parts"
            TRANSPORTATION_VEHICLES_AND_SPARE_PARTS = (
                "transportation_vehicles_and_spare_parts"
            )
            VEGETABLES = "vegetables"
            WOOD_AND_WOOD_PRODUCTS = "wood_and_wood_products"

        name = "xpi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class MPI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            ANIMAL_FEED_AND_RAW_MATERIALS = "animal_feed_and_raw_materials"
            AQUATIC_PRODUCTS = "aquatic_products"
            CAMERAS_CAMCORDERS_AND_COMPONENTS = "cameras_camcorders_and_components"
            CASHEW_NUTS = "cashew_nuts"
            CASSAVA_AND_CASSAVA_PRODUCTS = "cassava_and_cassava_products"
            CHEMICAL_PRODUCTS = "chemical_products"
            CHEMICALS = "chemicals"
            CLINKER_AND_CEMENT = "clinker_and_cement"
            COFFEE = "coffee"
            CONFECTIONERY_AND_CEREAL_PRODUCTS = "confectionery_and_cereal_products"
            CRUDE_OIL = "crude_oil"
            DOMESTIC_ECONOMIC_SECTOR = "domestic_economic_sector"
            ELECTRICAL_WIRES_AND_CABLES = "electrical_wires_and_cables"
            ELECTRONICS_COMPUTERS_AND_COMPONENTS = (
                "electronics_computers_and_components"
            )
            FOOTWEAR = "footwear"
            FOREIGN_INVESTED_SECTOR = "foreign_invested_sector"
            FOREIGN_CRUDE_OIL = "foreign_crude_oil"
            FURNITURE_PRODUCTS_FROM_MATERIALS_OTHER_THAN_WOOD = (
                "furniture_products_from_materials_other_than_wood"
            )
            GLASS_AND_GLASS_PRODUCTS = "glass_and_glass_products"
            HANDBAGS_WALLETS_SUITCASES_HATS_UMBRELLAS = (
                "handbags_wallets_suitcases_hats_umbrellas"
            )
            IRON_AND_STEEL = "iron_and_steel"
            IRON_AND_STEEL_PRODUCTS = "iron_and_steel_products"
            MACHINERY_EQUIPMENT_TOOLS_SPARE_PARTS_OTHER = (
                "machinery_equipment_tools_spare_parts_other"
            )
            MAIN_PRODUCTS = "main_products"
            OTHER_BASE_METALS_AND_PRODUCTS = "other_base_metals_and_products"
            OTHER_GOODS = "other_goods"
            PAPER_AND_PAPER_PRODUCTS = "paper_and_paper_products"
            PEPPER = "pepper"
            PETROLEUM = "petroleum"
            PHONES_AND_COMPONENTS = "phones_and_components"
            PLASTIC_PRODUCTS = "plastic_products"
            RAW_PLASTICS = "raw_plastics"
            RICE = "rice"
            RUBBER = "rubber"
            RUBBER_PRODUCTS = "rubber_products"
            TEA = "tea"
            TEXTILE_FIBERS_YARNS_OF_ALL_KINDS = "textile_fibers_yarns_of_all_kinds"
            TEXTILE_GARMENT_LEATHER_FOOTWEAR_RAW_MATERIALS = (
                "textile_garment_leather_footwear_raw_materials"
            )
            TEXTILES_GARMENTS = "textiles_garments"
            TOTAL_VALUE = "total_value"
            TOYS_SPORTS_EQUIPMENT_AND_PARTS = "toys_sports_equipment_and_parts"
            TRANSPORTATION_VEHICLES_AND_SPARE_PARTS = (
                "transportation_vehicles_and_spare_parts"
            )
            VEGETABLES = "vegetables"
            WOOD_AND_WOOD_PRODUCTS = "wood_and_wood_products"

        name = "mpi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class POPULATION:
        class Column(Enum):
            YEAR = "year"
            POPULATION = "population"
            POPULATION_AREA_URBAN_RATE = "population_area_urban_rate"
            POPULATION_DENSITY = "population_density"
            POPULATION_GROWTH_RATE = "population_growth_rate"

        name = "population"
        primary_key = [Column.YEAR.value]

    class LABOR:
        class Column(Enum):
            YEAR = "year"
            AGRICULTURE_FORESTRY_AND_FISHERY = "agriculture_forestry_and_fishery"
            EMPLOYED_AMOUNT = "employed_amount"
            FEMALE = "female"
            INDUSTRY_CONSTRUCTION = "industry_construction"
            LABOR_FORCE_ANNUAL_CHANGE_PERCENT = "labor_force_annual_change_percent"
            LABOR_FORCE_PARTICIPATION_RATE_PERCENT = (
                "labor_force_participation_rate_percent"
            )
            MALE = "male"
            SERVICES = "services"
            UNEMPLOYED = "unemployed"
            URBAN_UNEMPLOYMENT_RATE = "urban_unemployment_rate"

        name = "labor"
        primary_key = [Column.YEAR.value]

    class RETAIL:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            ACCOMMODATION_AND_CATERING_SERVICE = "accommodation_and_catering_service"
            RETAIL_GROWTH = "retail_growth"
            RETAIL_SALE = "retail_sale"
            SERVICES = "services"
            TRAVELING_SERVICE = "traveling_service"

        name = "retail"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class PMI:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            PMI = "pmi"

        name = "pmi"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class IIP:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            APPAREL_MANUFACTURING = "apparel_manufacturing"
            BEVERAGE_PRODUCTION = "beverage_production"
            COAL_AND_LIGNITE_MINING = "coal_and_lignite_mining"
            CRUDE_OIL_AND_NATURAL_GAS_EXTRACTION = (
                "crude_oil_and_natural_gas_extraction"
            )
            ENTIRE_INDUSTRIAL_SECTOR = "entire_industrial_sector"
            FOOD_PRODUCTION_AND_PROCESSING = "food_production_and_processing"
            LEATHER_AND_RELATED_PRODUCT_MANUFACTURING = (
                "leather_and_related_product_manufacturing"
            )
            MANUFACTURE_OF_CHEMICALS_AND_CHEMICAL_PRODUCTS = (
                "manufacture_of_chemicals_and_chemical_products"
            )
            MANUFACTURE_OF_COKE_AND_REFINED_PETROLEUM_PRODUCTS = (
                "manufacture_of_coke_and_refined_petroleum_products"
            )
            MANUFACTURE_OF_ELECTRICAL_EQUIPMENT = "manufacture_of_electrical_equipment"
            MANUFACTURE_OF_ELECTRONIC_PRODUCTS_COMPUTERS_AND_OPTICAL_PRODUCTS = (
                "manufacture_of_electronic_products_computers_and_optical_products"
            )
            MANUFACTURE_OF_FABRICATED_METAL_PRODUCTS_EXCLUDING_MACHINERY_AND_EQUIPMENT = "manufacture_of_fabricated_metal_products_excluding_machinery_and_equipment"
            MANUFACTURE_OF_FURNITURE = "manufacture_of_furniture"
            MANUFACTURE_OF_MACHINERY_AND_EQUIPMENT_NOT_ELSEWHERE_CLASSIFIED = (
                "manufacture_of_machinery_and_equipment_not_elsewhere_classified"
            )
            MANUFACTURE_OF_METALS = "manufacture_of_metals"
            MANUFACTURE_OF_MOTOR_VEHICLES = "manufacture_of_motor_vehicles"
            MANUFACTURE_OF_OTHER_NON_METALLIC_MINERAL_PRODUCTS = (
                "manufacture_of_other_non_metallic_mineral_products"
            )
            MANUFACTURE_OF_OTHER_TRANSPORT_EQUIPMENT = (
                "manufacture_of_other_transport_equipment"
            )
            MANUFACTURE_OF_PHARMACEUTICALS_MEDICINAL_CHEMICALS_AND_BOTANICAL_PRODUCTS = "manufacture_of_pharmaceuticals_medicinal_chemicals_and_botanical_products"
            MANUFACTURE_OF_RUBBER_AND_PLASTIC_PRODUCTS = (
                "manufacture_of_rubber_and_plastic_products"
            )
            MANUFACTURING_INDUSTRY = "manufacturing_industry"
            METAL_ORE_MINING = "metal_ore_mining"
            MINING = "mining"
            MINING_SUPPORT_SERVICE_ACTIVITIES = "mining_support_service_activities"
            OTHER_MANUFACTURING_INDUSTRIES = "other_manufacturing_industries"
            OTHER_MINING = "other_mining"
            PAPER_AND_PAPER_PRODUCT_MANUFACTURING = (
                "paper_and_paper_product_manufacturing"
            )
            PRINTING_AND_REPRODUCTION_OF_RECORDED_MEDIA = (
                "printing_and_reproduction_of_recorded_media"
            )
            PRODUCTION_AND_DISTRIBUTION_OF_ELECTRICITY_GAS_HOT_WATER_STEAM_AND_AIR_CONDITIONING = "production_and_distribution_of_electricity_gas_hot_water_steam_and_air_conditioning"
            REPAIR_MAINTENANCE_AND_INSTALLATION_OF_MACHINERY_AND_EQUIPMENT = (
                "repair_maintenance_and_installation_of_machinery_and_equipment"
            )
            TEXTILE_MANUFACTURING = "textile_manufacturing"
            TOBACCO_PRODUCT_MANUFACTURING = "tobacco_product_manufacturing"
            WASTE_COLLECTION_TREATMENT_AND_DISPOSAL_ACTIVITIES_RECYCLING_OF_WASTE = (
                "waste_collection_treatment_and_disposal_activities_recycling_of_waste"
            )
            WASTEWATER_COLLECTION_AND_TREATMENT = "wastewater_collection_and_treatment"
            WATER_COLLECTION_TREATMENT_AND_SUPPLY = (
                "water_collection_treatment_and_supply"
            )
            WATER_SUPPLY_WASTE_MANAGEMENT_AND_TREATMENT_ACTIVITIES = (
                "water_supply_waste_management_and_treatment_activities"
            )

        name = "iip"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class IPV:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            ALUMINIUM = "aluminium"
            ANIMAL_FEED = "animal_feed"
            AQUATIC_FEED = "aquatic_feed"
            BEER = "beer"
            CARS = "cars"
            CASUAL_CLOTHES = "casual_clothes"
            CEMENT = "cement"
            CHEMICAL_PAINTS = "chemical_paints"
            CIGARETTES = "cigarettes"
            COAL_CLEAN_COAL = "coal_clean_coal"
            COMMERCIAL_TAP_WATER = "commercial_tap_water"
            ELECTRICITY_PRODUCED = "electricity_produced"
            EXTRACTED_CRUDE_OIL = "extracted_crude_oil"
            FRESH_MILK = "fresh_milk"
            GASOLINE_OIL = "gasoline_oil"
            GRANULATED_SUGAR = "granulated_sugar"
            IRON_CRUDE_STEEL = "iron_crude_steel"
            LEATHER_SHOES_AND_SANDALS = "leather_shoes_and_sandals"
            LIQUIDIZED_GAS_LPG = "liquidized_gas_lpg"
            MOBILE_PHONES = "mobile_phones"
            MONONATRI_GLUTAMAT = "mononatri_glutamat"
            MOTORCYCLES = "motorcycles"
            NPK_MIXED_FERTILIZERS = "npk_mixed_fertilizers"
            NATURAL_FABRICS = "natural_fabrics"
            NATURAL_GAS_AIR = "natural_gas_in_the_form_of_air"
            PHONE_ACCESSORIES = "phone_accessories"
            POWDERED_MILK = "powdered_milk"
            PROCESSED_SEAFOOD = "processed_seafood"
            ROLLED_STEEL = "rolled_steel"
            STEEL_BARS_ANGLE_STEEL = "steel_bars_angle_steel"
            SYNTHETIC_FABRICS = "synthetic_or_artificial_fabrics"
            TELEVISION = "television"
            UREA_FERTILIZER = "urea_fertilizer"

        name = "ipv"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class IPV_BY_INDUSTRY:
        class Column(Enum):
            YEAR = "year"
            MANUFACTURING_INDUSTRY = "manufacturing_industry"
            TEXTILES = "textiles"
            MINING_SUPPORT_SERVICES = "mining_support_services"
            PRINTING_AND_COPYING = "printing_and_copying"
            MINING = "mining"
            OTHER_MINING = "other_mining"
            OIL_AND_GAS_EXTRACTION = "oil_and_gas_extraction"
            METAL_ORE_MINING = "metal_ore_mining"
            HARD_AND_SOFT_COAL_MINING = "hard_and_soft_coal_mining"
            FOOD_PROCESSING = "food_processing"
            LEATHER_PRODUCTS = "leather_products"
            PAPER_PRODUCTS = "paper_products"
            CHEMICAL_PRODUCTS = "chemical_products"
            METAL_PRODUCTS = "metal_products"
            TOBACCO_PRODUCTS = "tobacco_products"
            RUBBER_AND_PLASTIC_PRODUCTS = "rubber_and_plastic_products"
            OTHER_NON_METAL_MINERAL_PRODUCTS = "other_non_metal_mineral_products"
            PREFAB_METAL_PRODUCTS = "prefab_metal_products"
            COKE_AND_REFINED_PETROLEUM_PRODUCTS = "coke_and_refined_petroleum_products"
            PHARMACEUTICAL_PRODUCTS = "pharmaceutical_products"
            CLOTHING = "clothing"
            BEVERAGES = "beverages"
            TOTAL = "total"

        name = "ipv_by_industry"
        primary_key = [Column.YEAR.value]

    class MIP:
        class Column(Enum):
            YEAR = "year"
            AIR_CONDITIONERS = "air_conditioners"
            ANIMAL_AND_POULTRY_FEED = "animal_and_poultry_feed"
            ANTIMONY_ORE_AND_ANTIMONY_CONCENTRATE = (
                "antimony_ore_and_antimony_concentrate"
            )
            APATITE_ORE = "apatite_ore"
            AQUACULTURE_FEED = "aquaculture_feed"
            ASSEMBLED_CARS = "assembled_cars"
            ASSEMBLED_MOTORCYCLES_AND_MOPEDS = "assembled_motorcycles_and_mopeds"
            ASSEMBLED_TVS = "assembled_tvs"
            BATH_MILK_AND_FACIAL_CLEANSER = "bath_milk_and_facial_cleanser"
            BEER = "beer"
            CANNED_FRUITS_AND_NUTS = "canned_fruits_and_nuts"
            CANNED_MEAT = "canned_meat"
            CANNED_SEAFOOD = "canned_seafood"
            CANNED_VEGETABLES = "canned_vegetables"
            CAR_AND_TRACTOR_TIRES_INFLATABLE = "car_and_tractor_tires_inflatable"
            CAST_OR_OTHER_ROUGH_IRON_AND_STEEL = "cast_or_other_rough_iron_and_steel"
            CASUAL_CLOTHING = "casual_clothing"
            CEMENT = "cement"
            CHEMICAL_FERTILIZERS = "chemical_fertilizers"
            CLEAN_COAL = "clean_coal"
            COMMERCIAL_TAP_WATER = "commercial_tap_water"
            COPPER_ORE_AND_COPPER_CONCENTRATE = "copper_ore_and_copper_concentrate"
            CRUDE_OIL_EXTRACTION = "crude_oil_extraction"
            DIGITAL_CAMERAS = "digital_cameras"
            DOMESTIC_CERAMICS = "domestic_ceramics"
            DOMESTIC_CRUDE_OIL_EXTRACTION = "domestic_crude_oil_extraction"
            EXTRACTED_STONE = "extracted_stone"
            FABRIC = "fabric"
            FABRIC_SHOES = "fabric_shoes"
            FIBER = "fiber"
            FIBER_CEMENT_ROOFING_SHEETS = "fiber_cement_roofing_sheets"
            FIRED_BRICKS = "fired_bricks"
            FIRED_TILES = "fired_tiles"
            FISH_SAUCE = "fish_sauce"
            FRESH_MILK = "fresh_milk"
            FROZEN_SEAFOOD = "frozen_seafood"
            GENERATED_ELECTRICITY = "generated_electricity"
            GRANULATED_SUGAR = "granulated_sugar"
            GRAVEL_AND_PEBBLES = "gravel_and_pebbles"
            GROUND_COFFEE_AND_INSTANT_COFFEE = "ground_coffee_and_instant_coffee"
            HERBICIDES = "herbicides"
            HOUSEHOLD_ELECTRIC_FANS = "household_electric_fans"
            HOUSEHOLD_REFRIGERATORS_AND_FREEZERS = (
                "household_refrigerators_and_freezers"
            )
            HOUSEHOLD_WASHING_MACHINES = "household_washing_machines"
            IRON_ORE_AND_IRON_CONCENTRATE = "iron_ore_and_iron_concentrate"
            LANDLINE_PHONES = "landline_phones"
            LAUNDRY_DETERGENT_AND_CLEANING_PRODUCTS = (
                "laundry_detergent_and_cleaning_products"
            )
            LEATHER_SHOES_AND_BOOTS = "leather_shoes_and_boots"
            LIGHT_BULBS = "light_bulbs"
            MILLED_RICE = "milled_rice"
            MINERAL_WATER = "mineral_water"
            MOBILE_PHONES = "mobile_phones"
            MOTORCYCLE_AND_BICYCLE_TIRES_INFLATABLE = (
                "motorcycle_and_bicycle_tires_inflatable"
            )
            MSG_MONOSODIUM_GLUTAMATE = "msg_monosodium_glutamate"
            NATURAL_GAS_IN_GAS_FORM = "natural_gas_in_gas_form"
            NPK_FERTILIZERS = "npk_fertilizers"
            PAPER_AND_CARDBOARD = "paper_and_cardboard"
            PESTICIDES = "pesticides"
            PLASTIC_PACKAGING_AND_BAGS = "plastic_packaging_and_bags"
            POWDERED_MILK = "powdered_milk"
            PRINTED_NEWSPAPERS_AND_OTHER_PRINTING_PRODUCTS = (
                "printed_newspapers_and_other_printing_products"
            )
            PRINTERS = "printers"
            PROCESSED_TEA = "processed_tea"
            PURIFIED_WATER = "purified_water"
            REFINED_VEGETABLE_OIL = "refined_vegetable_oil"
            ROLLED_STEEL_AND_SHAPED_STEEL = "rolled_steel_and_shaped_steel"
            SANITARY_WARE = "sanitary_ware"
            SAWN_TIMBER = "sawn_timber"
            SEA_SALT = "sea_salt"
            SHAMPOO_AND_CONDITIONER = "shampoo_and_conditioner"
            SPIRITS_AND_WHITE_WINE = "spirits_and_white_wine"
            SPORTS_SHOES = "sports_shoes"
            STANDARD_BATTERIES_1_5V = "standard_batteries_15v"
            THRESHING_MACHINES = "threshing_machines"
            TITANIUM_ORE_AND_TITANIUM_CONCENTRATE = (
                "titanium_ore_and_titanium_concentrate"
            )
            TOBACCO = "tobacco"
            TOOTHPASTE = "toothpaste"
            TUBES_FOR_BICYCLES_AND_MOTORCYCLES = "tubes_for_bicycles_and_motorcycles"
            TUBES_FOR_CARS_AND_AIRCRAFT = "tubes_for_cars_and_aircraft"
            VARIOUS_TYPES_OF_BATTERIES = "various_types_of_batteries"
            VARIOUS_TYPES_OF_BICYCLES = "various_types_of_bicycles"
            VARIOUS_TYPES_OF_SAND = "various_types_of_sand"
            YELLOW_PHOSPHORUS = "yellow_phosphorus"

        name = "mip"
        primary_key = [Column.YEAR.value]

    class FA_BY_HOUSE_TYPES:
        class Column(Enum):
            YEAR = "year"
            _16_20_FLOORS = "_16_20_floors"
            _21_25_FLOORS = "_21_25_floors"
            _26_FLOORS_AND_ABOVE = "_26_floors_and_above"
            _5_FLOORS_AND_BELOW = "_5_floors_and_below"
            _6_8_FLOORS = "_6_8_floors"
            _9_15_FLOORS = "_9_15_floors"
            APARTMENT_BUILDINGS = "apartment_buildings"
            SINGLE_FAMILY_HOMES = "single_family_homes"
            SINGLE_FAMILY_HOMES_4_FLOORS_AND_ABOVE = (
                "single_family_homes_4_floors_and_above"
            )
            SINGLE_FAMILY_HOMES_BELOW_4_FLOORS = "single_family_homes_below_4_floors"
            TOTAL = "total"
            VILLAS = "villas"

        name = "fa_by_house_types"
        primary_key = [Column.YEAR.value]

    class IT_BOP:
        class Column(Enum):
            YEAR = "year"
            QUARTER = "quarter"
            A_CURRENT_ACCOUNT = "a_current_account"
            B_CAPITAL_ACCOUNT = "b_capital_account"
            BORROWING_AND_EXTERNAL_DEBT_REPAYMENT = (
                "borrowing_and_external_debt_repayment"
            )
            C_FINANCIAL_ACCOUNT = "c_financial_account"
            CAPITAL_ACCOUNT_PAYMENTS = "capital_account_payments"
            CAPITAL_ACCOUNT_RECEIPTS = "capital_account_receipts"
            CAPITAL_WITHDRAWAL = "capital_withdrawal"
            CURRENT_TRANSFERS_SECONDARY_INCOME_NET = (
                "current_transfers_secondary_income_net"
            )
            CURRENT_TRANSFERS_SECONDARY_INCOME_PAYMENTS = (
                "current_transfers_secondary_income_payments"
            )
            CURRENT_TRANSFERS_SECONDARY_INCOME_RECEIPTS = (
                "current_transfers_secondary_income_receipts"
            )
            D_ERRORS_AND_OMISSIONS = "d_errors_and_omissions"
            DIRECT_INVESTMENT_ABROAD_ASSETS = "direct_investment_abroad_assets"
            DIRECT_INVESTMENT_IN_VIETNAM_LIABILITIES = (
                "direct_investment_in_vietnam_liabilities"
            )
            DIRECT_INVESTMENT_NET = "direct_investment_net"
            E_OVERALL_BALANCE = "e_overall_balance"
            F_RESERVES_AND_RELATED_ITEMS = "f_reserves_and_related_items"
            FINANCIAL_INSTITUTIONS = "financial_institutions"
            GOODS_EXPORTS_FOB = "goods_exports_fob"
            GOODS_IMPORTS_FOB = "goods_imports_fob"
            GOODS_NET = "goods_net"
            GOVERNMENT = "government"
            IMF_CREDITS_AND_LOANS = "imf_credits_and_loans"
            INVESTMENT_INCOME_PRIMARY_INCOME_NET = (
                "investment_income_primary_income_net"
            )
            INVESTMENT_INCOME_PRIMARY_INCOME_PAYMENTS = (
                "investment_income_primary_income_payments"
            )
            INVESTMENT_INCOME_PRIMARY_INCOME_RECEIPTS = (
                "investment_income_primary_income_receipts"
            )
            LOANS_AND_EXTERNAL_DEBT_COLLECTION = "loans_and_external_debt_collection"
            LONG_TERM = "long_term"
            MONEY_AND_DEPOSITS = "money_and_deposits"
            OTHER_INVESTMENT_ASSETS = "other_investment_assets"
            OTHER_INVESTMENT_LIABILITIES = "other_investment_liabilities"
            OTHER_INVESTMENT_NET = "other_investment_net"
            OTHER_RECEIVABLESPAYABLES = "other_receivablespayables"
            PORTFOLIO_INVESTMENT_ABROAD_ASSETS = "portfolio_investment_abroad_assets"
            PORTFOLIO_INVESTMENT_IN_VIETNAM_LIABILITIES = (
                "portfolio_investment_in_vietnam_liabilities"
            )
            PORTFOLIO_INVESTMENT_NET = "portfolio_investment_net"
            PRINCIPAL_REPAYMENT = "principal_repayment"
            PRIVATE_SECTOR = "private_sector"
            RESERVE_ASSETS = "reserve_assets"
            RESIDENTS = "residents"
            SERVICES_EXPORTS = "services_exports"
            SERVICES_IMPORTS = "services_imports"
            SERVICES_NET = "services_net"
            SHORT_TERM = "short_term"
            SPECIAL_FINANCING = "special_financing"
            TOTAL_CURRENT_AND_CAPITAL_ACCOUNT_BALANCE = (
                "total_current_and_capital_account_balance"
            )
            TRADE_CREDITS_AND_ADVANCES = "trade_credits_and_advances"

        name = "it_bop"
        primary_key = [Column.YEAR.value, Column.QUARTER.value]

    class TSBR:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            AGRICULTURAL_LAND_USE_TAX = "agricultural_land_use_tax"
            AID_REVENUE = "aid_revenue"
            DOMESTIC_REVENUE = "domestic_revenue"
            ENVIRONMENTAL_PROTECTION_TAX = "environmental_protection_tax"
            ENVIRONMENTAL_PROTECTION_TAX_ON_IMPORTED_GOODS = (
                "environmental_protection_tax_on_imported_goods"
            )
            EXPORT_TAX = "export_tax"
            FEES_AND_CHARGES = "fees_and_charges"
            IMPORT_TAX = "import_tax"
            NON_AGRICULTURAL_LAND_USE_TAX = "non_agricultural_land_use_tax"
            OTHER_BUDGET_REVENUES = "other_budget_revenues"
            OTHER_REVENUE = "other_revenue"
            PERSONAL_INCOME_TAX = "personal_income_tax"
            RECOVERY_OF_CAPITAL_DIVIDENDS_POST_TAX_PROFITS_SURPLUS_REVENUE_AND_EXPENDITURE_OF_THE_STATE_BANK = "recovery_of_capital_dividends_post_tax_profits_surplus_revenue_and_expenditure_of_the_state_bank"
            REVENUE_BALANCE_FROM_IMPORT_EXPORT_ACTIVITIES = (
                "revenue_balance_from_import_export_activities"
            )
            REVENUE_FROM_CRUDE_OIL = "revenue_from_crude_oil"
            REVENUE_FROM_FOREIGN_INVESTED_ENTERPRISES = (
                "revenue_from_foreign_invested_enterprises"
            )
            REVENUE_FROM_HOUSING_AND_LAND = "revenue_from_housing_and_land"
            REVENUE_FROM_LAND_AND_WATER_SURFACE_LEASING = (
                "revenue_from_land_and_water_surface_leasing"
            )
            REVENUE_FROM_LAND_USE = "revenue_from_land_use"
            REVENUE_FROM_LEASING_AND_SALE_OF_STATE_OWNED_HOUSING = (
                "revenue_from_leasing_and_sale_of_state_owned_housing"
            )
            REVENUE_FROM_LOTTERY_ACTIVITIES = "revenue_from_lottery_activities"
            REVENUE_FROM_MINING_RIGHTS_LICENSING = (
                "revenue_from_mining_rights_licensing"
            )
            REVENUE_FROM_NON_STATE_ECONOMIC_SECTOR = (
                "revenue_from_non_state_economic_sector"
            )
            REVENUE_FROM_PUBLIC_LAND_FUNDS_AND_OTHER_PUBLIC_ASSET_BENEFITS = (
                "revenue_from_public_land_funds_and_other_public_asset_benefits"
            )
            REVENUE_FROM_STATE_OWNED_ENTERPRISES = (
                "revenue_from_state_owned_enterprises"
            )
            SPECIAL_CONSUMPTION_TAX_ON_IMPORTED_GOODS = (
                "special_consumption_tax_on_imported_goods"
            )
            TOTAL_REVENUE_FROM_IMPORT_EXPORT_ACTIVITIES = (
                "total_revenue_from_import_export_activities"
            )
            TOTAL_STATE_BUDGET_REVENUE = "total_state_budget_revenue"
            VALUE_ADDED_TAX_ON_IMPORTED_GOODS = "value_added_tax_on_imported_goods"
            VALUE_ADDED_TAX_REFUND = "value_added_tax_refund"

        name = "tsbr"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class TSBE:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            AID_EXPENDITURE = "aid_expenditure"
            DEBT_INTEREST_PAYMENT_EXPENDITURE = "debt_interest_payment_expenditure"
            DEVELOPMENT_INVESTMENT_EXPENDITURE = "development_investment_expenditure"
            EXPENDITURE_FOR_EDUCATION_TRAINING_AND_VOCATIONAL_EDUCATION = (
                "expenditure_for_education_training_and_vocational_education"
            )
            EXPENDITURE_FOR_SCIENCE_AND_TECHNOLOGY = (
                "expenditure_for_science_and_technology"
            )
            EXPENDITURE_FOR_WAGE_REFORM_AND_STREAMLINING_PERSONNEL = (
                "expenditure_for_wage_reform_and_streamlining_personnel"
            )
            REGULAR_EXPENDITURE = "regular_expenditure"
            STATE_BUDGET_CONTINGENCY = "state_budget_contingency"
            SUPPLEMENTARY_EXPENDITURE_FOR_FINANCIAL_RESERVE_FUND = (
                "supplementary_expenditure_for_financial_reserve_fund"
            )
            TOTAL_STATE_BUDGET_EXPENDITURE = "total_state_budget_expenditure"

        name = "tsbe"
        primary_key = [Column.YEAR.value, Column.MONTH.value]

    class GD:
        class Column(Enum):
            YEAR = "year"
            DEBT_BALANCE = "debt_balance"
            DOMESTIC_DEBT = "domestic_debt"
            FOREIGN_DEBT = "foreign_debt"
            TOTAL_DEBT_PAYMENTS_DURING_THE_PERIOD = (
                "total_debt_payments_during_the_period"
            )
            TOTAL_INTEREST_AND_FEES_PAID_DURING_THE_PERIOD = (
                "total_interest_and_fees_paid_during_the_period"
            )
            TOTAL_PRINCIPAL_REPAYMENT_DURING_THE_PERIOD = (
                "total_principal_repayment_during_the_period"
            )
            WITHDRAWALS_DURING_THE_PERIOD = "withdrawals_during_the_period"

        name = "gd"
        primary_key = [Column.YEAR.value]

    class BRD:
        class Column(Enum):
            YEAR = "year"
            MONTH = "month"
            ENTERPRISES_COMPLETING_DISSOLUTION = "enterprises_completing_dissolution"
            ENTERPRISES_RESUMING_OPERATIONS = "enterprises_resuming_operations"
            ENTERPRISES_TEMPORARILY_SUSPENDED_AWAITING_DISSOLUTION = (
                "enterprises_temporarily_suspended_awaiting_dissolution"
            )
            NEWLY_ESTABLISHED_ENTERPRISES = "newly_established_enterprises"
            REGISTERED_CAPITAL = "registered_capital"
            REGISTERED_LABOR = "registered_labor"

        name = "brd"
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
