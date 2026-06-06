# src\utils\enums.py

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Union, Tuple, Optional

from utils.constants import *


# COMMON ENUMS
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


class GenerateDateTimeType(Enum):
    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"
    DAY = "day"
    DATE = "date"


# DATABASE DRIVER ENUMS
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


class DataQuality(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class CleanAction(Enum):
    REMOVE_RECORD_IF_COLUMN_IS_NULL = "remove_record_if_column_is_null"
    REMOVE_IF_ALL_COLUMNS_ARE_NULL = "remove_if_all_columns_are_null"
    ORDER_BY = "order_by"
    REMOVE_COLUMN = "remove_column"
    REMOVE_DUPLICATE_COLUMNS = "remove_duplicate_columns"


class CleanLayer:
    """
    Represents a cleaning step. Can have parameters like column_name.
    """

    def __init__(self, action: CleanAction, **kwargs):
        self.action = action
        self.params = kwargs

    @classmethod
    def REMOVE_RECORD_IF_COLUMN_IS_NULL(cls, column_name: str):
        return cls(CleanAction.REMOVE_RECORD_IF_COLUMN_IS_NULL, column_name=column_name)

    @classmethod
    def REMOVE_IF_ALL_COLUMNS_ARE_NULL(cls):
        return cls(CleanAction.REMOVE_IF_ALL_COLUMNS_ARE_NULL)

    @classmethod
    def ORDER_BY(cls, column_list: List[str]):
        return cls(CleanAction.ORDER_BY, column_list=column_list)

    @classmethod
    def REMOVE_COLUMN(cls, column_list: List[str]):
        return cls(CleanAction.REMOVE_COLUMN, column_list=column_list)

    @classmethod
    def REMOVE_DUPLICATE_COLUMNS(cls, keep: str = "first"):
        return cls(
            CleanAction.REMOVE_DUPLICATE_COLUMNS,
            keep=keep,
        )


class TransformAction(Enum):
    EXTRACT_DATETIME_FEATURE = "extract_datetime_feature"


class TransformLayer:
    """
    Represents a transformation step. Can have parameters like column_name.
    """

    def __init__(self, action: TransformAction, **kwargs):
        self.action = action
        self.params = kwargs

    @classmethod
    def EXTRACT_DATETIME_FEATURE(cls, column_name: str = "date"):
        return cls(TransformAction.EXTRACT_DATETIME_FEATURE, column_name=column_name)


class ScrapeMainType(Enum):
    STOCKS = "stocks"
    FUNDS = "funds"
    FUTURES = "futures"
    FOREX = "forex"
    CRYPTO = "crypto"
    INDICES = "indices"
    BONDS = "bonds"
    ECONOMY = "economy"
    OPTIONS = "options"


class Country(Enum):
    # NORTH AMERICA
    USA = "usa"
    CANADA = "canada"

    # EUROPE
    AUSTRIA = "austria"
    BELGIUM = "belgium"
    BULGARIA = "bulgaria"
    CROATIA = "croatia"
    CYPRUS = "cyprus"
    CZECH_REPUBLIC = "czech_republic"
    DENMARK = "denmark"
    ESTONIA = "estonia"
    EUROPEAN_UNION = "european_union"
    FINLAND = "finland"
    FRANCE = "france"
    GERMANY = "germany"
    GREECE = "greece"
    HUNGARY = "hungary"
    ICELAND = "iceland"
    IRELAND = "ireland"
    ITALY = "italy"
    LATVIA = "latvia"
    LITHUANIA = "lithuania"
    LUXEMBOURG = "luxembourg"
    NETHERLANDS = "netherlands"
    NORWAY = "norway"
    POLAND = "poland"
    PORTUGAL = "portugal"
    ROMANIA = "romania"
    RUSSIA = "russia"
    SERBIA = "serbia"
    SLOVAKIA = "slovakia"
    SLOVENIA = "slovenia"
    SPAIN = "spain"
    SWEDEN = "sweden"
    SWITZERLAND = "switzerland"
    TURKEY = "turkey"
    UNITED_KINGDOM = "united_kingdom"

    # MIDDLE EAST / AFRICA
    BAHRAIN = "bahrain"
    EGYPT = "egypt"
    ISRAEL = "israel"
    KENYA = "kenya"
    KUWAIT = "kuwait"
    MOROCCO = "morocco"
    NIGERIA = "nigeria"
    QATAR = "qatar"
    SAUDI_ARABIA = "saudi_arabia"
    SOUTH_AFRICA = "south_africa"
    TUNISIA = "tunisia"
    UNITED_ARAB_EMIRATES = "united_arab_emirates"

    # MEXICO AND SOUTH AMERICA
    ARGENTINA = "argentina"
    BRAZIL = "brazil"
    CHILE = "chile"
    COLOMBIA = "colombia"
    MEXICO = "mexico"
    PERU = "peru"
    VENEZUELA = "venezuela"

    # ASIA / PACIFIC
    AUSTRALIA = "australia"
    BANGLADESH = "bangladesh"
    HONG_KONG_CHINA = "hong_kong_china"
    INDIA = "india"
    INDONESIA = "indonesia"
    JAPAN = "japan"
    MAINLAND_CHINA = "mainland_china"
    MALAYSIA = "malaysia"
    NEW_ZEALAND = "new_zealand"
    PAKISTAN = "pakistan"
    PHILIPPINES = "philippines"
    SINGAPORE = "singapore"
    SOUTH_KOREA = "south_korea"
    SRI_LANKA = "sri_lanka"
    TAIWAN_CHINA = "taiwan_china"
    THAILAND = "thailand"
    VIETNAM = "vietnam"


class StockType(Enum):
    COMMON_STOCK = "common_stock"
    PREFERRED_STOCK = "preferred_stock"
    DEPOSITORY_RECEIPT = "depository_receipt"
    WARRANT = "warrant"
    PRE_IPO = "pre_ipo"


class StockSector(Enum):
    COMMERCIAL_SERVICES = "commercial_services"
    COMMUNICATIONS = "communications"
    CONSUMER_DURABLES = "consumer_durables"
    CONSUMER_NON_DURABLES = "consumer_non_durables"
    CONSUMER_SERVICES = "consumer_services"
    DISTRIBUTION_SERVICES = "distribution_services"
    ELECTRONIC_TECHNOLOGY = "electronic_technology"
    ENERGY_MINERALS = "energy_minerals"
    FINANCE = "finance"
    GOVERNMENT_SECTOR = "government_sector"
    HEALTH_SERVICES = "health_services"
    HEALTH_TECHNOLOGY = "health_technology"
    INDUSTRIAL_SERVICES = "industrial_services"
    MISCELLANEOUS = "miscellaneous"
    NON_ENERGY_MINERALS = "non_energy_minerals"
    PROCESS_INDUSTRIES = "process_industries"
    PRODUCER_MANUFACTURING = "producer_manufacturing"
    RETAIL_TRADE = "retail_trade"
    TECHNOLOGY_SERVICES = "technology_services"
    TRANSPORTATION = "transportation"
    UTILITIES = "utilities"


class FundType(Enum):
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"
    TRUST = "trust"
    REIT = "reit"


class FutureCategory(Enum):
    SINGLE_STOCK = "single_stock"
    WORLD_INDICES = "world_indices"
    CURRENCIES = "currencies"
    INTEREST_RATES = "interest_rates"
    ENERGY = "energy"
    AGRICULTURE = "agriculture"
    METALS = "metals"
    WEATHER = "weather"
    BUILDING_MATERIALS = "building_materials"
    CHEMICALS = "chemicals"


class ForexSource(Enum):
    # Forex & CFD
    ACTIVTRADES = "activtrades"
    B2PRIME = "b2prime"
    BLACKBULL_MARKETS = "blackbull_markets"
    BLUEBERRY = "blueberry"
    CAPITAL_COM = "capital_com"
    CFI = "cfi"
    CITY_INDEX = "city_index"
    CMC_MARKETS = "cmc_markets"
    CXM = "cxm"
    DERIV = "deriv"
    EASYMARKETS = "easymarkets"
    EIGHTCAP = "eightcap"
    ERRANTE = "errante"
    ESAFX = "esafx"
    FOREX_COM = "forex_com"
    FP_MARKETS = "fp_markets"
    FUSION_MARKETS = "fusion_markets"
    FXCM = "fxcm"
    FXPRO = "fxpro"
    FXTF = "fxtf"
    GBE_BROKERS = "gbe_brokers"
    GO_MARKETS = "go_markets"
    IBROKER = "ibroker"
    IC_MARKETS = "ic_markets"
    ICE_DATA_SERVICES = "ice_data_services"
    IG = "ig"
    INTERACTIVE_BROKERS = "interactive_brokers"
    JFX = "jfx"
    MATSUI = "matsui"
    OANDA = "oanda"
    OPOFINANCE = "opofinance"
    OSMANLI_FX = "osmanli_fx"
    PEPPERSTONE = "pepperstone"
    PHILLIP_NOVA = "phillip_nova"
    PURPLE_TRADING = "purple_trading"
    SAXO = "saxo"
    SKILLING = "skilling"
    SPREADEX = "spreadex"
    SWISSQUOTE = "swissquote"
    TASTYFX = "tastyfx"
    THINKMARKETS = "thinkmarkets"
    TICKMILL = "tickmill"
    TRADE_NATION = "trade_nation"
    TRIVE = "trive"
    VANTAGE = "vantage"
    VELOCITY_TRADE = "velocity_trade"
    WH_SELFINVEST = "wh_selfinvest"


class CryptoSource(Enum):
    # Cryptocurrency
    AERODROME_BASE = "aerodrome_base"
    AERODROME_SLIPSTREAM_BASE = "aerodrome_slipstream_base"
    ANTARCTIC = "antarctic"
    BASESWAP_BASE = "baseswap_base"
    BCHAIN_NASDAQ_DATA_LINK = "bchain_nasdaq_data_link"
    BINANCE = "binance"
    BINANCE_US = "binance_us"
    BINGX = "bingx"
    BISWAP_V2_BNB_CHAIN = "biswap_v2_bnb_chain"
    BITAZZA = "bitazza"
    BITFINEX = "bitfinex"
    BITFLYER = "bitflyer"
    BITGET = "bitget"
    BITHUMB = "bithumb"
    BITKUB = "bitkub"
    BITMART = "bitmart"
    BITMEX = "bitmex"
    BITRUE = "bitrue"
    BITSO = "bitso"
    BITSTAMP = "bitstamp"
    BITTREX = "bittrex"
    BITUNIX = "bitunix"
    BITVAVO = "bitvavo"
    BLACKHOLE_V3_AVALANCHE = "blackhole_v3_avalanche"
    BLOFIN = "blofin"
    BLUEFIN_SUI = "bluefin_sui"
    BRAVE_NEW_COIN = "brave_new_coin"
    BTCC = "btcc"
    BTSE = "btse"
    BYBIT = "bybit"
    BYDFI = "bydfi"
    CAMELOT_V2_ARBITRUM = "camelot_v2_arbitrum"
    CAMELOT_V3_ARBITRUM = "camelot_v3_arbitrum"
    CETUS_SUI = "cetus_sui"
    COIN_METRICS = "coin_metrics"
    COINBASE = "coinbase"
    COINDESK_INDICES = "coindesk_indices"
    COINEX = "coinex"
    COINW = "coinw"
    CRYPTO_COM = "crypto_com"
    CURVE_ARBITRUM = "curve_arbitrum"
    CURVE_ETHEREUM = "curve_ethereum"
    DEDUST_IO_TON = "dedust_io_ton"
    DEEPCOIN = "deepcoin"
    DEFILLAMA = "defillama"
    DELTA_EXCHANGE = "delta_exchange"
    DELTA_EXCHANGE_INDIA = "delta_exchange_india"
    DERIBIT = "deribit"
    DYDX = "dydx"
    GATE = "gate"
    GEMINI = "gemini"
    GLASSNODE = "glassnode"
    HONEYSWAP_V2_GNOSIS = "honeyswap_v2_gnosis"
    HTX = "htx"
    KATANA_RONIN = "katana_ronin"
    KATANA_V3_RONIN = "katana_v3_ronin"
    KCEX = "kcex"
    KRAKEN = "kraken"
    KUCOIN = "kucoin"
    LBANK = "lbank"
    LFJ_V2_2_AVALANCHE = "lfj_v2_2_avalanche"
    LUNARCRUSH = "lunarcrush"
    METEORA_DLMM_SOLANA = "meteora_dlmm_solana"
    METEORA_DYN_SOLANA = "meteora_dyn_solana"
    MEXC = "mexc"
    MM_FINANCE_CRONOS = "mm_finance_cronos"
    OKX = "okx"
    ORCA_SOLANA = "orca_solana"
    OSMOSIS = "osmosis"
    PANCAKESWAP_V2_BNB_CHAIN = "pancakeswap_v2_bnb_chain"
    PANCAKESWAP_V3_ARBITRUM = "pancakeswap_v3_arbitrum"
    PANCAKESWAP_V3_BASE = "pancakeswap_v3_base"
    PANCAKESWAP_V3_BNB_CHAIN = "pancakeswap_v3_bnb_chain"
    PANCAKESWAP_V3_ETHEREUM = "pancakeswap_v3_ethereum"
    PANCAKESWAP_V3_ZKSYNC = "pancakeswap_v3_zksync"
    PANGOLIN_V2_AVALANCHE = "pangolin_v2_avalanche"
    PHARAOH_AVALANCHE = "pharaoh_avalanche"
    PHEMEX = "phemex"
    PIONEX = "pionex"
    POLONIEX = "poloniex"
    PULSEX_PULSECHAIN = "pulsex_pulsechain"
    PULSEX_V2_PULSECHAIN = "pulsex_v2_pulsechain"
    PYTH = "pyth"
    QUICKSWAP_V2_POLYGON = "quickswap_v2_polygon"
    QUICKSWAP_V3_POLYGON_ZKEVM = "quickswap_v3_polygon_zkevm"
    QUICKSWAP_V3_POLYGON = "quickswap_v3_polygon"
    RAMSES_V2_ARBITRUM = "ramses_v2_arbitrum"
    RAYDIUM_SOLANA = "raydium_solana"
    RAYDIUM_CLMM_SOLANA = "raydium_clmm_solana"
    RAYDIUM_CPMM_SOLANA = "raydium_cpmm_solana"
    SPOOKYSWAP_V2_FANTOM = "spookyswap_v2_fantom"
    STON_FI_TON = "ston_fi_ton"
    STON_FI_V2_TON = "ston_fi_v2_ton"
    SUNSWAP_V2_TRON = "sunswap_v2_tron"
    SUSHISWAP_V2_ETHEREUM = "sushiswap_v2_ethereum"
    SUSHISWAP_V2_POLYGON = "sushiswap_v2_polygon"
    SYNCSWAP_ZKSYNC = "syncswap_zksync"
    SYNCSWAP_V2_ZKSYNC = "syncswap_v2_zksync"
    TOOBIT = "toobit"
    TRADER_JOE_V2_AVALANCHE = "trader_joe_v2_avalanche"
    TURBOS_FINANCE_SUI = "turbos_finance_sui"
    UNISWAP_V2_BASE = "uniswap_v2_base"
    UNISWAP_V2_ETHEREUM = "uniswap_v2_ethereum"
    UNISWAP_V2_UNICHAIN = "uniswap_v2_unichain"
    UNISWAP_V3_ARBITRUM = "uniswap_v3_arbitrum"
    UNISWAP_V3_AVALANCHE = "uniswap_v3_avalanche"
    UNISWAP_V3_BASE = "uniswap_v3_base"
    UNISWAP_V3_BNB_CHAIN = "uniswap_v3_bnb_chain"
    UNISWAP_V3_ETHEREUM = "uniswap_v3_ethereum"
    UNISWAP_V3_OPTIMISM = "uniswap_v3_optimism"
    UNISWAP_V3_POLYGON = "uniswap_v3_polygon"
    UPBIT = "upbit"
    VELODROME_OPTIMISM = "velodrome_optimism"
    VELODROME_SLIPSTREAM_OPTIMISM = "velodrome_slipstream_optimism"
    VELODROME_V2_OPTIMISM = "velodrome_v2_optimism"
    VOLMEX = "volmex"
    VVS_FINANCE_CRONOS = "vvs_finance_cronos"
    VVS_V3_CRONOS = "vvs_v3_cronos"
    WEBULL_PAY = "webull_pay"
    WEEX = "weex"
    WHITEBIT = "whitebit"
    WOO_X = "woo_x"
    XEXCHANGE = "xexchange"
    XT_COM = "xt_com"
    ZKSWAP_ZKSYNC = "zkswap_zksync"
    ZOOMEX = "zoomex"

    # Forex & CFD
    ACTIVTRADES = "activtrades"
    B2PRIME = "b2prime"
    BLACKBULL_MARKETS = "blackbull_markets"
    BLUEBERRY = "blueberry"
    CAPITAL_COM = "capital_com"
    CFI = "cfi"
    CITY_INDEX = "city_index"
    CMC_MARKETS = "cmc_markets"
    CXM = "cxm"
    DERIV = "deriv"
    EASYMARKETS = "easymarkets"
    EIGHTCAP = "eightcap"
    ERRANTE = "errante"
    ESAFX = "esafx"
    FOREX_COM = "forex_com"
    FP_MARKETS = "fp_markets"
    FUSION_MARKETS = "fusion_markets"
    FXCM = "fxcm"
    FXPRO = "fxpro"
    FXTF = "fxtf"
    GBE_BROKERS = "gbe_brokers"
    GO_MARKETS = "go_markets"
    IBROKER = "ibroker"
    IC_MARKETS = "ic_markets"
    ICE_DATA_SERVICES = "ice_data_services"
    IG = "ig"
    INTERACTIVE_BROKERS = "interactive_brokers"
    JFX = "jfx"
    MATSUI = "matsui"
    OANDA = "oanda"
    OPOFINANCE = "opofinance"
    OSMANLI_FX = "osmanli_fx"
    PEPPERSTONE = "pepperstone"
    PHILLIP_NOVA = "phillip_nova"
    PURPLE_TRADING = "purple_trading"
    SAXO = "saxo"
    SKILLING = "skilling"
    SPREADEX = "spreadex"
    SWISSQUOTE = "swissquote"
    TASTYFX = "tastyfx"
    THINKMARKETS = "thinkmarkets"
    TICKMILL = "tickmill"
    TRADE_NATION = "trade_nation"
    TRIVE = "trive"
    VANTAGE = "vantage"
    VELOCITY_TRADE = "velocity_trade"
    WH_SELFINVEST = "wh_selfinvest"


class CryptoType(Enum):
    SPOT = "spot"
    SWAP = "swap"
    FUTURES = "futures"
    INDEX = "index"
    FUNDAMENTAL = "fundamental"


class CryptoExchangeType(Enum):
    CEX = "cex"
    DEX = "dex"


class BondsType(Enum):
    GOVERNMENT = "government"
    CORPORATE = "corporate"


class EconomySource(Enum):
    WORLD_BANK = "world_bank"
    EUROSTAT = "eurostat"
    AKAMAI = "akamai"
    TRANSPARENCY_INTERNATIONAL = "transparency_international"
    ORGANISATION_FOR_ECONOMIC_CO_OPERATION_AND_DEVELOPMENT = (
        "organisation_for_economic_co_operation_and_development"
    )
    WORLD_ECONOMIC_FORUM = "world_economic_forum"
    WAGEINDICATOR_FOUNDATION = "wageindicator_foundation"
    BUREAU_OF_LABOUR_STATISTICS = "bureau_of_labour_statistics"
    FEDERAL_RESERVE = "federal_reserve"
    STOCKHOLM_INTERNATIONAL_PEACE_RESEARCH_INSTITUTE = (
        "stockholm_international_peace_research_institute"
    )
    INSTITUTE_FOR_ECONOMICS_AND_PEACE = "institute_for_economics_and_peace"
    BUREAU_OF_ECONOMICS_ANALYSIS = "bureau_of_economics_analysis"
    WORLD_GOLD_COUNCIL = "world_gold_council"
    CENSUS_BUREAU = "census_bureau"
    CENTRAL_BANK_OF_WEST_AFRICAN_STATES_BCEAO = (
        "central_bank_of_west_african_states_bceao"
    )
    INTERNATIONAL_MONETARY_FUND_IMF = "international_monetary_fund_imf"
    US_ENERGY_INFORMATION_ADMINISTRATION = "us_energy_information_administration"
    STATISTICS_CANADA = "statistics_canada"
    OFFICE_FOR_NATIONAL_STATISTICS = "office_for_national_statistics"
    STATISTICS_NORWAY = "statistics_norway"


class EconomyCategory(Enum):
    GDP = "gdp"
    LABOR = "labor"
    PRICES = "prices"
    HEALTH = "health"
    MONEY = "money"
    TRADE = "trade"
    GOVERNMENT = "government"
    BUSINESS = "business"
    CONSUMER = "consumer"
    HOUSING = "housing"
    TAXES = "taxes"


class ScrapeActionType(Enum):
    CLICK_BUTTON = "click_button"
    INPUT_TEXT = "input_text"
    GO_TO_LINK = "go_to_link"
    EXECUTE_SCRIPT = "execute_script"


# ──────────────────────────────────────────────────────────────────────────────
# TradingView XPaths  (structural only – no per-sector / per-type entries)
# ──────────────────────────────────────────────────────────────────────────────
class TradingViewXpath(Enum):
    # ── Search / symbol panel ──────────────────────────────────────────────
    SEARCH_BUTTON = "/html/body/div[3]/div[3]/div[2]/div[2]/div/div/div/button[1]"
    SYMBOLS_BUTTON = '//*[@id="Symbols"]'

    # ── Asset-class buttons ────────────────────────────────────────────────
    STOCKS_BUTTON = '//*[@id="stocks"]'
    FUNDS_BUTTON = '//*[@id="funds"]'
    FUTURES_BUTTON = '//*[@id="futures"]'
    FOREX_BUTTON = '//*[@id="forex"]'
    CRYPTO_BUTTON = '//*[@id="crypto"]'
    INDICES_BUTTON = '//*[@id="indices"]'
    BONDS_BUTTON = '//*[@id="bonds"]'
    ECONOMY_BUTTON = '//*[@id="economy"]'
    OPTIONS_BUTTON = '//*[@id="options"]'

    # ── Shared country filter (reused across stocks / funds / bonds /
    #    indices / economy / options) ────────────────────────────────────────
    COUNTRIES_BUTTON = (
        '//*[@id="overlap-manager-root"]/div[2]/div/div[2]/div/div/div[1]'
        "/div/div[2]/div/div[3]/div[1]/div/div/div/button"
    )
    COUNTRIES_INPUT = (
        '//*[@id="overlap-manager-root"]/div[2]/div/div[2]/div/div/div[1]'
        "/div/div[2]/div/div/div[2]/div/input"
    )
    # First result in the country search dropdown (row=0, col=0)
    COUNTRIES_FIRST_RESULT = '//*[@id="source-item-5-0-0"]'

    # ── Stocks-specific filter dropdowns ──────────────────────────────────
    STOCKS_TYPES_BUTTON = (
        '//*[@data-qa-id="stock-type-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )
    STOCKS_SECTORS_BUTTON = (
        '//*[@data-qa-id="stock-sector-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # ── Funds-specific filter dropdowns ───────────────────────────────────
    FUNDS_TYPES_BUTTON = (
        '//*[@data-qa-id="fund-type-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # ── Futures-specific filter dropdown ──────────────────────────────────
    FUTURES_CATEGORIES_BUTTON = (
        '//*[@data-qa-id="futures-category-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # ── Forex-specific filter dropdown ────────────────────────────────────
    FOREX_SOURCES_BUTTON = (
        '//*[@data-qa-id="forex-source-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # ── Crypto-specific filter dropdowns ──────────────────────────────────
    CRYPTO_SOURCES_BUTTON = (
        '//*[@data-qa-id="crypto-source-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )
    CRYPTO_TYPES_BUTTON = (
        '//*[@data-qa-id="crypto-type-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )
    CRYPTO_EXCHANGE_TYPES_BUTTON = (
        '//*[@data-qa-id="crypto-exchange-type-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # ── Bonds-specific filter dropdown ────────────────────────────────────
    BONDS_TYPES_BUTTON = (
        '//*[@data-qa-id="bond-type-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # ── Economy-specific filter dropdown ──────────────────────────────────
    ECONOMY_CATEGORIES_BUTTON = (
        '//*[@data-qa-id="economy-category-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # Kept for backwards-compatibility (aliased to the shared versions)
    STOCKS_COUNTRIES_BUTTON = (
        '//*[@id="overlap-manager-root"]/div[2]/div/div[2]/div/div/div[1]'
        "/div/div[2]/div/div[3]/div[1]/div/div/div/button"
    )
    STOCKS_COUNTRIES_INPUT = (
        '//*[@id="overlap-manager-root"]/div[2]/div/div[2]/div/div/div[1]'
        "/div/div[2]/div/div/div[2]/div/input"
    )
    STOCKS_COUNTRIES_FIRST_RESULT = '//*[@id="source-item-5-0-0"]'


# ──────────────────────────────────────────────────────────────────────────────
# Aria-label maps  (how TradingView labels each enum value in its UI)
# ──────────────────────────────────────────────────────────────────────────────

# TradingView aria-labels for StockType checkboxes
STOCK_TYPE_ARIA_LABEL: Dict[StockType, str] = {
    StockType.COMMON_STOCK: "Common stock",
    StockType.PREFERRED_STOCK: "Preferred stock",
    StockType.DEPOSITORY_RECEIPT: "Depository receipt",
    StockType.WARRANT: "Warrant",
    StockType.PRE_IPO: "Pre-IPO",
}

# TradingView aria-labels for StockSector checkboxes
STOCK_SECTOR_ARIA_LABEL: Dict[StockSector, str] = {
    StockSector.COMMERCIAL_SERVICES: "Commercial Services",
    StockSector.COMMUNICATIONS: "Communications",
    StockSector.CONSUMER_DURABLES: "Consumer Durables",
    StockSector.CONSUMER_NON_DURABLES: "Consumer Non-Durables",
    StockSector.CONSUMER_SERVICES: "Consumer Services",
    StockSector.DISTRIBUTION_SERVICES: "Distribution Services",
    StockSector.ELECTRONIC_TECHNOLOGY: "Electronic Technology",
    StockSector.ENERGY_MINERALS: "Energy Minerals",
    StockSector.FINANCE: "Finance",
    StockSector.GOVERNMENT_SECTOR: "Government",
    StockSector.HEALTH_SERVICES: "Health Services",
    StockSector.HEALTH_TECHNOLOGY: "Health Technology",
    StockSector.INDUSTRIAL_SERVICES: "Industrial Services",
    StockSector.MISCELLANEOUS: "Miscellaneous",
    StockSector.NON_ENERGY_MINERALS: "Non-Energy Minerals",
    StockSector.PROCESS_INDUSTRIES: "Process Industries",
    StockSector.PRODUCER_MANUFACTURING: "Producer Manufacturing",
    StockSector.RETAIL_TRADE: "Retail Trade",
    StockSector.TECHNOLOGY_SERVICES: "Technology Services",
    StockSector.TRANSPORTATION: "Transportation",
    StockSector.UTILITIES: "Utilities",
}

# TradingView aria-labels for FundType checkboxes
FUND_TYPE_ARIA_LABEL: Dict[FundType, str] = {
    FundType.ETF: "ETF",
    FundType.MUTUAL_FUND: "Mutual fund",
    FundType.TRUST: "Trust",
    FundType.REIT: "REIT",
}

# TradingView aria-labels for FutureCategory checkboxes
FUTURE_CATEGORY_ARIA_LABEL: Dict[FutureCategory, str] = {
    FutureCategory.SINGLE_STOCK: "Single Stock",
    FutureCategory.WORLD_INDICES: "World Indices",
    FutureCategory.CURRENCIES: "Currencies",
    FutureCategory.INTEREST_RATES: "Interest Rates",
    FutureCategory.ENERGY: "Energy",
    FutureCategory.AGRICULTURE: "Agriculture",
    FutureCategory.METALS: "Metals",
    FutureCategory.WEATHER: "Weather",
    FutureCategory.BUILDING_MATERIALS: "Building Materials",
    FutureCategory.CHEMICALS: "Chemicals",
}

# TradingView aria-labels for ForexSource checkboxes
FOREX_SOURCE_ARIA_LABEL: Dict[ForexSource, str] = {
    src: src.value.replace("_", " ").title() for src in ForexSource
}

# TradingView aria-labels for CryptoSource checkboxes
CRYPTO_SOURCE_ARIA_LABEL: Dict[CryptoSource, str] = {
    src: src.value.replace("_", " ").title() for src in CryptoSource
}

# TradingView aria-labels for CryptoType checkboxes
CRYPTO_TYPE_ARIA_LABEL: Dict[CryptoType, str] = {
    CryptoType.SPOT: "Spot",
    CryptoType.SWAP: "Swap",
    CryptoType.FUTURES: "Futures",
    CryptoType.INDEX: "Index",
    CryptoType.FUNDAMENTAL: "Fundamental",
}

# TradingView aria-labels for CryptoExchangeType checkboxes
CRYPTO_EXCHANGE_TYPE_ARIA_LABEL: Dict[CryptoExchangeType, str] = {
    CryptoExchangeType.CEX: "CEX",
    CryptoExchangeType.DEX: "DEX",
}

# TradingView aria-labels for BondsType checkboxes
BONDS_TYPE_ARIA_LABEL: Dict[BondsType, str] = {
    BondsType.GOVERNMENT: "Government",
    BondsType.CORPORATE: "Corporate",
}

# TradingView aria-labels for EconomyCategory checkboxes
ECONOMY_CATEGORY_ARIA_LABEL: Dict[EconomyCategory, str] = {
    EconomyCategory.GDP: "GDP",
    EconomyCategory.LABOR: "Labor",
    EconomyCategory.PRICES: "Prices",
    EconomyCategory.HEALTH: "Health",
    EconomyCategory.MONEY: "Money",
    EconomyCategory.TRADE: "Trade",
    EconomyCategory.GOVERNMENT: "Government",
    EconomyCategory.BUSINESS: "Business",
    EconomyCategory.CONSUMER: "Consumer",
    EconomyCategory.HOUSING: "Housing",
    EconomyCategory.TAXES: "Taxes",
}


# ──────────────────────────────────────────────────────────────────────────────
# ScrapeAction  — must be defined before any function that constructs one
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ScrapeAction:
    scrape_action_type: ScrapeActionType
    # Either a TradingViewXpath enum member OR a raw xpath string
    xpath: Union[TradingViewXpath, str]
    value: Optional[str]

    @property
    def xpath_str(self) -> str:
        """Always return a plain string regardless of whether xpath is an enum or str."""
        return (
            self.xpath.value if isinstance(self.xpath, TradingViewXpath) else self.xpath
        )


# Country search-term map  (what to type into the country search box)
COUNTRY_SEARCH_TERM: Dict[Country, str] = {
    Country.VIETNAM: "vietnam",
    Country.USA: "united states",
    Country.UNITED_KINGDOM: "united kingdom",
    Country.GERMANY: "germany",
    Country.FRANCE: "france",
    Country.JAPAN: "japan",
    Country.INDIA: "india",
    Country.MAINLAND_CHINA: "mainland china",
    # … extend as needed
}

# Countries to scrape in add_trading_view_links_scraping_tasks.
# Add / remove entries here to control which countries are processed.
TRADING_VIEW_SCRAPE_COUNTRIES: List[Country] = [
    Country.VIETNAM,
    # Country.USA,
    # Country.UNITED_KINGDOM,
    # Country.GERMANY,
    # Country.FRANCE,
    # Country.JAPAN,
    # Country.INDIA,
    # Country.MAINLAND_CHINA,
]


# ──────────────────────────────────────────────────────────────────────────────
# TradingView localStorage key constants
# Discovered by inspecting localStorage on a live TradingView session.
# ──────────────────────────────────────────────────────────────────────────────

# Maps Country enum → TradingView's internal country code stored in localStorage
COUNTRY_CODE: Dict[Country, str] = {
    Country.VIETNAM: "vn",
    Country.USA: "us",
    Country.UNITED_KINGDOM: "gb",
    Country.GERMANY: "de",
    Country.FRANCE: "fr",
    Country.JAPAN: "jp",
    Country.INDIA: "in",
    Country.MAINLAND_CHINA: "cn",
    # extend as needed
}

# localStorage key that stores the active asset-class tab
_LS_FILTER_KEY = "tradingview.symboledit.filter"

# localStorage key that stores per-asset-class country selections
# shape: {"stocks": "vn", "funds": "vn", "futures": "", ...}
_LS_SOURCES_KEY = "tradingview.symboledit.selectedSearchSources"

# localStorage key that stores per-asset-class filter dropdown values
# shape: {"funds": {"funds-type-select": "etf"}, "stocks": {...}, ...}
_LS_FILTER_VALUES_KEY = "tradingview.selectedSymbolSearchFilterValues"

# Maps ScrapeMainType.value → tab name used in localStorage
_ASSET_LS_TAB: Dict[str, str] = {
    ScrapeMainType.STOCKS.value: "stocks",
    ScrapeMainType.FUNDS.value: "funds",
    ScrapeMainType.FUTURES.value: "futures",
    ScrapeMainType.FOREX.value: "forex",
    ScrapeMainType.CRYPTO.value: "crypto",
    ScrapeMainType.INDICES.value: "index",
    ScrapeMainType.BONDS.value: "bond",
    ScrapeMainType.ECONOMY.value: "economic",
    ScrapeMainType.OPTIONS.value: "options",
}

# Maps FundType → value stored in {"funds-type-select": <value>}
FUND_TYPE_LS_VALUE: Dict[FundType, str] = {
    FundType.ETF: "etf",
    FundType.MUTUAL_FUND: "mutual_fund",
    FundType.TRUST: "trust",
    FundType.REIT: "reit",
}

# Maps FutureCategory → value stored in {"futures-category-select": <value>}
FUTURE_CATEGORY_LS_VALUE: Dict[FutureCategory, str] = {
    FutureCategory.SINGLE_STOCK: "single_stock",
    FutureCategory.WORLD_INDICES: "world_indices",
    FutureCategory.CURRENCIES: "currencies",
    FutureCategory.INTEREST_RATES: "interest_rates",
    FutureCategory.ENERGY: "energy",
    FutureCategory.AGRICULTURE: "agriculture",
    FutureCategory.METALS: "metals",
    FutureCategory.WEATHER: "weather",
    FutureCategory.BUILDING_MATERIALS: "building_materials",
    FutureCategory.CHEMICALS: "chemicals",
}

# Maps StockType → value stored in {"stock-type-select": <value>}
STOCK_TYPE_LS_VALUE: Dict[StockType, str] = {
    StockType.COMMON_STOCK: "common_stock",
    StockType.PREFERRED_STOCK: "preferred_stock",
    StockType.DEPOSITORY_RECEIPT: "depository_receipt",
    StockType.WARRANT: "warrant",
    StockType.PRE_IPO: "ipo_stock",
}

# Maps StockSector → value stored in {"stock-sector-select": <value>}
STOCK_SECTOR_LS_VALUE: Dict[StockSector, str] = {
    StockSector.COMMERCIAL_SERVICES: "Commercial Services",
    StockSector.COMMUNICATIONS: "Communications",
    StockSector.CONSUMER_DURABLES: "Consumer Durables",
    StockSector.CONSUMER_NON_DURABLES: "Consumer Non-Durables",
    StockSector.CONSUMER_SERVICES: "Consumer Services",
    StockSector.DISTRIBUTION_SERVICES: "Distribution Services",
    StockSector.ELECTRONIC_TECHNOLOGY: "Electronic Technology",
    StockSector.ENERGY_MINERALS: "Energy Minerals",
    StockSector.FINANCE: "Finance",
    StockSector.GOVERNMENT_SECTOR: "Government",
    StockSector.HEALTH_SERVICES: "Health Services",
    StockSector.HEALTH_TECHNOLOGY: "Health Technology",
    StockSector.INDUSTRIAL_SERVICES: "Industrial Services",
    StockSector.MISCELLANEOUS: "Miscellaneous",
    StockSector.NON_ENERGY_MINERALS: "Non-Energy Minerals",
    StockSector.PROCESS_INDUSTRIES: "Process Industries",
    StockSector.PRODUCER_MANUFACTURING: "Producer Manufacturing",
    StockSector.RETAIL_TRADE: "Retail Trade",
    StockSector.TECHNOLOGY_SERVICES: "Technology Services",
    StockSector.TRANSPORTATION: "Transportation",
    StockSector.UTILITIES: "Utilities",
}

# Maps BondsType → value stored in {"bond-type-select": <value>}
BONDS_TYPE_LS_VALUE: Dict[BondsType, str] = {
    BondsType.GOVERNMENT: "bond_gov",
    BondsType.CORPORATE: "bond_corp",
}

# Maps EconomyCategory → value stored in {"economy-category-select": <value>}
ECONOMY_CATEGORY_LS_VALUE: Dict[EconomyCategory, str] = {
    EconomyCategory.GDP: "gdp",
    EconomyCategory.LABOR: "labor",
    EconomyCategory.PRICES: "prices",
    EconomyCategory.HEALTH: "health",
    EconomyCategory.MONEY: "money",
    EconomyCategory.TRADE: "trade",
    EconomyCategory.GOVERNMENT: "government",
    EconomyCategory.BUSINESS: "business",
    EconomyCategory.CONSUMER: "consumer",
    EconomyCategory.HOUSING: "housing",
    EconomyCategory.TAXES: "taxes",
}


# ──────────────────────────────────────────────────────────────────────────────
# localStorage injection helper
# ──────────────────────────────────────────────────────────────────────────────


def _build_ls_inject_js(
    asset_tab: str,
    country_code: str,
    filter_values: Dict[str, Optional[str]],
) -> str:
    """
    Build a JS snippet that sets the three TradingView localStorage keys
    needed to pre-select an asset class, country, and filter values so the
    screener panel opens with the correct state without any clicking.

    Parameters
    ----------
    asset_tab : str
        The asset-class tab name as TradingView stores it, e.g. "funds", "stocks".
    country_code : str
        Two-letter country code, e.g. "vn", "us".  Empty string for asset
        classes that have no country filter (futures, forex, crypto).
    filter_values : dict
        Mapping of filter-select key → value, e.g.
        {"funds-type-select": "etf"} or {"stock-type-select": "common_stock"}.
        Values that are None are stored as JSON null (clears that filter).
    """
    import json as _json

    set_tab = f"localStorage.setItem({_json.dumps(_LS_FILTER_KEY)}, {_json.dumps(asset_tab)});"

    set_sources = f"""
(function() {{
    var src = JSON.parse(localStorage.getItem({_json.dumps(_LS_SOURCES_KEY)}) || '{{}}');
    src[{_json.dumps(asset_tab)}] = {_json.dumps(country_code)};
    localStorage.setItem({_json.dumps(_LS_SOURCES_KEY)}, JSON.stringify(src));
}})();"""

    filter_patch = _json.dumps(filter_values)
    set_filters = f"""
(function() {{
    var fv = JSON.parse(localStorage.getItem({_json.dumps(_LS_FILTER_VALUES_KEY)}) || '{{}}');
    fv[{_json.dumps(asset_tab)}] = {filter_patch};
    localStorage.setItem({_json.dumps(_LS_FILTER_VALUES_KEY)}, JSON.stringify(fv));
}})();"""

    return set_tab + set_sources + set_filters


# ──────────────────────────────────────────────────────────────────────────────
# Internal helper – shared opening sequence (open panel → Symbols tab)
# ──────────────────────────────────────────────────────────────────────────────


def _open_symbols_panel() -> List[ScrapeAction]:
    """Open the search panel and click the Symbols tab."""
    return [
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.SEARCH_BUTTON, None
        ),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.SYMBOLS_BUTTON, None
        ),
    ]


def _inject_and_reload(js: str) -> List[ScrapeAction]:
    """
    Execute the localStorage injection JS, then reload the page so TradingView
    picks up the new filter state on startup.
    """
    return [
        ScrapeAction(ScrapeActionType.EXECUTE_SCRIPT, "", js),
        ScrapeAction(ScrapeActionType.GO_TO_LINK, "", TRADING_VIEW_HOME_PAGE_URL),
    ]


def _select_country(country: Country) -> List[ScrapeAction]:
    """Legacy click-based country selection — kept for reference only.
    Replaced by localStorage injection in all factory functions below.
    """
    country_term = COUNTRY_SEARCH_TERM.get(country, country.value)
    return [
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.COUNTRIES_BUTTON, None
        ),
        ScrapeAction(
            ScrapeActionType.INPUT_TEXT, TradingViewXpath.COUNTRIES_INPUT, country_term
        ),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.COUNTRIES_FIRST_RESULT, None
        ),
    ]


def _checkbox(label: str) -> ScrapeAction:
    """Return a CLICK_BUTTON action for a menuitemcheckbox with the given aria-label."""
    return ScrapeAction(
        ScrapeActionType.CLICK_BUTTON,
        f'//*[@role="menuitemcheckbox" and @aria-label="{label}"]',
        None,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Factory functions – one per TradingView asset class
# All use localStorage injection to set country + filter values, then open
# the screener panel.  No fragile click sequences for filter dropdowns.
# ──────────────────────────────────────────────────────────────────────────────


def build_stock_link_scrape_actions(
    country: Country,
    stock_type: StockType,
    sector: StockSector,
) -> List[ScrapeAction]:
    """Stocks: country × stock_type × sector  (used for common_stock)."""
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.STOCKS.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={
            "stock-type-select": STOCK_TYPE_LS_VALUE[stock_type],
            "stock-sector-select": STOCK_SECTOR_LS_VALUE[sector],
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.STOCKS_BUTTON, None
        ),
    ]


def build_stock_no_sector_link_scrape_actions(
    country: Country,
    stock_type: StockType,
) -> List[ScrapeAction]:
    """Stocks: country × stock_type only, no sector filter."""
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.STOCKS.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={
            "stock-type-select": STOCK_TYPE_LS_VALUE[stock_type],
            "stock-sector-select": None,
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.STOCKS_BUTTON, None
        ),
    ]


def build_fund_link_scrape_actions(
    country: Country,
    fund_type: FundType,
) -> List[ScrapeAction]:
    """Funds: country × fund_type."""
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.FUNDS.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={
            "funds-type-select": FUND_TYPE_LS_VALUE[fund_type],
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.FUNDS_BUTTON, None
        ),
    ]


def build_futures_link_scrape_actions(
    country: Country,
    category: FutureCategory,
) -> List[ScrapeAction]:
    """Futures: country × category.

    TradingView stores the futures country in selectedSearchSources["futures"]
    (confirmed: "futures": "vn") and the category in
    selectedSymbolSearchFilterValues["futures"]["futures-category-select"].
    """
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.FUTURES.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={
            "futures-category-select": FUTURE_CATEGORY_LS_VALUE[category],
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.FUTURES_BUTTON, None
        ),
    ]


def build_forex_link_scrape_actions(
    source: ForexSource,
) -> List[ScrapeAction]:
    """Forex: source / provider only.

    TradingView stores the forex source in selectedSearchSources["forex"]
    as an UPPERCASE string (e.g. "OANDA", "PEPPERSTONE"), confirmed by
    inspecting localStorage on a live session.  It does NOT use
    selectedSymbolSearchFilterValues for forex source.
    """
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.FOREX.value],
        country_code=source.value.upper(),  # TradingView uses uppercase: "OANDA" not "oanda"
        filter_values={},  # forex has no sub-filter
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.FOREX_BUTTON, None
        ),
    ]


def build_crypto_link_scrape_actions(
    source: CryptoSource,
    crypto_type: CryptoType,
    exchange_type: CryptoExchangeType,
) -> List[ScrapeAction]:
    """Crypto: source × crypto_type × exchange_type."""
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.CRYPTO.value],
        country_code="",
        filter_values={
            "crypto-source-select": source.value,
            "crypto-type-select": crypto_type.value,
            "crypto-exchange-type-select": exchange_type.value,
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.CRYPTO_BUTTON, None
        ),
    ]


def build_indices_link_scrape_actions(
    country: Country,
) -> List[ScrapeAction]:
    """Indices: country only.

    localStorage filter='index' already opens the search panel directly on the
    Indices tab, so we do NOT click an INDICES_BUTTON afterwards — the active
    tab button is hidden/replaced and the click would time out.
    """
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.INDICES.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={},
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        # No INDICES_BUTTON click — the Indices tab is already active from localStorage.
    ]


def build_bonds_link_scrape_actions(
    country: Country,
    bonds_type: BondsType,
) -> List[ScrapeAction]:
    """Bonds: country × bonds_type.

    localStorage filter='bond' already opens the search panel directly on the
    Bonds tab, so we do NOT click a BONDS_BUTTON afterwards — same reason as
    indices.
    """
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.BONDS.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={
            "bond-type-select": BONDS_TYPE_LS_VALUE[bonds_type],
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        # No BONDS_BUTTON click — the Bonds tab is already active from localStorage.
    ]


def build_economy_link_scrape_actions(
    country: Country,
    category: EconomyCategory,
) -> List[ScrapeAction]:
    """Economy: country × category."""
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.ECONOMY.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={
            "economy-category-select": ECONOMY_CATEGORY_LS_VALUE[category],
        },
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.ECONOMY_BUTTON, None
        ),
    ]


def build_options_link_scrape_actions(
    country: Country,
) -> List[ScrapeAction]:
    """Options: country only."""
    js = _build_ls_inject_js(
        asset_tab=_ASSET_LS_TAB[ScrapeMainType.OPTIONS.value],
        country_code=COUNTRY_CODE.get(country, ""),
        filter_values={},
    )
    return [
        *_inject_and_reload(js),
        *_open_symbols_panel(),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.OPTIONS_BUTTON, None
        ),
    ]


# MODEL TRAIN ENUMS
class ModelAchitectureType(Enum):
    LSTM = "lstm"
    CNN = "cnn"


class WindowType(Enum):
    EXPANDING = "expanding"
    SLIDING = "sliding"


class OptimizerType(Enum):
    ADAM = "adam"
    SGD = "sgd"


class LossFunctionType(Enum):
    MSE = "mse"


class ScalerType(Enum):
    MINMAX = "minmax"
    STANDARD = "standard"


class MetricType(Enum):
    MAPE = "mape"
