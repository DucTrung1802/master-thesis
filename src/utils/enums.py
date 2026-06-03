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


# ──────────────────────────────────────────────────────────────────────────────
# TradingView XPaths  (structural only – no per-sector / per-type entries)
# ──────────────────────────────────────────────────────────────────────────────
class TradingViewXpath(Enum):
    # Search / symbol panel
    SEARCH_BUTTON = "/html/body/div[3]/div[3]/div[2]/div[2]/div/div/div/button[1]"
    SYMBOLS_BUTTON = '//*[@id="Symbols"]'

    # Asset class
    STOCKS_BUTTON = '//*[@id="stocks"]'

    # Country filter
    STOCKS_COUNTRIES_BUTTON = (
        '//*[@id="overlap-manager-root"]/div[2]/div/div[2]/div/div/div[1]'
        "/div/div[2]/div/div[3]/div[1]/div/div/div/button"
    )
    STOCKS_COUNTRIES_INPUT = (
        '//*[@id="overlap-manager-root"]/div[2]/div/div[2]/div/div/div[1]'
        "/div/div[2]/div/div/div[2]/div/input"
    )
    # Country result items follow the pattern //*[@id="source-item-5-{row}-{col}"]
    # The Vietnam result is always the first hit → row=0, col=0
    STOCKS_COUNTRIES_FIRST_RESULT = '//*[@id="source-item-5-0-0"]'

    # Stock-type dropdown opener
    STOCKS_TYPES_BUTTON = (
        '//*[@data-qa-id="stock-type-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )

    # Stock-sector dropdown opener
    STOCKS_SECTORS_BUTTON = (
        '//*[@data-qa-id="stock-sector-select"]'
        '//*[@data-qa-id="ss-filter-select-button"]'
    )


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
# ScrapeAction  (unchanged shape, but xpath field is now Optional[TradingViewXpath]
# so we can also carry a raw xpath string for dynamic cases)
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


# ──────────────────────────────────────────────────────────────────────────────
# Factory: build the full action list for any country × stock_type × sector
# ──────────────────────────────────────────────────────────────────────────────
def build_stock_link_scrape_actions(
    country: Country,
    stock_type: StockType,
    sector: StockSector,
) -> List[ScrapeAction]:
    """
    Dynamically build the TradingView filter action sequence for the given
    country / stock_type / sector combination.

    No hardcoded per-sector or per-type entries are needed; the XPaths for the
    checkbox items are derived from the aria-label maps at call time.
    """
    type_label = STOCK_TYPE_ARIA_LABEL[stock_type]
    sector_label = STOCK_SECTOR_ARIA_LABEL[sector]
    country_term = COUNTRY_SEARCH_TERM.get(country, country.value)

    type_checkbox_xpath = (
        f'//*[@role="menuitemcheckbox" and @aria-label="{type_label}"]'
    )
    sector_checkbox_xpath = (
        f'//*[@role="menuitemcheckbox" and @aria-label="{sector_label}"]'
    )

    return [
        # 1. Open symbol search panel
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.SEARCH_BUTTON, None
        ),
        # 2. Switch to Symbols tab
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.SYMBOLS_BUTTON, None
        ),
        # 3. Select Stocks asset class
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.STOCKS_BUTTON, None
        ),
        # 4. Open country filter and pick the country
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON,
            TradingViewXpath.STOCKS_COUNTRIES_BUTTON,
            None,
        ),
        ScrapeAction(
            ScrapeActionType.INPUT_TEXT,
            TradingViewXpath.STOCKS_COUNTRIES_INPUT,
            country_term,
        ),
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON,
            TradingViewXpath.STOCKS_COUNTRIES_FIRST_RESULT,
            None,
        ),
        # 5. Open stock-type filter and pick the type
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.STOCKS_TYPES_BUTTON, None
        ),
        ScrapeAction(ScrapeActionType.CLICK_BUTTON, type_checkbox_xpath, None),
        # 6. Open sector filter and pick the sector
        ScrapeAction(
            ScrapeActionType.CLICK_BUTTON, TradingViewXpath.STOCKS_SECTORS_BUTTON, None
        ),
        ScrapeAction(ScrapeActionType.CLICK_BUTTON, sector_checkbox_xpath, None),
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
