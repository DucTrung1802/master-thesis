from dataclasses import dataclass
import datetime
from enum import Enum
from typing import List


class Market_3(Enum):
    """
    Represents 03 base financial markets in Vietnam.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
    """

    HOSE = "HOSE"  # Ho Chi Minh City Stock Exchange
    HNX = "HNX"  # Hanoi Stock Exchange
    UPCOM = "UPCOM"  # Unlisted Public Company Market


class Market_4(Market_3):
    """
    Represents Market_3.
    Add DER.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
    """

    DER = "DER"  # Derivatives Market


class Market_5(Market_4):
    """
    Represents Market_4.
    Add BOND.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
        BOND: Bond Market.
    """

    BOND = "BOND"  # Bond Market


class Exchange_2(Enum):
    """
    Represents 02 base financial markets in Vietnam.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
    """

    HOSE = "HOSE"  # Ho Chi Minh City Stock Exchange
    HNX = "HNX"  # Hanoi Stock Exchange


class Exchange_5(Exchange_2):
    """
    Represents Exchange_2.
    Add HNXBOND, UPCOM, DER

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        HNXBOND: Hanoi Stock Bond Market.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
    """

    HNXBOND = "HNXBOND"
    UPCOM = "UPCOM"
    DER = "DER"


class SecurityType(Enum):
    ST = "Stock"
    CW = "Covered Warrant"
    FU = "Futures"
    EF = "ETF"
    BO = "BOND"
    OF = "OEF"
    MF = "Mutual Fund"


class TradingSession(Enum):
    ATO = "Opening Call Auction"
    LO = "Continuous Trading"
    ATC = "Closing All Auction"
    PT = "Putthrough"
    C = "Market Close"
    BREAK = "Lunch Break"
    HALT = "Market Halt"


# Base Models
@dataclass
class BaseOutputModel:
    message: str
    status: int
    totalRecord: int


# POST AccessToken Models
@dataclass
class AccessTokenInputModel:
    consumerID: str
    consumerSecret: str


@dataclass
class AccessTokenDataModel:
    accessToken: str


@dataclass
class AccessTokenOutputModel(BaseOutputModel):
    data: AccessTokenDataModel


# GET Securities Models
@dataclass
class SecuritiesInputModel:
    market: Market_4
    pageIndex: int = 1
    pageSize: int = 10


@dataclass
class SecuritiesDataModel:
    market: Market_4
    symbol: str
    stockName: str
    stockEnName: str


@dataclass
class SecuritiesOutputModel(BaseOutputModel):
    data: List[SecuritiesDataModel]


# GET SecuritiesDetails Models
@dataclass
class SecuritiesDetailsInputModel:
    market: Market_4
    symbol: str
    pageIndex: int = 1
    pageSize: int = 10


@dataclass
class SecuritiesDetailsDataRepeatedInfoModel:
    isin: str
    symbol: str
    symbolName: str
    symbolEngName: str
    secType: SecurityType
    marketId: Market_5
    exchange: Market_5
    issuer: str
    lotSize: int
    issueDate: datetime
    maturityDate: datetime
    firstTradingDate: datetime
    lastTradingDate: datetime
    contractMultiplier: str
    settlMethod: str
    underlying: str
    putOrCall: str
    exercisePrice: float
    exerciseStyle: str
    excerciseRatio: float
    listedShare: int
    tickPrice1: int
    tickIncrement1: int
    tickPrice2: int
    tickIncrement2: int
    tickPrice3: int
    tickIncrement3: int
    tickPrice4: int
    tickIncrement4: int


@dataclass
class SecuritiesDetailsDataModel:
    RType: str
    reportDate: str  # dd/MM/yyyy
    totalNoSym: int
    repeatedinfoList: List[SecuritiesDetailsDataRepeatedInfoModel]


@dataclass
class SecuritiesDetailsOutputModel(BaseOutputModel):
    data: List[SecuritiesDetailsDataModel]


# GET IndexComponents
@dataclass
class IndexComponentsInputModel:
    indexCode: str
    pageIndex: int = 1
    pageSize: int = 10


@dataclass
class IndexComponentsDataIndexComponentModel:
    isin: str
    stockSymbol: str


@dataclass
class IndexComponentsDataModel:
    indexCode: str
    indexName: str
    exchange: Exchange_2
    totalSymbolNo: int
    indexComponent: List[IndexComponentsDataIndexComponentModel]


@dataclass
class IndexComponentsOutputModel(BaseOutputModel):
    data: List[IndexComponentsDataModel]


# GET IndexList
@dataclass
class IndexListInputModel:
    exchange: Exchange_5
    pageIndex: int = 1
    pageSize: int = 10


@dataclass
class IndexListDataModel:
    indexCode: str
    indexName: str
    exchange: Exchange_5


@dataclass
class IndexListOutputModel(BaseOutputModel):
    data: List[IndexListDataModel]


# GET DailyOhlc
@dataclass
class DailyOhlcInputModel:
    symbol: str
    fromDate: datetime
    toDate: datetime
    pageIndex: int = 1
    pageSize: int = 10
    ascending: bool


@dataclass
class DailyOhlcDataModel:
    symbol: str
    market: Market_5
    tradingDate: datetime
    time: datetime
    open: int
    high: int
    low: int
    close: int
    volume: int
    value: float


@dataclass
class DailyOhlcOutputModel(BaseOutputModel):
    data: List[DailyOhlcDataModel]


# GET IntradayOhlc
@dataclass
class IntradayOhlcInputModel:
    symbol: str
    fromDate: datetime
    toDate: datetime
    pageIndex: int = 1
    pageSize: int = 10
    ascending: bool
    resollution: int = 1


@dataclass
class IntradayOhlcDataModel:
    symbol: str
    market: Market_3
    tradingDate: datetime
    time: datetime
    open: int
    high: int
    low: int
    close: int
    volume: int


@dataclass
class IntradayOhlcOutputModel(BaseOutputModel):
    data: List[IntradayOhlcDataModel]


# GET DailyIndex
@dataclass
class DailyIndexInputModel:
    indexId: str
    fromDate: datetime
    toDate: datetime
    pageIndex: int = 1
    pageSize: int = 10
    ascending: bool


@dataclass
class DailyIndexDataModel:
    indexCode: str
    indexValue: float
    tradingDate: datetime
    time: datetime
    change: float
    ratioChange: float
    totalTrade: int
    totalMatchVol: int
    totalMatchVal: int
    typeIndex: str
    indexName: str
    advances: int
    noChanges: int
    declines: int
    ceilings: int
    floors: int
    totalDealVol: int
    totalDealVal: int
    totalVol: int
    totalVal: int
    tradingSession: TradingSession


@dataclass
class DailyIndexOutputModel(BaseOutputModel):
    data: List[DailyIndexDataModel]


# GET DailyStockPrice
@dataclass
class DailyStockPriceInputModel:
    symbol: str
    fromDate: datetime
    toDate: datetime
    pageIndex: int = 1
    pageSize: int = 10
    market: Market_5


@dataclass
class DailyStockPriceDataModel:
    symbol: str
    tradingDate: datetime
    time: datetime
    priceChange: int
    perPriceChange: float
    ceilingPrice: int
    floorPrice: int
    refPrice: int
    openPrice: int
    highestPrice: int
    lowestPrice: int
    closePrice: int
    averagePrice: int
    closePriceAdjusted: int
    totalMatchVol: int
    totalMatchVal: int
    totalDealVal: int
    totalDealVol: int
    foreignBuyVolTotal: int
    foreignCurrentRoom: int
    foreignSellVolTotal: int
    foreignBuyValTotal: int
    foreignSellValTotal: int
    totalBuyTrade: int
    totalBuyTradeVol: int
    totalSellTrade: int
    totalSellTradeVol: int
    netBuySellVol: int
    netBuySellVal: int
    totalTradedVol: int
    totalTradedValue: int


@dataclass
class DailyStockPriceOutputModel:
    data: List[DailyStockPriceDataModel]
